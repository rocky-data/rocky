//! The runner's custody of a drafting / repair worker's output (#1515).
//!
//! Before this module, the drafting window ran from worker dispatch to the
//! Phase B read, and whatever sat in `models/<model>.{sql,toml}` at the end
//! of it was merged, hashed and committed. The worker's own `draft_model`
//! writes were the intended content; anything else that wrote the tree in
//! that window — a human editing the sidecar mid-draft, a second tool, a
//! stale editor buffer — became the committed content just as silently,
//! because Phase B had no expected bytes to compare against and preserves
//! worker-added tests by design.
//!
//! Elicitation never had this problem: its worker hands a candidate off
//! through the task outbox, the runner verifies the digest, and the runner
//! performs the confined write. This module is that pattern applied to the
//! other two arms:
//!
//! ```text
//! worker-profile draft_model ─▶ models/<m>.{sql,toml}      (the compile loop, unchanged)
//!                           └▶ outbox/model.{sql,toml}     (the hand-off, mirrored)
//! driver collects the outbox  ─▶ DriverOutcome::Drafting { bytes, digests }
//! runner: bytes match digests?                             else refuse
//!         tree on disk == hand-off?                        else refuse
//!         confined re-write of models/<m>.{sql,toml}       then Phase A byte-verify
//! ```
//!
//! What this buys, stated exactly. Phase B now merges bytes the RUNNER
//! wrote, from a hand-off the worker produced, after checking nothing else
//! wrote the tree in the meantime. A mid-window edit is refused, loudly,
//! with the file named — not merged, and not silently overwritten either.
//!
//! What it does NOT buy: protection from a hostile same-user process. Such
//! a process can write the outbox as easily as the model tree. That is the
//! adversary v0 concedes and `lib.rs` discloses; it is #1491's problem, not
//! this module's.

use std::path::{Path, PathBuf};

use rocky_core::product::commit::{
    contained_write_target, read_no_follow_bytes, write_new_no_follow,
};
use rocky_core::product::manifest::content_digest;

/// A drafting / repair worker's hand-off, as the driver collected it.
#[derive(Debug, Clone, Copy)]
pub struct Handoff<'a> {
    /// Hand-off bytes for `models/<model>.sql`.
    pub model_sql: &'a [u8],
    /// Hand-off bytes for `models/<model>.toml`.
    pub model_sidecar: &'a [u8],
    /// `sha256:<hex>` the driver claims for `model_sql`.
    pub expected_sql_digest: &'a str,
    /// `sha256:<hex>` the driver claims for `model_sidecar`.
    pub expected_sidecar_digest: &'a str,
}

/// Take custody of a hand-off: verify it, check the tree still matches it,
/// then write it into `models/` through the confined staged path.
///
/// Every check runs before any write, so a refusal leaves the tree exactly
/// as the worker left it — the operator can diff it against the outbox.
///
/// # Errors
///
/// A human-readable reason, naming the file, when:
/// - a claimed digest does not match its bytes (the driver's integrity
///   drill: a hand-off whose digest lies is refused, as for elicitation);
/// - either model file on disk is missing or differs from the hand-off —
///   something other than the worker's last `draft_model` wrote the tree
///   inside the drafting window;
/// - a target path fails containment, or a write fails.
pub fn commit_model_handoff(root: &Path, model: &str, handoff: Handoff<'_>) -> Result<(), String> {
    // Integrity first: the bytes must be the bytes the driver digested.
    for (what, bytes, claimed) in [
        ("model.sql", handoff.model_sql, handoff.expected_sql_digest),
        (
            "model.toml",
            handoff.model_sidecar,
            handoff.expected_sidecar_digest,
        ),
    ] {
        let actual = content_digest(bytes);
        if actual != claimed {
            return Err(format!(
                "hand-off digest mismatch for {what}: bytes digest to {actual}, the driver \
                 claimed {claimed} — refusing the hand-off"
            ));
        }
    }

    // Resolve both targets before touching either, so a containment
    // refusal on the sidecar cannot leave a rewritten SQL behind.
    let targets: Vec<(String, PathBuf, &[u8])> = [
        (format!("models/{model}.sql"), handoff.model_sql),
        (format!("models/{model}.toml"), handoff.model_sidecar),
    ]
    .into_iter()
    .map(|(rel, bytes)| {
        contained_write_target(root, &rel)
            .map(|target| (rel, target, bytes))
            .map_err(|reason| format!("hand-off write refused: {reason}"))
    })
    .collect::<Result<_, _>>()?;

    // The tree must still be what the worker handed off. Both files are
    // compared before either is written — a mismatch on the second must
    // not leave the first rewritten.
    for (rel, target, bytes) in &targets {
        let on_disk = read_no_follow_bytes(target).map_err(|err| {
            format!(
                "{rel} is not readable as a regular file ({err}) — the worker handed it off, \
                 so something changed the model tree inside the drafting window; refusing to \
                 commit bytes nobody handed off"
            )
        })?;
        if on_disk != *bytes {
            return Err(format!(
                "{rel} on disk does not match the worker's hand-off — something wrote the \
                 model tree inside the drafting window; refusing to commit bytes nobody \
                 handed off. The hand-off is in the task outbox; diff it against the file"
            ));
        }
    }

    // The runner's confined write, same shape as the candidate spec:
    // O_EXCL tmp beside the target (never through a link), then rename.
    for (rel, target, bytes) in &targets {
        let tmp = target.with_extension(match rel.rsplit('.').next() {
            Some("sql") => "sql.ff-handoff-tmp",
            _ => "toml.ff-handoff-tmp",
        });
        write_new_no_follow(&tmp, bytes)
            .map_err(|err| format!("staging {}: {err}", tmp.display()))?;
        std::fs::rename(&tmp, target)
            .map_err(|err| format!("renaming into {}: {err}", target.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(sql: &[u8], toml: &[u8]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).expect("models dir");
        std::fs::write(models.join("m1.sql"), sql).expect("sql");
        std::fs::write(models.join("m1.toml"), toml).expect("toml");
        dir
    }

    fn handoff<'a>(sql: &'a [u8], toml: &'a [u8], sql_d: &'a str, toml_d: &'a str) -> Handoff<'a> {
        Handoff {
            model_sql: sql,
            model_sidecar: toml,
            expected_sql_digest: sql_d,
            expected_sidecar_digest: toml_d,
        }
    }

    /// The ordinary round: the tree is what the worker handed off, so the
    /// runner takes custody and the files are byte-identical afterwards —
    /// as fresh regular files, not whatever inode was there.
    #[test]
    fn matching_tree_is_committed_from_the_handoff() {
        let sql = b"SELECT 1 AS id\n";
        let toml = b"name = \"m1\"\n";
        let dir = project(sql, toml);
        let (sd, td) = (content_digest(sql), content_digest(toml));
        commit_model_handoff(dir.path(), "m1", handoff(sql, toml, &sd, &td)).expect("commits");
        let models = dir.path().join("models");
        assert_eq!(std::fs::read(models.join("m1.sql")).unwrap(), sql);
        assert_eq!(std::fs::read(models.join("m1.toml")).unwrap(), toml);
        assert!(
            !models.join("m1.sql.ff-handoff-tmp").exists(),
            "no scratch left"
        );
    }

    /// The hazard this module exists for: something edited the sidecar
    /// after the worker's last `draft_model`. Refused, the file is named,
    /// and NEITHER file is rewritten — the SQL matched, but a mismatch on
    /// the second file must not leave the first committed.
    #[test]
    fn a_tree_edited_inside_the_window_is_refused_and_untouched() {
        let sql = b"SELECT 1 AS id\n";
        let toml = b"name = \"m1\"\n";
        let dir = project(
            sql,
            b"name = \"m1\"\n[[tests]]\ntype = \"expression\"\nexpression = \"1=1\"\n",
        );
        let (sd, td) = (content_digest(sql), content_digest(toml));
        let before = std::fs::read(dir.path().join("models/m1.toml")).unwrap();

        let err = commit_model_handoff(dir.path(), "m1", handoff(sql, toml, &sd, &td))
            .expect_err("an edited tree must be refused");
        assert!(err.contains("models/m1.toml"), "names the file: {err}");
        assert!(err.contains("inside the drafting window"), "{err}");
        assert_eq!(
            std::fs::read(dir.path().join("models/m1.toml")).unwrap(),
            before,
            "the edited file is left for the operator to inspect, not overwritten"
        );
    }

    /// A missing file is the same refusal — the worker handed it off, so
    /// its absence means the tree changed.
    #[test]
    fn a_missing_model_file_is_refused() {
        let sql = b"SELECT 1 AS id\n";
        let toml = b"name = \"m1\"\n";
        let dir = project(sql, toml);
        std::fs::remove_file(dir.path().join("models/m1.sql")).unwrap();
        let (sd, td) = (content_digest(sql), content_digest(toml));
        let err = commit_model_handoff(dir.path(), "m1", handoff(sql, toml, &sd, &td))
            .expect_err("a missing file must be refused");
        assert!(err.contains("models/m1.sql"), "{err}");
    }

    /// The driver's integrity drill, as for elicitation: a hand-off whose
    /// digest does not match its bytes is refused before the tree is read.
    #[test]
    fn a_lying_digest_is_refused_before_any_write() {
        let sql = b"SELECT 1 AS id\n";
        let toml = b"name = \"m1\"\n";
        let dir = project(sql, toml);
        let td = content_digest(toml);
        let err = commit_model_handoff(dir.path(), "m1", handoff(sql, toml, "sha256:0000", &td))
            .expect_err("a lying digest must be refused");
        assert!(err.contains("digest mismatch for model.sql"), "{err}");
    }

    /// Containment: a model name that escapes `models/` is refused by the
    /// shared containment primitive, before any read.
    #[test]
    fn an_escaping_model_name_is_refused() {
        let sql = b"x\n";
        let toml = b"y\n";
        let dir = project(sql, toml);
        let (sd, td) = (content_digest(sql), content_digest(toml));
        let err = commit_model_handoff(dir.path(), "../escape", handoff(sql, toml, &sd, &td))
            .expect_err("traversal must be refused");
        assert!(err.contains("refused"), "{err}");
    }
}
