//! `rocky state schedule spool` and the backing read for
//! `GET /api/v1/schedule/spool` — what the webhook ingress has accepted and a
//! tick has not yet consumed.
//!
//! [`crate::commands::schedule_status`] reports *claims*, which come into
//! existence only once a tick picks a demand up. A demand sitting in the spool
//! has no claim, so it appears nowhere in `GET /api/v1/schedule`. This is the
//! other half of that picture.
//!
//! The config is **not** loaded here. All this read needs is the project root,
//! and a pending demand records the pipeline that was validated when it was
//! accepted — which the config may since have renamed or removed. Reporting
//! the stored value is honest about what will be attempted; re-validating it
//! against today's config would answer a different question.

use std::path::Path;

use chrono::{DateTime, Utc};
use rocky_core::schedule::spool;

use crate::output::{ScheduleSpoolOutput, SpoolPendingEntry, SpoolSkippedEntry};

/// Why a spool read failed.
///
/// One variant on purpose: the only condition that stops this command is a
/// spool that is present but cannot be enumerated. Everything else is reported
/// *in* the output — a single unreadable file is a `skipped` entry, not a
/// failure of the whole listing, because refusing to report nine healthy
/// demands over one bad file helps nobody.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleSpoolError {
    /// The spool directory exists but could not be read.
    ///
    /// Never conflated with "no demands are pending". That conflation is the
    /// bug class behind #1710/#1752/#1731: a spool nobody could read answered
    /// "empty", so every wrapper saw a healthy tick while webhook demand was
    /// silently not firing.
    #[error("{0}")]
    Unreadable(String),
}

/// Read the spool and build the output document.
///
/// The single producer behind both the CLI verb and the route, so the two
/// cannot drift.
pub fn compute_schedule_spool(config_path: &Path) -> Result<ScheduleSpoolOutput, ScheduleSpoolError> {
    let rocky_dir = crate::commands::scheduler::rocky_dir_for_config(config_path);
    let spool_path = spool::spool_dir(&rocky_dir);

    // Fail-closed: `list_pending_files` distinguishes an ABSENT spool (`Ok`,
    // empty — no webhook has ever been accepted here) from a present one it
    // cannot enumerate (`Err`). Only the latter stops us.
    let files = spool::list_pending_files(&rocky_dir)
        .map_err(|e| ScheduleSpoolError::Unreadable(e.to_string()))?;

    let corrupt = spool::count_corrupt(&rocky_dir)
        .map_err(|e| ScheduleSpoolError::Unreadable(e.to_string()))?;

    let mut pending = Vec::new();
    let mut skipped = Vec::new();

    for path in files {
        // A consumed demand is not outstanding work: `.done` tombstones hold
        // the 24h idempotency window for `kind = id`.
        if spool::is_tombstoned(&path) {
            continue;
        }
        let file = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();

        let demand = match spool::read_pending(&path) {
            Ok(d) => d,
            Err(e) => {
                // `read_pending` folds both "cannot read the bytes" and "the
                // JSON is malformed" into `io::Error`; `InvalidData` is the
                // marker it uses for the parse failure.
                let reason = if e.kind() == std::io::ErrorKind::InvalidData {
                    "malformed"
                } else {
                    "unreadable"
                };
                skipped.push(SpoolSkippedEntry {
                    file,
                    reason: reason.to_string(),
                    detail: e.to_string(),
                });
                continue;
            }
        };

        // `received_at` is stored as a string. Parse it so a consumer gets a
        // real instant to sort and age, and report a value that will not parse
        // rather than dropping the entry or inventing a timestamp for it.
        let received_at = match DateTime::parse_from_rfc3339(&demand.received_at) {
            Ok(ts) => ts.with_timezone(&Utc),
            Err(e) => {
                skipped.push(SpoolSkippedEntry {
                    file,
                    reason: "bad_timestamp".to_string(),
                    detail: format!("received_at {:?}: {e}", demand.received_at),
                });
                continue;
            }
        };

        pending.push(SpoolPendingEntry {
            demand_uid: demand.demand_uid,
            pipeline: demand.pipeline,
            kind: kind_label(demand.kind).to_string(),
            token: demand.token,
            received_at,
            body_hash: demand.body_hash,
        });
    }

    // Arrival order for a human triaging the queue. `demand_uid` breaks ties so
    // two demands accepted in the same instant still order totally.
    pending.sort_by(|a, b| {
        a.received_at
            .cmp(&b.received_at)
            .then_with(|| a.demand_uid.cmp(&b.demand_uid))
    });
    skipped.sort_by(|a, b| a.file.cmp(&b.file));

    Ok(ScheduleSpoolOutput::new(
        spool_path.display().to_string(),
        pending,
        skipped,
        corrupt,
    ))
}

/// The wire label for a demand's dedup discipline.
///
/// Matched exhaustively rather than via `serde` so adding a `WebhookKind`
/// variant fails this build instead of silently emitting a label no consumer
/// knows.
fn kind_label(kind: spool::WebhookKind) -> &'static str {
    match kind {
        spool::WebhookKind::Id => "id",
        spool::WebhookKind::Body => "body",
    }
}

/// `rocky state schedule spool`.
pub fn state_schedule_spool(config_path: &Path, json: bool) -> anyhow::Result<()> {
    let out = compute_schedule_spool(config_path)?;
    if json {
        crate::output::print_json(&out)?;
    } else {
        render_spool_text(&out);
    }
    Ok(())
}

fn render_spool_text(out: &ScheduleSpoolOutput) {
    println!("Webhook spool: {}", out.spool_path);

    if out.pending.is_empty() {
        println!("  no demands pending");
    } else {
        println!();
        for entry in &out.pending {
            // The token is the operator's join key to their provider's
            // delivery log — the reason they are reading this at all — so it
            // is on the line, not just in the JSON.
            println!(
                "  {}  {}  {}  token {}  received {}",
                entry.demand_uid,
                entry.pipeline,
                entry.kind,
                entry.token,
                entry.received_at.to_rfc3339()
            );
        }
    }

    if !out.skipped.is_empty() {
        println!();
        println!("  {} entry/entries could not be read:", out.skipped.len());
        for entry in &out.skipped {
            println!("    {} ({}): {}", entry.file, entry.reason, entry.detail);
        }
    }

    // A non-zero corrupt count means demands were accepted and never ran, so
    // it is stated even though those files are not listed above.
    if out.counts.corrupt > 0 {
        println!();
        println!(
            "  {} quarantined corrupt file(s) in the spool",
            out.counts.corrupt
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::schedule::spool::{AcceptOutcome, WebhookKind, accept};
    use std::path::PathBuf;

    /// A project whose config sits at `<tmp>/rocky.toml`, so `rocky_dir` is
    /// `<tmp>/.rocky` — the derivation the ingress and the reconciler use.
    fn project() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        std::fs::write(&config, "").unwrap();
        (dir, config)
    }

    fn rocky_dir(dir: &tempfile::TempDir) -> PathBuf {
        dir.path().join(".rocky")
    }

    fn spool_one(dir: &tempfile::TempDir, pipeline: &str, token: &str, at: &str) -> PathBuf {
        let now = DateTime::parse_from_rfc3339(at).unwrap().with_timezone(&Utc);
        let outcome = accept(
            &rocky_dir(dir),
            pipeline,
            WebhookKind::Id,
            token,
            "deadbeef",
            now,
        )
        .unwrap();
        assert!(matches!(outcome, AcceptOutcome::Created(_)));
        // The dedup filename is derived, so find the file we just wrote.
        spool::list_pending_files(&rocky_dir(dir))
            .unwrap()
            .into_iter()
            .find(|p| {
                spool::read_pending(p)
                    .map(|d| d.token == token)
                    .unwrap_or(false)
            })
            .expect("the accepted demand is on disk")
    }

    #[test]
    fn an_absent_spool_is_an_empty_queue_not_an_error() {
        // No webhook has ever been accepted: nothing is pending, and that is a
        // complete answer rather than a fault.
        let (dir, config) = project();
        let out = compute_schedule_spool(&config).unwrap();
        assert!(out.pending.is_empty());
        assert_eq!(out.counts.pending, 0);
        assert_eq!(out.counts.corrupt, 0);
        assert!(out.spool_path.ends_with("pending-demands"));
        let _ = dir;
    }

    #[cfg(unix)]
    #[test]
    fn a_present_but_unreadable_spool_is_an_error_not_an_empty_queue() {
        // The whole point (#1710/#1752/#1731): a spool nobody can read must
        // never answer "nothing is queued". A dangling symlink is present —
        // `classify_not_found` says so — but cannot be enumerated.
        let (dir, config) = project();
        std::fs::create_dir_all(rocky_dir(&dir)).unwrap();
        let spool_path = rocky_dir(&dir).join("pending-demands");
        std::os::unix::fs::symlink(dir.path().join("nowhere"), &spool_path).unwrap();

        let err = compute_schedule_spool(&config).unwrap_err();
        assert!(
            err.to_string().contains("cannot be read"),
            "expected a fail-closed read error, got: {err}"
        );
    }

    #[test]
    fn a_pending_demand_is_reported_with_its_stored_fields() {
        let (dir, config) = project();
        spool_one(&dir, "orders", "delivery-1", "2026-09-10T10:00:00Z");

        let out = compute_schedule_spool(&config).unwrap();
        assert_eq!(out.counts.pending, 1);
        let entry = &out.pending[0];
        assert_eq!(entry.pipeline, "orders");
        assert_eq!(entry.kind, "id");
        assert_eq!(entry.token, "delivery-1");
        assert_eq!(entry.body_hash, "deadbeef");
        assert_eq!(entry.received_at.to_rfc3339(), "2026-09-10T10:00:00+00:00");
        assert!(!entry.demand_uid.is_empty());
    }

    #[test]
    fn pending_demands_are_ordered_by_arrival() {
        // The spool filename is a blake3 dedup hash, so on-disk order says
        // nothing about time. An operator triaging a queue wants oldest first.
        let (dir, config) = project();
        spool_one(&dir, "orders", "third", "2026-09-10T12:00:00Z");
        spool_one(&dir, "orders", "first", "2026-09-10T10:00:00Z");
        spool_one(&dir, "orders", "second", "2026-09-10T11:00:00Z");

        let out = compute_schedule_spool(&config).unwrap();
        let tokens: Vec<&str> = out.pending.iter().map(|e| e.token.as_str()).collect();
        assert_eq!(tokens, ["first", "second", "third"]);
    }

    #[test]
    fn a_consumed_demand_is_not_pending() {
        // `.done` is the 24h idempotency window for `kind = id`, not work.
        let (dir, config) = project();
        let path = spool_one(&dir, "orders", "delivery-1", "2026-09-10T10:00:00Z");
        spool::dispose(&path, WebhookKind::Id).unwrap();

        let out = compute_schedule_spool(&config).unwrap();
        assert!(
            out.pending.is_empty(),
            "a disposed demand is still queued: {:?}",
            out.pending
        );
    }

    #[test]
    fn a_malformed_entry_is_skipped_not_fatal() {
        // One bad file must not hide the healthy demands beside it.
        let (dir, config) = project();
        spool_one(&dir, "orders", "good", "2026-09-10T10:00:00Z");
        let bad = rocky_dir(&dir).join("pending-demands").join("notjson");
        std::fs::write(&bad, b"{ not json").unwrap();

        let out = compute_schedule_spool(&config).unwrap();
        assert_eq!(out.counts.pending, 1, "the healthy demand is still listed");
        assert_eq!(out.counts.skipped, 1);
        assert_eq!(out.skipped[0].file, "notjson");
        assert_eq!(out.skipped[0].reason, "malformed");
    }

    #[test]
    fn an_unparsable_timestamp_is_skipped_not_dropped() {
        // Never invent an instant for a record that has none, and never let
        // the entry vanish — it is still a demand blocking the queue.
        let (dir, config) = project();
        let path = spool_one(&dir, "orders", "good", "2026-09-10T10:00:00Z");
        let mut demand: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        demand["received_at"] = serde_json::json!("not-a-timestamp");
        std::fs::write(&path, serde_json::to_vec(&demand).unwrap()).unwrap();

        let out = compute_schedule_spool(&config).unwrap();
        assert!(out.pending.is_empty());
        assert_eq!(out.counts.skipped, 1);
        assert_eq!(out.skipped[0].reason, "bad_timestamp");
        assert!(
            out.skipped[0].detail.contains("not-a-timestamp"),
            "the detail names the offending value: {}",
            out.skipped[0].detail
        );
    }

    #[test]
    fn counts_match_the_lists_they_summarise() {
        // `new` derives the counts, so they cannot drift from the arrays.
        let (dir, config) = project();
        spool_one(&dir, "orders", "good", "2026-09-10T10:00:00Z");
        std::fs::write(
            rocky_dir(&dir).join("pending-demands").join("notjson"),
            b"{",
        )
        .unwrap();

        let out = compute_schedule_spool(&config).unwrap();
        assert_eq!(out.counts.pending, out.pending.len());
        assert_eq!(out.counts.skipped, out.skipped.len());
    }
}
