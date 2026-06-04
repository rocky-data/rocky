//! Per-namespace state-file resolution + concurrency.
//!
//! Covers the A2 "redb single-writer relief" feature: opt-in, default-off
//! per-pipeline / per-client state-file namespacing. Each namespace gets its
//! own `<models>/.rocky-state/<ns>.redb` file (its own advisory `.redb.lock`
//! and its own redb handle), so independent fan-out runs stop serializing on
//! one global lock.
//!
//! The non-negotiable here is the *default-safe floor*: with no namespace,
//! resolution is byte-identical to today (R1). The relief claim is that N
//! distinct-namespace writers proceed with zero lock contention (R2), while
//! the single-file guard is unweakened (R3) and namespaces never cross-talk
//! (R4).
//!
//! Concurrency discipline mirrors `state_lock.rs`: same-file contention is
//! simulated by holding the `fs4` advisory lock externally (so redb's own
//! per-file `DatabaseAlreadyOpen` doesn't muddy the assertion), and the
//! N-writer relief test opens N *distinct* files, which is exactly the
//! cross-process fan-out shape — distinct files have no shared redb handle.

use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

use chrono::Utc;
use fs4::FileExt;
use rocky_core::state::{
    STATE_FILE_NAME, STATE_NAMESPACE_DIR, StateError, StateStore, resolve_state_path,
    resolve_state_path_ns, validate_namespace,
};
use rocky_ir::WatermarkState;
use tempfile::TempDir;

fn watermark(value: chrono::DateTime<Utc>) -> WatermarkState {
    WatermarkState {
        last_value: value,
        updated_at: value,
    }
}

// ---------------------------------------------------------------------------
// R1 — transparency invariant (the default-safe floor, most important test).
//
// `resolve_state_path_ns(explicit, models, None)` must be byte-identical to
// `resolve_state_path(explicit, models)` across every resolution case, so a
// user who never opts in sees identical behavior.
// ---------------------------------------------------------------------------

#[test]
fn r1_none_namespace_is_byte_identical_to_legacy_resolver() {
    // Case 1: explicit --state-path (hard override).
    let explicit = std::path::PathBuf::from("/tmp/custom-state.redb");
    let models = std::path::Path::new("models");
    assert_eq!(
        resolve_state_path_ns(Some(&explicit), models, None),
        resolve_state_path(Some(&explicit), models),
    );

    // Case 5: fresh project, models dir exists.
    let dir = TempDir::new().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    assert_eq!(
        resolve_state_path_ns(None, &models_dir, None),
        resolve_state_path(None, &models_dir),
    );

    // Case 5: fresh project, no models dir (CWD fallback path).
    let no_models = dir.path().join("nope");
    assert_eq!(
        resolve_state_path_ns(None, &no_models, None),
        resolve_state_path(None, &no_models),
    );

    // Case 2: canonical <models>/.rocky-state.redb already exists.
    std::fs::write(models_dir.join(STATE_FILE_NAME), b"x").unwrap();
    assert_eq!(
        resolve_state_path_ns(None, &models_dir, None),
        resolve_state_path(None, &models_dir),
    );
}

#[test]
fn r1_explicit_path_disables_namespacing_even_when_namespace_set() {
    // An explicit --state-path is a hard override: namespacing is disabled for
    // that invocation, so the result equals the legacy resolver's verbatim
    // honoring of the explicit path.
    let explicit = std::path::PathBuf::from("/tmp/pinned-state.redb");
    let models = std::path::Path::new("models");
    assert_eq!(
        resolve_state_path_ns(Some(&explicit), models, Some("acme")),
        resolve_state_path(Some(&explicit), models),
    );
}

// ---------------------------------------------------------------------------
// R7 — lock-path derivation under namespacing.
//
// `<models>/.rocky-state/acme.redb` must yield advisory lock
// `<models>/.rocky-state/acme.redb.lock` — distinct per namespace, so two
// namespaces never share a lock file.
// ---------------------------------------------------------------------------

#[test]
fn r7_namespaced_path_and_lock_derivation() {
    let dir = TempDir::new().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();

    let resolved = resolve_state_path_ns(None, &models_dir, Some("acme"));
    let expected = models_dir.join(STATE_NAMESPACE_DIR).join("acme.redb");
    assert_eq!(resolved.path(), expected.as_path());
    assert!(resolved.warning.is_none());

    // The parent .rocky-state/ dir is created on resolution so the later
    // advisory-lock open doesn't fail on a missing parent.
    assert!(models_dir.join(STATE_NAMESPACE_DIR).is_dir());

    // `with_extension("redb.lock")` (used by StateStore::open) composes with
    // the nested path: acme.redb -> acme.redb.lock, in the namespace dir.
    let lock = resolved.path().with_extension("redb.lock");
    assert_eq!(
        lock,
        models_dir.join(STATE_NAMESPACE_DIR).join("acme.redb.lock"),
    );

    // A second namespace lands on a sibling file with its own lock.
    let other = resolve_state_path_ns(None, &models_dir, Some("globex"));
    assert_ne!(other.path(), resolved.path());
    assert_ne!(
        other.path().with_extension("redb.lock"),
        resolved.path().with_extension("redb.lock"),
    );
}

#[test]
fn namespace_validation_rejects_path_traversal() {
    assert!(validate_namespace("acme").is_ok());
    assert!(validate_namespace("acme_us_west_1").is_ok());
    assert!(validate_namespace("Tenant123").is_ok());

    // Path traversal / separators / dots / spaces are all rejected — the
    // namespace becomes a file segment.
    assert!(validate_namespace("../etc").is_err());
    assert!(validate_namespace("a/b").is_err());
    assert!(validate_namespace("a.b").is_err());
    assert!(validate_namespace("has space").is_err());
    assert!(validate_namespace("").is_err());

    // An unvalidated invalid namespace must never compose a traversal path:
    // resolve_state_path_ns falls back to the global resolution instead.
    let dir = TempDir::new().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    let resolved = resolve_state_path_ns(None, &models_dir, Some("../escape"));
    // Falls back to the global path; never lands outside the project.
    assert_eq!(
        resolved.path(),
        resolve_state_path(None, &models_dir).path()
    );
    assert!(!resolved.path().to_string_lossy().contains("escape"));
}

// FIX 3 — when the `.rocky-state/` namespace dir can't be created because the
// path already exists as a *file* (e.g. a stray dotfile), resolution falls
// back to the global state path instead of composing a path that would only
// fail later at lock-open with an opaque I/O error.
#[test]
fn ns_dir_collides_with_file_falls_back_to_global() {
    let dir = TempDir::new().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();

    // Plant a regular file where the namespace directory would go.
    let collision = models_dir.join(STATE_NAMESPACE_DIR);
    std::fs::write(&collision, b"not a directory").unwrap();

    let resolved = resolve_state_path_ns(None, &models_dir, Some("acme"));
    // Falls back to the global resolution rather than the namespaced file.
    assert_eq!(
        resolved.path(),
        resolve_state_path(None, &models_dir).path(),
        "a non-directory namespace path must fall back to the global state file"
    );
    assert!(!resolved.path().ends_with("acme.redb"));
}

// ---------------------------------------------------------------------------
// R2 — N concurrent namespaced writers, zero lock contention (core relief).
//
// N writers each open a *distinct* `.rocky-state/ns_{i}.redb` and run a write
// loop. Distinct files => distinct advisory locks => distinct redb handles, so
// there must be zero `LockHeldByOther` / `Busy` and every commit must succeed.
// ---------------------------------------------------------------------------

#[test]
fn r2_n_concurrent_namespaced_writers_no_lock_errors() {
    const N: usize = 6;
    const WRITES_PER_WRITER: usize = 25;

    let dir = TempDir::new().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();

    // Resolve each namespace up front (creates the shared .rocky-state/ dir).
    let paths: Vec<_> = (0..N)
        .map(|i| {
            resolve_state_path_ns(None, &models_dir, Some(&format!("ns_{i}")))
                .path()
                .to_path_buf()
        })
        .collect();

    let barrier = Arc::new(Barrier::new(N));
    let handles: Vec<_> = paths
        .into_iter()
        .enumerate()
        .map(|(i, path)| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                // Open the writer BEFORE the barrier so all N hold their
                // advisory lock simultaneously — this is the contended window
                // a global file would serialize on.
                let store = StateStore::open(&path).expect("namespaced open must succeed");
                barrier.wait();
                for w in 0..WRITES_PER_WRITER {
                    let ts = Utc::now();
                    store
                        .set_watermark(&format!("cat.sch.t_{i}_{w}"), &watermark(ts))
                        .expect("watermark write must succeed under concurrency");
                }
                // Every write committed; read one back to prove durability.
                let got = store
                    .get_watermark(&format!("cat.sch.t_{i}_0"))
                    .expect("read back");
                assert!(got.is_some());
            })
        })
        .collect();

    for h in handles {
        h.join()
            .expect("no writer thread panicked (no lock contention)");
    }
}

// ---------------------------------------------------------------------------
// R3 — shared file still serializes (single-file guard unweakened).
//
// Mirrors state_lock.rs::second_writer_fails_when_lock_held_externally, but on
// a *namespaced* file: holding the fs4 advisory lock externally must still
// make a same-file StateStore::open fail with LockHeldByOther. Proves we did
// not weaken the single-file guard — same namespace still serializes.
// ---------------------------------------------------------------------------

#[test]
fn r3_same_namespaced_file_still_serializes() {
    let dir = TempDir::new().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    let path = resolve_state_path_ns(None, &models_dir, Some("acme"))
        .path()
        .to_path_buf();

    // Simulate another process holding the advisory lock on THIS namespace's
    // lock file (no redb open, exactly like state_lock.rs).
    let lock_path = path.with_extension("redb.lock");
    let external = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    FileExt::try_lock(&external).expect("external lock acquired in clean temp dir");

    match StateStore::open(&path) {
        Err(StateError::LockHeldByOther { .. }) => {}
        Err(other) => panic!("expected LockHeldByOther on the same namespaced file, got {other:?}"),
        Ok(_) => panic!("same-namespace open must fail while the lock is held externally"),
    }

    // A DIFFERENT namespace is unaffected — its lock is a separate file.
    let other_path = resolve_state_path_ns(None, &models_dir, Some("globex"))
        .path()
        .to_path_buf();
    let _other = StateStore::open(&other_path)
        .expect("a different namespace must open while acme's lock is held");
}

// ---------------------------------------------------------------------------
// R4 — no cross-talk between namespaces.
//
// Writer A writes WATERMARKS[t1]=X to ns_a; writer B writes WATERMARKS[t1]=Y
// to ns_b. Each file must read back its OWN value (no bleed), and opening one
// namespace must never observe the other's run history / watermarks.
// ---------------------------------------------------------------------------

#[test]
fn r4_no_cross_talk_between_namespaces() {
    let dir = TempDir::new().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();

    let path_a = resolve_state_path_ns(None, &models_dir, Some("ns_a"))
        .path()
        .to_path_buf();
    let path_b = resolve_state_path_ns(None, &models_dir, Some("ns_b"))
        .path()
        .to_path_buf();
    assert_ne!(path_a, path_b);

    let ts_x = Utc::now();
    let ts_y = ts_x + chrono::Duration::hours(1);

    {
        let a = StateStore::open(&path_a).unwrap();
        let b = StateStore::open(&path_b).unwrap();
        // Same logical key, different namespace -> different file.
        a.set_watermark("cat.sch.t1", &watermark(ts_x)).unwrap();
        b.set_watermark("cat.sch.t1", &watermark(ts_y)).unwrap();
        // A also writes a key B never sees.
        a.set_watermark("cat.sch.only_a", &watermark(ts_x)).unwrap();
    }

    // Reopen each and assert each reads back ONLY its own value.
    let a = StateStore::open(&path_a).unwrap();
    let b = StateStore::open(&path_b).unwrap();

    assert_eq!(
        a.get_watermark("cat.sch.t1").unwrap().unwrap().last_value,
        ts_x,
    );
    assert_eq!(
        b.get_watermark("cat.sch.t1").unwrap().unwrap().last_value,
        ts_y,
    );
    // ns_a's exclusive key must be invisible in ns_b.
    assert!(b.get_watermark("cat.sch.only_a").unwrap().is_none());

    // Full watermark listing is disjoint: ns_b never sees ns_a's only_a key.
    let b_keys: Vec<String> = b
        .list_watermarks()
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(!b_keys.iter().any(|k| k == "cat.sch.only_a"));
}

// ---------------------------------------------------------------------------
// R10 — migration non-destructiveness (the core safety guarantee).
//
// A pre-existing legacy <models>/.rocky-state.redb with watermarks must be
// byte-unchanged when namespacing is on, and the namespaced file must start
// FRESH (no auto-seed).
// ---------------------------------------------------------------------------

#[test]
fn r10_legacy_file_untouched_namespaced_file_starts_fresh() {
    let dir = TempDir::new().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();

    // Seed a legacy global file with a watermark.
    let legacy_path = models_dir.join(STATE_FILE_NAME);
    {
        let legacy = StateStore::open(&legacy_path).unwrap();
        legacy
            .set_watermark("cat.sch.legacy", &watermark(Utc::now()))
            .unwrap();
    }
    let legacy_bytes_before = std::fs::read(&legacy_path).unwrap();

    // Open a namespaced store with namespacing on.
    let ns_path = resolve_state_path_ns(None, &models_dir, Some("acme"))
        .path()
        .to_path_buf();
    {
        let ns = StateStore::open(&ns_path).unwrap();
        // No auto-seed: the legacy watermark must NOT be present.
        assert!(
            ns.get_watermark("cat.sch.legacy").unwrap().is_none(),
            "namespaced file must start fresh, not inherit legacy watermarks"
        );
        // The namespaced file is genuinely a fresh, separate file.
        assert_ne!(ns_path, legacy_path);
    }

    // The legacy file is byte-unchanged on disk (never moved, deleted, or
    // mutated by enabling namespacing).
    let legacy_bytes_after = std::fs::read(&legacy_path).unwrap();
    assert_eq!(
        legacy_bytes_before, legacy_bytes_after,
        "legacy global state file must be byte-identical after namespacing is enabled"
    );
}
