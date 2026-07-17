//! Fault-injecting [`ObjectStore`] decorator for remote-state tests.
//!
//! Compiled only for the crate's own tests or under the `test-support`
//! feature — the `rocky` binary never enables either, so nothing here can
//! reach a production build.
//!
//! [`FaultingStore::wrap`] decorates any store (typically
//! `object_store::memory::InMemory`) and returns a cloneable [`FaultHandle`]
//! that arms deterministic failures per operation and reads unconditional
//! per-operation call counters. Because the shared state lives behind an
//! `Arc<Mutex<..>>`, arming and counting work across OS threads — a handle
//! held by a test observes calls made from a spawned runtime thread.
//!
//! Injected failures surface as [`object_store::Error::Generic`] with
//! `store: "FaultingStore"`, which the state-sync error taxonomy classifies
//! like any other backend failure.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Logical object-store operation a fault can target.
///
/// The pinned `object_store` 0.14 trait routes several public operations
/// through one entry point, so the mapping is by *intent*, not method name:
/// `Head` is a `get_opts` call with `GetOptions::head` set (it backs
/// `ObjectStoreProvider::exists`), `Put` covers both `put_opts` and
/// `put_multipart_opts`, `List` covers `list` and `list_with_delimiter`,
/// `Delete` fires per path inside `delete_stream`, and `Copy` covers
/// `copy_opts` (so the `copy` / `copy_if_not_exists` extension methods too).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultOp {
    Get,
    Put,
    Head,
    List,
    Delete,
    Copy,
}

/// How an armed fault fires across successive calls of its [`FaultOp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    /// Fail the next `n` calls, then recover (the arm disarms itself).
    FailNext(u32),
    /// Fail exactly the `n`-th call (1-based, counted from the store's
    /// creation — not from arming), letting every other call through.
    FailNth(u64),
    /// Fail every call until [`FaultHandle::clear`] or a re-arm.
    FailAll,
}

/// Shared fault/counter state between a [`FaultingStore`] and its
/// [`FaultHandle`]s.
#[derive(Debug, Default)]
struct FaultState {
    /// Unconditional per-op call counters (every call counts, faulted or not).
    counts: HashMap<FaultOp, u64>,
    /// Currently-armed fault per op.
    armed: HashMap<FaultOp, FaultMode>,
}

fn lock(state: &Mutex<FaultState>) -> std::sync::MutexGuard<'_, FaultState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Count the call and decide whether the armed fault (if any) fires for it.
/// An exhausted [`FaultMode::FailNext`] disarms itself so later calls recover.
fn record_and_check(state: &Mutex<FaultState>, op: FaultOp) -> Result<(), object_store::Error> {
    let mut guard = lock(state);
    let count = guard.counts.entry(op).or_insert(0);
    *count += 1;
    let call_number = *count;
    let fire = match guard.armed.get_mut(&op) {
        None => false,
        Some(FaultMode::FailAll) => true,
        Some(FaultMode::FailNth(n)) => call_number == *n,
        Some(FaultMode::FailNext(remaining)) => {
            if *remaining == 0 {
                false
            } else {
                *remaining -= 1;
                true
            }
        }
    };
    if guard.armed.get(&op) == Some(&FaultMode::FailNext(0)) {
        guard.armed.remove(&op);
    }
    drop(guard);
    if fire {
        Err(object_store::Error::Generic {
            store: "FaultingStore",
            source: format!("injected fault: {op:?} call #{call_number}").into(),
        })
    } else {
        Ok(())
    }
}

/// Cloneable control handle for a [`FaultingStore`].
///
/// Arms faults and reads counters through the same `Arc`-backed state the
/// store consults, so it works from any thread and stays live for as long as
/// any clone of the wrapped store does.
#[derive(Debug, Clone)]
pub struct FaultHandle(Arc<Mutex<FaultState>>);

impl FaultHandle {
    /// Arm `mode` for `op`, replacing any fault previously armed for it.
    pub fn arm(&self, op: FaultOp, mode: FaultMode) {
        lock(&self.0).armed.insert(op, mode);
    }

    /// Disarm every fault. Counters are intentionally NOT reset — counting is
    /// unconditional for the store's lifetime.
    pub fn clear(&self) {
        lock(&self.0).armed.clear();
    }

    /// Total calls observed for `op` (faulted calls included).
    pub fn count(&self, op: FaultOp) -> u64 {
        lock(&self.0).counts.get(&op).copied().unwrap_or(0)
    }
}

/// Fault-injecting decorator over an inner [`ObjectStore`].
///
/// Construct via [`FaultingStore::wrap`]; drive it through the returned
/// [`FaultHandle`]. Un-faulted calls delegate verbatim to the inner store.
#[derive(Debug)]
pub struct FaultingStore {
    inner: Arc<dyn ObjectStore>,
    state: Arc<Mutex<FaultState>>,
}

impl FaultingStore {
    /// Wrap `inner`, returning the decorated store and its control handle.
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> (Arc<dyn ObjectStore>, FaultHandle) {
        let state = Arc::new(Mutex::new(FaultState::default()));
        let handle = FaultHandle(Arc::clone(&state));
        let store: Arc<dyn ObjectStore> = Arc::new(Self { inner, state });
        (store, handle)
    }

    fn check(&self, op: FaultOp) -> Result<(), object_store::Error> {
        record_and_check(&self.state, op)
    }
}

impl fmt::Display for FaultingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check(FaultOp::Put)?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check(FaultOp::Put)?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // `ObjectStoreExt::head` (which backs `ObjectStoreProvider::exists`)
        // is a `get_opts` with the `head` flag — count and fault it as `Head`.
        let op = if options.head {
            FaultOp::Head
        } else {
            FaultOp::Get
        };
        self.check(op)?;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        // Per-path injection: each location is counted/faulted individually,
        // matching how `ObjectStoreExt::delete` drives this with a one-item
        // stream. Delegation is per path (not a forwarded batch), which is
        // indistinguishable for the in-memory backends this decorator wraps.
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let state = Arc::clone(&state);
                async move {
                    let location = location?;
                    record_and_check(&state, FaultOp::Delete)?;
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Err(e) = self.check(FaultOp::List) {
            return futures::stream::once(async move { Err(e) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.check(FaultOp::List)?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.check(FaultOp::Copy)?;
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;

    fn wrapped() -> (Arc<dyn ObjectStore>, FaultHandle) {
        FaultingStore::wrap(Arc::new(InMemory::new()))
    }

    fn payload() -> PutPayload {
        PutPayload::from(Bytes::from_static(b"bytes"))
    }

    #[tokio::test]
    async fn fail_next_put_then_recover() {
        let (store, faults) = wrapped();
        let key = ObjectPath::from("k");
        faults.arm(FaultOp::Put, FaultMode::FailNext(1));

        let err = store.put(&key, payload()).await.unwrap_err();
        assert!(
            err.to_string().contains("injected fault"),
            "the armed put must fail with the injected error, got: {err}"
        );

        // The one-shot has disarmed itself: the retry lands and the bytes
        // are readable through the inner store.
        store
            .put(&key, payload())
            .await
            .expect("second put recovers");
        let got = store.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.as_ref(), b"bytes");
    }

    #[tokio::test]
    async fn fail_all_get() {
        let (store, faults) = wrapped();
        let key = ObjectPath::from("k");
        store.put(&key, payload()).await.unwrap();

        faults.arm(FaultOp::Get, FaultMode::FailAll);
        assert!(store.get(&key).await.is_err(), "first get faults");
        assert!(store.get(&key).await.is_err(), "FailAll keeps faulting");

        // A FailAll on Get must not bleed into Head (the `exists` probe).
        store.head(&key).await.expect("head is a distinct op");

        faults.clear();
        store.get(&key).await.expect("get recovers after clear");
    }

    #[tokio::test]
    async fn counts_every_op() {
        let (store, faults) = wrapped();
        let from = ObjectPath::from("a");
        let to = ObjectPath::from("b");

        store.put(&from, payload()).await.unwrap();
        store.get(&from).await.unwrap();
        store.head(&from).await.unwrap();
        let listed: Vec<_> = store.list(None).collect().await;
        assert_eq!(listed.len(), 1);
        store.copy(&from, &to).await.unwrap();
        store.delete(&to).await.unwrap();

        // Counting is unconditional — no fault was armed at any point.
        assert_eq!(faults.count(FaultOp::Put), 1);
        assert_eq!(faults.count(FaultOp::Get), 1);
        assert_eq!(faults.count(FaultOp::Head), 1);
        assert_eq!(faults.count(FaultOp::List), 1);
        assert_eq!(faults.count(FaultOp::Copy), 1);
        assert_eq!(faults.count(FaultOp::Delete), 1);
    }

    /// `FailNth` targets one absolute call number and lets every other call
    /// through — the shape a "the 3rd upload of the run dies" test needs.
    #[tokio::test]
    async fn fail_nth_targets_exact_call() {
        let (store, faults) = wrapped();
        let key = ObjectPath::from("k");
        faults.arm(FaultOp::Put, FaultMode::FailNth(2));

        store.put(&key, payload()).await.expect("call #1 passes");
        assert!(store.put(&key, payload()).await.is_err(), "call #2 faults");
        store.put(&key, payload()).await.expect("call #3 passes");
        assert_eq!(faults.count(FaultOp::Put), 3);
    }
}
