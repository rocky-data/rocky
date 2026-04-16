//! Re-export of the adaptive throttle primitive.
//!
//! The implementation lives in [`rocky_adapter_sdk::throttle`] — this module
//! is kept as a back-compat shim so existing `rocky_databricks::throttle::*`
//! imports continue to work without a ripple across adapters.

pub use rocky_adapter_sdk::throttle::AdaptiveThrottle;
