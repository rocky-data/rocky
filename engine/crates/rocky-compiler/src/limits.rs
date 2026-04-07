//! Compile-time resource limits to prevent runaway compilations.
//!
//! Configurable via `[compile]` section in rocky.toml or CLI flags:
//! - `max_models`: Maximum number of models allowed (default: unlimited)
//! - `max_dag_depth`: Maximum DAG depth (default: 100)
//! - `timeout_seconds`: Compilation timeout (default: 300s for CI, unlimited otherwise)

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LimitError {
    #[error("model count {actual} exceeds limit of {limit}")]
    TooManyModels { actual: usize, limit: usize },

    #[error(
        "DAG depth {actual} exceeds limit of {limit} — check for excessively deep dependency chains"
    )]
    DagTooDeep { actual: usize, limit: usize },

    #[error("compilation timed out after {elapsed_secs}s (limit: {limit_secs}s)")]
    Timeout { elapsed_secs: u64, limit_secs: u64 },
}

/// Resource limits for compilation.
#[derive(Debug, Clone)]
pub struct CompileLimits {
    /// Maximum number of models. 0 = unlimited.
    pub max_models: usize,
    /// Maximum DAG depth. 0 = unlimited.
    pub max_dag_depth: usize,
    /// Compilation timeout in seconds. 0 = unlimited.
    pub timeout_seconds: u64,
}

impl Default for CompileLimits {
    fn default() -> Self {
        Self {
            max_models: 0, // unlimited
            max_dag_depth: 100,
            timeout_seconds: 0, // unlimited
        }
    }
}

impl CompileLimits {
    /// Check model count against the limit.
    pub fn check_model_count(&self, count: usize) -> Result<(), LimitError> {
        if self.max_models > 0 && count > self.max_models {
            return Err(LimitError::TooManyModels {
                actual: count,
                limit: self.max_models,
            });
        }
        Ok(())
    }

    /// Check DAG depth against the limit.
    pub fn check_dag_depth(&self, depth: usize) -> Result<(), LimitError> {
        if self.max_dag_depth > 0 && depth > self.max_dag_depth {
            return Err(LimitError::DagTooDeep {
                actual: depth,
                limit: self.max_dag_depth,
            });
        }
        Ok(())
    }

    /// Check elapsed time against the timeout.
    pub fn check_timeout(&self, start: std::time::Instant) -> Result<(), LimitError> {
        if self.timeout_seconds > 0 {
            let elapsed = start.elapsed().as_secs();
            if elapsed > self.timeout_seconds {
                return Err(LimitError::Timeout {
                    elapsed_secs: elapsed,
                    limit_secs: self.timeout_seconds,
                });
            }
        }
        Ok(())
    }

    /// Convenience: limits suitable for CI environments.
    pub fn ci_defaults() -> Self {
        Self {
            max_models: 0,
            max_dag_depth: 100,
            timeout_seconds: 300, // 5 minutes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_count_unlimited() {
        let limits = CompileLimits::default();
        assert!(limits.check_model_count(100_000).is_ok());
    }

    #[test]
    fn test_model_count_exceeded() {
        let limits = CompileLimits {
            max_models: 1000,
            ..Default::default()
        };
        assert!(limits.check_model_count(500).is_ok());
        assert!(limits.check_model_count(1001).is_err());
    }

    #[test]
    fn test_dag_depth_exceeded() {
        let limits = CompileLimits {
            max_dag_depth: 50,
            ..Default::default()
        };
        assert!(limits.check_dag_depth(30).is_ok());
        assert!(limits.check_dag_depth(51).is_err());
    }

    #[test]
    fn test_timeout_not_exceeded() {
        let limits = CompileLimits {
            timeout_seconds: 60,
            ..Default::default()
        };
        let start = std::time::Instant::now();
        assert!(limits.check_timeout(start).is_ok());
    }
}
