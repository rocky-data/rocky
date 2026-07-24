//! Process-lifetime OTLP metrics for the resident scheduler.
//!
//! `rocky serve --scheduler` is Rocky's first long-lived process, so its
//! reconciler needs metrics that outlive a single tick. The per-run collector
//! in [`crate::metrics`] snapshots counters at the end of one CLI invocation;
//! that shape does not fit a loop that ticks indefinitely and needs labeled,
//! monotonic counters. This module provides a dedicated meter pipeline instead:
//!
//! - [`SchedulerMeterGuard`] stands up an OTLP meter provider that stays
//!   resident for the process lifetime and flushes a final scrape on drop.
//! - [`SchedulerMetrics`] is the cheap, cloneable handle the loop records
//!   through — a counter per tick, per due/executed demand, per skip (by
//!   reason), a lag histogram, and a per-pipeline consecutive-failure gauge.
//!
//! Both are no-ops unless the `otel` feature is compiled **and**
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set — mirroring how the tracer initialises
//! (`init_tracing`). A build without a collector configured installs nothing
//! and pays nothing: no meter provider, no background reader.
//!
//! # Instruments
//!
//! | Instrument | Kind | Attributes |
//! |---|---|---|
//! | `rocky.scheduler.ticks` | counter | — |
//! | `rocky.scheduler.due` | counter | — |
//! | `rocky.scheduler.executed` | counter | — |
//! | `rocky.scheduler.skipped` | counter | `reason` |
//! | `rocky.scheduler.lag_seconds` | histogram | — |
//! | `rocky.scheduler.consecutive_failures` | gauge | `pipeline` |
//!
//! The `reason` attribute is drawn from a bounded, config-independent set
//! ([`crate::span_attrs::SCHEDULER_SKIP_REASONS`]); the `pipeline` attribute is
//! a configured pipeline name, so both label sets are bounded by construction —
//! no free-form string (an error message, a user id) ever reaches a label.

#[cfg(feature = "otel")]
pub use enabled::{SchedulerMeterGuard, SchedulerMetrics};

#[cfg(all(feature = "otel", feature = "test-support"))]
pub use enabled::testing;

#[cfg(not(feature = "otel"))]
pub use disabled::{SchedulerMeterGuard, SchedulerMetrics};

#[cfg(feature = "otel")]
mod enabled {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use opentelemetry::KeyValue;
    use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider};
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    /// Meter scope for every scheduler instrument.
    const METER_NAME: &str = "rocky.scheduler";

    const INSTR_TICKS: &str = "rocky.scheduler.ticks";
    const INSTR_DUE: &str = "rocky.scheduler.due";
    const INSTR_EXECUTED: &str = "rocky.scheduler.executed";
    const INSTR_SKIPPED: &str = "rocky.scheduler.skipped";
    const INSTR_LAG: &str = "rocky.scheduler.lag_seconds";
    const INSTR_CONSECUTIVE_FAILURES: &str = "rocky.scheduler.consecutive_failures";

    const ATTR_REASON: &str = "reason";
    const ATTR_PIPELINE: &str = "pipeline";
    const ATTR_OUTCOME: &str = "outcome";

    const DEFAULT_ENDPOINT: &str = "http://localhost:4317";
    const DEFAULT_SERVICE_NAME: &str = "rocky";
    /// How often the periodic reader pushes to the collector between the
    /// final on-drop flush.
    const EXPORT_INTERVAL: Duration = Duration::from_secs(30);

    /// The scheduler's OTel instruments plus the in-process failure-streak map
    /// backing the `consecutive_failures` gauge.
    struct Instruments {
        ticks: Counter<u64>,
        due: Counter<u64>,
        executed: Counter<u64>,
        skipped: Counter<u64>,
        lag_seconds: Histogram<f64>,
        consecutive_failures: Gauge<u64>,
        /// Per-pipeline running count of consecutive failed runs. In-process
        /// only: resets to zero on restart, and a pipeline removed from config
        /// leaves its last gauge value as a stale series. Bounded by the
        /// configured pipeline count, so it can never grow unbounded.
        streaks: Mutex<HashMap<String, u64>>,
    }

    impl Instruments {
        fn new(meter: &Meter) -> Self {
            Self {
                ticks: meter
                    .u64_counter(INSTR_TICKS)
                    .with_description(
                        "Reconciliation passes the resident scheduler has run, by outcome.",
                    )
                    .build(),
                due: meter
                    .u64_counter(INSTR_DUE)
                    .with_description("Demands that came due, summed across ticks.")
                    .build(),
                executed: meter
                    .u64_counter(INSTR_EXECUTED)
                    .with_description(
                        "Demands that ran to a terminal outcome, summed across ticks.",
                    )
                    .build(),
                skipped: meter
                    .u64_counter(INSTR_SKIPPED)
                    .with_description(
                        "Demands suppressed this tick, by reason, summed across ticks.",
                    )
                    .build(),
                lag_seconds: meter
                    .f64_histogram(INSTR_LAG)
                    .with_unit("s")
                    .with_description(
                        "Seconds between a demand's logical timestamp and its execution.",
                    )
                    .build(),
                consecutive_failures: meter
                    .u64_gauge(INSTR_CONSECUTIVE_FAILURES)
                    .with_description("Current run of consecutive failed runs, per pipeline.")
                    .build(),
                streaks: Mutex::new(HashMap::new()),
            }
        }
    }

    /// A cheap, cloneable handle the scheduler loop records metrics through.
    ///
    /// Cloning shares the underlying instruments (they are internally
    /// reference-counted). A handle from [`SchedulerMetrics::disabled`] — or
    /// from a guard with no configured endpoint — drops every call, so the loop
    /// records unconditionally and pays nothing when metrics are off.
    #[derive(Clone, Default)]
    pub struct SchedulerMetrics {
        inner: Option<Arc<Instruments>>,
    }

    impl SchedulerMetrics {
        pub(crate) fn from_meter(meter: &Meter) -> Self {
            Self {
                inner: Some(Arc::new(Instruments::new(meter))),
            }
        }

        /// A handle that records nothing.
        #[must_use]
        pub fn disabled() -> Self {
            Self { inner: None }
        }

        /// Whether this handle is backed by a live meter.
        #[must_use]
        pub fn is_enabled(&self) -> bool {
            self.inner.is_some()
        }

        /// Count one reconciliation pass, attributed to its terminal
        /// `outcome`. Summed across outcomes this is the loop's liveness
        /// signal; split by outcome it makes the execution-blocking passes
        /// (`config_error`, `permit_held`, `fault`, `lock_skipped`) visible and
        /// alertable rather than metrically identical to a healthy idle tick.
        /// The caller must pass a value from
        /// [`crate::span_attrs::SCHEDULER_OUTCOMES`] so the label stays bounded.
        pub fn record_tick_outcome(&self, outcome: &'static str) {
            if let Some(i) = &self.inner {
                i.ticks.add(1, &[KeyValue::new(ATTR_OUTCOME, outcome)]);
            }
        }

        /// Add this tick's due count.
        pub fn record_due(&self, count: u64) {
            if let Some(i) = &self.inner {
                i.due.add(count, &[]);
            }
        }

        /// Add this tick's executed count.
        pub fn record_executed(&self, count: u64) {
            if let Some(i) = &self.inner {
                i.executed.add(count, &[]);
            }
        }

        /// Count one skipped demand, attributed to `reason`. The caller must
        /// pass a value from [`crate::span_attrs::SCHEDULER_SKIP_REASONS`] so
        /// the label set stays bounded.
        pub fn record_skip(&self, reason: &'static str) {
            if let Some(i) = &self.inner {
                i.skipped.add(1, &[KeyValue::new(ATTR_REASON, reason)]);
            }
        }

        /// Record the execution lag (now − the demand's logical timestamp) for
        /// one executed demand, in seconds.
        pub fn record_lag_seconds(&self, seconds: f64) {
            if let Some(i) = &self.inner {
                i.lag_seconds.record(seconds, &[]);
            }
        }

        /// Update the per-pipeline consecutive-failure streak from one executed
        /// demand's outcome and emit the resulting gauge value. A success
        /// resets the streak to zero; any other terminal outcome increments it.
        pub fn record_execution_outcome(&self, pipeline: &str, succeeded: bool) {
            let Some(i) = &self.inner else {
                return;
            };
            let streak = {
                let mut streaks = i
                    .streaks
                    .lock()
                    .expect("scheduler failure-streak map poisoned");
                let entry = streaks.entry(pipeline.to_string()).or_insert(0);
                *entry = if succeeded { 0 } else { *entry + 1 };
                *entry
            };
            i.consecutive_failures.record(
                streak,
                &[KeyValue::new(ATTR_PIPELINE, pipeline.to_string())],
            );
        }
    }

    /// Owns the process-lifetime meter provider and flushes a final scrape when
    /// the resident scheduler stops.
    ///
    /// Hold this for the lifetime of the `serve --scheduler` process — dropping
    /// it flushes and shuts the meter provider down, after which recorded
    /// metrics go nowhere. Drop it **after** the scheduler task has finished so
    /// the last tick's metrics reach the collector.
    pub struct SchedulerMeterGuard {
        provider: Option<SdkMeterProvider>,
    }

    impl SchedulerMeterGuard {
        /// Stand up the OTLP meter provider when `OTEL_EXPORTER_OTLP_ENDPOINT`
        /// is set; otherwise return a guard that installs nothing.
        ///
        /// Must be called from within a Tokio runtime — the tonic gRPC
        /// transport the OTLP exporter uses requires one.
        #[must_use]
        pub fn init_if_enabled() -> Self {
            if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none() {
                return Self { provider: None };
            }
            match build_provider() {
                Ok(provider) => {
                    tracing::info!("scheduler OTLP metrics exporter initialised");
                    Self {
                        provider: Some(provider),
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "scheduler OTLP metrics init failed; continuing without metrics export",
                    );
                    Self { provider: None }
                }
            }
        }

        /// A guard that installs nothing — used when the scheduler is not
        /// running, so `serve` without `--scheduler` never stands up a meter.
        #[must_use]
        pub fn disabled() -> Self {
            Self { provider: None }
        }

        /// A recording handle bound to this guard's meter, or a no-op handle
        /// when no provider was installed.
        #[must_use]
        pub fn metrics(&self) -> SchedulerMetrics {
            match &self.provider {
                Some(provider) => SchedulerMetrics::from_meter(&provider.meter(METER_NAME)),
                None => SchedulerMetrics::disabled(),
            }
        }
    }

    impl Drop for SchedulerMeterGuard {
        fn drop(&mut self) {
            let Some(provider) = self.provider.take() else {
                return;
            };
            // `shutdown` flushes the final scrape and then closes the provider,
            // so a single call exports the last tick's metrics — no separate
            // `force_flush` (which would be a second export attempt, doubling the
            // worst-case hang against a black-hole endpoint). The blocking here is
            // bounded by the OTLP exporter's request timeout
            // (`OTEL_EXPORTER_OTLP_TIMEOUT`, default 10s), so an unreachable
            // collector cannot delay process exit indefinitely.
            if let Err(e) = provider.shutdown() {
                tracing::warn!(error = %e, "scheduler meter provider shutdown failed");
            }
        }
    }

    fn build_provider() -> Result<SdkMeterProvider, Box<dyn std::error::Error + Send + Sync>> {
        use opentelemetry_otlp::WithExportConfig;
        use opentelemetry_sdk::Resource;
        use opentelemetry_sdk::metrics::PeriodicReader;

        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
        let service_name =
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| DEFAULT_SERVICE_NAME.to_string());

        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()?;

        let reader = PeriodicReader::builder(exporter)
            .with_interval(EXPORT_INTERVAL)
            .build();

        let resource = Resource::builder().with_service_name(service_name).build();

        Ok(SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build())
    }

    /// In-memory test harness for asserting instrument emission without a live
    /// collector. Gated behind `test-support` so it is never compiled into a
    /// release binary.
    #[cfg(feature = "test-support")]
    pub mod testing {
        use std::collections::BTreeMap;

        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
        use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

        use super::{METER_NAME, SchedulerMetrics};

        /// A single flattened observation, reduced to the fields the tests
        /// assert on.
        #[derive(Debug, Clone)]
        pub struct MetricPoint {
            /// The instrument name (e.g. `rocky.scheduler.ticks`).
            pub name: String,
            /// The data point's attributes, keyed by attribute name.
            pub attrs: BTreeMap<String, String>,
            /// Sums and gauges: the point value. Histograms: the observation
            /// sum.
            pub value: f64,
            /// Histograms: the number of recorded observations. Zero for sums
            /// and gauges.
            pub histogram_count: u64,
        }

        impl MetricPoint {
            /// The value of the named attribute, if present.
            #[must_use]
            pub fn attr(&self, key: &str) -> Option<&str> {
                self.attrs.get(key).map(String::as_str)
            }
        }

        /// An in-memory meter pipeline plus the recording handle bound to it.
        pub struct MetricsHarness {
            provider: SdkMeterProvider,
            exporter: InMemoryMetricExporter,
            metrics: SchedulerMetrics,
        }

        impl Default for MetricsHarness {
            fn default() -> Self {
                Self::new()
            }
        }

        impl MetricsHarness {
            /// Build a harness backed by an in-memory exporter.
            #[must_use]
            pub fn new() -> Self {
                let exporter = InMemoryMetricExporter::default();
                let provider = SdkMeterProvider::builder()
                    .with_periodic_exporter(exporter.clone())
                    .build();
                let metrics = SchedulerMetrics::from_meter(&provider.meter(METER_NAME));
                Self {
                    provider,
                    exporter,
                    metrics,
                }
            }

            /// A recording handle sharing this harness's instruments.
            #[must_use]
            pub fn metrics(&self) -> SchedulerMetrics {
                self.metrics.clone()
            }

            /// Force a periodic flush and return the points from this export.
            ///
            /// The exporter is reset afterwards so a subsequent `collect` sees
            /// only the newest export — a gauge then reads as its current
            /// value, not a stale one accumulated across flushes.
            #[must_use]
            pub fn collect(&self) -> Vec<MetricPoint> {
                self.provider.force_flush().expect("force_flush");
                let points = read_points(&self.exporter);
                self.exporter.reset();
                points
            }

            /// Shut the provider down (mirroring the guard's `Drop`) and return
            /// the points the exporter received — proves the final scrape
            /// survives shutdown.
            #[must_use]
            pub fn shutdown_and_collect(self) -> Vec<MetricPoint> {
                self.provider.shutdown().expect("shutdown");
                read_points(&self.exporter)
            }
        }

        fn read_points(exporter: &InMemoryMetricExporter) -> Vec<MetricPoint> {
            let mut out = Vec::new();
            let resource_metrics = exporter
                .get_finished_metrics()
                .expect("finished metrics available");
            for rm in &resource_metrics {
                for sm in rm.scope_metrics() {
                    for metric in sm.metrics() {
                        let name = metric.name().to_string();
                        match metric.data() {
                            AggregatedMetrics::U64(data) => push_u64(&mut out, &name, data),
                            AggregatedMetrics::F64(data) => push_f64(&mut out, &name, data),
                            AggregatedMetrics::I64(_) => {}
                        }
                    }
                }
            }
            out
        }

        fn attrs_of<'a>(
            kvs: impl Iterator<Item = &'a opentelemetry::KeyValue>,
        ) -> BTreeMap<String, String> {
            kvs.map(|kv| (kv.key.as_str().to_string(), kv.value.to_string()))
                .collect()
        }

        fn push_u64(out: &mut Vec<MetricPoint>, name: &str, data: &MetricData<u64>) {
            match data {
                MetricData::Sum(sum) => {
                    for dp in sum.data_points() {
                        out.push(MetricPoint {
                            name: name.to_string(),
                            attrs: attrs_of(dp.attributes()),
                            value: dp.value() as f64,
                            histogram_count: 0,
                        });
                    }
                }
                MetricData::Gauge(gauge) => {
                    for dp in gauge.data_points() {
                        out.push(MetricPoint {
                            name: name.to_string(),
                            attrs: attrs_of(dp.attributes()),
                            value: dp.value() as f64,
                            histogram_count: 0,
                        });
                    }
                }
                MetricData::Histogram(hist) => {
                    for dp in hist.data_points() {
                        out.push(MetricPoint {
                            name: name.to_string(),
                            attrs: attrs_of(dp.attributes()),
                            value: dp.sum() as f64,
                            histogram_count: dp.count(),
                        });
                    }
                }
                MetricData::ExponentialHistogram(_) => {}
            }
        }

        fn push_f64(out: &mut Vec<MetricPoint>, name: &str, data: &MetricData<f64>) {
            match data {
                MetricData::Sum(sum) => {
                    for dp in sum.data_points() {
                        out.push(MetricPoint {
                            name: name.to_string(),
                            attrs: attrs_of(dp.attributes()),
                            value: dp.value(),
                            histogram_count: 0,
                        });
                    }
                }
                MetricData::Gauge(gauge) => {
                    for dp in gauge.data_points() {
                        out.push(MetricPoint {
                            name: name.to_string(),
                            attrs: attrs_of(dp.attributes()),
                            value: dp.value(),
                            histogram_count: 0,
                        });
                    }
                }
                MetricData::Histogram(hist) => {
                    for dp in hist.data_points() {
                        out.push(MetricPoint {
                            name: name.to_string(),
                            attrs: attrs_of(dp.attributes()),
                            value: dp.sum(),
                            histogram_count: dp.count(),
                        });
                    }
                }
                MetricData::ExponentialHistogram(_) => {}
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::testing::MetricsHarness;

        fn point<'a>(
            points: &'a [super::testing::MetricPoint],
            name: &str,
        ) -> Option<&'a super::testing::MetricPoint> {
            points.iter().find(|p| p.name == name)
        }

        /// Serialises the one test that mutates `OTEL_EXPORTER_OTLP_ENDPOINT`
        /// against any future sibling, so its remove/restore window cannot race
        /// under a threaded `cargo test` run.
        fn env_lock() -> std::sync::MutexGuard<'static, ()> {
            static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
            LOCK.get_or_init(|| std::sync::Mutex::new(()))
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        /// With no OTLP endpoint configured, `init_if_enabled` installs no meter
        /// provider and hands back a no-op recording handle. This is the
        /// "meter leaking on the no-endpoint path" guard, asserted rather than
        /// only reasoned about.
        #[test]
        fn init_without_endpoint_installs_no_meter() {
            let _serialize = env_lock();
            let prior = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT");
            // SAFETY: env mutation is serialised under `env_lock`, and no other
            // test in this crate mutates this variable.
            unsafe {
                std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
            }

            let meter_guard = super::SchedulerMeterGuard::init_if_enabled();
            assert!(
                !meter_guard.metrics().is_enabled(),
                "no endpoint ⇒ no meter provider, no recording handle",
            );

            if let Some(v) = prior {
                // SAFETY: as above — still under `env_lock`.
                unsafe {
                    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", v);
                }
            }
        }

        #[test]
        fn disabled_handle_records_nothing_and_never_panics() {
            let metrics = super::SchedulerMetrics::disabled();
            assert!(!metrics.is_enabled());
            // None of these may panic or observe anything.
            metrics.record_tick_outcome("completed");
            metrics.record_due(3);
            metrics.record_executed(2);
            metrics.record_skip("not_due");
            metrics.record_lag_seconds(1.0);
            metrics.record_execution_outcome("raw", false);
        }

        #[test]
        fn disabled_guard_installs_no_provider() {
            let guard = super::SchedulerMeterGuard::disabled();
            assert!(!guard.metrics().is_enabled());
            // Dropping a no-op guard must not panic.
            drop(guard);
        }

        #[test]
        fn counters_accumulate_across_ticks() {
            let harness = MetricsHarness::new();
            let m = harness.metrics();
            m.record_tick_outcome("completed");
            m.record_due(2);
            m.record_executed(1);
            m.record_tick_outcome("completed");
            m.record_due(1);
            m.record_executed(1);

            let points = harness.collect();
            assert_eq!(point(&points, "rocky.scheduler.ticks").unwrap().value, 2.0);
            assert_eq!(point(&points, "rocky.scheduler.due").unwrap().value, 3.0);
            assert_eq!(
                point(&points, "rocky.scheduler.executed").unwrap().value,
                2.0
            );
        }

        #[test]
        fn tick_counter_splits_by_outcome_attribute() {
            let harness = MetricsHarness::new();
            let m = harness.metrics();
            m.record_tick_outcome("completed");
            m.record_tick_outcome("completed");
            m.record_tick_outcome("config_error");

            let points = harness.collect();
            let by_outcome: std::collections::BTreeMap<&str, f64> = points
                .iter()
                .filter(|p| p.name == "rocky.scheduler.ticks")
                .map(|p| (p.attr("outcome").expect("outcome attr present"), p.value))
                .collect();
            assert_eq!(by_outcome.get("completed"), Some(&2.0));
            assert_eq!(
                by_outcome.get("config_error"),
                Some(&1.0),
                "an execution-blocking outcome is its own alertable series",
            );
        }

        #[test]
        fn skip_counter_splits_by_reason_attribute() {
            let harness = MetricsHarness::new();
            let m = harness.metrics();
            m.record_skip("not_due");
            m.record_skip("not_due");
            m.record_skip("disabled");

            let points = harness.collect();
            let by_reason: std::collections::BTreeMap<&str, f64> = points
                .iter()
                .filter(|p| p.name == "rocky.scheduler.skipped")
                .map(|p| (p.attr("reason").expect("reason attr present"), p.value))
                .collect();
            assert_eq!(by_reason.get("not_due"), Some(&2.0));
            assert_eq!(by_reason.get("disabled"), Some(&1.0));
        }

        #[test]
        fn lag_histogram_records_each_observation() {
            let harness = MetricsHarness::new();
            let m = harness.metrics();
            m.record_lag_seconds(5.0);
            m.record_lag_seconds(15.0);

            let points = harness.collect();
            let lag = point(&points, "rocky.scheduler.lag_seconds").unwrap();
            assert_eq!(lag.histogram_count, 2);
            assert_eq!(lag.value, 20.0);
        }

        #[test]
        fn consecutive_failures_gauge_climbs_then_resets_per_pipeline() {
            let harness = MetricsHarness::new();
            let m = harness.metrics();
            m.record_execution_outcome("raw", false);
            m.record_execution_outcome("raw", false);
            m.record_execution_outcome("other", false);
            let points = harness.collect();
            let raw = points
                .iter()
                .find(|p| {
                    p.name == "rocky.scheduler.consecutive_failures"
                        && p.attr("pipeline") == Some("raw")
                })
                .unwrap();
            assert_eq!(raw.value, 2.0, "two consecutive failures ⇒ streak 2");

            // A success resets only `raw`.
            m.record_execution_outcome("raw", true);
            let points = harness.collect();
            let raw = points
                .iter()
                .find(|p| {
                    p.name == "rocky.scheduler.consecutive_failures"
                        && p.attr("pipeline") == Some("raw")
                })
                .unwrap();
            assert_eq!(raw.value, 0.0, "a success resets the streak to zero");
        }

        #[test]
        fn shutdown_exports_the_final_scrape() {
            let harness = MetricsHarness::new();
            let m = harness.metrics();
            m.record_tick_outcome("completed");
            // No explicit collect(): the shutdown path alone must flush — this
            // is what the guard's `Drop` relies on to export the last tick.
            let points = harness.shutdown_and_collect();
            assert_eq!(point(&points, "rocky.scheduler.ticks").unwrap().value, 1.0);
        }
    }
}

#[cfg(not(feature = "otel"))]
mod disabled {
    /// No-op recording handle when the `otel` feature is disabled.
    #[derive(Clone, Default)]
    pub struct SchedulerMetrics;

    impl SchedulerMetrics {
        /// A handle that records nothing.
        #[must_use]
        pub fn disabled() -> Self {
            Self
        }

        /// Always `false` without the `otel` feature.
        #[must_use]
        pub fn is_enabled(&self) -> bool {
            false
        }

        /// No-op.
        pub fn record_tick_outcome(&self, _outcome: &'static str) {}

        /// No-op.
        pub fn record_due(&self, _count: u64) {}

        /// No-op.
        pub fn record_executed(&self, _count: u64) {}

        /// No-op.
        pub fn record_skip(&self, _reason: &'static str) {}

        /// No-op.
        pub fn record_lag_seconds(&self, _seconds: f64) {}

        /// No-op.
        pub fn record_execution_outcome(&self, _pipeline: &str, _succeeded: bool) {}
    }

    /// No-op guard when the `otel` feature is disabled.
    pub struct SchedulerMeterGuard;

    impl SchedulerMeterGuard {
        /// No-op: no meter provider exists without the `otel` feature.
        #[must_use]
        pub fn init_if_enabled() -> Self {
            Self
        }

        /// No-op guard.
        #[must_use]
        pub fn disabled() -> Self {
            Self
        }

        /// Always a no-op handle without the `otel` feature.
        #[must_use]
        pub fn metrics(&self) -> SchedulerMetrics {
            SchedulerMetrics::disabled()
        }
    }
}
