use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct MetricsSnapshot {
    pub wal_appends_total: u64,
    pub wal_sync_total: u64,
    pub flush_jobs_total: u64,
    pub compactions_total: u64,
    pub bytes_written_total: u64,
    pub bytes_read_total: u64,
    pub tables_touched_total: u64,
    pub wal_append_latency: LatencySummarySnapshot,
    pub flush_duration: LatencySummarySnapshot,
    pub compaction_duration: LatencySummarySnapshot,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct LatencySummarySnapshot {
    pub samples: u64,
    pub total_nanos: u64,
    pub min_nanos: u64,
    pub max_nanos: u64,
    pub avg_nanos: u64,
    pub last_nanos: u64,
}

#[derive(Clone, Default)]
pub struct MetricsHandle {
    inner: Arc<MetricsInner>,
}

#[derive(Default)]
struct MetricsInner {
    wal_appends: AtomicU64,
    wal_sync: AtomicU64,
    flush_jobs: AtomicU64,
    compactions: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
    tables_touched: AtomicU64,
    wal_append_latency: RollingLatency,
    flush_duration: RollingLatency,
    compaction_duration: RollingLatency,
}

struct RollingLatency {
    samples: AtomicU64,
    total_nanos: AtomicU64,
    min_nanos: AtomicU64,
    max_nanos: AtomicU64,
    last_nanos: AtomicU64,
}

impl Default for RollingLatency {
    fn default() -> Self {
        Self {
            samples: AtomicU64::new(0),
            total_nanos: AtomicU64::new(0),
            min_nanos: AtomicU64::new(u64::MAX),
            max_nanos: AtomicU64::new(0),
            last_nanos: AtomicU64::new(0),
        }
    }
}

impl MetricsHandle {
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            wal_appends_total: self.inner.wal_appends.load(Ordering::Relaxed),
            wal_sync_total: self.inner.wal_sync.load(Ordering::Relaxed),
            flush_jobs_total: self.inner.flush_jobs.load(Ordering::Relaxed),
            compactions_total: self.inner.compactions.load(Ordering::Relaxed),
            bytes_written_total: self.inner.bytes_written.load(Ordering::Relaxed),
            bytes_read_total: self.inner.bytes_read.load(Ordering::Relaxed),
            tables_touched_total: self.inner.tables_touched.load(Ordering::Relaxed),
            wal_append_latency: self.inner.wal_append_latency.snapshot(),
            flush_duration: self.inner.flush_duration.snapshot(),
            compaction_duration: self.inner.compaction_duration.snapshot(),
        }
    }

    pub fn record_wal_append(&self, bytes: usize) {
        self.inner.wal_appends.fetch_add(1, Ordering::Relaxed);
        self.add_bytes_written(bytes);
    }

    pub fn record_wal_append_with_latency(&self, bytes: usize, elapsed: Duration) {
        self.record_wal_append(bytes);
        self.inner.wal_append_latency.record(elapsed);
    }

    pub fn record_wal_sync(&self) {
        self.inner.wal_sync.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_flush_job(&self) {
        self.inner.flush_jobs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_flush_duration(&self, elapsed: Duration) {
        self.inner.flush_duration.record(elapsed);
    }

    pub fn record_flush_job_with_duration(&self, elapsed: Duration) {
        self.record_flush_job();
        self.record_flush_duration(elapsed);
    }

    pub fn record_compaction(&self) {
        self.inner.compactions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_compaction_duration(&self, elapsed: Duration) {
        self.inner.compaction_duration.record(elapsed);
    }

    pub fn record_compaction_with_duration(&self, elapsed: Duration) {
        self.record_compaction();
        self.record_compaction_duration(elapsed);
    }

    pub fn add_bytes_written(&self, bytes: usize) {
        self.inner
            .bytes_written
            .fetch_add(bytes_to_u64(bytes), Ordering::Relaxed);
    }

    pub fn add_bytes_read(&self, bytes: usize) {
        self.inner
            .bytes_read
            .fetch_add(bytes_to_u64(bytes), Ordering::Relaxed);
    }

    pub fn add_tables_touched(&self, count: usize) {
        self.inner
            .tables_touched
            .fetch_add(bytes_to_u64(count), Ordering::Relaxed);
    }
}

impl RollingLatency {
    fn record(&self, elapsed: Duration) {
        let nanos = duration_to_nanos(elapsed);
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.total_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.last_nanos.store(nanos, Ordering::Relaxed);
        update_min(&self.min_nanos, nanos);
        update_max(&self.max_nanos, nanos);
    }

    fn snapshot(&self) -> LatencySummarySnapshot {
        let samples = self.samples.load(Ordering::Relaxed);
        let total_nanos = self.total_nanos.load(Ordering::Relaxed);
        let min_raw = self.min_nanos.load(Ordering::Relaxed);
        let max_nanos = self.max_nanos.load(Ordering::Relaxed);
        let last_nanos = self.last_nanos.load(Ordering::Relaxed);
        let min_nanos = if samples == 0 || min_raw == u64::MAX {
            0
        } else {
            min_raw
        };
        let avg_nanos = if samples == 0 {
            0
        } else {
            total_nanos / samples
        };

        LatencySummarySnapshot {
            samples,
            total_nanos,
            min_nanos,
            max_nanos,
            avg_nanos,
            last_nanos,
        }
    }
}

fn update_min(cell: &AtomicU64, value: u64) {
    let mut current = cell.load(Ordering::Relaxed);
    while value < current {
        match cell.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn update_max(cell: &AtomicU64, value: u64) {
    let mut current = cell.load(Ordering::Relaxed);
    while value > current {
        match cell.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn duration_to_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn bytes_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
