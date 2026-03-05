use std::{
    collections::{HashMap, HashSet},
    fs,
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tracing::{error, info};

use crate::{
    compaction::{CompactionJob, CompactionReason, spawn_compaction_worker},
    config::{CompressionCodec, EngineOptions},
    error::Result,
    flush::{FlushJob, spawn_flush_worker},
    manifest::{ManifestState, ManifestStore, PendingDeleteEntry},
    memtable::{MemTable, MemTableSet},
    merge_iter::{MergeIterator, MergeMode, SourceCursor},
    metrics::{MetricsHandle, MetricsSnapshot},
    snapshot::Snapshot,
    sstable::{
        BlockEncodingKind, FooterMetadata, SsTableMeta, block_cache::BlockCache,
        reader::SsTableReader, writer::SsTableWriter,
    },
    types::{
        BatchOp, InternalEntry, Key, KvStore, ScanBounds, ScanItem, ScanOptions, Value, ValueEntry,
        WriteBatch,
    },
    wal::{WalBatchOp, WalOp, WalWriter, replay_wal},
};

const SSTABLE_FILE_EXT: &str = "sst";
const DEFAULT_BACKGROUND_CHANNEL_CAPACITY: usize = 128;
const SOURCE_RANK_L0_BASE: u32 = 2;
const SOURCE_RANK_L1_BASE: u32 = 1_000_000;

#[derive(Clone)]
pub struct AetherEngine {
    inner: Arc<Inner>,
}

struct Inner {
    write_state: Mutex<WriteState>,
    read_state: RwLock<ReadState>,
    background: BackgroundTaskChannels,
    metrics: MetricsHandle,
    memtable_max_bytes: usize,
    sst_target_block_bytes: usize,
    l0_compaction_trigger: usize,
    max_open_snapshots: usize,
    max_write_batch_ops: usize,
    max_write_batch_bytes: usize,
    compression_codec: CompressionCodec,
    prefix_restart_interval: u16,
    min_compress_size_bytes: usize,
    enable_mmap_reads: bool,
    block_cache: Option<Arc<BlockCache>>,
    metrics_log_interval: Option<Duration>,
    last_metrics_log_at: Mutex<Option<Instant>>,
    background_error: Mutex<Option<String>>,
    snapshot_registry: Mutex<SnapshotRegistry>,
    obsolete_table_files: Mutex<Vec<ObsoleteTableFile>>,
    #[cfg(test)]
    compaction_test_fail_after_output_entries: Mutex<Option<usize>>,
}

struct WriteState {
    db_dir: PathBuf,
    wal: WalWriter,
    manifest: ManifestState,
}

struct ReadState {
    memtables: MemTableSet,
    manifest_snapshot: ManifestState,
    table_cache: TableCache,
}

#[derive(Default)]
struct TableCache {
    l0: Vec<TableHandle>,
    l1: Vec<TableHandle>,
    l1_non_overlapping: bool,
}

struct TableHandle {
    meta: SsTableMeta,
    reader: Arc<SsTableReader>,
}

struct PointReadView {
    mutable: Arc<MemTable>,
    immutable: Option<Arc<MemTable>>,
    l0_tables: Vec<TableHandle>,
    l1_tables: Vec<TableHandle>,
    l1_non_overlapping: bool,
}

pub(crate) struct PinnedReadState {
    visible_seq: u64,
    mutable: Arc<MemTable>,
    immutable: Option<Arc<MemTable>>,
    l0_tables: Vec<TableSource>,
    l1_tables: Vec<TableSource>,
    l1_non_overlapping: bool,
}

struct ReadSnapshot {
    visible_seq: u64,
    mutable_entries: Vec<InternalEntry>,
    immutable_entries: Vec<InternalEntry>,
    l0_tables: Vec<TableSource>,
    l1_tables: Vec<TableSource>,
}

pub(crate) struct TableSource {
    reader: Arc<SsTableReader>,
    source_rank: u32,
    min_key: Bytes,
    max_key: Bytes,
}

#[derive(Default)]
struct SnapshotRegistry {
    next_id: u64,
    active: HashMap<u64, SnapshotMeta>,
}

struct SnapshotMeta {
    visible_seq: u64,
}

struct ObsoleteTableFile {
    table_id: u64,
    level: u8,
    path: PathBuf,
    max_seq: u64,
}

pub(crate) struct SnapshotHandle {
    snapshot_id: u64,
    engine: std::sync::Weak<Inner>,
}

impl PinnedReadState {
    pub(crate) const fn visible_seq(&self) -> u64 {
        self.visible_seq
    }
}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        let Some(inner) = self.engine.upgrade() else {
            return;
        };

        let engine = AetherEngine { inner };
        engine.unregister_snapshot(self.snapshot_id);
        engine.try_cleanup_obsolete_tables();
    }
}

struct CompactionSelection {
    selected_l0: Vec<SsTableMeta>,
    selected_l1: Vec<SsTableMeta>,
    min_key: Key,
    max_key: Key,
}

struct CompactionPlan {
    reason: CompactionReason,
    reserved_table_id: u64,
    db_dir: PathBuf,
    settings: SstableWriteSettings,
    selection: CompactionSelection,
    removed_tables: Vec<SsTableMeta>,
    removed_ids: HashSet<u64>,
    input_l0_table_ids: Vec<u64>,
    input_l1_table_ids: Vec<u64>,
    drop_tombstones: bool,
    sources: Vec<TableSource>,
}

struct CompactionOutput {
    new_l1_table: Option<TableHandle>,
    output_entries: usize,
    output_table_ids: Vec<u64>,
}

#[derive(Clone)]
struct SstableWriteSettings {
    block_target_bytes: usize,
    compression_codec: CompressionCodec,
    prefix_restart_interval: u16,
    min_compress_size_bytes: usize,
    enable_mmap_reads: bool,
    block_cache: Option<Arc<BlockCache>>,
}

struct BackgroundTaskChannels {
    flush_tx: SyncSender<FlushJob>,
    compaction_tx: SyncSender<CompactionJob>,
}

const fn compaction_reason_label(reason: CompactionReason) -> &'static str {
    match reason {
        CompactionReason::L0ThresholdReached => "l0_threshold_reached",
    }
}

impl AetherEngine {
    /// Opens (or creates) an engine rooted at `opts.db_dir`.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be initialized.
    pub fn open(opts: EngineOptions) -> Result<Self> {
        if opts.max_open_snapshots == 0 {
            return Err(invalid_input("max_open_snapshots must be > 0").into());
        }

        fs::create_dir_all(&opts.db_dir)?;

        let metrics = MetricsHandle::default();
        let block_cache = build_block_cache(opts.block_cache_capacity_bytes, &metrics);

        let mut manifest = ManifestStore::load_or_create(&opts.db_dir)?;
        let wal_path = wal_path_for_generation(&opts.db_dir, manifest.wal_generation);
        let replayed_entries = replay_wal(&wal_path)?;
        let wal = WalWriter::open_with_min_seq(
            &wal_path,
            opts.fsync_policy,
            manifest.next_sequence_number,
        )?;

        let mut memtables = MemTableSet::new();
        for entry in replayed_entries {
            memtables.mutable_mut().upsert(entry);
        }

        let loaded_tables = load_initial_tables(
            &opts.db_dir,
            &mut manifest,
            opts.enable_mmap_reads,
            block_cache.as_ref(),
        )?;

        let next_table_id = max_table_id(&manifest.levels)
            .and_then(|last| last.checked_add(1))
            .unwrap_or(0);
        manifest.next_table_id = manifest.next_table_id.max(next_table_id);
        manifest.next_sequence_number = manifest
            .next_sequence_number
            .max(wal.next_sequence_number());
        manifest.wal_durable_checkpoint_offset = wal.write_offset();
        ManifestStore::commit(&opts.db_dir, &manifest)?;
        let pending_obsolete_files = obsolete_table_files_from_manifest(&opts.db_dir, &manifest);

        let (background, flush_rx, compaction_rx) =
            BackgroundTaskChannels::new(DEFAULT_BACKGROUND_CHANNEL_CAPACITY);

        let engine = Self {
            inner: Arc::new(Inner {
                write_state: Mutex::new(WriteState {
                    db_dir: opts.db_dir,
                    wal,
                    manifest: manifest.clone(),
                }),
                read_state: RwLock::new(ReadState {
                    memtables,
                    manifest_snapshot: manifest,
                    table_cache: TableCache::from_tables(loaded_tables),
                }),
                background,
                metrics,
                memtable_max_bytes: opts.memtable_max_bytes,
                sst_target_block_bytes: opts.sst_target_block_bytes,
                l0_compaction_trigger: opts.l0_compaction_trigger,
                max_open_snapshots: opts.max_open_snapshots,
                max_write_batch_ops: opts.max_write_batch_ops,
                max_write_batch_bytes: opts.max_write_batch_bytes,
                compression_codec: opts.compression_codec,
                prefix_restart_interval: opts.prefix_restart_interval,
                min_compress_size_bytes: opts.min_compress_size_bytes,
                enable_mmap_reads: opts.enable_mmap_reads,
                block_cache,
                metrics_log_interval: opts
                    .enable_metrics_log_interval
                    .filter(|millis| *millis > 0)
                    .map(Duration::from_millis),
                last_metrics_log_at: Mutex::new(None),
                background_error: Mutex::new(None),
                snapshot_registry: Mutex::new(SnapshotRegistry::default()),
                obsolete_table_files: Mutex::new(pending_obsolete_files),
                #[cfg(test)]
                compaction_test_fail_after_output_entries: Mutex::new(None),
            }),
        };

        let flush_inner = Arc::downgrade(&engine.inner);
        let _ = spawn_flush_worker(flush_rx, move |job| {
            let Some(inner) = flush_inner.upgrade() else {
                return Ok(());
            };
            Self { inner }.handle_flush_job(job)
        });

        let compaction_inner = Arc::downgrade(&engine.inner);
        let _ = spawn_compaction_worker(compaction_rx, move |job| {
            let Some(inner) = compaction_inner.upgrade() else {
                return Ok(());
            };
            Self { inner }.handle_compaction_job(job)
        });

        engine.try_cleanup_obsolete_tables();

        Ok(engine)
    }

    /// Inserts or updates `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL append or in-memory application fails.
    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        <Self as KvStore>::put(self, key, value)
    }

    /// Returns the latest value for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if lookups fail due to storage corruption or I/O errors.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        <Self as KvStore>::get(self, key)
    }

    /// Marks `key` as deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL append or in-memory application fails.
    pub fn delete(&self, key: Key) -> Result<()> {
        <Self as KvStore>::delete(self, key)
    }

    /// Forces flushing current in-memory state to an L0 table.
    ///
    /// # Errors
    ///
    /// Returns an error if flush/manifest/WAL rotation steps fail.
    pub fn flush(&self) -> Result<()> {
        <Self as KvStore>::flush(self)
    }

    /// Scans key-value entries in a bounded range.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot construction or source iteration fails.
    pub fn scan(&self, options: ScanOptions) -> Result<Vec<ScanItem>> {
        <Self as KvStore>::scan(self, options)
    }

    /// Applies a batch of write operations.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or batch writes are unsupported.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        <Self as KvStore>::write_batch(self, batch)
    }

    /// Opens a read-only snapshot pinned to the current visible sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if background workers are unhealthy.
    pub fn open_snapshot(&self) -> Result<Snapshot> {
        self.ensure_background_workers_healthy()?;
        let visible_seq = self.current_visible_sequence_number();
        let handle = Arc::new(self.register_snapshot(visible_seq)?);
        let pinned = match self.build_pinned_read_state(visible_seq) {
            Ok(state) => Arc::new(state),
            Err(err) => {
                drop(handle);
                return Err(err);
            }
        };
        Ok(Snapshot::new(self.clone(), pinned, handle))
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    fn current_visible_sequence_number(&self) -> u64 {
        let read_state = self.inner.read_state.read();
        read_state
            .manifest_snapshot
            .next_sequence_number
            .saturating_sub(1)
    }

    fn build_pinned_read_state(&self, visible_seq: u64) -> Result<PinnedReadState> {
        let read_state = self.inner.read_state.read();
        let mutable = read_state.memtables.mutable_arc();
        let immutable = read_state.memtables.immutable_arc();
        let l1_non_overlapping = read_state.table_cache.l1_non_overlapping;

        let mut l0_tables = Vec::with_capacity(read_state.table_cache.l0.len());
        for (index, table) in read_state.table_cache.l0.iter().enumerate() {
            let offset = u32::try_from(index)
                .map_err(|_| invalid_data("L0 table count exceeds source-rank range"))?;
            let source_rank = SOURCE_RANK_L0_BASE
                .checked_add(offset)
                .ok_or_else(|| invalid_data("L0 source-rank overflow"))?;
            l0_tables.push(TableSource {
                reader: Arc::clone(&table.reader),
                source_rank,
                min_key: table.meta.min_key.clone(),
                max_key: table.meta.max_key.clone(),
            });
        }

        let mut l1_tables = Vec::with_capacity(read_state.table_cache.l1.len());
        for (index, table) in read_state.table_cache.l1.iter().enumerate() {
            let offset = u32::try_from(index)
                .map_err(|_| invalid_data("L1 table count exceeds source-rank range"))?;
            let source_rank = SOURCE_RANK_L1_BASE
                .checked_add(offset)
                .ok_or_else(|| invalid_data("L1 source-rank overflow"))?;
            l1_tables.push(TableSource {
                reader: Arc::clone(&table.reader),
                source_rank,
                min_key: table.meta.min_key.clone(),
                max_key: table.meta.max_key.clone(),
            });
        }

        drop(read_state);

        Ok(PinnedReadState {
            visible_seq,
            mutable,
            immutable,
            l0_tables,
            l1_tables,
            l1_non_overlapping,
        })
    }

    fn build_point_read_view(&self) -> PointReadView {
        let read_state = self.inner.read_state.read();
        let mutable = read_state.memtables.mutable_arc();
        let immutable = read_state.memtables.immutable_arc();
        let l1_non_overlapping = read_state.table_cache.l1_non_overlapping;
        let l0_tables = read_state
            .table_cache
            .l0
            .iter()
            .map(|table| TableHandle {
                meta: table.meta.clone(),
                reader: Arc::clone(&table.reader),
            })
            .collect();
        let l1_tables = read_state
            .table_cache
            .l1
            .iter()
            .map(|table| TableHandle {
                meta: table.meta.clone(),
                reader: Arc::clone(&table.reader),
            })
            .collect();
        drop(read_state);

        PointReadView {
            mutable,
            immutable,
            l0_tables,
            l1_tables,
            l1_non_overlapping,
        }
    }

    fn build_read_snapshot(&self, options: &ScanOptions, visible_seq: u64) -> Result<ReadSnapshot> {
        let read_state = self.inner.read_state.read();
        let mutable_entries = read_state
            .memtables
            .mutable()
            .range_entries(&options.bounds, visible_seq);
        let immutable_entries = read_state
            .memtables
            .immutable()
            .map_or_else(Vec::new, |memtable| {
                memtable.range_entries(&options.bounds, visible_seq)
            });

        let mut l0_tables = Vec::new();
        for (index, table) in read_state.table_cache.l0.iter().enumerate() {
            if !table_overlaps_bounds(&table.meta, &options.bounds) {
                continue;
            }

            let offset = u32::try_from(index)
                .map_err(|_| invalid_data("L0 table count exceeds source-rank range"))?;
            let source_rank = SOURCE_RANK_L0_BASE
                .checked_add(offset)
                .ok_or_else(|| invalid_data("L0 source-rank overflow"))?;

            l0_tables.push(TableSource {
                reader: Arc::clone(&table.reader),
                source_rank,
                min_key: table.meta.min_key.clone(),
                max_key: table.meta.max_key.clone(),
            });
        }

        let mut l1_tables = Vec::new();
        for (index, table) in read_state.table_cache.l1.iter().enumerate() {
            if !table_overlaps_bounds(&table.meta, &options.bounds) {
                continue;
            }

            let offset = u32::try_from(index)
                .map_err(|_| invalid_data("L1 table count exceeds source-rank range"))?;
            let source_rank = SOURCE_RANK_L1_BASE
                .checked_add(offset)
                .ok_or_else(|| invalid_data("L1 source-rank overflow"))?;

            l1_tables.push(TableSource {
                reader: Arc::clone(&table.reader),
                source_rank,
                min_key: table.meta.min_key.clone(),
                max_key: table.meta.max_key.clone(),
            });
        }

        drop(read_state);

        Ok(ReadSnapshot {
            visible_seq,
            mutable_entries,
            immutable_entries,
            l0_tables,
            l1_tables,
        })
    }

    pub(crate) fn scan_at_sequence(
        &self,
        options: ScanOptions,
        visible_seq: u64,
    ) -> Result<Vec<ScanItem>> {
        let ScanOptions { bounds, limit } = options;

        if has_invalid_scan_bounds(&bounds) {
            return Err(invalid_input("scan start bound is greater than end bound").into());
        }

        if matches!(limit, Some(0)) {
            return Ok(Vec::new());
        }

        let options = ScanOptions { bounds, limit };
        let snapshot = self.build_read_snapshot(&options, visible_seq)?;
        let mut sources = vec![
            SourceCursor::from_entries(snapshot.mutable_entries, 0),
            SourceCursor::from_entries(snapshot.immutable_entries, 1),
        ];

        for source in snapshot.l0_tables {
            let TableSource {
                reader,
                source_rank,
                ..
            } = source;
            let iter = reader.iter_range(&options.bounds, snapshot.visible_seq)?;
            sources.push(SourceCursor::from_sstable_iter(iter, source_rank));
        }

        for source in snapshot.l1_tables {
            let TableSource {
                reader,
                source_rank,
                ..
            } = source;
            let iter = reader.iter_range(&options.bounds, snapshot.visible_seq)?;
            sources.push(SourceCursor::from_sstable_iter(iter, source_rank));
        }

        let limit = options.limit.unwrap_or(usize::MAX);
        let mut output = Vec::new();
        let mut merge_iter = MergeIterator::new(MergeMode::UserScan, sources);
        while output.len() < limit {
            let Some(entry) = merge_iter.next_entry()? else {
                break;
            };

            if let ValueEntry::Put(value) = entry.value {
                output.push(ScanItem {
                    key: entry.key,
                    value,
                });
            }
        }

        Ok(output)
    }

    pub(crate) fn scan_pinned(
        &self,
        options: ScanOptions,
        pinned: &PinnedReadState,
    ) -> Result<Vec<ScanItem>> {
        self.ensure_background_workers_healthy()?;
        let ScanOptions { bounds, limit } = options;

        if has_invalid_scan_bounds(&bounds) {
            return Err(invalid_input("scan start bound is greater than end bound").into());
        }

        if matches!(limit, Some(0)) {
            return Ok(Vec::new());
        }

        let options = ScanOptions { bounds, limit };
        let mutable_entries = pinned
            .mutable
            .range_entries(&options.bounds, pinned.visible_seq);
        let immutable_entries = pinned.immutable.as_ref().map_or_else(Vec::new, |memtable| {
            memtable.range_entries(&options.bounds, pinned.visible_seq)
        });
        let mut sources = vec![
            SourceCursor::from_entries(mutable_entries, 0),
            SourceCursor::from_entries(immutable_entries, 1),
        ];

        for source in &pinned.l0_tables {
            let iter = source
                .reader
                .iter_range(&options.bounds, pinned.visible_seq)?;
            sources.push(SourceCursor::from_sstable_iter(iter, source.source_rank));
        }

        for source in &pinned.l1_tables {
            let iter = source
                .reader
                .iter_range(&options.bounds, pinned.visible_seq)?;
            sources.push(SourceCursor::from_sstable_iter(iter, source.source_rank));
        }

        let limit = options.limit.unwrap_or(usize::MAX);
        let mut output = Vec::new();
        let mut merge_iter = MergeIterator::new(MergeMode::UserScan, sources);
        while output.len() < limit {
            let Some(entry) = merge_iter.next_entry()? else {
                break;
            };

            if let ValueEntry::Put(value) = entry.value {
                output.push(ScanItem {
                    key: entry.key,
                    value,
                });
            }
        }

        Ok(output)
    }

    pub(crate) fn get_pinned(&self, key: &[u8], pinned: &PinnedReadState) -> Result<Option<Value>> {
        self.ensure_background_workers_healthy()?;
        self.inner.metrics.add_bytes_read(key.len());

        let (result, tables_touched) = if let Some(entry) = pinned.mutable.get(key) {
            if entry.seq <= pinned.visible_seq {
                (decode_entry(entry), 0)
            } else {
                lookup_sstable_sources_at_sequence(
                    key,
                    pinned.visible_seq,
                    &pinned.l0_tables,
                    &pinned.l1_tables,
                    pinned.l1_non_overlapping,
                )?
            }
        } else if let Some(immutable) = pinned.immutable.as_ref() {
            if let Some(entry) = immutable.get(key) {
                if entry.seq <= pinned.visible_seq {
                    (decode_entry(entry), 0)
                } else {
                    lookup_sstable_sources_at_sequence(
                        key,
                        pinned.visible_seq,
                        &pinned.l0_tables,
                        &pinned.l1_tables,
                        pinned.l1_non_overlapping,
                    )?
                }
            } else {
                lookup_sstable_sources_at_sequence(
                    key,
                    pinned.visible_seq,
                    &pinned.l0_tables,
                    &pinned.l1_tables,
                    pinned.l1_non_overlapping,
                )?
            }
        } else {
            lookup_sstable_sources_at_sequence(
                key,
                pinned.visible_seq,
                &pinned.l0_tables,
                &pinned.l1_tables,
                pinned.l1_non_overlapping,
            )?
        };

        self.inner.metrics.add_tables_touched(tables_touched);
        self.maybe_emit_metrics_snapshot("read");
        Ok(result)
    }

    fn apply_write(&self, key: Key, value: ValueEntry) -> Result<()> {
        self.ensure_background_workers_healthy()?;
        let mut write_state = self.inner.write_state.lock();
        let mut read_state = self.inner.read_state.write();

        let wal_op = match &value {
            ValueEntry::Put(stored) => WalOp::Put {
                key: key.as_ref(),
                value: stored.as_ref(),
            },
            ValueEntry::Tombstone => WalOp::Delete { key: key.as_ref() },
        };

        let wal_append_started_at = Instant::now();
        let append = write_state.wal.append(wal_op)?;
        let estimated_wal_bytes = estimate_wal_record_bytes(key.len(), &value);
        self.inner
            .metrics
            .record_wal_append_with_latency(estimated_wal_bytes, wal_append_started_at.elapsed());

        read_state.memtables.mutable_mut().upsert(InternalEntry {
            seq: append.seq,
            key,
            value,
        });

        write_state.manifest.next_sequence_number = write_state.wal.next_sequence_number();
        write_state.manifest.wal_durable_checkpoint_offset = write_state.wal.write_offset();
        read_state.manifest_snapshot.next_sequence_number =
            write_state.manifest.next_sequence_number;
        read_state.manifest_snapshot.wal_durable_checkpoint_offset =
            write_state.manifest.wal_durable_checkpoint_offset;

        let mut flush_job: Option<FlushJob> = None;
        let rotated = read_state
            .memtables
            .maybe_rotate(self.inner.memtable_max_bytes.max(1));
        if rotated && let Some(memtable) = read_state.memtables.take_immutable() {
            let generation = write_state.manifest.wal_generation;
            flush_job = Some(FlushJob {
                generation,
                memtable,
                completion: None,
            });
        }

        drop(read_state);
        drop(write_state);

        if let Some(job) = flush_job {
            self.inner.background.enqueue_flush(job)?;
        }

        self.maybe_emit_metrics_snapshot("write");
        Ok(())
    }

    fn apply_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut write_state = self.inner.write_state.lock();
        let mut read_state = self.inner.read_state.write();

        let wal_append_started_at = Instant::now();
        let append = {
            let wal_batch_ops = batch
                .ops
                .iter()
                .map(|op| match op {
                    BatchOp::Put { key, value } => WalBatchOp::Put {
                        key: key.as_ref(),
                        value: value.as_ref(),
                    },
                    BatchOp::Delete { key } => WalBatchOp::Delete { key: key.as_ref() },
                })
                .collect::<Vec<_>>();
            write_state.wal.append(WalOp::Batch {
                ops: &wal_batch_ops,
            })?
        };
        let estimated_wal_bytes = estimate_wal_batch_record_bytes(&batch);
        self.inner
            .metrics
            .record_wal_append_with_latency(estimated_wal_bytes, wal_append_started_at.elapsed());

        for op in batch.ops {
            match op {
                BatchOp::Put { key, value } => {
                    read_state.memtables.mutable_mut().upsert(InternalEntry {
                        seq: append.seq,
                        key,
                        value: ValueEntry::Put(value),
                    });
                }
                BatchOp::Delete { key } => {
                    read_state.memtables.mutable_mut().upsert(InternalEntry {
                        seq: append.seq,
                        key,
                        value: ValueEntry::Tombstone,
                    });
                }
            }
        }

        write_state.manifest.next_sequence_number = write_state.wal.next_sequence_number();
        write_state.manifest.wal_durable_checkpoint_offset = write_state.wal.write_offset();
        read_state.manifest_snapshot.next_sequence_number =
            write_state.manifest.next_sequence_number;
        read_state.manifest_snapshot.wal_durable_checkpoint_offset =
            write_state.manifest.wal_durable_checkpoint_offset;

        let mut flush_job: Option<FlushJob> = None;
        let rotated = read_state
            .memtables
            .maybe_rotate(self.inner.memtable_max_bytes.max(1));
        if rotated && let Some(memtable) = read_state.memtables.take_immutable() {
            let generation = write_state.manifest.wal_generation;
            flush_job = Some(FlushJob {
                generation,
                memtable,
                completion: None,
            });
        }

        drop(read_state);
        drop(write_state);

        if let Some(job) = flush_job {
            self.inner.background.enqueue_flush(job)?;
        }

        self.maybe_emit_metrics_snapshot("write");
        Ok(())
    }

    fn maybe_schedule_compaction(&self, l0_tables: usize) {
        if l0_tables >= self.inner.l0_compaction_trigger.max(1) {
            self.inner.background.enqueue_compaction(CompactionJob {
                reason: CompactionReason::L0ThresholdReached,
            });
        }
    }

    fn handle_flush_job(&self, job: FlushJob) -> Result<()> {
        let FlushJob {
            generation,
            memtable,
            completion: _,
        } = job;
        let flush_started_at = Instant::now();
        let input_bytes = memtable.approx_size_bytes;
        let input_entries = memtable.len();
        info!(
            event = "flush.start",
            generation, input_bytes, input_entries, "flush started"
        );

        let result = (|| {
            let mut write_state = self.inner.write_state.lock();
            write_state.wal.sync()?;
            self.inner.metrics.record_wal_sync();

            let table_id = write_state.manifest.next_table_id;
            write_state.manifest.next_table_id = write_state
                .manifest
                .next_table_id
                .checked_add(1)
                .ok_or_else(|| invalid_data("SSTable table-id overflow"))?;
            let db_dir = write_state.db_dir.clone();

            // Keep write lock during SST generation so acknowledged writes cannot
            // land in a WAL generation that won't be replayed.
            let table = flush_memtable_to_sstable(
                &db_dir,
                memtable.as_ref(),
                table_id,
                &sstable_write_settings(&self.inner),
            )?;
            let output_table_bytes =
                table_file_size_bytes(&db_dir, table.meta.table_id, table.meta.level)?;

            add_table_to_manifest_levels(&mut write_state.manifest.levels, table.meta.clone());
            write_state.manifest.wal_durable_checkpoint_offset = write_state.wal.write_offset();
            ManifestStore::commit(&write_state.db_dir, &write_state.manifest)?;

            let mut read_state = self.inner.read_state.write();
            read_state.table_cache.insert(table);
            read_state.manifest_snapshot = write_state.manifest.clone();

            let l0_count = read_state.table_cache.l0.len();
            drop(read_state);
            drop(write_state);

            self.inner
                .metrics
                .record_flush_job_with_duration(flush_started_at.elapsed());
            info!(
                event = "flush.end",
                generation,
                output_table_id = table_id,
                output_table_level = 0_u8,
                input_bytes,
                output_table_bytes,
                elapsed_ms = flush_started_at.elapsed().as_millis(),
                "flush completed"
            );
            self.maybe_emit_metrics_snapshot("flush");
            self.maybe_schedule_compaction(l0_count);
            Ok(())
        })();

        if let Err(err) = &result {
            self.record_background_error(format!("flush worker failure: {err}"));
            error!(
                event = "flush.error",
                generation,
                input_bytes,
                input_entries,
                error = %err,
                "flush failed"
            );
        }

        result
    }

    fn handle_compaction_job(&self, job: CompactionJob) -> Result<()> {
        let compaction_started_at = Instant::now();

        let maybe_plan = {
            let mut write_state = self.inner.write_state.lock();
            let read_state = self.inner.read_state.read();
            self.capture_compaction_plan(job, &mut write_state, &read_state)?
        };
        let Some(plan) = maybe_plan else {
            return Ok(());
        };

        let reason = compaction_reason_label(plan.reason);
        let input_l0_tables = plan.selection.selected_l0.len();
        let input_l1_tables = plan.selection.selected_l1.len();
        let input_l0_table_ids = plan.input_l0_table_ids.clone();
        let input_l1_table_ids = plan.input_l1_table_ids.clone();
        info!(
            event = "compaction.start",
            reason,
            input_l0_tables,
            input_l1_tables,
            ?input_l0_table_ids,
            ?input_l1_table_ids,
            "compaction started"
        );

        let output = self.stream_compaction_output(&plan)?;
        let output_entries = output.output_entries;
        let output_table_ids = output.output_table_ids.clone();
        let obsolete_files = obsolete_table_files_from_metas(&plan.db_dir, &plan.removed_tables);
        let CompactionOutput {
            new_l1_table,
            output_entries: _,
            output_table_ids: _,
        } = output;

        {
            let mut write_state = self.inner.write_state.lock();
            let mut read_state = self.inner.read_state.write();
            Self::apply_compaction_commit_locked(
                &mut write_state,
                &mut read_state,
                &plan,
                new_l1_table,
            )?;
        }

        self.enqueue_obsolete_tables(obsolete_files);
        self.try_cleanup_obsolete_tables();

        self.inner
            .metrics
            .record_compaction_with_duration(compaction_started_at.elapsed());
        info!(
            event = "compaction.end",
            reason,
            input_l0_tables,
            input_l1_tables,
            output_tables = output_table_ids.len(),
            output_entries,
            ?output_table_ids,
            elapsed_ms = compaction_started_at.elapsed().as_millis(),
            "compaction completed"
        );
        self.maybe_emit_metrics_snapshot("compaction");
        Ok(())
    }

    fn capture_compaction_plan(
        &self,
        job: CompactionJob,
        write_state: &mut WriteState,
        read_state: &ReadState,
    ) -> Result<Option<CompactionPlan>> {
        if read_state.table_cache.l0.len() < self.inner.l0_compaction_trigger.max(1) {
            return Ok(None);
        }

        let Some(selection) = CompactionSelection::from_cache(&read_state.table_cache)? else {
            return Ok(None);
        };

        let reserved_table_id = write_state.manifest.next_table_id;
        write_state.manifest.next_table_id = write_state
            .manifest
            .next_table_id
            .checked_add(1)
            .ok_or_else(|| invalid_data("SSTable table-id overflow"))?;

        let input_l0_table_ids = selection
            .selected_l0
            .iter()
            .map(|meta| meta.table_id)
            .collect::<Vec<_>>();
        let input_l1_table_ids = selection
            .selected_l1
            .iter()
            .map(|meta| meta.table_id)
            .collect::<Vec<_>>();
        let l1_metas = read_state
            .table_cache
            .l1
            .iter()
            .map(|table| table.meta.clone())
            .collect::<Vec<_>>();
        let removed_tables = collect_removed_tables(&selection);
        let removed_ids = removed_tables
            .iter()
            .map(|meta| meta.table_id)
            .collect::<HashSet<_>>();
        let drop_tombstones = can_drop_compaction_tombstones_from_l1(&l1_metas, &selection);
        let sources = build_compaction_sources(&read_state.table_cache, &selection)?;

        Ok(Some(CompactionPlan {
            reason: job.reason,
            reserved_table_id,
            db_dir: write_state.db_dir.clone(),
            settings: sstable_write_settings(&self.inner),
            selection,
            removed_tables,
            removed_ids,
            input_l0_table_ids,
            input_l1_table_ids,
            drop_tombstones,
            sources,
        }))
    }

    fn stream_compaction_output(&self, plan: &CompactionPlan) -> Result<CompactionOutput> {
        #[cfg(not(test))]
        let _ = &self.inner;

        let source_cursors = build_compaction_sources_from_snapshot(&plan.sources)?;
        let mut merge_iter = MergeIterator::new(MergeMode::Compaction, source_cursors);

        let output_path = sstable_path(&plan.db_dir, plan.reserved_table_id, 1);
        let stream_result: Result<(usize, Option<TableHandle>)> = (|| {
            let mut writer = None;
            let mut output_entries = 0_usize;

            while let Some(entry) = merge_iter.next_entry()? {
                if plan.drop_tombstones && matches!(entry.value, ValueEntry::Tombstone) {
                    continue;
                }

                if writer.is_none() {
                    writer = Some(create_sstable_writer(
                        &output_path,
                        plan.reserved_table_id,
                        1,
                        &plan.settings,
                    )?);
                }
                if let Some(active_writer) = writer.as_mut() {
                    active_writer.add_entry(&entry)?;
                }
                output_entries = output_entries
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("compaction output entry count overflow"))?;
                #[cfg(test)]
                self.maybe_inject_compaction_stream_failure(output_entries)?;
            }

            let new_l1_table = if let Some(active_writer) = writer {
                let meta = active_writer.finish()?;
                let reader = SsTableReader::open(
                    &output_path,
                    meta.clone(),
                    plan.settings.enable_mmap_reads,
                    plan.settings.block_cache.clone(),
                )?;
                Some(TableHandle {
                    meta,
                    reader: Arc::new(reader),
                })
            } else {
                None
            };

            Ok((output_entries, new_l1_table))
        })();
        let (output_entries, new_l1_table) = match stream_result {
            Ok(ok) => ok,
            Err(err) => {
                cleanup_partial_compaction_output(&output_path);
                return Err(err);
            }
        };
        let output_table_ids = new_l1_table
            .as_ref()
            .map(|table| vec![table.meta.table_id])
            .unwrap_or_default();

        Ok(CompactionOutput {
            new_l1_table,
            output_entries,
            output_table_ids,
        })
    }

    fn apply_compaction_commit_locked(
        write_state: &mut WriteState,
        read_state: &mut ReadState,
        plan: &CompactionPlan,
        new_l1_table: Option<TableHandle>,
    ) -> Result<()> {
        apply_compaction_manifest_changes(
            &mut write_state.manifest,
            &plan.removed_ids,
            &plan.removed_tables,
            new_l1_table.as_ref(),
        );

        ManifestStore::commit(&write_state.db_dir, &write_state.manifest)?;

        read_state.table_cache.remove_tables(&plan.removed_ids);
        if let Some(table) = new_l1_table {
            read_state.table_cache.insert(table);
        }
        read_state.manifest_snapshot = write_state.manifest.clone();

        Ok(())
    }

    fn maybe_emit_metrics_snapshot(&self, reason: &'static str) {
        let Some(interval) = self.inner.metrics_log_interval else {
            return;
        };

        let mut last_logged = self.inner.last_metrics_log_at.lock();
        let should_emit = last_logged
            .as_ref()
            .is_none_or(|last| last.elapsed() >= interval);
        if !should_emit {
            return;
        }

        *last_logged = Some(Instant::now());
        drop(last_logged);
        let snapshot = self.inner.metrics.snapshot();
        info!(
            event = "metrics.snapshot",
            reason,
            wal_appends_total = snapshot.wal_appends_total,
            wal_sync_total = snapshot.wal_sync_total,
            flush_jobs_total = snapshot.flush_jobs_total,
            compactions_total = snapshot.compactions_total,
            bytes_written_total = snapshot.bytes_written_total,
            bytes_read_total = snapshot.bytes_read_total,
            tables_touched_total = snapshot.tables_touched_total,
            block_cache_hits_total = snapshot.block_cache_hits_total,
            block_cache_misses_total = snapshot.block_cache_misses_total,
            block_cache_evictions_total = snapshot.block_cache_evictions_total,
            block_cache_bytes_current = snapshot.block_cache_bytes_current,
            "metrics snapshot"
        );
    }

    fn record_background_error(&self, message: String) {
        let mut slot = self.inner.background_error.lock();
        if slot.is_none() {
            *slot = Some(message);
        }
    }

    #[cfg(test)]
    fn set_compaction_test_fail_after_output_entries(&self, fail_after: Option<usize>) {
        let mut slot = self.inner.compaction_test_fail_after_output_entries.lock();
        *slot = fail_after;
    }

    #[cfg(test)]
    fn maybe_inject_compaction_stream_failure(&self, output_entries: usize) -> Result<()> {
        let fail_after = *self.inner.compaction_test_fail_after_output_entries.lock();
        if fail_after.is_some_and(|threshold| output_entries >= threshold) {
            return Err(invalid_data("injected compaction stream failure").into());
        }
        Ok(())
    }

    fn ensure_background_workers_healthy(&self) -> Result<()> {
        let maybe_error = self.inner.background_error.lock().clone();
        if let Some(message) = maybe_error {
            return Err(std::io::Error::other(message).into());
        }
        Ok(())
    }

    fn register_snapshot(&self, visible_seq: u64) -> Result<SnapshotHandle> {
        let mut registry = self.inner.snapshot_registry.lock();

        if registry.active.len() >= self.inner.max_open_snapshots {
            return Err(invalid_input("max_open_snapshots exceeded").into());
        }

        let snapshot_id = registry.next_id;
        registry.next_id = registry
            .next_id
            .checked_add(1)
            .ok_or_else(|| invalid_data("snapshot id overflow"))?;
        registry
            .active
            .insert(snapshot_id, SnapshotMeta { visible_seq });
        drop(registry);

        Ok(SnapshotHandle {
            snapshot_id,
            engine: Arc::downgrade(&self.inner),
        })
    }

    fn unregister_snapshot(&self, snapshot_id: u64) {
        let mut registry = self.inner.snapshot_registry.lock();
        let _ = registry.active.remove(&snapshot_id);
    }

    fn enqueue_obsolete_tables(&self, files: Vec<ObsoleteTableFile>) {
        let mut queue = self.inner.obsolete_table_files.lock();
        queue.extend(files);
    }

    fn oldest_active_snapshot_seq(&self) -> Option<u64> {
        let registry = self.inner.snapshot_registry.lock();
        registry.active.values().map(|meta| meta.visible_seq).min()
    }

    fn try_cleanup_obsolete_tables(&self) {
        let oldest_snapshot_seq = self.oldest_active_snapshot_seq();

        let to_delete = {
            let mut queue = self.inner.obsolete_table_files.lock();
            let (deletable, keep): (Vec<_>, Vec<_>) = queue
                .drain(..)
                .partition(|file| oldest_snapshot_seq.is_none_or(|oldest| file.max_seq <= oldest));
            *queue = keep;
            deletable
        };

        let mut deleted_files = Vec::new();
        let mut failed_deletes = Vec::new();
        for file in to_delete {
            match fs::remove_file(&file.path) {
                Ok(()) => deleted_files.push(file),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => deleted_files.push(file),
                Err(err) => {
                    error!(
                        event = "deferred_delete.error",
                        table_id = file.table_id,
                        level = file.level,
                        error = %err,
                        "failed to delete obsolete SST file"
                    );
                    failed_deletes.push(file);
                }
            }
        }

        if !deleted_files.is_empty() {
            let mut write_state = self.inner.write_state.lock();
            let mut updated_manifest = write_state.manifest.clone();
            let mut changed = false;
            for file in &deleted_files {
                changed |= updated_manifest.dequeue_pending_delete(file.table_id, file.level);
            }

            if changed {
                match ManifestStore::commit(&write_state.db_dir, &updated_manifest) {
                    Ok(()) => {
                        write_state.manifest = updated_manifest.clone();
                        drop(write_state);
                        let mut read_state = self.inner.read_state.write();
                        read_state.manifest_snapshot = updated_manifest;
                    }
                    Err(err) => {
                        error!(
                            event = "deferred_delete.manifest_commit_error",
                            error = %err,
                            "failed to persist pending-delete cleanup to manifest"
                        );
                        failed_deletes.extend(deleted_files);
                    }
                }
            }
        }

        if !failed_deletes.is_empty() {
            let mut queue = self.inner.obsolete_table_files.lock();
            queue.extend(failed_deletes);
        }
    }
}

impl KvStore for AetherEngine {
    fn put(&self, key: Key, value: Value) -> Result<()> {
        self.ensure_background_workers_healthy()?;
        self.apply_write(key, ValueEntry::Put(value))
    }

    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.ensure_background_workers_healthy()?;
        self.inner.metrics.add_bytes_read(key.len());
        let view = self.build_point_read_view();

        let (result, tables_touched) = if let Some(entry) = view.mutable.get(key) {
            (decode_entry(entry), 0)
        } else if let Some(immutable) = view.immutable.as_ref()
            && let Some(entry) = immutable.get(key)
        {
            (decode_entry(entry), 0)
        } else {
            lookup_sstables_in_view_at_sequence(
                key,
                u64::MAX,
                &view.l0_tables,
                &view.l1_tables,
                view.l1_non_overlapping,
            )?
        };

        self.inner.metrics.add_tables_touched(tables_touched);
        self.maybe_emit_metrics_snapshot("read");
        Ok(result)
    }

    fn delete(&self, key: Key) -> Result<()> {
        self.ensure_background_workers_healthy()?;
        self.apply_write(key, ValueEntry::Tombstone)
    }

    fn flush(&self) -> Result<()> {
        self.ensure_background_workers_healthy()?;
        let mut read_state = self.inner.read_state.write();

        if read_state.memtables.immutable().is_none() {
            let _ = read_state.memtables.freeze_mutable();
        }

        let Some(immutable) = read_state.memtables.take_immutable() else {
            return Ok(());
        };
        drop(read_state);

        let (done_tx, done_rx) = sync_channel(1);
        let generation = self.inner.write_state.lock().manifest.wal_generation;

        self.inner.background.enqueue_flush(FlushJob {
            generation,
            memtable: immutable,
            completion: Some(done_tx),
        })?;

        let result = done_rx
            .recv()
            .unwrap_or_else(|_| Err(invalid_data("flush worker stopped before completion").into()));
        if result.is_ok() {
            self.maybe_emit_metrics_snapshot("manual_flush");
        }
        result
    }

    fn scan(&self, options: ScanOptions) -> Result<Vec<ScanItem>> {
        self.ensure_background_workers_healthy()?;
        self.scan_at_sequence(options, self.current_visible_sequence_number())
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.ensure_background_workers_healthy()?;

        if batch.ops.len() > self.inner.max_write_batch_ops {
            return Err(invalid_input("write batch exceeds max_write_batch_ops").into());
        }

        let estimated_bytes = batch.approx_bytes();
        if estimated_bytes > self.inner.max_write_batch_bytes {
            return Err(invalid_input("write batch exceeds max_write_batch_bytes").into());
        }

        let canonical_batch = batch.canonicalize_last_write_wins();
        self.apply_batch(canonical_batch)
    }
}

impl BackgroundTaskChannels {
    fn new(capacity: usize) -> (Self, Receiver<FlushJob>, Receiver<CompactionJob>) {
        let (flush_tx, flush_rx) = sync_channel(capacity);
        let (compaction_tx, compaction_rx) = sync_channel(capacity);
        (
            Self {
                flush_tx,
                compaction_tx,
            },
            flush_rx,
            compaction_rx,
        )
    }

    fn enqueue_flush(&self, job: FlushJob) -> Result<()> {
        self.flush_tx
            .send(job)
            .map_err(|_| invalid_data("flush worker is not running").into())
    }

    fn enqueue_compaction(&self, job: CompactionJob) {
        match self.compaction_tx.try_send(job) {
            Ok(()) | Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {}
        }
    }
}

impl TableCache {
    fn from_tables(tables: Vec<TableHandle>) -> Self {
        let mut cache = Self::default();
        for table in tables {
            cache.insert(table);
        }
        cache.refresh_l1_non_overlapping();
        cache
    }

    fn insert(&mut self, table: TableHandle) {
        if table.meta.level == 0 {
            self.l0.push(table);
            self.l0
                .sort_by(|left, right| right.meta.table_id.cmp(&left.meta.table_id));
            return;
        }

        self.l1.push(table);
        self.l1.sort_by(|left, right| {
            left.meta
                .min_key
                .as_ref()
                .cmp(right.meta.min_key.as_ref())
                .then(left.meta.table_id.cmp(&right.meta.table_id))
        });
        self.refresh_l1_non_overlapping();
    }

    fn remove_tables(&mut self, table_ids: &HashSet<u64>) {
        self.l0
            .retain(|table| !table_ids.contains(&table.meta.table_id));
        self.l1
            .retain(|table| !table_ids.contains(&table.meta.table_id));
        self.refresh_l1_non_overlapping();
    }

    fn refresh_l1_non_overlapping(&mut self) {
        self.l1_non_overlapping = l1_ranges_non_overlapping_table_handles(&self.l1);
    }
}

impl CompactionSelection {
    fn from_cache(cache: &TableCache) -> Result<Option<Self>> {
        let selected_l0 = cache
            .l0
            .iter()
            .map(|table| table.meta.clone())
            .collect::<Vec<_>>();
        if selected_l0.is_empty() {
            return Ok(None);
        }

        let min_key = selected_l0
            .iter()
            .map(|meta| meta.min_key.clone())
            .min_by(|left, right| left.as_ref().cmp(right.as_ref()))
            .ok_or_else(|| invalid_data("compaction missing L0 min key"))?;
        let max_key = selected_l0
            .iter()
            .map(|meta| meta.max_key.clone())
            .max_by(|left, right| left.as_ref().cmp(right.as_ref()))
            .ok_or_else(|| invalid_data("compaction missing L0 max key"))?;

        let selected_l1 = cache
            .l1
            .iter()
            .filter(|table| {
                !(table.meta.max_key.as_ref() < min_key.as_ref()
                    || table.meta.min_key.as_ref() > max_key.as_ref())
            })
            .map(|table| table.meta.clone())
            .collect::<Vec<_>>();

        Ok(Some(Self {
            selected_l0,
            selected_l1,
            min_key,
            max_key,
        }))
    }
}

fn build_compaction_sources(
    table_cache: &TableCache,
    selection: &CompactionSelection,
) -> Result<Vec<TableSource>> {
    let selected_l0_ids = selection
        .selected_l0
        .iter()
        .map(|meta| meta.table_id)
        .collect::<HashSet<_>>();
    let selected_l1_ids = selection
        .selected_l1
        .iter()
        .map(|meta| meta.table_id)
        .collect::<HashSet<_>>();

    let mut sources = Vec::new();

    for (index, table) in table_cache.l0.iter().enumerate() {
        if !selected_l0_ids.contains(&table.meta.table_id) {
            continue;
        }

        let offset = u32::try_from(index)
            .map_err(|_| invalid_data("L0 table count exceeds source-rank range"))?;
        let source_rank = SOURCE_RANK_L0_BASE
            .checked_add(offset)
            .ok_or_else(|| invalid_data("L0 source-rank overflow"))?;
        sources.push(TableSource {
            reader: Arc::clone(&table.reader),
            source_rank,
            min_key: table.meta.min_key.clone(),
            max_key: table.meta.max_key.clone(),
        });
    }

    for (index, table) in table_cache.l1.iter().enumerate() {
        if !selected_l1_ids.contains(&table.meta.table_id) {
            continue;
        }

        let offset = u32::try_from(index)
            .map_err(|_| invalid_data("L1 table count exceeds source-rank range"))?;
        let source_rank = SOURCE_RANK_L1_BASE
            .checked_add(offset)
            .ok_or_else(|| invalid_data("L1 source-rank overflow"))?;
        sources.push(TableSource {
            reader: Arc::clone(&table.reader),
            source_rank,
            min_key: table.meta.min_key.clone(),
            max_key: table.meta.max_key.clone(),
        });
    }

    Ok(sources)
}

fn build_compaction_sources_from_snapshot(sources: &[TableSource]) -> Result<Vec<SourceCursor>> {
    let mut cursors = Vec::with_capacity(sources.len());
    for source in sources {
        let iter = source.reader.iter_all()?;
        cursors.push(SourceCursor::from_sstable_iter(iter, source.source_rank));
    }
    Ok(cursors)
}

fn can_drop_compaction_tombstones_from_l1(
    l1_metas: &[SsTableMeta],
    selection: &CompactionSelection,
) -> bool {
    if !l1_ranges_non_overlapping(l1_metas) {
        return false;
    }

    let overlapping_l1_ids = l1_metas
        .iter()
        .filter(|meta| {
            table_key_ranges_overlap(meta, selection.min_key.as_ref(), selection.max_key.as_ref())
        })
        .map(|meta| meta.table_id)
        .collect::<HashSet<_>>();
    let selected_l1_ids = selection
        .selected_l1
        .iter()
        .map(|meta| meta.table_id)
        .collect::<HashSet<_>>();

    overlapping_l1_ids == selected_l1_ids
}

fn l1_ranges_non_overlapping(l1_metas: &[SsTableMeta]) -> bool {
    l1_metas.windows(2).all(|pair| {
        let left = &pair[0];
        let right = &pair[1];
        left.max_key.as_ref() < right.min_key.as_ref()
    })
}

fn l1_ranges_non_overlapping_table_handles(l1_tables: &[TableHandle]) -> bool {
    l1_tables.windows(2).all(|pair| {
        let left = &pair[0];
        let right = &pair[1];
        left.meta.max_key.as_ref() < right.meta.min_key.as_ref()
    })
}

fn table_key_ranges_overlap(meta: &SsTableMeta, min_key: &[u8], max_key: &[u8]) -> bool {
    !(meta.max_key.as_ref() < min_key || meta.min_key.as_ref() > max_key)
}

fn load_initial_tables(
    db_dir: &Path,
    manifest: &mut ManifestState,
    enable_mmap_reads: bool,
    block_cache: Option<&Arc<BlockCache>>,
) -> Result<Vec<TableHandle>> {
    if manifest.levels.is_empty() {
        let discovered = discover_sstables(db_dir, enable_mmap_reads, block_cache)?;
        if discovered.is_empty() {
            Ok(Vec::new())
        } else {
            manifest.levels = group_sstables_by_level(
                discovered
                    .iter()
                    .map(|table| table.meta.clone())
                    .collect::<Vec<_>>(),
            );
            Ok(discovered)
        }
    } else {
        load_sstables_from_manifest(db_dir, manifest, enable_mmap_reads, block_cache)
    }
}

fn discover_sstables(
    db_dir: &Path,
    enable_mmap_reads: bool,
    block_cache: Option<&Arc<BlockCache>>,
) -> Result<Vec<TableHandle>> {
    let mut discovered = Vec::new();
    for entry in fs::read_dir(db_dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some((level, table_id)) = parse_sstable_filename(file_name) else {
            continue;
        };

        let path = entry.path();
        let meta = SsTableReader::load_meta(&path, table_id, level)?;
        let reader =
            SsTableReader::open(&path, meta.clone(), enable_mmap_reads, block_cache.cloned())?;
        discovered.push(TableHandle {
            meta,
            reader: Arc::new(reader),
        });
    }

    Ok(discovered)
}

fn load_sstables_from_manifest(
    db_dir: &Path,
    manifest: &ManifestState,
    enable_mmap_reads: bool,
    block_cache: Option<&Arc<BlockCache>>,
) -> Result<Vec<TableHandle>> {
    let mut loaded = Vec::new();
    for level in &manifest.levels {
        for meta in level {
            let path = sstable_path(db_dir, meta.table_id, meta.level);
            let reader =
                SsTableReader::open(&path, meta.clone(), enable_mmap_reads, block_cache.cloned())?;
            loaded.push(TableHandle {
                meta: meta.clone(),
                reader: Arc::new(reader),
            });
        }
    }

    Ok(loaded)
}

fn collect_removed_tables(selection: &CompactionSelection) -> Vec<SsTableMeta> {
    selection
        .selected_l0
        .iter()
        .chain(selection.selected_l1.iter())
        .cloned()
        .collect()
}

fn apply_compaction_manifest_changes(
    manifest: &mut ManifestState,
    removed_ids: &HashSet<u64>,
    removed_tables: &[SsTableMeta],
    new_l1_table: Option<&TableHandle>,
) {
    remove_tables_from_manifest(&mut manifest.levels, removed_ids);
    for meta in removed_tables {
        manifest.enqueue_pending_delete(PendingDeleteEntry {
            table_id: meta.table_id,
            level: meta.level,
            max_seq: meta.max_seq,
        });
    }
    if let Some(table) = new_l1_table {
        add_table_to_manifest_levels(&mut manifest.levels, table.meta.clone());
    }
}

fn obsolete_table_files_from_manifest(
    db_dir: &Path,
    manifest: &ManifestState,
) -> Vec<ObsoleteTableFile> {
    manifest
        .pending_deletes
        .iter()
        .map(|entry| obsolete_table_file(db_dir, entry.table_id, entry.level, entry.max_seq))
        .collect()
}

fn obsolete_table_files_from_metas(db_dir: &Path, metas: &[SsTableMeta]) -> Vec<ObsoleteTableFile> {
    metas
        .iter()
        .map(|meta| obsolete_table_file(db_dir, meta.table_id, meta.level, meta.max_seq))
        .collect()
}

fn obsolete_table_file(db_dir: &Path, table_id: u64, level: u8, max_seq: u64) -> ObsoleteTableFile {
    ObsoleteTableFile {
        table_id,
        level,
        path: sstable_path(db_dir, table_id, level),
        max_seq,
    }
}

fn cleanup_partial_compaction_output(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            error!(
                event = "compaction.cleanup_partial_output_error",
                path = %path.display(),
                error = %err,
                "failed to cleanup partial compaction output"
            );
        }
    }
}

fn parse_sstable_filename(file_name: &str) -> Option<(u8, u64)> {
    let stem = file_name.strip_suffix(&format!(".{SSTABLE_FILE_EXT}"))?;
    let (level_part, table_part) = stem.split_once("-t")?;
    let level_text = level_part.strip_prefix('l')?;
    let level = level_text.parse::<u8>().ok()?;
    let table_id = table_part.parse::<u64>().ok()?;
    Some((level, table_id))
}

fn table_overlaps_bounds(meta: &SsTableMeta, bounds: &ScanBounds) -> bool {
    use std::ops::Bound;

    let above_start = match &bounds.start {
        Bound::Included(start) => meta.max_key.as_ref() >= start.as_ref(),
        Bound::Excluded(start) => meta.max_key.as_ref() > start.as_ref(),
        Bound::Unbounded => true,
    };
    let below_end = match &bounds.end {
        Bound::Included(end) => meta.min_key.as_ref() <= end.as_ref(),
        Bound::Excluded(end) => meta.min_key.as_ref() < end.as_ref(),
        Bound::Unbounded => true,
    };

    above_start && below_end
}

fn decode_entry(entry: &InternalEntry) -> Option<Value> {
    match &entry.value {
        ValueEntry::Put(value) => Some(value.clone()),
        ValueEntry::Tombstone => None,
    }
}

fn point_bounds(key: &[u8]) -> ScanBounds {
    use std::ops::Bound;

    let key = Bytes::copy_from_slice(key);
    ScanBounds {
        start: Bound::Included(key.clone()),
        end: Bound::Included(key),
    }
}

fn lookup_sstables_in_view_at_sequence(
    key: &[u8],
    visible_seq: u64,
    l0_tables: &[TableHandle],
    l1_tables: &[TableHandle],
    l1_non_overlapping: bool,
) -> Result<(Option<Value>, usize)> {
    let mut tables_touched = 0_usize;
    let mut l0_hit: Option<Option<Value>> = None;

    if visible_seq == u64::MAX {
        for table in l0_tables {
            tables_touched = tables_touched.saturating_add(1);
            if let Some(entry) = table.reader.get(key)? {
                l0_hit = Some(decode_entry(&entry));
                break;
            }
        }
    } else {
        let bounds = point_bounds(key);
        for table in l0_tables {
            tables_touched = tables_touched.saturating_add(1);
            let mut iter = table.reader.iter_range(&bounds, visible_seq)?;
            if let Some(entry) = iter.next_entry()? {
                l0_hit = Some(decode_entry(&entry));
                break;
            }
        }
    }

    if let Some(value) = l0_hit {
        return Ok((value, tables_touched));
    }

    let mut best_l1_entry: Option<InternalEntry> = None;
    if l1_non_overlapping {
        if let Some(index) = find_l1_table_index_by_key(key, l1_tables) {
            let table = &l1_tables[index];
            tables_touched = tables_touched.saturating_add(1);
            if visible_seq == u64::MAX {
                best_l1_entry = table.reader.get(key)?;
            } else {
                let bounds = point_bounds(key);
                let mut iter = table.reader.iter_range(&bounds, visible_seq)?;
                best_l1_entry = iter.next_entry()?;
            }
        }
    } else if visible_seq == u64::MAX {
        for table in l1_tables {
            if key < table.meta.min_key.as_ref() || key > table.meta.max_key.as_ref() {
                continue;
            }

            tables_touched = tables_touched.saturating_add(1);
            if let Some(entry) = table.reader.get(key)? {
                match &best_l1_entry {
                    Some(current_best) if current_best.seq > entry.seq => {}
                    _ => best_l1_entry = Some(entry),
                }
            }
        }
    } else {
        let bounds = point_bounds(key);
        for table in l1_tables {
            if key < table.meta.min_key.as_ref() || key > table.meta.max_key.as_ref() {
                continue;
            }

            tables_touched = tables_touched.saturating_add(1);
            let mut iter = table.reader.iter_range(&bounds, visible_seq)?;
            if let Some(entry) = iter.next_entry()? {
                match &best_l1_entry {
                    Some(current_best) if current_best.seq > entry.seq => {}
                    _ => best_l1_entry = Some(entry),
                }
            }
        }
    }

    Ok((
        best_l1_entry.as_ref().and_then(decode_entry),
        tables_touched,
    ))
}

fn lookup_sstable_sources_at_sequence(
    key: &[u8],
    visible_seq: u64,
    l0_tables: &[TableSource],
    l1_tables: &[TableSource],
    l1_non_overlapping: bool,
) -> Result<(Option<Value>, usize)> {
    let mut tables_touched = 0_usize;
    let bounds = point_bounds(key);

    for table in l0_tables {
        tables_touched = tables_touched.saturating_add(1);
        let mut iter = table.reader.iter_range(&bounds, visible_seq)?;
        if let Some(entry) = iter.next_entry()? {
            return Ok((decode_entry(&entry), tables_touched));
        }
    }

    let mut best_l1_entry: Option<InternalEntry> = None;
    if l1_non_overlapping {
        if let Some(index) = find_l1_source_index_by_key(key, l1_tables) {
            let table = &l1_tables[index];
            tables_touched = tables_touched.saturating_add(1);
            let mut iter = table.reader.iter_range(&bounds, visible_seq)?;
            best_l1_entry = iter.next_entry()?;
        }
    } else {
        for table in l1_tables {
            tables_touched = tables_touched.saturating_add(1);
            let mut iter = table.reader.iter_range(&bounds, visible_seq)?;
            if let Some(entry) = iter.next_entry()? {
                match &best_l1_entry {
                    Some(current_best) if current_best.seq > entry.seq => {}
                    _ => best_l1_entry = Some(entry),
                }
            }
        }
    }

    Ok((
        best_l1_entry.as_ref().and_then(decode_entry),
        tables_touched,
    ))
}

fn find_l1_table_index_by_key(key: &[u8], l1_tables: &[TableHandle]) -> Option<usize> {
    let upper_bound = l1_tables.partition_point(|table| table.meta.min_key.as_ref() <= key);
    upper_bound
        .checked_sub(1)
        .filter(|index| key <= l1_tables[*index].meta.max_key.as_ref())
}

fn find_l1_source_index_by_key(key: &[u8], l1_tables: &[TableSource]) -> Option<usize> {
    let upper_bound = l1_tables.partition_point(|table| table.min_key.as_ref() <= key);
    upper_bound
        .checked_sub(1)
        .filter(|index| key <= l1_tables[*index].max_key.as_ref())
}

fn has_invalid_scan_bounds(bounds: &ScanBounds) -> bool {
    use std::cmp::Ordering;
    use std::ops::Bound;

    let (
        Bound::Included(start) | Bound::Excluded(start),
        Bound::Included(end) | Bound::Excluded(end),
    ) = (&bounds.start, &bounds.end)
    else {
        return false;
    };

    match start.as_ref().cmp(end.as_ref()) {
        Ordering::Greater => true,
        Ordering::Equal => matches!(
            (&bounds.start, &bounds.end),
            (Bound::Excluded(_), Bound::Excluded(_) | Bound::Included(_))
        ),
        Ordering::Less => false,
    }
}

fn sstable_path(db_dir: &Path, table_id: u64, level: u8) -> PathBuf {
    db_dir.join(format!("l{level}-t{table_id}.{SSTABLE_FILE_EXT}"))
}

fn wal_path_for_generation(db_dir: &Path, wal_generation: u64) -> PathBuf {
    db_dir.join(format!("wal-{wal_generation}.log"))
}

fn build_block_cache(capacity_bytes: usize, metrics: &MetricsHandle) -> Option<Arc<BlockCache>> {
    if capacity_bytes == 0 {
        None
    } else {
        Some(Arc::new(BlockCache::new(capacity_bytes, metrics.clone())))
    }
}

fn sstable_write_settings(inner: &Inner) -> SstableWriteSettings {
    SstableWriteSettings {
        block_target_bytes: inner.sst_target_block_bytes,
        compression_codec: inner.compression_codec,
        prefix_restart_interval: inner.prefix_restart_interval,
        min_compress_size_bytes: inner.min_compress_size_bytes,
        enable_mmap_reads: inner.enable_mmap_reads,
        block_cache: inner.block_cache.clone(),
    }
}

fn flush_memtable_to_sstable(
    db_dir: &Path,
    mutable: &MemTable,
    table_id: u64,
    settings: &SstableWriteSettings,
) -> Result<TableHandle> {
    let entries = mutable.sorted_entries();
    if entries.is_empty() {
        return Err(invalid_data("cannot flush empty memtable").into());
    }

    write_entries_to_sstable(db_dir, &entries, table_id, 0, settings)
}

fn write_entries_to_sstable(
    db_dir: &Path,
    entries: &[InternalEntry],
    table_id: u64,
    level: u8,
    settings: &SstableWriteSettings,
) -> Result<TableHandle> {
    if entries.is_empty() {
        return Err(invalid_data("cannot flush empty entry set").into());
    }

    let path = sstable_path(db_dir, table_id, level);
    let mut writer = create_sstable_writer(&path, table_id, level, settings)?;
    for entry in entries {
        writer.add_entry(entry)?;
    }
    let meta = writer.finish()?;
    let reader = SsTableReader::open(
        &path,
        meta.clone(),
        settings.enable_mmap_reads,
        settings.block_cache.clone(),
    )?;

    Ok(TableHandle {
        meta,
        reader: Arc::new(reader),
    })
}

fn create_sstable_writer(
    path: &Path,
    table_id: u64,
    level: u8,
    settings: &SstableWriteSettings,
) -> Result<SsTableWriter> {
    SsTableWriter::create_with_block_target_and_options(
        path,
        table_id,
        level,
        settings.block_target_bytes,
        FooterMetadata {
            block_encoding_kind: BlockEncodingKind::Prefix,
            default_compression_codec: settings.compression_codec.into(),
            prefix_restart_interval: settings.prefix_restart_interval,
        },
        settings.min_compress_size_bytes,
    )
}

fn table_file_size_bytes(db_dir: &Path, table_id: u64, level: u8) -> Result<u64> {
    let path = sstable_path(db_dir, table_id, level);
    let file_size = fs::metadata(path)?.len();
    Ok(file_size)
}

fn remove_tables_from_manifest(levels: &mut Vec<Vec<SsTableMeta>>, table_ids: &HashSet<u64>) {
    for level in levels {
        level.retain(|meta| !table_ids.contains(&meta.table_id));
    }
}

fn add_table_to_manifest_levels(levels: &mut Vec<Vec<SsTableMeta>>, meta: SsTableMeta) {
    let level_index = usize::from(meta.level);
    if levels.len() <= level_index {
        levels.resize_with(level_index + 1, Vec::new);
    }
    levels[level_index].insert(0, meta);
}

fn group_sstables_by_level(metas: Vec<SsTableMeta>) -> Vec<Vec<SsTableMeta>> {
    let mut levels = Vec::new();
    for meta in metas {
        add_table_to_manifest_levels(&mut levels, meta);
    }
    levels
}

fn max_table_id(levels: &[Vec<SsTableMeta>]) -> Option<u64> {
    levels
        .iter()
        .flat_map(|level| level.iter().map(|meta| meta.table_id))
        .max()
}

const fn estimate_wal_record_bytes(key_len: usize, value: &ValueEntry) -> usize {
    const CRC_LEN: usize = size_of::<u32>();
    const RECORD_LEN_LEN: usize = size_of::<u32>();
    const SEQ_LEN: usize = size_of::<u64>();
    const OP_LEN: usize = size_of::<u8>();
    const FIELD_LEN_LEN: usize = size_of::<u32>();

    let value_len = match value {
        ValueEntry::Put(bytes) => bytes.len(),
        ValueEntry::Tombstone => 0,
    };

    CRC_LEN
        .saturating_add(RECORD_LEN_LEN)
        .saturating_add(SEQ_LEN)
        .saturating_add(OP_LEN)
        .saturating_add(FIELD_LEN_LEN)
        .saturating_add(key_len)
        .saturating_add(FIELD_LEN_LEN)
        .saturating_add(value_len)
}

fn estimate_wal_batch_record_bytes(batch: &WriteBatch) -> usize {
    const CRC_LEN: usize = size_of::<u32>();
    const RECORD_LEN_LEN: usize = size_of::<u32>();
    const SEQ_LEN: usize = size_of::<u64>();
    const OP_LEN: usize = size_of::<u8>();
    const BATCH_COUNT_LEN: usize = size_of::<u32>();
    const FIELD_LEN_LEN: usize = size_of::<u32>();

    let payload_len = batch.ops.iter().fold(
        SEQ_LEN
            .saturating_add(OP_LEN)
            .saturating_add(BATCH_COUNT_LEN),
        |acc, op| {
            let (key_len, value_len) = match op {
                BatchOp::Put { key, value } => (key.len(), value.len()),
                BatchOp::Delete { key } => (key.len(), 0),
            };
            acc.saturating_add(OP_LEN)
                .saturating_add(FIELD_LEN_LEN)
                .saturating_add(key_len)
                .saturating_add(FIELD_LEN_LEN)
                .saturating_add(value_len)
        },
    );

    CRC_LEN
        .saturating_add(RECORD_LEN_LEN)
        .saturating_add(payload_len)
}

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        ops::Bound,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use bytes::Bytes;

    use super::{AetherEngine, sstable_path};
    use crate::{
        config::EngineOptions,
        error::AetherError,
        manifest::{ManifestState, ManifestStore, PendingDeleteEntry},
        types::{BatchOp, ScanBounds, ScanOptions, WriteBatch},
    };

    fn temp_db_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("aether-{prefix}-{nanos}"))
    }

    fn wait_until<F>(timeout: Duration, mut predicate: F) -> bool
    where
        F: FnMut() -> bool,
    {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if predicate() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        predicate()
    }

    fn wait_for_path_exists(path: &std::path::Path) -> bool {
        wait_until(Duration::from_secs(2), || path.exists())
    }

    fn wait_for_path_missing(path: &std::path::Path) -> bool {
        wait_until(Duration::from_secs(2), || !path.exists())
    }

    fn cleanup_db_dir(db_dir: &std::path::Path) {
        let removed = wait_until(Duration::from_secs(2), || {
            match fs::remove_dir_all(db_dir) {
                Ok(()) => true,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
                Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => false,
                Err(_) => false,
            }
        });
        assert!(removed, "cleanup should succeed");
    }

    fn is_transient_reopen_error(err: &AetherError) -> bool {
        match err {
            AetherError::Io(io_err) => matches!(
                io_err.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ),
            _ => false,
        }
    }

    fn open_with_transient_retry(opts: &EngineOptions) -> AetherEngine {
        let reopen_started = Instant::now();
        loop {
            match AetherEngine::open(opts.clone()) {
                Ok(engine) => return engine,
                Err(err) => {
                    assert!(
                        is_transient_reopen_error(&err),
                        "reopen should not fail with non-transient error: {err}"
                    );
                    assert!(
                        reopen_started.elapsed() < Duration::from_secs(2),
                        "reopen should succeed after transient errors: {err}"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    fn assert_loaded_tables_use_mmap(engine: &AetherEngine) {
        let read_state = engine.inner.read_state.read();
        let loaded_tables = read_state.table_cache.l0.len() + read_state.table_cache.l1.len();
        assert!(
            loaded_tables > 0,
            "expected at least one loaded SSTable for mmap assertion"
        );
        for table in read_state
            .table_cache
            .l0
            .iter()
            .chain(read_state.table_cache.l1.iter())
        {
            assert!(
                table.reader.is_mmap_backend(),
                "expected table {} at L{} to use mmap backend",
                table.meta.table_id,
                table.meta.level
            );
        }
    }

    #[test]
    fn put_survives_restart_via_wal_replay() {
        let db_dir = temp_db_dir("wal-replay");

        {
            let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };
            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v")) {
                panic!("put should succeed: {err}");
            }
        }

        let reopened = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("reopen should succeed: {err}"),
        };

        let value = match reopened.get(b"k") {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(value, Some(Bytes::from_static(b"v")));

        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn flush_survives_restart_via_sstable() {
        let db_dir = temp_db_dir("flush-restart");

        {
            let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }
        }

        let reopened = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("reopen should succeed: {err}"),
        };

        let value = match reopened.get(b"k") {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(value, Some(Bytes::from_static(b"v")));

        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn flush_does_not_rotate_wal_generation() {
        let db_dir = temp_db_dir("flush-wal-generation");
        let wal0 = db_dir.join("wal-0.log");
        let wal1 = db_dir.join("wal-1.log");

        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        assert!(wal0.exists(), "wal-0.log should exist after flush");
        assert!(
            !wal1.exists(),
            "flush should not rotate WAL generations to wal-1.log"
        );

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn flush_failure_marks_engine_unhealthy() {
        let db_dir = temp_db_dir("flush-failure-health");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("remove dir should succeed: {err}");
        }

        let flush_result = engine.flush();
        assert!(
            flush_result.is_err(),
            "flush should fail when db dir is gone"
        );

        let put_after_failure = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"b"));
        assert!(
            put_after_failure.is_err(),
            "engine should reject writes after a background flush failure"
        );
    }

    #[test]
    fn scan_includes_last_table_blocks_when_end_bound_exceeds_table_max_key() {
        let db_dir = temp_db_dir("scan-end-bound-over-max");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.put(Bytes::from_static(b"c"), Bytes::from_static(b"3")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let items = match engine.scan(ScanOptions {
            bounds: ScanBounds {
                start: Bound::Unbounded,
                end: Bound::Included(Bytes::from_static(b"zz")),
            },
            limit: None,
        }) {
            Ok(items) => items,
            Err(err) => panic!("scan should succeed: {err}"),
        };
        let keys = items
            .iter()
            .map(|item| item.key.as_ref().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn scan_rejects_invalid_bounds() {
        let db_dir = temp_db_dir("scan-invalid-bounds");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let scan_result = engine.scan(ScanOptions {
            bounds: ScanBounds {
                start: Bound::Included(Bytes::from_static(b"z")),
                end: Bound::Excluded(Bytes::from_static(b"a")),
            },
            limit: None,
        });
        assert!(scan_result.is_err(), "scan should reject invalid bounds");

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn write_batch_applies_atomically() {
        let db_dir = temp_db_dir("write-batch-atomic");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let batch = WriteBatch {
            ops: vec![
                BatchOp::Put {
                    key: Bytes::from_static(b"a"),
                    value: Bytes::from_static(b"1"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"b"),
                    value: Bytes::from_static(b"2"),
                },
                BatchOp::Delete {
                    key: Bytes::from_static(b"a"),
                },
            ],
        };
        if let Err(err) = engine.write_batch(batch) {
            panic!("write_batch should succeed: {err}");
        }

        let value_a = match engine.get(b"a") {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(value_a, None);

        let value_b = match engine.get(b"b") {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(value_b, Some(Bytes::from_static(b"2")));

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    fn scan_keys(items: &[crate::types::ScanItem]) -> Vec<Vec<u8>> {
        items
            .iter()
            .map(|item| item.key.as_ref().to_vec())
            .collect()
    }

    fn scan_kv(items: &[crate::types::ScanItem]) -> Vec<(Vec<u8>, Vec<u8>)> {
        items
            .iter()
            .map(|item| (item.key.as_ref().to_vec(), item.value.as_ref().to_vec()))
            .collect()
    }

    fn unbounded_scan() -> ScanOptions {
        ScanOptions {
            bounds: ScanBounds {
                start: Bound::Unbounded,
                end: Bound::Unbounded,
            },
            limit: None,
        }
    }

    struct SnapshotReadState {
        scan: Vec<(Vec<u8>, Vec<u8>)>,
        get_a: Option<Bytes>,
        get_b: Option<Bytes>,
        get_c: Option<Bytes>,
    }

    fn capture_snapshot_state(snapshot: &crate::snapshot::Snapshot) -> SnapshotReadState {
        let scan_items = match snapshot.scan(unbounded_scan()) {
            Ok(items) => items,
            Err(err) => panic!("snapshot scan should succeed: {err}"),
        };
        let get_a = match snapshot.get(b"a") {
            Ok(value) => value,
            Err(err) => panic!("snapshot get(a) should succeed: {err}"),
        };
        let get_b = match snapshot.get(b"b") {
            Ok(value) => value,
            Err(err) => panic!("snapshot get(b) should succeed: {err}"),
        };
        let get_c = match snapshot.get(b"c") {
            Ok(value) => value,
            Err(err) => panic!("snapshot get(c) should succeed: {err}"),
        };
        SnapshotReadState {
            scan: scan_kv(&scan_items),
            get_a,
            get_b,
            get_c,
        }
    }

    fn assert_snapshot_state(
        snapshot: &crate::snapshot::Snapshot,
        expected_scan: &[(Vec<u8>, Vec<u8>)],
        context: &str,
    ) {
        let state = capture_snapshot_state(snapshot);
        assert_eq!(
            state.scan, expected_scan,
            "{context}: snapshot scan mismatch"
        );
        assert_eq!(
            state.get_a,
            Some(Bytes::from_static(b"1")),
            "{context}: snapshot get(a) must remain pinned to pre-update value",
        );
        assert_eq!(
            state.get_b,
            Some(Bytes::from_static(b"2")),
            "{context}: snapshot get(b) must remain pinned",
        );
        assert_eq!(
            state.get_c, None,
            "{context}: snapshot get(c) must not observe post-snapshot writes",
        );
    }

    #[test]
    fn scan_unbounded_returns_all_live_keys_in_order() {
        let db_dir = temp_db_dir("scan-unbounded-all");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        for (key, value) in [
            (b"c", b"3"),
            (b"a", b"1"),
            (b"e", b"5"),
            (b"b", b"2"),
            (b"d", b"4"),
        ] {
            if let Err(err) = engine.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            {
                panic!("put should succeed: {err}");
            }
        }

        let items = match engine.scan(unbounded_scan()) {
            Ok(items) => items,
            Err(err) => panic!("scan should succeed: {err}"),
        };
        assert_eq!(
            scan_keys(&items),
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec()
            ]
        );

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn scan_included_bounds_filters_range() {
        let db_dir = temp_db_dir("scan-included-bounds");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        for (key, value) in [
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
            (b"e", b"5"),
        ] {
            if let Err(err) = engine.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            {
                panic!("put should succeed: {err}");
            }
        }

        let items = match engine.scan(ScanOptions {
            bounds: ScanBounds {
                start: Bound::Included(Bytes::from_static(b"b")),
                end: Bound::Included(Bytes::from_static(b"d")),
            },
            limit: None,
        }) {
            Ok(items) => items,
            Err(err) => panic!("scan should succeed: {err}"),
        };
        assert_eq!(
            scan_keys(&items),
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn scan_excluded_bounds_filters_range() {
        let db_dir = temp_db_dir("scan-excluded-bounds");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        for (key, value) in [
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
            (b"e", b"5"),
        ] {
            if let Err(err) = engine.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            {
                panic!("put should succeed: {err}");
            }
        }

        let items = match engine.scan(ScanOptions {
            bounds: ScanBounds {
                start: Bound::Excluded(Bytes::from_static(b"b")),
                end: Bound::Excluded(Bytes::from_static(b"e")),
            },
            limit: None,
        }) {
            Ok(items) => items,
            Err(err) => panic!("scan should succeed: {err}"),
        };
        assert_eq!(scan_keys(&items), vec![b"c".to_vec(), b"d".to_vec()]);

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn scan_respects_limit() {
        let db_dir = temp_db_dir("scan-limit");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        for (key, value) in [
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
            (b"e", b"5"),
        ] {
            if let Err(err) = engine.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            {
                panic!("put should succeed: {err}");
            }
        }

        let items = match engine.scan(ScanOptions {
            bounds: ScanBounds {
                start: Bound::Unbounded,
                end: Bound::Unbounded,
            },
            limit: Some(2),
        }) {
            Ok(items) => items,
            Err(err) => panic!("scan should succeed: {err}"),
        };
        assert_eq!(scan_keys(&items), vec![b"a".to_vec(), b"b".to_vec()]);

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn scan_zero_limit_returns_empty() {
        let db_dir = temp_db_dir("scan-zero-limit");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
            panic!("put should succeed: {err}");
        }

        let items = match engine.scan(ScanOptions {
            bounds: ScanBounds {
                start: Bound::Unbounded,
                end: Bound::Unbounded,
            },
            limit: Some(0),
        }) {
            Ok(items) => items,
            Err(err) => panic!("scan should succeed: {err}"),
        };
        assert!(items.is_empty(), "scan with limit=0 should return empty");

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn scan_mutable_overrides_flushed_l0() {
        let db_dir = temp_db_dir("scan-mutable-over-l0");

        let items = {
            let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"old")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"new")) {
                panic!("put should succeed: {err}");
            }

            match engine.scan(unbounded_scan()) {
                Ok(items) => items,
                Err(err) => panic!("scan should succeed: {err}"),
            }
        };

        assert_eq!(scan_kv(&items), vec![(b"k".to_vec(), b"new".to_vec())]);

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn scan_tombstone_hides_flushed_key() {
        let db_dir = temp_db_dir("scan-tombstone");

        let items = {
            let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            if let Err(err) = engine.delete(Bytes::from_static(b"a")) {
                panic!("delete should succeed: {err}");
            }

            match engine.scan(unbounded_scan()) {
                Ok(items) => items,
                Err(err) => panic!("scan should succeed: {err}"),
            }
        };

        assert_eq!(scan_kv(&items), vec![(b"b".to_vec(), b"2".to_vec())]);

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn scan_across_l0_and_l1_after_compaction() {
        let db_dir = temp_db_dir("scan-l0-l1-compaction");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.l0_compaction_trigger = 2;

        {
            let engine = match AetherEngine::open(opts.clone()) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            if let Err(err) = engine.put(Bytes::from_static(b"c"), Bytes::from_static(b"3")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.put(Bytes::from_static(b"d"), Bytes::from_static(b"4")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            // Allow background compaction to settle before drop.
            std::thread::sleep(Duration::from_millis(200));
        }

        // Reopen to get a deterministic view after compaction (L1).
        let items = {
            let engine = match AetherEngine::open(opts) {
                Ok(engine) => engine,
                Err(err) => panic!("reopen should succeed: {err}"),
            };

            // Add a fresh L0 after compaction.
            if let Err(err) = engine.put(Bytes::from_static(b"e"), Bytes::from_static(b"5")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            match engine.scan(unbounded_scan()) {
                Ok(items) => items,
                Err(err) => panic!("scan should succeed: {err}"),
            }
        };

        assert_eq!(
            scan_kv(&items),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
                (b"e".to_vec(), b"5".to_vec()),
            ]
        );

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn scan_precedence_mutable_over_l0_over_l1() {
        let db_dir = temp_db_dir("scan-precedence");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.l0_compaction_trigger = 2;

        // Phase 1: create L1 data via compaction.
        {
            let engine = match AetherEngine::open(opts.clone()) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v1")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            // Second L0 flush triggers compaction (trigger=2).
            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v2")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            std::thread::sleep(Duration::from_millis(200));
        }

        // Phase 2: reopen, add L0 + mutable layers.
        let (items, get_value) = {
            let engine = match AetherEngine::open(opts) {
                Ok(engine) => engine,
                Err(err) => panic!("reopen should succeed: {err}"),
            };

            // Write into L0.
            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v3")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            // Write into mutable memtable.
            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v4")) {
                panic!("put should succeed: {err}");
            }

            let scan_items = match engine.scan(unbounded_scan()) {
                Ok(items) => items,
                Err(err) => panic!("scan should succeed: {err}"),
            };

            // Consistency check: point-get should agree.
            let value = match engine.get(b"k") {
                Ok(value) => value,
                Err(err) => panic!("get should succeed: {err}"),
            };

            (scan_items, value)
        };

        assert_eq!(
            scan_kv(&items),
            vec![(b"k".to_vec(), b"v4".to_vec())],
            "mutable memtable value should win over L0 and L1"
        );
        assert_eq!(get_value, Some(Bytes::from_static(b"v4")));

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn open_snapshot_enforces_max_open_snapshots() {
        let db_dir = temp_db_dir("snapshot-cap");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.max_open_snapshots = 1;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let snapshot = match engine.open_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => panic!("first open_snapshot should succeed: {err}"),
        };

        let second = engine.open_snapshot();
        assert!(
            second.is_err(),
            "second open_snapshot should fail when cap is reached"
        );

        drop(snapshot);

        let reopened = engine.open_snapshot();
        assert!(
            reopened.is_ok(),
            "open_snapshot should succeed after previous snapshot drops"
        );

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn snapshot_clone_keeps_registry_slot_until_last_drop() {
        let db_dir = temp_db_dir("snapshot-clone-lifecycle");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.max_open_snapshots = 1;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let snapshot = match engine.open_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => panic!("open_snapshot should succeed: {err}"),
        };
        let snapshot_clone = snapshot.clone();

        drop(snapshot);

        let still_blocked = engine.open_snapshot();
        assert!(
            still_blocked.is_err(),
            "snapshot clone should keep registry slot active"
        );

        drop(snapshot_clone);

        let now_open = engine.open_snapshot();
        assert!(
            now_open.is_ok(),
            "open_snapshot should succeed after last clone drops"
        );

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn snapshot_read_view_stays_stable_across_write_flush_and_compaction() {
        let db_dir = temp_db_dir("snapshot-stable-across-compaction");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.l0_compaction_trigger = 2;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let snapshot = match engine.open_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => panic!("open_snapshot should succeed: {err}"),
        };

        let expected_scan = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        assert_snapshot_state(
            &snapshot,
            &expected_scan,
            "baseline before later writes/flush/compaction",
        );

        if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"9")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.put(Bytes::from_static(b"c"), Bytes::from_static(b"3")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let l1_t2 = sstable_path(&db_dir, 2, 1);
        assert!(
            wait_for_path_exists(&l1_t2),
            "compaction output should appear after second flush"
        );

        assert_snapshot_state(
            &snapshot,
            &expected_scan,
            "after writes + flush + compaction",
        );
        assert_eq!(
            expected_scan,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ],
            "test invariant: expected snapshot scan should be pre-update view",
        );

        drop(snapshot);
        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn compaction_keeps_obsolete_files_while_snapshot_is_open() {
        let db_dir = temp_db_dir("deferred-delete-open-snapshot");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.l0_compaction_trigger = 2;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let snapshot = match engine.open_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => panic!("open_snapshot should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let l0_t0 = sstable_path(&db_dir, 0, 0);
        let l0_t1 = sstable_path(&db_dir, 1, 0);
        let l1_t2 = sstable_path(&db_dir, 2, 1);

        assert!(
            wait_for_path_exists(&l1_t2),
            "compaction output should appear"
        );
        assert!(
            l0_t1.exists(),
            "newer obsolete file should be retained while snapshot is open"
        );

        drop(snapshot);

        assert!(
            wait_for_path_missing(&l0_t0),
            "older obsolete file should be eventually removed after snapshot release"
        );
        assert!(
            wait_for_path_missing(&l0_t1),
            "newer obsolete file should be removed after snapshot release"
        );

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn compaction_without_snapshots_deletes_obsolete_files() {
        let db_dir = temp_db_dir("deferred-delete-no-snapshot");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.l0_compaction_trigger = 2;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }
        if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let l0_t0 = sstable_path(&db_dir, 0, 0);
        let l0_t1 = sstable_path(&db_dir, 1, 0);
        let l1_t2 = sstable_path(&db_dir, 2, 1);

        assert!(
            wait_for_path_exists(&l1_t2),
            "compaction output should appear"
        );
        assert!(
            wait_for_path_missing(&l0_t0),
            "obsolete L0 table 0 should be deleted without snapshots"
        );
        assert!(
            wait_for_path_missing(&l0_t1),
            "obsolete L0 table 1 should be deleted without snapshots"
        );

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn compaction_stream_failure_cleans_partial_output_file() {
        let db_dir = temp_db_dir("compaction-stream-failure-cleanup");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.l0_compaction_trigger = 2;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };
        engine.set_compaction_test_fail_after_output_entries(Some(1));

        if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }
        if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let l1_t2 = sstable_path(&db_dir, 2, 1);
        assert!(
            wait_until(Duration::from_secs(2), || {
                engine.inner.write_state.lock().manifest.next_table_id >= 3
            }),
            "compaction should reserve an output table id"
        );
        assert!(
            wait_for_path_missing(&l1_t2),
            "partial compaction output SST should be cleaned on stream failure"
        );

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn oldest_snapshot_controls_deferred_cleanup_threshold() {
        let db_dir = temp_db_dir("deferred-delete-multi-snapshot");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.l0_compaction_trigger = 3;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v0")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }
        let older_snapshot = match engine.open_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => panic!("older open_snapshot should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v1")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }
        let newer_snapshot = match engine.open_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => panic!("newer open_snapshot should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v2")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let l0_t0 = sstable_path(&db_dir, 0, 0);
        let l0_t1 = sstable_path(&db_dir, 1, 0);
        let l0_t2 = sstable_path(&db_dir, 2, 0);
        let l1_t3 = sstable_path(&db_dir, 3, 1);

        assert!(
            wait_for_path_exists(&l1_t3),
            "compaction output should appear"
        );
        assert!(
            l0_t2.exists(),
            "highest-seq obsolete file should remain while oldest snapshot is still open"
        );

        drop(older_snapshot);
        assert!(
            wait_for_path_missing(&l0_t1),
            "middle obsolete file should become deletable after oldest snapshot drops"
        );
        assert!(
            l0_t2.exists(),
            "newest obsolete file should still be retained while newer snapshot is open"
        );

        drop(newer_snapshot);
        assert!(
            wait_for_path_missing(&l0_t0),
            "oldest obsolete file should be gone after final snapshot drop"
        );
        assert!(
            wait_for_path_missing(&l0_t2),
            "newest obsolete file should be removed after final snapshot drop"
        );

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn deferred_cleanup_waits_for_last_snapshot_clone() {
        let db_dir = temp_db_dir("deferred-delete-clone");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.l0_compaction_trigger = 2;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let snapshot = match engine.open_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => panic!("open_snapshot should succeed: {err}"),
        };
        let snapshot_clone = snapshot.clone();

        if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }

        let l0_t1 = sstable_path(&db_dir, 1, 0);
        let l1_t2 = sstable_path(&db_dir, 2, 1);
        assert!(
            wait_for_path_exists(&l1_t2),
            "compaction output should appear"
        );
        assert!(
            l0_t1.exists(),
            "obsolete file should be retained while snapshot clones are alive"
        );

        drop(snapshot);
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            l0_t1.exists(),
            "dropping one clone must not release deferred cleanup"
        );

        drop(snapshot_clone);
        assert!(
            wait_for_path_missing(&l0_t1),
            "obsolete file should be removed after last clone drops"
        );

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn open_replays_manifest_pending_deletes_and_clears_them_durably() {
        let db_dir = temp_db_dir("deferred-delete-reopen");
        if let Err(err) = fs::create_dir_all(&db_dir) {
            panic!("db dir create should succeed: {err}");
        }

        let stale_table_path = sstable_path(&db_dir, 99, 0);
        if let Err(err) = fs::write(&stale_table_path, b"obsolete") {
            panic!("stale table file create should succeed: {err}");
        }

        let manifest = ManifestState {
            next_sequence_number: 0,
            next_table_id: 100,
            wal_generation: 0,
            wal_durable_checkpoint_offset: 0,
            levels: vec![Vec::new()],
            pending_deletes: vec![PendingDeleteEntry {
                table_id: 99,
                level: 0,
                max_seq: 0,
            }],
        };
        if let Err(err) = ManifestStore::commit(&db_dir, &manifest) {
            panic!("manifest seed commit should succeed: {err}");
        }

        let opts = EngineOptions::with_db_dir(db_dir.clone());
        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        assert!(
            wait_for_path_missing(&stale_table_path),
            "startup cleanup should remove obsolete file listed in pending deletes"
        );
        let loaded_manifest = match ManifestStore::load_or_create(&db_dir) {
            Ok(state) => state,
            Err(err) => panic!("manifest reload should succeed: {err}"),
        };
        assert!(
            loaded_manifest.pending_deletes.is_empty(),
            "startup cleanup should durably clear pending deletes after removal"
        );

        drop(engine);
        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn mmap_get_after_flush_returns_value() {
        let db_dir = temp_db_dir("mmap-get-flush");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.enable_mmap_reads = true;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v")) {
            panic!("put should succeed: {err}");
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }
        assert_loaded_tables_use_mmap(&engine);

        let value = match engine.get(b"k") {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(value, Some(Bytes::from_static(b"v")));

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn mmap_scan_after_flush_returns_ordered_entries() {
        let db_dir = temp_db_dir("mmap-scan-flush");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.enable_mmap_reads = true;

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        for (key, value) in [
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
            (b"e", b"5"),
        ] {
            if let Err(err) = engine.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            {
                panic!("put should succeed: {err}");
            }
        }
        if let Err(err) = engine.flush() {
            panic!("flush should succeed: {err}");
        }
        assert_loaded_tables_use_mmap(&engine);

        let items = match engine.scan(unbounded_scan()) {
            Ok(items) => items,
            Err(err) => panic!("scan should succeed: {err}"),
        };
        assert_eq!(
            scan_kv(&items),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
                (b"e".to_vec(), b"5".to_vec()),
            ]
        );

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn mmap_get_after_compaction_reads_l1() {
        let db_dir = temp_db_dir("mmap-get-compaction");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.enable_mmap_reads = true;
        opts.l0_compaction_trigger = 2;

        {
            let engine = match AetherEngine::open(opts.clone()) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }
            if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            let l1_t2 = sstable_path(&db_dir, 2, 1);
            let l0_t0 = sstable_path(&db_dir, 0, 0);
            let l0_t1 = sstable_path(&db_dir, 1, 0);
            assert!(
                wait_for_path_exists(&l1_t2),
                "compaction output should appear before reopen"
            );
            assert!(
                wait_for_path_missing(&l0_t0),
                "obsolete l0 table 0 should be cleaned before reopen"
            );
            assert!(
                wait_for_path_missing(&l0_t1),
                "obsolete l0 table 1 should be cleaned before reopen"
            );
            assert_loaded_tables_use_mmap(&engine);
        }

        let engine = open_with_transient_retry(&opts);
        assert_loaded_tables_use_mmap(&engine);

        let val_a = match engine.get(b"a") {
            Ok(value) => value,
            Err(err) => panic!("get a should succeed: {err}"),
        };
        let val_b = match engine.get(b"b") {
            Ok(value) => value,
            Err(err) => panic!("get b should succeed: {err}"),
        };
        assert_eq!(val_a, Some(Bytes::from_static(b"1")));
        assert_eq!(val_b, Some(Bytes::from_static(b"2")));

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn mmap_scan_across_l0_and_l1_after_compaction() {
        let db_dir = temp_db_dir("mmap-scan-l0-l1");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.enable_mmap_reads = true;
        opts.l0_compaction_trigger = 2;

        {
            let engine = match AetherEngine::open(opts.clone()) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            if let Err(err) = engine.put(Bytes::from_static(b"c"), Bytes::from_static(b"3")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.put(Bytes::from_static(b"d"), Bytes::from_static(b"4")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }

            let l1_t2 = sstable_path(&db_dir, 2, 1);
            let l0_t0 = sstable_path(&db_dir, 0, 0);
            let l0_t1 = sstable_path(&db_dir, 1, 0);
            assert!(
                wait_for_path_exists(&l1_t2),
                "compaction output should appear before mixed-level scan"
            );
            assert!(
                wait_for_path_missing(&l0_t0),
                "obsolete l0 table 0 should be cleaned before reopen"
            );
            assert!(
                wait_for_path_missing(&l0_t1),
                "obsolete l0 table 1 should be cleaned before reopen"
            );
            assert_loaded_tables_use_mmap(&engine);
        }

        let items = {
            let engine = open_with_transient_retry(&opts);
            assert_loaded_tables_use_mmap(&engine);

            if let Err(err) = engine.put(Bytes::from_static(b"e"), Bytes::from_static(b"5")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }
            assert_loaded_tables_use_mmap(&engine);

            let l0_t3 = sstable_path(&db_dir, 3, 0);
            let l1_t2 = sstable_path(&db_dir, 2, 1);
            assert!(
                wait_for_path_exists(&l0_t3),
                "fresh L0 table should exist for mixed-level scan"
            );
            assert!(
                wait_for_path_exists(&l1_t2),
                "compacted L1 table should exist for mixed-level scan"
            );

            match engine.scan(unbounded_scan()) {
                Ok(items) => items,
                Err(err) => panic!("scan should succeed: {err}"),
            }
        };

        assert_eq!(
            scan_kv(&items),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
                (b"e".to_vec(), b"5".to_vec()),
            ]
        );

        cleanup_db_dir(&db_dir);
    }

    #[test]
    fn mmap_reopen_reads_existing_sstables() {
        let db_dir = temp_db_dir("mmap-reopen");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.enable_mmap_reads = true;

        {
            let engine = match AetherEngine::open(opts.clone()) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"k"), Bytes::from_static(b"v")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }
            assert_loaded_tables_use_mmap(&engine);
        }

        let reopened = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("reopen should succeed: {err}"),
        };
        assert_loaded_tables_use_mmap(&reopened);

        let value = match reopened.get(b"k") {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(value, Some(Bytes::from_static(b"v")));

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn mmap_delete_tombstone_hides_flushed_key() {
        let db_dir = temp_db_dir("mmap-tombstone");
        let mut opts = EngineOptions::with_db_dir(db_dir.clone());
        opts.enable_mmap_reads = true;

        {
            let engine = match AetherEngine::open(opts.clone()) {
                Ok(engine) => engine,
                Err(err) => panic!("open should succeed: {err}"),
            };

            if let Err(err) = engine.put(Bytes::from_static(b"a"), Bytes::from_static(b"1")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.put(Bytes::from_static(b"b"), Bytes::from_static(b"2")) {
                panic!("put should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }
            assert_loaded_tables_use_mmap(&engine);

            if let Err(err) = engine.delete(Bytes::from_static(b"a")) {
                panic!("delete should succeed: {err}");
            }
            if let Err(err) = engine.flush() {
                panic!("flush should succeed: {err}");
            }
            assert_loaded_tables_use_mmap(&engine);
        }

        let engine = match AetherEngine::open(opts) {
            Ok(engine) => engine,
            Err(err) => panic!("reopen should succeed: {err}"),
        };
        assert_loaded_tables_use_mmap(&engine);

        let val_a = match engine.get(b"a") {
            Ok(value) => value,
            Err(err) => panic!("get a should succeed: {err}"),
        };
        let val_b = match engine.get(b"b") {
            Ok(value) => value,
            Err(err) => panic!("get b should succeed: {err}"),
        };
        assert_eq!(val_a, None, "tombstoned key should not be visible");
        assert_eq!(val_b, Some(Bytes::from_static(b"2")));

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }
}
