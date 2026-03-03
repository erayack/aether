use std::{
    collections::{BTreeMap, HashSet},
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
    config::EngineOptions,
    error::Result,
    flush::{FlushJob, spawn_flush_worker},
    manifest::{ManifestState, ManifestStore},
    memtable::{MemTable, MemTableSet},
    merge_iter::{MergeIterator, MergeMode, SourceCursor},
    metrics::{MetricsHandle, MetricsSnapshot},
    sstable::{SsTableMeta, reader::SsTableReader, writer::SsTableWriter},
    types::{
        BatchOp, InternalEntry, Key, KvStore, ScanBounds, ScanItem, ScanOptions, Value, ValueEntry,
        WriteBatch,
    },
    wal::{WalOp, WalWriter, replay_wal},
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
    max_write_batch_ops: usize,
    max_write_batch_bytes: usize,
    metrics_log_interval: Option<Duration>,
    last_metrics_log_at: Mutex<Option<Instant>>,
    background_error: Mutex<Option<String>>,
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
}

struct TableHandle {
    meta: SsTableMeta,
    reader: Arc<SsTableReader>,
}

struct ReadSnapshot {
    visible_seq: u64,
    mutable_entries: Vec<InternalEntry>,
    immutable_entries: Vec<InternalEntry>,
    l0_tables: Vec<TableSource>,
    l1_tables: Vec<TableSource>,
}

struct TableSource {
    reader: Arc<SsTableReader>,
    source_rank: u32,
}

struct CompactionSelection {
    selected_l0: Vec<SsTableMeta>,
    selected_l1: Vec<SsTableMeta>,
    min_key: Key,
    max_key: Key,
}

struct BackgroundTaskChannels {
    flush_tx: SyncSender<FlushJob>,
    compaction_tx: SyncSender<CompactionJob>,
}

impl AetherEngine {
    /// Opens (or creates) an engine rooted at `opts.db_dir`.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be initialized.
    pub fn open(opts: EngineOptions) -> Result<Self> {
        fs::create_dir_all(&opts.db_dir)?;

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

        let loaded_tables = if manifest.levels.is_empty() {
            let discovered = discover_sstables(&opts.db_dir)?;
            if discovered.is_empty() {
                Vec::new()
            } else {
                manifest.levels = group_sstables_by_level(
                    discovered
                        .iter()
                        .map(|table| table.meta.clone())
                        .collect::<Vec<_>>(),
                );
                discovered
            }
        } else {
            load_sstables_from_manifest(&opts.db_dir, &manifest)?
        };

        let next_table_id = max_table_id(&manifest.levels)
            .and_then(|last| last.checked_add(1))
            .unwrap_or(0);
        manifest.next_table_id = manifest.next_table_id.max(next_table_id);
        manifest.next_sequence_number = manifest
            .next_sequence_number
            .max(wal.next_sequence_number());
        manifest.wal_durable_checkpoint_offset = wal.write_offset();
        ManifestStore::commit(&opts.db_dir, &manifest)?;

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
                metrics: MetricsHandle::default(),
                memtable_max_bytes: opts.memtable_max_bytes,
                sst_target_block_bytes: opts.sst_target_block_bytes,
                l0_compaction_trigger: opts.l0_compaction_trigger,
                max_write_batch_ops: opts.max_write_batch_ops,
                max_write_batch_bytes: opts.max_write_batch_bytes,
                metrics_log_interval: opts
                    .enable_metrics_log_interval
                    .filter(|millis| *millis > 0)
                    .map(Duration::from_millis),
                last_metrics_log_at: Mutex::new(None),
                background_error: Mutex::new(None),
            }),
        };

        let flush_engine = engine.clone();
        let _ = spawn_flush_worker(flush_rx, move |job| flush_engine.handle_flush_job(job));

        let compaction_engine = engine.clone();
        let _ = spawn_compaction_worker(compaction_rx, move |job| {
            compaction_engine.handle_compaction_job(job)
        });

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

    #[must_use]
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    fn build_read_snapshot(&self, options: &ScanOptions) -> Result<ReadSnapshot> {
        let read_state = self.inner.read_state.read();
        let visible_seq = read_state
            .manifest_snapshot
            .next_sequence_number
            .saturating_sub(1);
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

    fn maybe_schedule_compaction(&self, l0_tables: usize) {
        if l0_tables >= self.inner.l0_compaction_trigger.max(1) {
            self.inner.background.enqueue_compaction(CompactionJob {
                reason: CompactionReason::L0ThresholdReached,
            });
        }
    }

    fn handle_flush_job(&self, job: FlushJob) -> Result<()> {
        let flush_started_at = Instant::now();
        let input_bytes = job.memtable.approx_size_bytes;
        let input_entries = job.memtable.len();
        info!(
            event = "flush.start",
            generation = job.generation,
            input_bytes,
            input_entries,
            "flush started"
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
                job.memtable,
                table_id,
                self.inner.sst_target_block_bytes,
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
                generation = job.generation,
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
                generation = job.generation,
                input_bytes,
                input_entries,
                error = %err,
                "flush failed"
            );
        }

        result
    }

    fn handle_compaction_job(&self, job: CompactionJob) -> Result<()> {
        match job.reason {
            CompactionReason::L0ThresholdReached => {}
        }

        let compaction_started_at = Instant::now();

        let mut write_state = self.inner.write_state.lock();
        let mut read_state = self.inner.read_state.write();

        if read_state.table_cache.l0.len() < self.inner.l0_compaction_trigger.max(1) {
            return Ok(());
        }

        let Some(selection) = CompactionSelection::from_cache(&read_state.table_cache)? else {
            return Ok(());
        };
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
        info!(
            event = "compaction.start",
            reason = "l0_threshold_reached",
            input_l0_tables = input_l0_table_ids.len(),
            input_l1_tables = input_l1_table_ids.len(),
            ?input_l0_table_ids,
            ?input_l1_table_ids,
            "compaction started"
        );

        let output_entries = collect_compaction_entries(&read_state.table_cache, &selection)?;
        let new_l1_table = if output_entries.is_empty() {
            None
        } else {
            let table_id = write_state.manifest.next_table_id;
            write_state.manifest.next_table_id = write_state
                .manifest
                .next_table_id
                .checked_add(1)
                .ok_or_else(|| invalid_data("SSTable table-id overflow"))?;

            Some(write_entries_to_sstable(
                &write_state.db_dir,
                &output_entries,
                table_id,
                1,
                self.inner.sst_target_block_bytes,
            )?)
        };
        let output_table_ids = new_l1_table
            .as_ref()
            .map(|table| vec![table.meta.table_id])
            .unwrap_or_default();

        let removed_ids = selection
            .selected_l0
            .iter()
            .chain(selection.selected_l1.iter())
            .map(|meta| meta.table_id)
            .collect::<HashSet<_>>();

        remove_tables_from_manifest(&mut write_state.manifest.levels, &removed_ids);
        if let Some(table) = &new_l1_table {
            add_table_to_manifest_levels(&mut write_state.manifest.levels, table.meta.clone());
        }

        ManifestStore::commit(&write_state.db_dir, &write_state.manifest)?;

        read_state.table_cache.remove_tables(&removed_ids);
        if let Some(table) = new_l1_table {
            read_state.table_cache.insert(table);
        }
        read_state.manifest_snapshot = write_state.manifest.clone();

        let removed_paths = selection
            .selected_l0
            .iter()
            .chain(selection.selected_l1.iter())
            .map(|meta| sstable_path(&write_state.db_dir, meta.table_id, meta.level))
            .collect::<Vec<_>>();
        drop(read_state);
        drop(write_state);

        for path in removed_paths {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }

        self.inner
            .metrics
            .record_compaction_with_duration(compaction_started_at.elapsed());
        info!(
            event = "compaction.end",
            reason = "l0_threshold_reached",
            input_l0_tables = input_l0_table_ids.len(),
            input_l1_tables = input_l1_table_ids.len(),
            output_tables = output_table_ids.len(),
            output_entries = output_entries.len(),
            ?output_table_ids,
            elapsed_ms = compaction_started_at.elapsed().as_millis(),
            "compaction completed"
        );
        self.maybe_emit_metrics_snapshot("compaction");
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
            "metrics snapshot"
        );
    }

    fn record_background_error(&self, message: String) {
        let mut slot = self.inner.background_error.lock();
        if slot.is_none() {
            *slot = Some(message);
        }
    }

    fn ensure_background_workers_healthy(&self) -> Result<()> {
        let maybe_error = self.inner.background_error.lock().clone();
        if let Some(message) = maybe_error {
            return Err(std::io::Error::other(message).into());
        }
        Ok(())
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
        let read_state = self.inner.read_state.read();

        let (result, tables_touched) = if let Some(entry) = read_state.memtables.mutable().get(key)
        {
            let value = match &entry.value {
                ValueEntry::Put(value) => Some(value.clone()),
                ValueEntry::Tombstone => None,
            };
            (value, 0)
        } else if let Some(immutable) = read_state.memtables.immutable()
            && let Some(entry) = immutable.get(key)
        {
            let value = match &entry.value {
                ValueEntry::Put(value) => Some(value.clone()),
                ValueEntry::Tombstone => None,
            };
            (value, 0)
        } else {
            let mut tables_touched = 0_usize;

            let mut l0_hit: Option<Option<Value>> = None;
            for table in &read_state.table_cache.l0 {
                tables_touched = tables_touched.saturating_add(1);
                if let Some(entry) = table.reader.get(key)? {
                    l0_hit = Some(match entry.value {
                        ValueEntry::Put(value) => Some(value),
                        ValueEntry::Tombstone => None,
                    });
                    break;
                }
            }

            if let Some(value) = l0_hit {
                (value, tables_touched)
            } else {
                let mut best_l1_entry: Option<InternalEntry> = None;
                for table in &read_state.table_cache.l1 {
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

                (
                    best_l1_entry.and_then(|entry| match entry.value {
                        ValueEntry::Put(value) => Some(value),
                        ValueEntry::Tombstone => None,
                    }),
                    tables_touched,
                )
            }
        };
        drop(read_state);

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

        if has_invalid_scan_bounds(&options.bounds) {
            return Err(invalid_input("scan start bound is greater than end bound").into());
        }

        if matches!(options.limit, Some(0)) {
            return Ok(Vec::new());
        }

        let snapshot = self.build_read_snapshot(&options)?;
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

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.ensure_background_workers_healthy()?;

        if batch.ops.len() > self.inner.max_write_batch_ops {
            return Err(invalid_input("write batch exceeds max_write_batch_ops").into());
        }

        let estimated_bytes = estimate_write_batch_bytes(&batch);
        if estimated_bytes > self.inner.max_write_batch_bytes {
            return Err(invalid_input("write batch exceeds max_write_batch_bytes").into());
        }

        let _ = batch;
        Err(unsupported("write_batch atomic semantics are not implemented yet").into())
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
    }

    fn remove_tables(&mut self, table_ids: &HashSet<u64>) {
        self.l0
            .retain(|table| !table_ids.contains(&table.meta.table_id));
        self.l1
            .retain(|table| !table_ids.contains(&table.meta.table_id));
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

fn collect_compaction_entries(
    table_cache: &TableCache,
    selection: &CompactionSelection,
) -> Result<Vec<InternalEntry>> {
    let mut merged: BTreeMap<Key, InternalEntry> = BTreeMap::new();

    for table in &table_cache.l0 {
        for entry in table.reader.scan_all_entries()? {
            match merged.get(entry.key.as_ref()) {
                Some(existing) if existing.seq > entry.seq => {}
                _ => {
                    merged.insert(entry.key.clone(), entry);
                }
            }
        }
    }

    for table in &table_cache.l1 {
        if table.meta.max_key.as_ref() < selection.min_key.as_ref()
            || table.meta.min_key.as_ref() > selection.max_key.as_ref()
        {
            continue;
        }
        for entry in table.reader.scan_all_entries()? {
            match merged.get(entry.key.as_ref()) {
                Some(existing) if existing.seq > entry.seq => {}
                _ => {
                    merged.insert(entry.key.clone(), entry);
                }
            }
        }
    }

    let mut output_entries = merged.into_values().collect::<Vec<_>>();
    output_entries.retain(|entry| !matches!(entry.value, ValueEntry::Tombstone));
    Ok(output_entries)
}

fn discover_sstables(db_dir: &Path) -> Result<Vec<TableHandle>> {
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
        let reader = SsTableReader::open(&path, meta.clone())?;
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
) -> Result<Vec<TableHandle>> {
    let mut loaded = Vec::new();
    for level in &manifest.levels {
        for meta in level {
            let path = sstable_path(db_dir, meta.table_id, meta.level);
            let reader = SsTableReader::open(&path, meta.clone())?;
            loaded.push(TableHandle {
                meta: meta.clone(),
                reader: Arc::new(reader),
            });
        }
    }

    Ok(loaded)
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

fn flush_memtable_to_sstable(
    db_dir: &Path,
    mutable: MemTable,
    table_id: u64,
    block_target_bytes: usize,
) -> Result<TableHandle> {
    let entries = mutable.into_sorted_entries();
    if entries.is_empty() {
        return Err(invalid_data("cannot flush empty memtable").into());
    }

    write_entries_to_sstable(db_dir, &entries, table_id, 0, block_target_bytes)
}

fn write_entries_to_sstable(
    db_dir: &Path,
    entries: &[InternalEntry],
    table_id: u64,
    level: u8,
    block_target_bytes: usize,
) -> Result<TableHandle> {
    if entries.is_empty() {
        return Err(invalid_data("cannot flush empty entry set").into());
    }

    let path = sstable_path(db_dir, table_id, level);
    let mut writer =
        SsTableWriter::create_with_block_target(&path, table_id, level, block_target_bytes)?;
    for entry in entries {
        writer.add_entry(entry)?;
    }
    let meta = writer.finish()?;
    let reader = SsTableReader::open(&path, meta.clone())?;

    Ok(TableHandle {
        meta,
        reader: Arc::new(reader),
    })
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

fn estimate_write_batch_bytes(batch: &WriteBatch) -> usize {
    batch.ops.iter().fold(0_usize, |acc, op| {
        let op_bytes = match op {
            BatchOp::Put { key, value } => key
                .len()
                .saturating_add(value.len())
                .saturating_add(size_of::<u8>())
                .saturating_add(size_of::<u32>() * 2),
            BatchOp::Delete { key } => key
                .len()
                .saturating_add(size_of::<u8>())
                .saturating_add(size_of::<u32>()),
        };
        acc.saturating_add(op_bytes)
    })
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

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn unsupported(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        ops::Bound,
        time::{SystemTime, UNIX_EPOCH},
    };

    use bytes::Bytes;

    use super::AetherEngine;
    use crate::{
        config::EngineOptions,
        error::AetherError,
        types::{BatchOp, ScanBounds, ScanOptions, WriteBatch},
    };

    fn temp_db_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("aether-{prefix}-{nanos}"))
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

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
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

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
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

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
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

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
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

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn write_batch_is_rejected_until_atomic_semantics_are_implemented() {
        let db_dir = temp_db_dir("write-batch-unsupported");
        let engine = match AetherEngine::open(EngineOptions::with_db_dir(db_dir.clone())) {
            Ok(engine) => engine,
            Err(err) => panic!("open should succeed: {err}"),
        };

        let batch = WriteBatch {
            ops: vec![BatchOp::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
            }],
        };
        let result = engine.write_batch(batch);
        match result {
            Ok(()) => panic!("write_batch should be unsupported"),
            Err(AetherError::Io(err)) => {
                assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
            }
            Err(err) => panic!("unexpected write_batch error: {err}"),
        }

        let value = match engine.get(b"k") {
            Ok(value) => value,
            Err(err) => panic!("get should succeed: {err}"),
        };
        assert_eq!(value, None, "write_batch should not apply partial writes");

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
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

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
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
            std::thread::sleep(std::time::Duration::from_millis(200));
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

            std::thread::sleep(std::time::Duration::from_millis(200));
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
}
