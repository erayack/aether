use std::path::{Path, PathBuf};

use aether_storage::{
    AetherEngine, EngineOptions,
    config::CompressionCodec,
    metrics::MetricsSnapshot,
    types::{BatchOp, FsyncPolicy, ScanBounds, ScanOptions, WriteBatch},
};
use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(name = "aether")]
#[command(about = "Aether embedded key-value storage engine CLI")]
struct Cli {
    #[command(flatten)]
    runtime: RuntimeArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
struct RuntimeArgs {
    #[arg(long, default_value_t = false, global = true)]
    flush_on_shutdown: bool,
    #[arg(long, default_value_t = false, global = true)]
    final_metrics_json: bool,
    #[arg(long, global = true)]
    memtable_max_bytes: Option<usize>,
    #[arg(long, global = true)]
    sst_target_block_bytes: Option<usize>,
    #[arg(long, global = true)]
    l0_compaction_trigger: Option<usize>,
    #[arg(long, global = true)]
    enable_metrics_log_interval_ms: Option<u64>,
    #[arg(long, value_enum, default_value_t = FsyncPolicyArg::Always, global = true)]
    fsync_policy: FsyncPolicyArg,
    #[arg(long, global = true)]
    fsync_interval_ms: Option<u64>,
    #[arg(long, global = true)]
    max_write_batch_ops: Option<usize>,
    #[arg(long, global = true)]
    max_write_batch_bytes: Option<usize>,
    #[arg(long, default_value_t = false, global = true)]
    enable_mmap_reads: bool,
    #[arg(long, global = true)]
    block_cache_capacity_bytes: Option<usize>,
    #[arg(long, global = true)]
    max_open_snapshots: Option<usize>,
    #[arg(long, value_enum, global = true)]
    compression_codec: Option<CompressionCodecArg>,
    #[arg(
        long,
        global = true,
        help = "Prefix restart interval for prefix-encoded SST blocks; must be > 0"
    )]
    prefix_restart_interval: Option<u16>,
    #[arg(
        long,
        global = true,
        help = "Minimum raw block size to consider compression; 0 enables attempt on all blocks"
    )]
    min_compress_size_bytes: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum FsyncPolicyArg {
    Always,
    EveryMillis,
    NeverForBenchOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CompressionCodecArg {
    None,
    Snappy,
    Zstd,
}

impl From<CompressionCodecArg> for CompressionCodec {
    fn from(value: CompressionCodecArg) -> Self {
        match value {
            CompressionCodecArg::None => Self::None,
            CompressionCodecArg::Snappy => Self::Snappy,
            CompressionCodecArg::Zstd => Self::Zstd,
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    Put {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        key: String,
        #[arg(long)]
        value: String,
    },
    Get {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        key: String,
    },
    Delete {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        key: String,
    },
    Flush {
        #[arg(long)]
        db: PathBuf,
    },
    Metrics {
        #[arg(long)]
        db: PathBuf,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Scan {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        start: Option<String>,
        #[arg(long, default_value_t = false)]
        start_exclusive: bool,
        #[arg(long)]
        end: Option<String>,
        #[arg(long, default_value_t = false)]
        end_inclusive: bool,
        #[arg(long)]
        limit: Option<usize>,
    },
    #[command(group(
        ArgGroup::new("batch_input")
            .args(["ops_json", "ops"])
            .required(true)
            .multiple(false)
    ))]
    WriteBatch {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        ops_json: Option<PathBuf>,
        #[arg(
            long = "op",
            help = "Secondary mode. Format: put:<k>:<v> or delete:<k>"
        )]
        ops: Vec<String>,
    },
}

#[derive(Clone, Debug)]
struct ResolvedRuntimeConfig {
    db_dir: PathBuf,
    engine_options: EngineOptions,
    flush_on_shutdown: bool,
    final_metrics_json: bool,
}

fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    run(&cli)
}

fn run(cli: &Cli) -> Result<()> {
    let resolved = resolve_config(cli)?;
    let engine = AetherEngine::open(resolved.engine_options.clone())
        .with_context(|| format!("failed to open engine at {}", resolved.db_dir.display()))?;

    let command_result = execute_command(&engine, &cli.command);
    let shutdown_result = graceful_shutdown(&engine, &resolved);

    if let Err(err) = shutdown_result {
        error!(
            event = "cli.shutdown",
            db = %resolved.db_dir.display(),
            status = "failure",
            error = %err,
            "graceful shutdown failed"
        );
        if command_result.is_ok() {
            return Err(err);
        }
    }

    command_result
}

fn resolve_config(cli: &Cli) -> Result<ResolvedRuntimeConfig> {
    let db_dir = command_db_path(&cli.command).to_path_buf();
    let mut engine_options = EngineOptions::with_db_dir(db_dir.clone());

    if let Some(bytes) = cli.runtime.memtable_max_bytes {
        engine_options.memtable_max_bytes = bytes;
    }
    if let Some(bytes) = cli.runtime.sst_target_block_bytes {
        engine_options.sst_target_block_bytes = bytes;
    }
    if let Some(trigger) = cli.runtime.l0_compaction_trigger {
        engine_options.l0_compaction_trigger = trigger;
    }
    if let Some(interval) = cli.runtime.enable_metrics_log_interval_ms {
        engine_options.enable_metrics_log_interval = Some(interval);
    }
    if let Some(max_ops) = cli.runtime.max_write_batch_ops {
        engine_options.max_write_batch_ops = max_ops;
    }
    if let Some(max_bytes) = cli.runtime.max_write_batch_bytes {
        engine_options.max_write_batch_bytes = max_bytes;
    }
    engine_options.enable_mmap_reads = cli.runtime.enable_mmap_reads;
    if let Some(capacity_bytes) = cli.runtime.block_cache_capacity_bytes {
        engine_options.block_cache_capacity_bytes = capacity_bytes;
    }
    if let Some(max_open_snapshots) = cli.runtime.max_open_snapshots {
        if max_open_snapshots == 0 {
            bail!("--max-open-snapshots must be > 0");
        }
        engine_options.max_open_snapshots = max_open_snapshots;
    }
    if let Some(codec) = cli.runtime.compression_codec {
        engine_options.compression_codec = codec.into();
    }
    if let Some(interval) = cli.runtime.prefix_restart_interval {
        if interval == 0 {
            bail!("--prefix-restart-interval must be > 0");
        }
        engine_options.prefix_restart_interval = interval;
    }
    if let Some(bytes) = cli.runtime.min_compress_size_bytes {
        if bytes > engine_options.sst_target_block_bytes {
            bail!(
                "--min-compress-size-bytes cannot exceed --sst-target-block-bytes ({} > {})",
                bytes,
                engine_options.sst_target_block_bytes
            );
        }
        engine_options.min_compress_size_bytes = bytes;
    }

    engine_options.fsync_policy =
        resolve_fsync_policy(cli.runtime.fsync_policy, cli.runtime.fsync_interval_ms)?;

    Ok(ResolvedRuntimeConfig {
        db_dir,
        engine_options,
        flush_on_shutdown: cli.runtime.flush_on_shutdown,
        final_metrics_json: cli.runtime.final_metrics_json,
    })
}

fn resolve_fsync_policy(policy: FsyncPolicyArg, interval_ms: Option<u64>) -> Result<FsyncPolicy> {
    match policy {
        FsyncPolicyArg::Always => {
            if interval_ms.is_some() {
                bail!("--fsync-interval-ms requires --fsync-policy every-millis");
            }
            Ok(FsyncPolicy::Always)
        }
        FsyncPolicyArg::EveryMillis => {
            let interval = interval_ms.filter(|value| *value > 0).ok_or_else(|| {
                anyhow::anyhow!("--fsync-interval-ms must be > 0 when --fsync-policy every-millis")
            })?;
            Ok(FsyncPolicy::EveryMillis(interval))
        }
        FsyncPolicyArg::NeverForBenchOnly => {
            if interval_ms.is_some() {
                bail!("--fsync-interval-ms requires --fsync-policy every-millis");
            }
            Ok(FsyncPolicy::NeverForBenchOnly)
        }
    }
}

fn command_db_path(command: &Command) -> &Path {
    match command {
        Command::Put { db, .. }
        | Command::Get { db, .. }
        | Command::Delete { db, .. }
        | Command::Flush { db }
        | Command::Metrics { db, .. }
        | Command::Scan { db, .. }
        | Command::WriteBatch { db, .. } => db,
    }
}

fn execute_command(engine: &AetherEngine, command: &Command) -> Result<()> {
    match command {
        Command::Put { key, value, .. } => handle_put(engine, key, value),
        Command::Get { key, .. } => handle_get(engine, key),
        Command::Delete { key, .. } => handle_delete(engine, key),
        Command::Flush { .. } => handle_flush(engine),
        Command::Metrics { json, .. } => handle_metrics(engine, *json),
        Command::Scan {
            start,
            start_exclusive,
            end,
            end_inclusive,
            limit,
            ..
        } => handle_scan(
            engine,
            start.as_deref(),
            *start_exclusive,
            end.as_deref(),
            *end_inclusive,
            *limit,
        ),
        Command::WriteBatch { ops_json, ops, .. } => {
            handle_write_batch(engine, ops_json.as_deref(), ops)
        }
    }
}

fn handle_put(engine: &AetherEngine, key: &str, value: &str) -> Result<()> {
    let parsed_key = parse_binary_input(key).context("invalid --key")?;
    let parsed_value = parse_binary_input(value).context("invalid --value")?;
    log_point_write(
        engine.put(parsed_key.clone(), parsed_value),
        "put",
        &parsed_key,
    )
}

fn handle_get(engine: &AetherEngine, key: &str) -> Result<()> {
    let parsed_key = parse_binary_input(key).context("invalid --key")?;
    match engine.get(parsed_key.as_ref()) {
        Ok(Some(value)) => {
            info!(
                event = "cli.point_op",
                op = "get",
                key_hex = %format_hex(parsed_key.as_ref()),
                status = "success",
                result = "found",
                value_hex = %format_hex(value.as_ref()),
                "point operation completed"
            );
            Ok(())
        }
        Ok(None) => {
            info!(
                event = "cli.point_op",
                op = "get",
                key_hex = %format_hex(parsed_key.as_ref()),
                status = "success",
                result = "not_found",
                "point operation completed"
            );
            Ok(())
        }
        Err(err) => {
            error!(
                event = "cli.point_op",
                op = "get",
                key_hex = %format_hex(parsed_key.as_ref()),
                status = "failure",
                error = %err,
                "point operation failed"
            );
            Err(err.into())
        }
    }
}

fn handle_delete(engine: &AetherEngine, key: &str) -> Result<()> {
    let parsed_key = parse_binary_input(key).context("invalid --key")?;
    log_point_write(engine.delete(parsed_key.clone()), "delete", &parsed_key)
}

fn handle_flush(engine: &AetherEngine) -> Result<()> {
    engine.flush()?;
    info!(event = "cli.flush", status = "success", "flush completed");
    Ok(())
}

fn handle_metrics(engine: &AetherEngine, json: bool) -> Result<()> {
    let snapshot = engine.metrics_snapshot();
    emit_metrics_snapshot("cli.metrics", &snapshot, json)?;
    Ok(())
}

fn handle_scan(
    engine: &AetherEngine,
    start: Option<&str>,
    start_exclusive: bool,
    end: Option<&str>,
    end_inclusive: bool,
    limit: Option<usize>,
) -> Result<()> {
    let options = parse_scan_options(start, start_exclusive, end, end_inclusive, limit)?;

    let items = engine.scan(options)?;
    info!(
        event = "cli.scan",
        status = "success",
        items = items.len(),
        "scan completed"
    );
    for item in items {
        info!(
            event = "cli.scan.item",
            key_hex = %format_hex(item.key.as_ref()),
            value_hex = %format_hex(item.value.as_ref()),
            "scan item"
        );
    }
    Ok(())
}

fn handle_write_batch(
    engine: &AetherEngine,
    ops_json: Option<&Path>,
    ops: &[String],
) -> Result<()> {
    let batch = resolve_write_batch(ops_json, ops)?;
    let op_count = batch.ops.len();
    engine.write_batch(batch)?;
    info!(
        event = "cli.write_batch",
        status = "success",
        ops = op_count,
        "write batch completed"
    );
    Ok(())
}

fn log_point_write(result: aether_storage::error::Result<()>, op: &str, key: &Bytes) -> Result<()> {
    match result {
        Ok(()) => {
            info!(
                event = "cli.point_op",
                op,
                key_hex = %format_hex(key.as_ref()),
                status = "success",
                "point operation completed"
            );
            Ok(())
        }
        Err(err) => {
            error!(
                event = "cli.point_op",
                op,
                key_hex = %format_hex(key.as_ref()),
                status = "failure",
                error = %err,
                "point operation failed"
            );
            Err(err.into())
        }
    }
}

fn graceful_shutdown(engine: &AetherEngine, cfg: &ResolvedRuntimeConfig) -> Result<()> {
    info!(
        event = "cli.shutdown",
        db = %cfg.db_dir.display(),
        flush_on_shutdown = cfg.flush_on_shutdown,
        "starting graceful shutdown"
    );

    if cfg.flush_on_shutdown {
        engine.flush()?;
        info!(
            event = "cli.shutdown.flush",
            db = %cfg.db_dir.display(),
            status = "success",
            "shutdown flush completed"
        );
    }

    let snapshot = engine.metrics_snapshot();
    emit_metrics_snapshot(
        "cli.shutdown.final_metrics",
        &snapshot,
        cfg.final_metrics_json,
    )?;

    info!(
        event = "cli.shutdown",
        db = %cfg.db_dir.display(),
        status = "success",
        "graceful shutdown completed"
    );
    Ok(())
}

fn emit_metrics_snapshot(
    event: &'static str,
    snapshot: &MetricsSnapshot,
    json: bool,
) -> Result<()> {
    if json {
        let payload = serde_json::to_string_pretty(snapshot)?;
        info!(
            event,
            output = "json",
            metrics_json = %payload,
            "metrics snapshot"
        );
    } else {
        info!(
            event,
            output = "text",
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

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum BatchOpInput {
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, serde::Deserialize)]
struct WriteBatchInput {
    ops: Vec<BatchOpInput>,
}

fn parse_scan_options(
    start: Option<&str>,
    start_exclusive: bool,
    end: Option<&str>,
    end_inclusive: bool,
    limit: Option<usize>,
) -> Result<ScanOptions> {
    let start_bound = match start {
        Some(raw) => {
            let key = parse_binary_input(raw).context("invalid --start")?;
            if start_exclusive {
                std::ops::Bound::Excluded(key)
            } else {
                std::ops::Bound::Included(key)
            }
        }
        None => std::ops::Bound::Unbounded,
    };

    let end_bound = match end {
        Some(raw) => {
            let key = parse_binary_input(raw).context("invalid --end")?;
            if end_inclusive {
                std::ops::Bound::Included(key)
            } else {
                std::ops::Bound::Excluded(key)
            }
        }
        None => std::ops::Bound::Unbounded,
    };

    Ok(ScanOptions {
        bounds: ScanBounds {
            start: start_bound,
            end: end_bound,
        },
        limit,
    })
}

fn resolve_write_batch(ops_json: Option<&Path>, ops: &[String]) -> Result<WriteBatch> {
    match (ops_json, ops.is_empty()) {
        (Some(_), false) => bail!("choose either --ops-json or repeated --op, not both"),
        (None, true) => bail!("write-batch requires --ops-json or at least one --op"),
        (Some(path), true) => load_write_batch_from_json(path),
        (None, false) => parse_inline_batch_ops(ops),
    }
}

fn load_write_batch_from_json(path: &Path) -> Result<WriteBatch> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read ops json from {}", path.display()))?;
    let parsed: WriteBatchInput = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse write batch json from {}", path.display()))?;

    let mut out = Vec::with_capacity(parsed.ops.len());
    for op in parsed.ops {
        out.push(parse_batch_op_input(op)?);
    }
    Ok(WriteBatch { ops: out })
}

fn parse_batch_op_input(input: BatchOpInput) -> Result<BatchOp> {
    match input {
        BatchOpInput::Put { key, value } => Ok(BatchOp::Put {
            key: parse_binary_input(&key).context("invalid put key")?,
            value: parse_binary_input(&value).context("invalid put value")?,
        }),
        BatchOpInput::Delete { key } => Ok(BatchOp::Delete {
            key: parse_binary_input(&key).context("invalid delete key")?,
        }),
    }
}

fn parse_inline_batch_ops(ops: &[String]) -> Result<WriteBatch> {
    let mut out = Vec::with_capacity(ops.len());
    for raw in ops {
        let (kind, payload) = raw.split_once(':').ok_or_else(|| {
            anyhow::anyhow!("invalid --op format: expected put:<k>:<v> or delete:<k>")
        })?;

        match kind {
            "put" => {
                let (key, value) = payload.split_once(':').ok_or_else(|| {
                    anyhow::anyhow!("invalid put --op format: expected put:<k>:<v>")
                })?;
                out.push(BatchOp::Put {
                    key: parse_binary_input(key).context("invalid inline put key")?,
                    value: parse_binary_input(value).context("invalid inline put value")?,
                });
            }
            "delete" => {
                out.push(BatchOp::Delete {
                    key: parse_binary_input(payload).context("invalid inline delete key")?,
                });
            }
            _ => {
                bail!("unsupported --op kind: {kind}");
            }
        }
    }

    Ok(WriteBatch { ops: out })
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .compact()
        .init();
}

fn parse_binary_input(raw: &str) -> Result<Bytes> {
    if let Some(hex_body) = raw.strip_prefix("hex:") {
        return parse_hex_bytes(hex_body).map(Bytes::from);
    }

    if let Some(hex_body) = raw.strip_prefix("0x") {
        return parse_hex_bytes(hex_body).map(Bytes::from);
    }

    Ok(Bytes::copy_from_slice(raw.as_bytes()))
}

fn parse_hex_bytes(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        bail!("hex input must have even length");
    }

    let mut out = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let high =
            decode_hex_nibble(pair[0]).with_context(|| format!("invalid hex byte: {hex}"))?;
        let low = decode_hex_nibble(pair[1]).with_context(|| format!("invalid hex byte: {hex}"))?;
        out.push((high << 4) | low);
    }

    Ok(out)
}

fn decode_hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok((value - b'a') + 10),
        b'A'..=b'F' => Ok((value - b'A') + 10),
        _ => bail!("invalid hex character"),
    }
}

fn format_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        BatchOp, BatchOpInput, Cli, Command, CompressionCodecArg, FsyncPolicyArg, RuntimeArgs,
        load_write_batch_from_json, parse_batch_op_input, parse_inline_batch_ops, resolve_config,
        resolve_write_batch,
    };
    use std::path::{Path, PathBuf};

    #[test]
    fn parse_batch_op_input_parses_put_and_delete() {
        let put = parse_batch_op_input(BatchOpInput::Put {
            key: "k".to_string(),
            value: "v".to_string(),
        });
        assert!(put.is_ok(), "put op should parse");
        match put {
            Ok(BatchOp::Put { key, value }) => {
                assert_eq!(key.as_ref(), b"k");
                assert_eq!(value.as_ref(), b"v");
            }
            Ok(BatchOp::Delete { .. }) => panic!("expected put op"),
            Err(err) => panic!("expected successful parse, got {err}"),
        }

        let delete = parse_batch_op_input(BatchOpInput::Delete {
            key: "k".to_string(),
        });
        assert!(delete.is_ok(), "delete op should parse");
        match delete {
            Ok(BatchOp::Delete { key }) => assert_eq!(key.as_ref(), b"k"),
            Ok(BatchOp::Put { .. }) => panic!("expected delete op"),
            Err(err) => panic!("expected successful parse, got {err}"),
        }
    }

    #[test]
    fn parse_inline_batch_ops_parses_repeated_ops() {
        let ops = vec!["put:k1:v1".to_string(), "delete:k2".to_string()];
        let parsed = parse_inline_batch_ops(&ops);
        assert!(parsed.is_ok(), "inline ops should parse");
        let batch = match parsed {
            Ok(batch) => batch,
            Err(err) => panic!("expected successful parse, got {err}"),
        };
        assert_eq!(batch.ops.len(), 2);
    }

    #[test]
    fn resolve_write_batch_rejects_both_sources() {
        let ops = vec!["delete:k".to_string()];
        let result = resolve_write_batch(Some(Path::new("ops.json")), &ops);
        assert!(result.is_err(), "should reject --ops-json with --op");
    }

    #[test]
    fn load_write_batch_from_json_reads_deterministic_format() {
        let raw = r#"{
  "ops": [
    { "op": "put", "key": "k1", "value": "v1" },
    { "op": "delete", "key": "k2" }
  ]
}"#;

        let tmp_dir = std::env::temp_dir();
        let path = tmp_dir.join(format!(
            "aether-write-batch-{}-{}.json",
            std::process::id(),
            monotonic_nanos()
        ));

        let write_result = std::fs::write(&path, raw.as_bytes());
        assert!(write_result.is_ok(), "failed to create temp ops file");

        let loaded = load_write_batch_from_json(&path);
        let cleanup_result = std::fs::remove_file(&path);
        assert!(
            cleanup_result.is_ok(),
            "failed to remove temp ops file {}",
            path.display()
        );

        assert!(loaded.is_ok(), "json batch should parse");
        let batch = match loaded {
            Ok(batch) => batch,
            Err(err) => panic!("expected successful parse, got {err}"),
        };
        assert_eq!(batch.ops.len(), 2);
    }

    #[test]
    fn resolve_config_rejects_zero_prefix_restart_interval() {
        let cli = build_cli(RuntimeArgs {
            prefix_restart_interval: Some(0),
            ..default_runtime_args()
        });
        let result = resolve_config(&cli);
        assert!(result.is_err(), "zero restart interval should be rejected");
    }

    #[test]
    fn resolve_config_rejects_min_compress_larger_than_block_target() {
        let cli = build_cli(RuntimeArgs {
            sst_target_block_bytes: Some(1024),
            min_compress_size_bytes: Some(2048),
            ..default_runtime_args()
        });
        let result = resolve_config(&cli);
        assert!(
            result.is_err(),
            "min compress threshold larger than target block should be rejected"
        );
    }

    #[test]
    fn resolve_config_applies_runtime_compression_knobs() {
        let cli = build_cli(RuntimeArgs {
            compression_codec: Some(CompressionCodecArg::Zstd),
            prefix_restart_interval: Some(32),
            min_compress_size_bytes: Some(512),
            sst_target_block_bytes: Some(4096),
            ..default_runtime_args()
        });
        let resolved = resolve_config(&cli);
        assert!(resolved.is_ok(), "config should resolve");
        let resolved = match resolved {
            Ok(resolved) => resolved,
            Err(err) => panic!("expected successful resolve, got {err}"),
        };
        assert_eq!(
            resolved.engine_options.compression_codec,
            aether_storage::config::CompressionCodec::Zstd
        );
        assert_eq!(resolved.engine_options.prefix_restart_interval, 32);
        assert_eq!(resolved.engine_options.min_compress_size_bytes, 512);
    }

    #[test]
    fn resolve_config_rejects_zero_max_open_snapshots() {
        let cli = build_cli(RuntimeArgs {
            max_open_snapshots: Some(0),
            ..default_runtime_args()
        });
        let result = resolve_config(&cli);
        assert!(
            result.is_err(),
            "zero max-open-snapshots should be rejected"
        );
    }

    fn monotonic_nanos() -> u128 {
        match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => duration.as_nanos(),
            Err(err) => panic!("system clock should be after unix epoch: {err}"),
        }
    }

    fn build_cli(runtime: RuntimeArgs) -> Cli {
        Cli {
            runtime,
            command: Command::Flush {
                db: PathBuf::from("/tmp/aether-cli-test"),
            },
        }
    }

    fn default_runtime_args() -> RuntimeArgs {
        RuntimeArgs {
            flush_on_shutdown: false,
            final_metrics_json: false,
            memtable_max_bytes: None,
            sst_target_block_bytes: None,
            l0_compaction_trigger: None,
            enable_metrics_log_interval_ms: None,
            fsync_policy: FsyncPolicyArg::Always,
            fsync_interval_ms: None,
            max_write_batch_ops: None,
            max_write_batch_bytes: None,
            enable_mmap_reads: false,
            block_cache_capacity_bytes: None,
            max_open_snapshots: None,
            compression_codec: None,
            prefix_restart_interval: None,
            min_compress_size_bytes: None,
        }
    }
}
