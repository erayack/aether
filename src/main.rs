use std::path::{Path, PathBuf};

use aether_storage::{AetherEngine, EngineOptions, metrics::MetricsSnapshot, types::FsyncPolicy};
use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum FsyncPolicyArg {
    Always,
    EveryMillis,
    NeverForBenchOnly,
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
        | Command::Metrics { db, .. } => db,
    }
}

fn execute_command(engine: &AetherEngine, command: &Command) -> Result<()> {
    match command {
        Command::Put { key, value, .. } => {
            let parsed_key = parse_binary_input(key).context("invalid --key")?;
            let parsed_value = parse_binary_input(value).context("invalid --value")?;
            log_point_write(
                engine.put(parsed_key.clone(), parsed_value),
                "put",
                &parsed_key,
            )
        }
        Command::Get { key, .. } => {
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
        Command::Delete { key, .. } => {
            let parsed_key = parse_binary_input(key).context("invalid --key")?;
            log_point_write(engine.delete(parsed_key.clone()), "delete", &parsed_key)
        }
        Command::Flush { .. } => {
            engine.flush()?;
            info!(event = "cli.flush", status = "success", "flush completed");
            Ok(())
        }
        Command::Metrics { json, .. } => {
            let snapshot = engine.metrics_snapshot();
            emit_metrics_snapshot("cli.metrics", &snapshot, *json)?;
            Ok(())
        }
    }
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
