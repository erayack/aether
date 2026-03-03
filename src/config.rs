use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::types::FsyncPolicy;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum CompressionCodec {
    None,
    Snappy,
    Zstd,
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EngineOptions {
    pub db_dir: PathBuf,
    #[serde(default = "default_memtable_max_bytes")]
    pub memtable_max_bytes: usize,
    #[serde(default = "default_sst_target_block_bytes")]
    pub sst_target_block_bytes: usize,
    #[serde(default)]
    pub fsync_policy: FsyncPolicy,
    #[serde(default = "default_l0_compaction_trigger")]
    pub l0_compaction_trigger: usize,
    #[serde(default = "default_max_write_batch_ops")]
    pub max_write_batch_ops: usize,
    #[serde(default = "default_max_write_batch_bytes")]
    pub max_write_batch_bytes: usize,
    #[serde(default)]
    pub compression_codec: CompressionCodec,
    #[serde(default = "default_prefix_restart_interval")]
    pub prefix_restart_interval: u16,
    #[serde(default = "default_min_compress_size_bytes")]
    pub min_compress_size_bytes: usize,
    #[serde(default)]
    pub enable_metrics_log_interval: Option<u64>,
}

impl EngineOptions {
    #[must_use]
    pub fn with_db_dir(db_dir: PathBuf) -> Self {
        Self {
            db_dir,
            memtable_max_bytes: default_memtable_max_bytes(),
            sst_target_block_bytes: default_sst_target_block_bytes(),
            fsync_policy: FsyncPolicy::default(),
            l0_compaction_trigger: default_l0_compaction_trigger(),
            max_write_batch_ops: default_max_write_batch_ops(),
            max_write_batch_bytes: default_max_write_batch_bytes(),
            compression_codec: CompressionCodec::default(),
            prefix_restart_interval: default_prefix_restart_interval(),
            min_compress_size_bytes: default_min_compress_size_bytes(),
            enable_metrics_log_interval: None,
        }
    }
}

const fn default_memtable_max_bytes() -> usize {
    4 * 1024 * 1024
}

const fn default_sst_target_block_bytes() -> usize {
    16 * 1024
}

const fn default_l0_compaction_trigger() -> usize {
    4
}

const fn default_max_write_batch_ops() -> usize {
    1024
}

const fn default_max_write_batch_bytes() -> usize {
    4 * 1024 * 1024
}

const fn default_prefix_restart_interval() -> u16 {
    16
}

const fn default_min_compress_size_bytes() -> usize {
    1024
}
