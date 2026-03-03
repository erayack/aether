use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::types::FsyncPolicy;

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
