use std::{
    sync::mpsc::Receiver,
    thread::{self, JoinHandle},
};

use crate::error::Result;

#[derive(Clone, Copy, Debug)]
pub enum CompactionReason {
    L0ThresholdReached,
}

#[derive(Clone, Copy, Debug)]
pub struct CompactionJob {
    pub reason: CompactionReason,
}

pub fn spawn_compaction_worker<F>(rx: Receiver<CompactionJob>, mut handler: F) -> JoinHandle<()>
where
    F: FnMut(CompactionJob) -> Result<()> + Send + 'static,
{
    thread::spawn(move || {
        while let Ok(job) = rx.recv() {
            let _ = handler(job);
        }
    })
}
