use std::{
    sync::Arc,
    sync::mpsc::{Receiver, SyncSender},
    thread::{self, JoinHandle},
};

use crate::{error::Result, memtable::MemTable};

pub struct FlushJob {
    pub generation: u64,
    pub memtable: Arc<MemTable>,
    pub completion: Option<SyncSender<Result<()>>>,
}

pub fn spawn_flush_worker<F>(rx: Receiver<FlushJob>, mut handler: F) -> JoinHandle<()>
where
    F: FnMut(FlushJob) -> Result<()> + Send + 'static,
{
    thread::spawn(move || {
        while let Ok(job) = rx.recv() {
            let completion = job.completion.clone();
            let result = handler(job);
            if let Some(done_tx) = completion {
                let _ = done_tx.send(result);
            }
        }
    })
}
