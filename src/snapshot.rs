use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;

use crate::{
    engine::{AetherEngine, PinnedReadState, SnapshotHandle},
    error::Result,
    types::{ScanBounds, ScanItem, ScanOptions, Value},
};

/// Read-only, sequence-pinned view over engine state.
#[derive(Clone)]
pub struct Snapshot {
    engine: AetherEngine,
    pinned: Arc<PinnedReadState>,
    _registration: Arc<SnapshotHandle>,
}

impl Snapshot {
    pub(crate) const fn new(
        engine: AetherEngine,
        pinned: Arc<PinnedReadState>,
        registration: Arc<SnapshotHandle>,
    ) -> Self {
        Self {
            engine,
            pinned,
            _registration: registration,
        }
    }

    #[must_use]
    pub fn sequence_number(&self) -> u64 {
        self.pinned.visible_seq()
    }

    /// Reads `key` as of this snapshot's sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if the read path fails due to corruption or I/O.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        let bounds = ScanBounds {
            start: Bound::Included(Bytes::copy_from_slice(key)),
            end: Bound::Included(Bytes::copy_from_slice(key)),
        };
        let options = ScanOptions {
            bounds,
            limit: Some(1),
        };
        let items = self.engine.scan_pinned(options, self.pinned.as_ref())?;
        Ok(items.into_iter().next().map(|item| item.value))
    }

    /// Scans key-value pairs as of this snapshot's sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot materialization or source iteration fails.
    pub fn scan(&self, options: ScanOptions) -> Result<Vec<ScanItem>> {
        self.engine.scan_pinned(options, self.pinned.as_ref())
    }
}
