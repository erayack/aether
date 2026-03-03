use bytes::Bytes;
use std::ops::Bound;

pub type Key = Bytes;
pub type Value = Bytes;
pub type SequenceNumber = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueEntry {
    Put(Value),
    Tombstone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InternalEntry {
    pub seq: SequenceNumber,
    pub key: Key,
    pub value: ValueEntry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadResult {
    Found(Value),
    Deleted,
    NotFound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanBounds {
    pub start: Bound<Key>,
    pub end: Bound<Key>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanOptions {
    pub bounds: ScanBounds,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanItem {
    pub key: Key,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchOp {
    Put { key: Key, value: Value },
    Delete { key: Key },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatch {
    pub ops: Vec<BatchOp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum FsyncPolicy {
    Always,
    EveryMillis(u64),
    NeverForBenchOnly,
}

impl Default for FsyncPolicy {
    fn default() -> Self {
        Self::Always
    }
}

pub trait KvStore {
    /// Inserts or replaces a value for `key`.
    ///
    /// # Errors
    ///
    /// Returns an engine-specific write error.
    fn put(&self, key: Key, value: Value) -> crate::error::Result<()>;

    /// Reads the latest value for `key`.
    ///
    /// # Errors
    ///
    /// Returns an engine-specific read error.
    fn get(&self, key: &[u8]) -> crate::error::Result<Option<Value>>;

    /// Marks `key` as deleted.
    ///
    /// # Errors
    ///
    /// Returns an engine-specific write error.
    fn delete(&self, key: Key) -> crate::error::Result<()>;

    /// Forces buffered state to be flushed toward durable storage.
    ///
    /// # Errors
    ///
    /// Returns an engine-specific flush error.
    fn flush(&self) -> crate::error::Result<()>;

    /// Scans the keyspace within the provided bounds.
    ///
    /// # Errors
    ///
    /// Returns an engine-specific scan error.
    fn scan(&self, options: ScanOptions) -> crate::error::Result<Vec<ScanItem>>;

    /// Applies multiple write operations.
    ///
    /// # Errors
    ///
    /// Returns an engine-specific batch write error, including when batch writes
    /// are not supported by the current engine implementation.
    fn write_batch(&self, batch: WriteBatch) -> crate::error::Result<()>;
}
