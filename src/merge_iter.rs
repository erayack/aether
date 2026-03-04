use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    error::Result,
    sstable::reader::SsTableRangeIter,
    types::{InternalEntry, ValueEntry},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMode {
    /// User-visible scan mode: resolve latest version per key and hide tombstones.
    UserScan,
    /// Compaction mode: resolve latest version per key and return tombstones as-is.
    /// Tombstone retention/drop policy is decided by the compaction caller.
    Compaction,
}

pub struct SourceCursor {
    source_rank: u32,
    kind: SourceKind,
}

enum SourceKind {
    Entries {
        entries: std::vec::IntoIter<InternalEntry>,
    },
    SsTable {
        iter: Box<SsTableRangeIter>,
    },
}

impl SourceCursor {
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn from_entries(entries: Vec<InternalEntry>, source_rank: u32) -> Self {
        Self {
            source_rank,
            kind: SourceKind::Entries {
                entries: entries.into_iter(),
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn from_sstable_iter(iter: SsTableRangeIter, source_rank: u32) -> Self {
        Self {
            source_rank,
            kind: SourceKind::SsTable {
                iter: Box::new(iter),
            },
        }
    }

    const fn source_rank(&self) -> u32 {
        self.source_rank
    }

    fn next_entry(&mut self) -> Result<Option<InternalEntry>> {
        match &mut self.kind {
            SourceKind::Entries { entries } => Ok(entries.next()),
            SourceKind::SsTable { iter } => iter.next_entry(),
        }
    }
}

pub struct MergeIterator {
    mode: MergeMode,
    sources: Vec<SourceCursor>,
    heap: BinaryHeap<HeapItem>,
    initialized: bool,
}

impl MergeIterator {
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn new(mode: MergeMode, sources: Vec<SourceCursor>) -> Self {
        Self {
            mode,
            sources,
            heap: BinaryHeap::new(),
            initialized: false,
        }
    }

    /// Advances the merge and returns the next resolved entry.
    ///
    /// # Errors
    ///
    /// Returns an error when reading a backing source cursor fails.
    pub fn next_entry(&mut self) -> Result<Option<InternalEntry>> {
        if !self.initialized {
            self.prime_sources()?;
        }

        while let Some(head) = self.heap.pop() {
            self.refill_source(head.source_index)?;
            self.discard_key_versions(head.entry.key.as_ref())?;

            match self.mode {
                MergeMode::UserScan => {
                    if matches!(&head.entry.value, ValueEntry::Tombstone) {
                        continue;
                    }
                    return Ok(Some(head.entry));
                }
                MergeMode::Compaction => return Ok(Some(head.entry)),
            }
        }

        Ok(None)
    }

    fn prime_sources(&mut self) -> Result<()> {
        for source_index in 0..self.sources.len() {
            self.refill_source(source_index)?;
        }
        self.initialized = true;
        Ok(())
    }

    fn discard_key_versions(&mut self, key: &[u8]) -> Result<()> {
        loop {
            let should_drop = self
                .heap
                .peek()
                .is_some_and(|peek| peek.entry.key.as_ref() == key);
            if !should_drop {
                return Ok(());
            }

            let Some(item) = self.heap.pop() else {
                return Ok(());
            };
            self.refill_source(item.source_index)?;
        }
    }

    fn refill_source(&mut self, source_index: usize) -> Result<()> {
        let Some(source) = self.sources.get_mut(source_index) else {
            return Ok(());
        };

        if let Some(entry) = source.next_entry()? {
            self.heap.push(HeapItem {
                entry,
                source_index,
                source_rank: source.source_rank(),
            });
        }

        Ok(())
    }
}

struct HeapItem {
    entry: InternalEntry,
    source_index: usize,
    source_rank: u32,
}

impl HeapItem {
    fn logical_cmp(&self, other: &Self) -> Ordering {
        self.entry
            .key
            .cmp(&other.entry.key)
            .then(other.entry.seq.cmp(&self.entry.seq))
            .then(self.source_rank.cmp(&other.source_rank))
            .then(self.source_index.cmp(&other.source_index))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.logical_cmp(other).reverse()
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key
            && self.entry.seq == other.entry.seq
            && self.source_rank == other.source_rank
            && self.source_index == other.source_index
    }
}

impl Eq for HeapItem {}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{MergeIterator, MergeMode, SourceCursor};
    use crate::{
        error::Result,
        types::{InternalEntry, ValueEntry},
    };

    fn put(seq: u64, key: &'static [u8], value: &'static [u8]) -> InternalEntry {
        InternalEntry {
            seq,
            key: Bytes::from_static(key),
            value: ValueEntry::Put(Bytes::from_static(value)),
        }
    }

    fn tombstone(seq: u64, key: &'static [u8]) -> InternalEntry {
        InternalEntry {
            seq,
            key: Bytes::from_static(key),
            value: ValueEntry::Tombstone,
        }
    }

    fn collect_entries(iter: &mut MergeIterator) -> Result<Vec<InternalEntry>> {
        let mut output = Vec::new();
        while let Some(entry) = iter.next_entry()? {
            output.push(entry);
        }
        Ok(output)
    }

    #[test]
    fn compaction_mode_emits_keys_in_ascending_order() {
        let sources = vec![
            SourceCursor::from_entries(vec![put(3, b"a", b"a3"), put(2, b"c", b"c2")], 0),
            SourceCursor::from_entries(vec![put(4, b"b", b"b4"), put(1, b"d", b"d1")], 1),
        ];
        let mut iter = MergeIterator::new(MergeMode::Compaction, sources);

        let entries = match collect_entries(&mut iter) {
            Ok(entries) => entries,
            Err(err) => panic!("unexpected merge error: {err}"),
        };
        let keys = entries
            .iter()
            .map(|entry| entry.key.as_ref().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
    }

    #[test]
    fn compaction_mode_prefers_higher_seq_for_same_key() {
        let sources = vec![
            SourceCursor::from_entries(vec![put(8, b"k", b"newer")], 0),
            SourceCursor::from_entries(vec![put(7, b"k", b"older"), put(1, b"z", b"z1")], 1),
        ];
        let mut iter = MergeIterator::new(MergeMode::Compaction, sources);

        let entries = match collect_entries(&mut iter) {
            Ok(entries) => entries,
            Err(err) => panic!("unexpected merge error: {err}"),
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key.as_ref(), b"k");
        assert_eq!(entries[0].seq, 8);
        match &entries[0].value {
            ValueEntry::Put(value) => assert_eq!(value.as_ref(), b"newer"),
            ValueEntry::Tombstone => panic!("unexpected tombstone"),
        }
    }

    #[test]
    fn compaction_mode_prefers_lower_source_rank_for_same_key_and_seq() {
        let sources = vec![
            SourceCursor::from_entries(vec![put(5, b"k", b"from-rank-0"), put(2, b"m", b"m2")], 0),
            SourceCursor::from_entries(vec![put(5, b"k", b"from-rank-1"), put(1, b"n", b"n1")], 1),
        ];
        let mut iter = MergeIterator::new(MergeMode::Compaction, sources);

        let entries = match collect_entries(&mut iter) {
            Ok(entries) => entries,
            Err(err) => panic!("unexpected merge error: {err}"),
        };
        assert_eq!(entries[0].key.as_ref(), b"k");
        match &entries[0].value {
            ValueEntry::Put(value) => assert_eq!(value.as_ref(), b"from-rank-0"),
            ValueEntry::Tombstone => panic!("unexpected tombstone"),
        }
    }

    #[test]
    fn user_scan_filters_tombstones_and_emits_latest_visible_per_key() {
        let sources = vec![
            SourceCursor::from_entries(
                vec![
                    put(6, b"a", b"a6"),
                    put(2, b"b", b"b2"),
                    put(1, b"c", b"c1"),
                ],
                0,
            ),
            SourceCursor::from_entries(vec![tombstone(7, b"b"), put(5, b"c", b"c5")], 1),
        ];
        let mut iter = MergeIterator::new(MergeMode::UserScan, sources);

        let entries = match collect_entries(&mut iter) {
            Ok(entries) => entries,
            Err(err) => panic!("unexpected merge error: {err}"),
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key.as_ref(), b"a");
        assert_eq!(entries[1].key.as_ref(), b"c");
        assert_eq!(entries[1].seq, 5);
        match &entries[1].value {
            ValueEntry::Put(value) => assert_eq!(value.as_ref(), b"c5"),
            ValueEntry::Tombstone => panic!("unexpected tombstone"),
        }
    }
}
