use std::{collections::BTreeMap, mem, mem::size_of};

use crate::types::{InternalEntry, Key, ValueEntry};

const ENTRY_OVERHEAD_BYTES: usize = 48;
const SEQUENCE_BYTES: usize = size_of::<u64>();

#[derive(Debug, Default)]
pub struct MemTable {
    pub approx_size_bytes: usize,
    // key -> latest version for current mutable generation
    entries: BTreeMap<Key, InternalEntry>,
}

impl MemTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn upsert(&mut self, entry: InternalEntry) {
        let new_size = estimated_entry_size_bytes(&entry);
        let key = entry.key.clone();
        if let Some(previous) = self.entries.insert(key, entry) {
            self.approx_size_bytes = self
                .approx_size_bytes
                .saturating_sub(estimated_entry_size_bytes(&previous));
        }
        self.approx_size_bytes = self.approx_size_bytes.saturating_add(new_size);
    }

    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&InternalEntry> {
        self.entries.get(key)
    }

    #[must_use]
    pub fn into_sorted_entries(self) -> Vec<InternalEntry> {
        self.entries.into_values().collect()
    }
}

#[derive(Debug, Default)]
pub struct MemTableSet {
    mutable: MemTable,
    immutable: Option<MemTable>,
}

#[allow(clippy::missing_const_for_fn)]
impl MemTableSet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn mutable_mut(&mut self) -> &mut MemTable {
        &mut self.mutable
    }

    #[must_use]
    pub fn mutable(&self) -> &MemTable {
        &self.mutable
    }

    #[must_use]
    pub fn immutable(&self) -> Option<&MemTable> {
        self.immutable.as_ref()
    }

    #[must_use]
    pub fn has_immutable(&self) -> bool {
        self.immutable.is_some()
    }

    /// Rotates mutable into immutable when threshold is crossed.
    ///
    /// Returns true when rotation happened.
    pub fn maybe_rotate(&mut self, threshold_bytes: usize) -> bool {
        if self.mutable.approx_size_bytes < threshold_bytes {
            return false;
        }

        if self.immutable.is_some() {
            return false;
        }

        self.immutable = Some(mem::take(&mut self.mutable));
        true
    }

    /// Forces mutable -> immutable rotation if possible.
    ///
    /// Returns true when rotation happened.
    pub fn freeze_mutable(&mut self) -> bool {
        if self.mutable.is_empty() || self.immutable.is_some() {
            return false;
        }

        self.immutable = Some(mem::take(&mut self.mutable));
        true
    }

    #[must_use]
    pub fn take_immutable(&mut self) -> Option<MemTable> {
        self.immutable.take()
    }

    #[must_use]
    pub fn take_mutable(&mut self) -> MemTable {
        mem::take(&mut self.mutable)
    }

    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&InternalEntry> {
        if let Some(entry) = self.mutable.get(key) {
            return Some(entry);
        }

        self.immutable.as_ref().and_then(|table| table.get(key))
    }
}

#[allow(clippy::missing_const_for_fn)]
#[must_use]
fn estimated_entry_size_bytes(entry: &InternalEntry) -> usize {
    let value_len = match &entry.value {
        ValueEntry::Put(value) => value.len(),
        ValueEntry::Tombstone => 0,
    };

    entry
        .key
        .len()
        .saturating_add(value_len)
        .saturating_add(SEQUENCE_BYTES)
        .saturating_add(ENTRY_OVERHEAD_BYTES)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{ENTRY_OVERHEAD_BYTES, MemTable, MemTableSet, SEQUENCE_BYTES};
    use crate::types::{InternalEntry, ValueEntry};

    fn put_entry(seq: u64, key: &'static [u8], value: &'static [u8]) -> InternalEntry {
        InternalEntry {
            seq,
            key: Bytes::from_static(key),
            value: ValueEntry::Put(Bytes::from_static(value)),
        }
    }

    fn tombstone_entry(seq: u64, key: &'static [u8]) -> InternalEntry {
        InternalEntry {
            seq,
            key: Bytes::from_static(key),
            value: ValueEntry::Tombstone,
        }
    }

    fn expected_size(key_len: usize, value_len: usize) -> usize {
        key_len + value_len + ENTRY_OVERHEAD_BYTES + SEQUENCE_BYTES
    }

    #[test]
    fn upsert_replaces_existing_entry_and_updates_size() {
        let mut table = MemTable::new();
        table.upsert(put_entry(1, b"k", b"1234"));
        assert_eq!(table.approx_size_bytes, expected_size(1, 4));

        table.upsert(put_entry(2, b"k", b"v"));
        assert_eq!(table.approx_size_bytes, expected_size(1, 1));

        let Some(found) = table.get(b"k") else {
            panic!("missing key after upsert");
        };
        assert_eq!(found.seq, 2);
    }

    #[test]
    fn tombstone_contributes_no_value_bytes() {
        let mut table = MemTable::new();
        table.upsert(tombstone_entry(1, b"dead"));
        assert_eq!(table.approx_size_bytes, expected_size(4, 0));
    }

    #[test]
    fn sorted_entries_follow_key_order() {
        let mut table = MemTable::new();
        table.upsert(put_entry(1, b"b", b"2"));
        table.upsert(put_entry(2, b"a", b"1"));

        let entries = table.into_sorted_entries();
        assert_eq!(entries[0].key.as_ref(), b"a");
        assert_eq!(entries[1].key.as_ref(), b"b");
    }

    #[test]
    fn get_prefers_mutable_over_immutable() {
        let mut set = MemTableSet::new();
        set.mutable_mut().upsert(put_entry(1, b"k", b"old"));
        let rotate_threshold = set.mutable().approx_size_bytes;
        let _ = set.maybe_rotate(rotate_threshold);

        set.mutable_mut().upsert(put_entry(2, b"k", b"new"));

        let Some(found) = set.get(b"k") else {
            panic!("key should exist in mutable table");
        };
        match &found.value {
            ValueEntry::Put(value) => assert_eq!(value.as_ref(), b"new"),
            ValueEntry::Tombstone => panic!("unexpected tombstone"),
        }
    }

    #[test]
    fn maybe_rotate_sets_immutable_once_threshold_crossed() {
        let mut set = MemTableSet::new();
        set.mutable_mut().upsert(put_entry(1, b"a", b"1"));

        assert!(set.immutable().is_none());

        let threshold = set.mutable().approx_size_bytes;
        let rotated = set.maybe_rotate(threshold);
        assert!(rotated);
        assert!(set.immutable().is_some());
        assert!(set.mutable().is_empty());
    }
}
