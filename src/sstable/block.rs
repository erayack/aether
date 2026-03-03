use std::mem::size_of;

use bytes::Bytes;

use crate::{
    error::Result,
    types::{InternalEntry, ValueEntry},
};

const BLOCK_ENTRY_COUNT_LEN: usize = size_of::<u32>();
const KEY_LEN_LEN: usize = size_of::<u32>();
const SEQ_LEN: usize = size_of::<u64>();
const ENTRY_KIND_LEN: usize = size_of::<u8>();
const VALUE_LEN_LEN: usize = size_of::<u32>();
const ENTRY_HEADER_LEN: usize = KEY_LEN_LEN + SEQ_LEN + ENTRY_KIND_LEN + VALUE_LEN_LEN;

const ENTRY_KIND_PUT: u8 = 1;
const ENTRY_KIND_TOMBSTONE: u8 = 2;

#[derive(Debug, Default)]
pub(super) struct BlockBuilder {
    entries: Vec<InternalEntry>,
    serialized_bytes: usize,
}

#[allow(clippy::missing_const_for_fn)]
impl BlockBuilder {
    #[must_use]
    pub(super) fn new() -> Self {
        Self {
            entries: Vec::new(),
            serialized_bytes: BLOCK_ENTRY_COUNT_LEN,
        }
    }

    #[must_use]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub(super) fn encoded_len_with(&self, entry: &InternalEntry) -> usize {
        self.serialized_bytes
            .saturating_add(encoded_entry_len(entry))
    }

    pub(super) fn push(&mut self, entry: InternalEntry) {
        self.serialized_bytes = self
            .serialized_bytes
            .saturating_add(encoded_entry_len(&entry));
        self.entries.push(entry);
    }

    #[must_use]
    pub(super) fn last_key(&self) -> Option<&[u8]> {
        self.entries.last().map(|entry| entry.key.as_ref())
    }

    pub(super) fn finish(self) -> Result<Vec<u8>> {
        encode_block_entries(&self.entries)
    }
}

fn encode_block_entries(entries: &[InternalEntry]) -> Result<Vec<u8>> {
    let entry_count = u32::try_from(entries.len())
        .map_err(|_| invalid_input("SSTable block has too many entries"))?;
    let mut out = Vec::with_capacity(
        BLOCK_ENTRY_COUNT_LEN + entries.iter().map(encoded_entry_len).sum::<usize>(),
    );
    out.extend_from_slice(&entry_count.to_le_bytes());

    for entry in entries {
        encode_entry(&mut out, entry)?;
    }

    Ok(out)
}

fn encode_entry(buf: &mut Vec<u8>, entry: &InternalEntry) -> Result<()> {
    let key_len = u32::try_from(entry.key.len())
        .map_err(|_| invalid_input("SSTable key exceeds u32 length"))?;

    let (entry_kind, value_slice): (u8, &[u8]) = match &entry.value {
        ValueEntry::Put(value) => (ENTRY_KIND_PUT, value.as_ref()),
        ValueEntry::Tombstone => (ENTRY_KIND_TOMBSTONE, &[]),
    };

    let value_len = u32::try_from(value_slice.len())
        .map_err(|_| invalid_input("SSTable value exceeds u32 length"))?;

    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(entry.key.as_ref());
    buf.extend_from_slice(&entry.seq.to_le_bytes());
    buf.push(entry_kind);
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(value_slice);

    Ok(())
}

pub(super) fn decode_block_entries(block: &[u8]) -> Result<Vec<InternalEntry>> {
    let mut cursor = 0_usize;
    let entry_count = read_u32(block, &mut cursor)?;
    let count = usize::try_from(entry_count)
        .map_err(|_| invalid_data("SSTable block entry count is not supported on this platform"))?;

    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(decode_entry(block, &mut cursor)?);
    }

    if cursor != block.len() {
        return Err(invalid_data("SSTable block has trailing bytes").into());
    }

    Ok(entries)
}

pub(super) fn find_in_block(block: &[u8], key: &[u8]) -> Result<Option<InternalEntry>> {
    let mut cursor = 0_usize;
    let entry_count = read_u32(block, &mut cursor)?;
    let count = usize::try_from(entry_count)
        .map_err(|_| invalid_data("SSTable block entry count is not supported on this platform"))?;

    for _ in 0..count {
        let entry = decode_entry(block, &mut cursor)?;
        match entry.key.as_ref().cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(Some(entry)),
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }

    if cursor != block.len() {
        return Err(invalid_data("SSTable block has trailing bytes").into());
    }

    Ok(None)
}

#[must_use]
#[allow(clippy::missing_const_for_fn)]
fn encoded_entry_len(entry: &InternalEntry) -> usize {
    let value_len = match &entry.value {
        ValueEntry::Put(value) => value.len(),
        ValueEntry::Tombstone => 0,
    };

    ENTRY_HEADER_LEN
        .saturating_add(entry.key.len())
        .saturating_add(value_len)
}

fn decode_entry(block: &[u8], cursor: &mut usize) -> Result<InternalEntry> {
    let key_len = usize::try_from(read_u32(block, cursor)?)
        .map_err(|_| invalid_data("SSTable key length not supported on this platform"))?;
    let key = read_slice(block, cursor, key_len)?;
    let seq = read_u64(block, cursor)?;
    let entry_kind = read_u8(block, cursor)?;
    let value_len = usize::try_from(read_u32(block, cursor)?)
        .map_err(|_| invalid_data("SSTable value length not supported on this platform"))?;
    let value = read_slice(block, cursor, value_len)?;

    let value_entry = match entry_kind {
        ENTRY_KIND_PUT => ValueEntry::Put(Bytes::copy_from_slice(value)),
        ENTRY_KIND_TOMBSTONE => {
            if !value.is_empty() {
                return Err(
                    invalid_data("SSTable tombstone entry must not include a value").into(),
                );
            }
            ValueEntry::Tombstone
        }
        _ => return Err(invalid_data("SSTable entry has unknown kind code").into()),
    };

    Ok(InternalEntry {
        seq,
        key: Bytes::copy_from_slice(key),
        value: value_entry,
    })
}

fn read_u32(input: &[u8], cursor: &mut usize) -> std::io::Result<u32> {
    let end = cursor
        .checked_add(KEY_LEN_LEN)
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable block truncated while reading u32"))?;
    *cursor = end;

    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64(input: &[u8], cursor: &mut usize) -> std::io::Result<u64> {
    let end = cursor
        .checked_add(SEQ_LEN)
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable block truncated while reading sequence"))?;
    *cursor = end;

    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn read_u8(input: &[u8], cursor: &mut usize) -> std::io::Result<u8> {
    let end = cursor
        .checked_add(ENTRY_KIND_LEN)
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable block truncated while reading entry kind"))?;
    *cursor = end;
    Ok(bytes[0])
}

fn read_slice<'a>(input: &'a [u8], cursor: &mut usize, len: usize) -> std::io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable block field length exceeds bounds"))?;
    *cursor = end;
    Ok(bytes)
}

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{BlockBuilder, decode_block_entries, find_in_block};
    use crate::types::{InternalEntry, ValueEntry};

    fn put(seq: u64, key: &'static [u8], value: &'static [u8]) -> InternalEntry {
        InternalEntry {
            seq,
            key: Bytes::from_static(key),
            value: ValueEntry::Put(Bytes::from_static(value)),
        }
    }

    fn delete(seq: u64, key: &'static [u8]) -> InternalEntry {
        InternalEntry {
            seq,
            key: Bytes::from_static(key),
            value: ValueEntry::Tombstone,
        }
    }

    #[test]
    fn block_roundtrip_preserves_entries() {
        let entries = vec![put(1, b"a", b"1"), delete(2, b"b")];
        let encoded = match super::encode_block_entries(&entries) {
            Ok(encoded) => encoded,
            Err(err) => panic!("encode should work: {err}"),
        };
        let decoded = match decode_block_entries(&encoded) {
            Ok(decoded) => decoded,
            Err(err) => panic!("decode should work: {err}"),
        };
        assert_eq!(decoded, entries);
    }

    #[test]
    fn find_in_block_short_circuits_on_sorted_keys() {
        let mut builder = BlockBuilder::new();
        builder.push(put(1, b"a", b"1"));
        builder.push(put(2, b"c", b"3"));
        builder.push(delete(3, b"d"));
        let block = match builder.finish() {
            Ok(block) => block,
            Err(err) => panic!("block should encode: {err}"),
        };

        let found = match find_in_block(&block, b"c") {
            Ok(found) => found,
            Err(err) => panic!("search should work: {err}"),
        };
        assert!(found.is_some());

        let missing = match find_in_block(&block, b"b") {
            Ok(missing) => missing,
            Err(err) => panic!("search should work: {err}"),
        };
        assert!(missing.is_none());
    }
}
