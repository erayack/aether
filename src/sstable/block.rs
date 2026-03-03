use std::mem::size_of;

use bytes::Bytes;

use crate::{
    error::Result,
    types::{InternalEntry, ValueEntry},
};

use super::BlockEncodingKind;

const BLOCK_ENTRY_COUNT_LEN: usize = size_of::<u32>();
const KEY_LEN_LEN: usize = size_of::<u32>();
const SEQ_LEN: usize = size_of::<u64>();
const ENTRY_KIND_LEN: usize = size_of::<u8>();
const VALUE_LEN_LEN: usize = size_of::<u32>();
const ENTRY_HEADER_LEN: usize = KEY_LEN_LEN + SEQ_LEN + ENTRY_KIND_LEN + VALUE_LEN_LEN;

const SHARED_PREFIX_LEN_LEN: usize = size_of::<u32>();
const SUFFIX_LEN_LEN: usize = size_of::<u32>();
const RESTART_OFFSET_LEN: usize = size_of::<u32>();
const RESTART_COUNT_LEN: usize = size_of::<u32>();
const PREFIX_ENTRY_HEADER_LEN: usize =
    SHARED_PREFIX_LEN_LEN + SUFFIX_LEN_LEN + SEQ_LEN + ENTRY_KIND_LEN + VALUE_LEN_LEN;

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

// ---------------------------------------------------------------------------
// Prefix-delta block builder (v2)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(super) struct PrefixBlockBuilder {
    entries: Vec<InternalEntry>,
    restart_interval: u16,
    serialized_bytes: usize,
}

impl PrefixBlockBuilder {
    #[must_use]
    pub(super) fn new(restart_interval: u16) -> Self {
        Self {
            entries: Vec::new(),
            restart_interval: restart_interval.max(1),
            serialized_bytes: BLOCK_ENTRY_COUNT_LEN + RESTART_COUNT_LEN,
        }
    }

    #[must_use]
    pub(super) const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub(super) fn encoded_len_with(&self, entry: &InternalEntry) -> usize {
        let next_index = self.entries.len();
        let is_restart = next_index.is_multiple_of(self.restart_interval as usize);
        let shared = if is_restart {
            0
        } else {
            self.entries
                .last()
                .map_or(0, |prev| common_prefix_len(&prev.key, &entry.key))
        };
        let suffix_len = entry.key.len().saturating_sub(shared);
        let value_len = match &entry.value {
            ValueEntry::Put(v) => v.len(),
            ValueEntry::Tombstone => 0,
        };
        let entry_bytes = PREFIX_ENTRY_HEADER_LEN
            .saturating_add(suffix_len)
            .saturating_add(value_len);
        let restart_bytes = if is_restart { RESTART_OFFSET_LEN } else { 0 };
        self.serialized_bytes
            .saturating_add(entry_bytes)
            .saturating_add(restart_bytes)
    }

    pub(super) fn push(&mut self, entry: InternalEntry) {
        let next_index = self.entries.len();
        let is_restart = next_index.is_multiple_of(self.restart_interval as usize);
        let shared = if is_restart {
            0
        } else {
            self.entries
                .last()
                .map_or(0, |prev| common_prefix_len(&prev.key, &entry.key))
        };
        let suffix_len = entry.key.len().saturating_sub(shared);
        let value_len = match &entry.value {
            ValueEntry::Put(v) => v.len(),
            ValueEntry::Tombstone => 0,
        };
        let entry_bytes = PREFIX_ENTRY_HEADER_LEN
            .saturating_add(suffix_len)
            .saturating_add(value_len);
        let restart_bytes = if is_restart { RESTART_OFFSET_LEN } else { 0 };
        self.serialized_bytes = self
            .serialized_bytes
            .saturating_add(entry_bytes)
            .saturating_add(restart_bytes);
        self.entries.push(entry);
    }

    #[must_use]
    pub(super) fn last_key(&self) -> Option<&[u8]> {
        self.entries.last().map(|entry| entry.key.as_ref())
    }

    pub(super) fn finish(self) -> Result<Vec<u8>> {
        encode_prefix_block_entries(&self.entries, self.restart_interval)
    }
}

// ---------------------------------------------------------------------------
// Unified block encoder enum
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(super) enum BlockEncoder {
    Plain(BlockBuilder),
    Prefix(PrefixBlockBuilder),
}

impl BlockEncoder {
    #[must_use]
    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Plain(b) => b.is_empty(),
            Self::Prefix(b) => b.is_empty(),
        }
    }

    #[must_use]
    pub(super) fn encoded_len_with(&self, entry: &InternalEntry) -> usize {
        match self {
            Self::Plain(b) => b.encoded_len_with(entry),
            Self::Prefix(b) => b.encoded_len_with(entry),
        }
    }

    pub(super) fn push(&mut self, entry: InternalEntry) {
        match self {
            Self::Plain(b) => b.push(entry),
            Self::Prefix(b) => b.push(entry),
        }
    }

    #[must_use]
    pub(super) fn last_key(&self) -> Option<&[u8]> {
        match self {
            Self::Plain(b) => b.last_key(),
            Self::Prefix(b) => b.last_key(),
        }
    }

    pub(super) fn finish(self) -> Result<Vec<u8>> {
        match self {
            Self::Plain(b) => b.finish(),
            Self::Prefix(b) => b.finish(),
        }
    }
}

impl Default for BlockEncoder {
    fn default() -> Self {
        Self::Plain(BlockBuilder::new())
    }
}

// ---------------------------------------------------------------------------
// Dispatch helpers for decode / point-lookup
// ---------------------------------------------------------------------------

pub(super) fn decode_block(
    block: &[u8],
    encoding: BlockEncodingKind,
) -> Result<Vec<InternalEntry>> {
    match encoding {
        BlockEncodingKind::Plain => decode_block_entries(block),
        BlockEncodingKind::Prefix => decode_prefix_block_entries(block),
    }
}

pub(super) fn find_in_block_dispatch(
    block: &[u8],
    key: &[u8],
    encoding: BlockEncodingKind,
) -> Result<Option<InternalEntry>> {
    match encoding {
        BlockEncodingKind::Plain => find_in_block(block, key),
        BlockEncodingKind::Prefix => find_in_prefix_block(block, key),
    }
}

// ---------------------------------------------------------------------------
// Prefix shared-length helper
// ---------------------------------------------------------------------------

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// V1 (plain) block encoding
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// V2 (prefix-delta) block encoding / decoding
// ---------------------------------------------------------------------------

fn encode_prefix_block_entries(
    entries: &[InternalEntry],
    restart_interval: u16,
) -> Result<Vec<u8>> {
    let entry_count = u32::try_from(entries.len())
        .map_err(|_| invalid_input("SSTable block has too many entries"))?;
    let interval = (restart_interval.max(1)) as usize;

    let mut out = Vec::new();
    out.extend_from_slice(&entry_count.to_le_bytes());

    let mut restart_offsets: Vec<u32> = Vec::new();
    let mut prev_key: &[u8] = &[];

    for (i, entry) in entries.iter().enumerate() {
        let is_restart = i % interval == 0;
        if is_restart {
            let data_offset = u32::try_from(out.len().saturating_sub(BLOCK_ENTRY_COUNT_LEN))
                .map_err(|_| invalid_input("SSTable prefix block data offset overflow"))?;
            restart_offsets.push(data_offset);
        }

        let shared = if is_restart {
            0
        } else {
            common_prefix_len(prev_key, &entry.key)
        };
        let suffix = &entry.key[shared..];

        encode_prefix_entry(&mut out, shared, suffix, entry)?;
        prev_key = &entry.key;
    }

    let restart_count = u32::try_from(restart_offsets.len())
        .map_err(|_| invalid_input("SSTable prefix block has too many restart points"))?;
    for offset in &restart_offsets {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    out.extend_from_slice(&restart_count.to_le_bytes());

    Ok(out)
}

fn encode_prefix_entry(
    buf: &mut Vec<u8>,
    shared: usize,
    suffix: &[u8],
    entry: &InternalEntry,
) -> Result<()> {
    let shared_u32 = u32::try_from(shared)
        .map_err(|_| invalid_input("SSTable shared prefix length exceeds u32"))?;
    let suffix_len = u32::try_from(suffix.len())
        .map_err(|_| invalid_input("SSTable key suffix exceeds u32 length"))?;

    let (entry_kind, value_slice): (u8, &[u8]) = match &entry.value {
        ValueEntry::Put(value) => (ENTRY_KIND_PUT, value.as_ref()),
        ValueEntry::Tombstone => (ENTRY_KIND_TOMBSTONE, &[]),
    };
    let value_len = u32::try_from(value_slice.len())
        .map_err(|_| invalid_input("SSTable value exceeds u32 length"))?;

    buf.extend_from_slice(&shared_u32.to_le_bytes());
    buf.extend_from_slice(&suffix_len.to_le_bytes());
    buf.extend_from_slice(suffix);
    buf.extend_from_slice(&entry.seq.to_le_bytes());
    buf.push(entry_kind);
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(value_slice);

    Ok(())
}

fn parse_restart_trailer(block: &[u8]) -> Result<(usize, Vec<u32>)> {
    if block.len() < RESTART_COUNT_LEN {
        return Err(invalid_data("SSTable prefix block too short for restart trailer").into());
    }

    let rc_offset = block.len().saturating_sub(RESTART_COUNT_LEN);
    let mut cursor = rc_offset;
    let restart_count = read_u32(block, &mut cursor)?;
    let rc = usize::try_from(restart_count)
        .map_err(|_| invalid_data("SSTable restart count not supported on this platform"))?;

    let trailer_len = RESTART_COUNT_LEN.saturating_add(rc.saturating_mul(RESTART_OFFSET_LEN));
    if block.len() < trailer_len {
        return Err(invalid_data("SSTable prefix block too short for restart offsets").into());
    }

    let offsets_start = block.len().saturating_sub(trailer_len);
    let mut oc = offsets_start;
    let mut offsets = Vec::with_capacity(rc);
    for _ in 0..rc {
        offsets.push(read_u32(block, &mut oc)?);
    }

    let data_end = offsets_start;
    Ok((data_end, offsets))
}

pub(super) fn decode_prefix_block_entries(block: &[u8]) -> Result<Vec<InternalEntry>> {
    let (data_end, restart_offsets) = parse_restart_trailer(block)?;

    let mut cursor = 0_usize;
    let entry_count = read_u32(block, &mut cursor)?;
    let count = usize::try_from(entry_count)
        .map_err(|_| invalid_data("SSTable block entry count is not supported on this platform"))?;

    validate_restart_offsets(count, data_end, &restart_offsets)?;

    let mut entries = Vec::with_capacity(count);
    let mut prev_key = Vec::new();
    let mut decoded_entry_starts = Vec::with_capacity(count);

    for _ in 0..count {
        decoded_entry_starts.push(cursor.saturating_sub(BLOCK_ENTRY_COUNT_LEN));
        let entry = decode_prefix_entry(block, &mut cursor, &mut prev_key)?;
        entries.push(entry);
    }

    if cursor != data_end {
        return Err(invalid_data("SSTable prefix block has unexpected data length").into());
    }

    validate_restart_offsets_against_entries(&restart_offsets, &decoded_entry_starts)?;

    Ok(entries)
}

fn find_in_prefix_block(block: &[u8], key: &[u8]) -> Result<Option<InternalEntry>> {
    let (data_end, restart_offsets) = parse_restart_trailer(block)?;

    let mut cursor = 0_usize;
    let entry_count = read_u32(block, &mut cursor)?;
    let count = usize::try_from(entry_count)
        .map_err(|_| invalid_data("SSTable block entry count is not supported on this platform"))?;

    validate_restart_offsets(count, data_end, &restart_offsets)?;

    let mut prev_key = Vec::new();
    for _ in 0..count {
        let entry = decode_prefix_entry(block, &mut cursor, &mut prev_key)?;
        match entry.key.as_ref().cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(Some(entry)),
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }

    if cursor != data_end {
        return Err(invalid_data("SSTable prefix block has unexpected data length").into());
    }

    Ok(None)
}

fn validate_restart_offsets(count: usize, data_end: usize, restart_offsets: &[u32]) -> Result<()> {
    if count == 0 {
        if !restart_offsets.is_empty() {
            return Err(
                invalid_data("SSTable empty prefix block must not have restart offsets").into(),
            );
        }
        return Ok(());
    }

    if restart_offsets.is_empty() {
        return Err(invalid_data("SSTable prefix block must include restart offsets").into());
    }
    if restart_offsets.len() > count {
        return Err(invalid_data("SSTable prefix block has too many restart offsets").into());
    }

    let data_len = data_end.saturating_sub(BLOCK_ENTRY_COUNT_LEN);
    let mut previous = None;
    for (index, offset_u32) in restart_offsets.iter().copied().enumerate() {
        let offset = usize::try_from(offset_u32)
            .map_err(|_| invalid_data("SSTable restart offset not supported on this platform"))?;
        if index == 0 && offset != 0 {
            return Err(
                invalid_data("SSTable prefix block first restart offset must be zero").into(),
            );
        }
        if offset >= data_len {
            return Err(invalid_data("SSTable restart offset exceeds block data bounds").into());
        }
        if let Some(prev) = previous
            && offset <= prev
        {
            return Err(invalid_data(
                "SSTable prefix block restart offsets must be strictly increasing",
            )
            .into());
        }
        previous = Some(offset);
    }

    Ok(())
}

fn validate_restart_offsets_against_entries(
    restart_offsets: &[u32],
    decoded_entry_starts: &[usize],
) -> Result<()> {
    for restart in restart_offsets {
        let restart = usize::try_from(*restart)
            .map_err(|_| invalid_data("SSTable restart offset not supported on this platform"))?;
        if !decoded_entry_starts.contains(&restart) {
            return Err(invalid_data(
                "SSTable prefix block restart offset does not point to an entry",
            )
            .into());
        }
    }
    Ok(())
}

fn decode_prefix_entry(
    block: &[u8],
    cursor: &mut usize,
    prev_key: &mut Vec<u8>,
) -> Result<InternalEntry> {
    let shared_prefix_len = usize::try_from(read_u32(block, cursor)?)
        .map_err(|_| invalid_data("SSTable shared prefix length not supported on this platform"))?;
    let suffix_len = usize::try_from(read_u32(block, cursor)?)
        .map_err(|_| invalid_data("SSTable suffix length not supported on this platform"))?;
    let suffix = read_slice(block, cursor, suffix_len)?;

    if shared_prefix_len > prev_key.len() {
        return Err(invalid_data("SSTable prefix entry shared length exceeds previous key").into());
    }

    let mut full_key = Vec::with_capacity(shared_prefix_len.saturating_add(suffix_len));
    full_key.extend_from_slice(&prev_key[..shared_prefix_len]);
    full_key.extend_from_slice(suffix);

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

    *prev_key = full_key.clone();

    Ok(InternalEntry {
        seq,
        key: Bytes::from(full_key),
        value: value_entry,
    })
}

// ---------------------------------------------------------------------------
// Prefix block cursor (lazy incremental decode)
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub(super) struct PrefixBlockCursor {
    block: Vec<u8>,
    entry_count: usize,
    data_end: usize,
    cursor: usize,
    entries_emitted: usize,
    prev_key: Vec<u8>,
}

#[allow(dead_code)]
impl PrefixBlockCursor {
    pub(super) fn new(block: Vec<u8>) -> Result<Self> {
        let (data_end, _restart_offsets) = parse_restart_trailer(&block)?;

        let mut cursor = 0_usize;
        let entry_count_u32 = read_u32(&block, &mut cursor)?;
        let entry_count = usize::try_from(entry_count_u32).map_err(|_| {
            invalid_data("SSTable block entry count is not supported on this platform")
        })?;

        Ok(Self {
            block,
            entry_count,
            data_end,
            cursor,
            entries_emitted: 0,
            prev_key: Vec::new(),
        })
    }

    pub(super) fn next_entry(&mut self) -> Result<Option<InternalEntry>> {
        if self.entries_emitted >= self.entry_count {
            if self.cursor != self.data_end {
                return Err(invalid_data("SSTable prefix block has unexpected data length").into());
            }
            return Ok(None);
        }
        let entry = decode_prefix_entry(&self.block, &mut self.cursor, &mut self.prev_key)?;
        self.entries_emitted = self.entries_emitted.saturating_add(1);
        Ok(Some(entry))
    }
}

// ---------------------------------------------------------------------------
// V1 helpers
// ---------------------------------------------------------------------------

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

    use super::{
        BlockBuilder, BlockEncoder, PrefixBlockBuilder, PrefixBlockCursor, decode_block,
        decode_block_entries, find_in_block, find_in_block_dispatch,
    };
    use crate::sstable::BlockEncodingKind;
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

    // -----------------------------------------------------------------------
    // Prefix block tests
    // -----------------------------------------------------------------------

    #[test]
    fn prefix_block_roundtrip_preserves_entries() {
        let entries = vec![
            put(1, b"prefix_aaa", b"v1"),
            put(2, b"prefix_aab", b"v2"),
            put(3, b"prefix_bbb", b"v3"),
            delete(4, b"prefix_bbc"),
        ];
        let mut builder = PrefixBlockBuilder::new(2);
        for e in &entries {
            builder.push(e.clone());
        }
        let block = match builder.finish() {
            Ok(b) => b,
            Err(err) => panic!("prefix encode should work: {err}"),
        };
        let decoded = match decode_block(block.as_ref(), BlockEncodingKind::Prefix) {
            Ok(d) => d,
            Err(err) => panic!("prefix decode should work: {err}"),
        };
        assert_eq!(decoded, entries);
    }

    #[test]
    fn prefix_block_find_returns_correct_entry() {
        let entries = vec![
            put(1, b"key_alpha", b"v1"),
            put(2, b"key_beta", b"v2"),
            put(3, b"key_gamma", b"v3"),
        ];
        let mut builder = PrefixBlockBuilder::new(16);
        for e in &entries {
            builder.push(e.clone());
        }
        let block = match builder.finish() {
            Ok(b) => b,
            Err(err) => panic!("encode should work: {err}"),
        };

        let found = match find_in_block_dispatch(&block, b"key_beta", BlockEncodingKind::Prefix) {
            Ok(f) => f,
            Err(err) => panic!("find should work: {err}"),
        };
        assert_eq!(found, Some(entries[1].clone()));

        let missing = match find_in_block_dispatch(&block, b"key_aaa", BlockEncodingKind::Prefix) {
            Ok(m) => m,
            Err(err) => panic!("find should work: {err}"),
        };
        assert!(missing.is_none());
    }

    #[test]
    fn prefix_block_cursor_iterates_all_entries() {
        let entries = vec![
            put(1, b"aa", b"1"),
            put(2, b"ab", b"2"),
            put(3, b"ba", b"3"),
        ];
        let mut builder = PrefixBlockBuilder::new(2);
        for e in &entries {
            builder.push(e.clone());
        }
        let block = match builder.finish() {
            Ok(b) => b,
            Err(err) => panic!("encode should work: {err}"),
        };

        let mut cursor = match PrefixBlockCursor::new(block) {
            Ok(c) => c,
            Err(err) => panic!("cursor init should work: {err}"),
        };

        let mut collected = Vec::new();
        loop {
            match cursor.next_entry() {
                Ok(Some(entry)) => collected.push(entry),
                Ok(None) => break,
                Err(err) => panic!("cursor next should work: {err}"),
            }
        }
        assert_eq!(collected, entries);
    }

    #[test]
    fn prefix_block_restart_stores_full_key() {
        let entries = vec![
            put(1, b"shared_a", b"v1"),
            put(2, b"shared_b", b"v2"),
            put(3, b"shared_c", b"v3"),
        ];
        // restart_interval = 1 means every entry is a restart (full key)
        let mut builder = PrefixBlockBuilder::new(1);
        for e in &entries {
            builder.push(e.clone());
        }
        let block = match builder.finish() {
            Ok(b) => b,
            Err(err) => panic!("encode should work: {err}"),
        };
        let decoded = match decode_block(&block, BlockEncodingKind::Prefix) {
            Ok(d) => d,
            Err(err) => panic!("decode should work: {err}"),
        };
        assert_eq!(decoded, entries);
    }

    #[test]
    fn block_encoder_enum_dispatches_correctly() {
        let entries = vec![put(1, b"x", b"1"), put(2, b"y", b"2")];

        // Plain variant
        let mut enc = BlockEncoder::Plain(BlockBuilder::new());
        for e in &entries {
            enc.push(e.clone());
        }
        assert!(!enc.is_empty());
        assert_eq!(enc.last_key(), Some(b"y".as_ref()));
        let block = match enc.finish() {
            Ok(b) => b,
            Err(err) => panic!("plain finish should work: {err}"),
        };
        let decoded = match decode_block(&block, BlockEncodingKind::Plain) {
            Ok(d) => d,
            Err(err) => panic!("plain decode should work: {err}"),
        };
        assert_eq!(decoded, entries);

        // Prefix variant
        let mut enc = BlockEncoder::Prefix(PrefixBlockBuilder::new(16));
        for e in &entries {
            enc.push(e.clone());
        }
        assert!(!enc.is_empty());
        assert_eq!(enc.last_key(), Some(b"y".as_ref()));
        let block = match enc.finish() {
            Ok(b) => b,
            Err(err) => panic!("prefix finish should work: {err}"),
        };
        let decoded = match decode_block(&block, BlockEncodingKind::Prefix) {
            Ok(d) => d,
            Err(err) => panic!("prefix decode should work: {err}"),
        };
        assert_eq!(decoded, entries);
    }

    #[test]
    fn prefix_decode_rejects_invalid_restart_offset() {
        let entries = vec![put(1, b"key_a", b"v1"), put(2, b"key_b", b"v2")];
        let mut builder = PrefixBlockBuilder::new(16);
        for e in &entries {
            builder.push(e.clone());
        }
        let mut block = match builder.finish() {
            Ok(b) => b,
            Err(err) => panic!("encode should work: {err}"),
        };

        // Corrupt first restart offset from 0 to 1.
        let restart_offset_pos = block
            .len()
            .saturating_sub(super::RESTART_COUNT_LEN + super::RESTART_OFFSET_LEN);
        block[restart_offset_pos..restart_offset_pos + super::RESTART_OFFSET_LEN]
            .copy_from_slice(&1_u32.to_le_bytes());

        let decoded = decode_block(&block, BlockEncodingKind::Prefix);
        assert!(decoded.is_err());
    }
}
