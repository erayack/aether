mod block;
mod bloom;
pub(crate) mod io;
pub mod reader;
pub mod writer;

use std::mem::size_of;

use bytes::Bytes;

use crate::{config::CompressionCodec, error::Result};

const SST_MAGIC: u64 = 0x4154_4852_5353_5431;
pub(crate) const SST_VERSION: u32 = 2;
const DEFAULT_BLOCK_TARGET_BYTES: usize = 32 * 1024;
const DEFAULT_PREFIX_RESTART_INTERVAL: u16 = 16;
pub(super) const DATA_BLOCK_HEADER_LEN: usize =
    size_of::<u8>() + size_of::<u32>() + size_of::<u32>();
const FOOTER_TRAILER_LEN: usize = size_of::<u32>() + size_of::<u32>() + size_of::<u64>();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlockHandle {
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexEntry {
    pub last_key: Bytes,
    pub handle: BlockHandle,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SsTableMeta {
    pub table_id: u64,
    pub level: u8,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub max_seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlockEncodingKind {
    Plain,
    Prefix,
}

impl BlockEncodingKind {
    const fn code(self) -> u8 {
        match self {
            Self::Plain => 0,
            Self::Prefix => 1,
        }
    }

    fn from_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::Plain),
            1 => Ok(Self::Prefix),
            _ => Err(invalid_data("SSTable footer has unknown block encoding kind").into()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FooterCompressionCodec {
    None,
    Snappy,
    Zstd,
}

impl FooterCompressionCodec {
    const fn code(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Snappy => 1,
            Self::Zstd => 2,
        }
    }

    fn from_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::None),
            1 => Ok(Self::Snappy),
            2 => Ok(Self::Zstd),
            _ => Err(invalid_data("SSTable footer has unknown compression codec").into()),
        }
    }
}

impl From<CompressionCodec> for FooterCompressionCodec {
    fn from(value: CompressionCodec) -> Self {
        match value {
            CompressionCodec::None => Self::None,
            CompressionCodec::Snappy => Self::Snappy,
            CompressionCodec::Zstd => Self::Zstd,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FooterMetadata {
    pub block_encoding_kind: BlockEncodingKind,
    pub default_compression_codec: FooterCompressionCodec,
    pub prefix_restart_interval: u16,
}

impl Default for FooterMetadata {
    fn default() -> Self {
        Self {
            block_encoding_kind: BlockEncodingKind::Plain,
            default_compression_codec: FooterCompressionCodec::None,
            prefix_restart_interval: DEFAULT_PREFIX_RESTART_INTERVAL,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableFooter {
    pub index: BlockHandle,
    pub bloom: BlockHandle,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub entry_count: u64,
    pub max_seq: u64,
    pub metadata: FooterMetadata,
}

impl TableFooter {
    const fn index_handle(&self) -> BlockHandle {
        self.index
    }

    const fn bloom_handle(&self) -> BlockHandle {
        self.bloom
    }

    const fn entry_count(&self) -> u64 {
        self.entry_count
    }

    const fn max_seq(&self) -> u64 {
        self.max_seq
    }

    const fn min_key(&self) -> &Bytes {
        &self.min_key
    }

    const fn max_key(&self) -> &Bytes {
        &self.max_key
    }

    const fn metadata(&self) -> FooterMetadata {
        self.metadata
    }
}

fn encode_index_entries(index_entries: &[IndexEntry]) -> Result<Vec<u8>> {
    let count = u32::try_from(index_entries.len())
        .map_err(|_| invalid_input("SSTable index has too many entries"))?;

    let mut out = Vec::new();
    out.extend_from_slice(&count.to_le_bytes());

    for entry in index_entries {
        let key_len = u32::try_from(entry.last_key.len())
            .map_err(|_| invalid_input("SSTable index key exceeds u32 length"))?;

        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(entry.last_key.as_ref());
        out.extend_from_slice(&entry.handle.offset.to_le_bytes());
        out.extend_from_slice(&entry.handle.len.to_le_bytes());
    }

    Ok(out)
}

fn decode_index_entries(raw: &[u8]) -> Result<Vec<IndexEntry>> {
    let mut cursor = 0_usize;
    let count_u32 = read_u32(raw, &mut cursor)?;
    let count = usize::try_from(count_u32)
        .map_err(|_| invalid_data("SSTable index count not supported on this platform"))?;

    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len_u32 = read_u32(raw, &mut cursor)?;
        let key_len = usize::try_from(key_len_u32)
            .map_err(|_| invalid_data("SSTable index key length not supported on this platform"))?;

        let key = read_slice(raw, &mut cursor, key_len)?;
        let offset = read_u64(raw, &mut cursor)?;
        let len = read_u32(raw, &mut cursor)?;

        out.push(IndexEntry {
            last_key: Bytes::copy_from_slice(key),
            handle: BlockHandle { offset, len },
        });
    }

    if cursor != raw.len() {
        return Err(invalid_data("SSTable index contains trailing bytes").into());
    }

    Ok(out)
}

fn encode_footer(footer: &TableFooter) -> Result<Vec<u8>> {
    let min_key_len = u32::try_from(footer.min_key.len())
        .map_err(|_| invalid_input("SSTable footer min key exceeds u32 length"))?;
    let max_key_len = u32::try_from(footer.max_key.len())
        .map_err(|_| invalid_input("SSTable footer max key exceeds u32 length"))?;

    let mut out = Vec::new();
    out.extend_from_slice(&footer.index.offset.to_le_bytes());
    out.extend_from_slice(&footer.index.len.to_le_bytes());
    out.extend_from_slice(&footer.bloom.offset.to_le_bytes());
    out.extend_from_slice(&footer.bloom.len.to_le_bytes());
    out.extend_from_slice(&footer.entry_count.to_le_bytes());
    out.extend_from_slice(&footer.max_seq.to_le_bytes());
    out.extend_from_slice(&min_key_len.to_le_bytes());
    out.extend_from_slice(footer.min_key.as_ref());
    out.extend_from_slice(&max_key_len.to_le_bytes());
    out.extend_from_slice(footer.max_key.as_ref());
    out.push(footer.metadata.block_encoding_kind.code());
    out.push(footer.metadata.default_compression_codec.code());
    out.extend_from_slice(&footer.metadata.prefix_restart_interval.to_le_bytes());
    Ok(out)
}

fn decode_footer(raw: &[u8]) -> Result<TableFooter> {
    let mut cursor = 0_usize;

    let index_offset = read_u64(raw, &mut cursor)?;
    let index_len = read_u32(raw, &mut cursor)?;
    let bloom_offset = read_u64(raw, &mut cursor)?;
    let bloom_len = read_u32(raw, &mut cursor)?;
    let entry_count = read_u64(raw, &mut cursor)?;
    let max_seq = read_u64(raw, &mut cursor)?;
    let min_key_len_u32 = read_u32(raw, &mut cursor)?;
    let min_key_len = usize::try_from(min_key_len_u32).map_err(|_| {
        invalid_data("SSTable footer min key length not supported on this platform")
    })?;
    let min_key = read_slice(raw, &mut cursor, min_key_len)?;
    let max_key_len_u32 = read_u32(raw, &mut cursor)?;
    let max_key_len = usize::try_from(max_key_len_u32).map_err(|_| {
        invalid_data("SSTable footer max key length not supported on this platform")
    })?;
    let max_key = read_slice(raw, &mut cursor, max_key_len)?;

    let block_encoding_kind = BlockEncodingKind::from_code(read_u8(raw, &mut cursor)?)?;
    let default_compression_codec = FooterCompressionCodec::from_code(read_u8(raw, &mut cursor)?)?;
    let prefix_restart_interval = read_u16(raw, &mut cursor)?;

    if cursor != raw.len() {
        return Err(invalid_data("SSTable footer contains trailing bytes").into());
    }

    Ok(TableFooter {
        index: BlockHandle {
            offset: index_offset,
            len: index_len,
        },
        bloom: BlockHandle {
            offset: bloom_offset,
            len: bloom_len,
        },
        min_key: Bytes::copy_from_slice(min_key),
        max_key: Bytes::copy_from_slice(max_key),
        entry_count,
        max_seq,
        metadata: FooterMetadata {
            block_encoding_kind,
            default_compression_codec,
            prefix_restart_interval,
        },
    })
}

fn decode_footer_trailer(raw: &[u8]) -> Result<(u32, u32, u64)> {
    if raw.len() != FOOTER_TRAILER_LEN {
        return Err(invalid_data("SSTable footer trailer has invalid size").into());
    }

    let mut cursor = 0_usize;
    let footer_len = read_u32(raw, &mut cursor)?;
    let version = read_u32(raw, &mut cursor)?;
    let magic = read_u64(raw, &mut cursor)?;

    Ok((footer_len, version, magic))
}

fn encode_footer_trailer(footer_len: u32, version: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(FOOTER_TRAILER_LEN);
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&SST_MAGIC.to_le_bytes());
    out
}

pub(super) fn encode_data_block_header(
    is_compressed: bool,
    raw_len: u32,
    stored_len: u32,
) -> [u8; DATA_BLOCK_HEADER_LEN] {
    let mut out = [0_u8; DATA_BLOCK_HEADER_LEN];
    out[0] = u8::from(is_compressed);
    out[1..5].copy_from_slice(&raw_len.to_le_bytes());
    out[5..9].copy_from_slice(&stored_len.to_le_bytes());
    out
}

pub(super) fn decode_data_block_header(raw: &[u8]) -> Result<(bool, u32, u32)> {
    if raw.len() != DATA_BLOCK_HEADER_LEN {
        return Err(invalid_data("SSTable data block header has invalid size").into());
    }

    let is_compressed = match raw[0] {
        0 => false,
        1 => true,
        _ => return Err(invalid_data("SSTable data block header has unknown flag").into()),
    };
    let raw_len = u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]);
    let stored_len = u32::from_le_bytes([raw[5], raw[6], raw[7], raw[8]]);

    Ok((is_compressed, raw_len, stored_len))
}

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn read_u32(input: &[u8], cursor: &mut usize) -> std::io::Result<u32> {
    let end = cursor
        .checked_add(size_of::<u32>())
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable payload truncated while reading u32"))?;
    *cursor = end;

    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64(input: &[u8], cursor: &mut usize) -> std::io::Result<u64> {
    let end = cursor
        .checked_add(size_of::<u64>())
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable payload truncated while reading u64"))?;
    *cursor = end;

    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn read_u16(input: &[u8], cursor: &mut usize) -> std::io::Result<u16> {
    let end = cursor
        .checked_add(size_of::<u16>())
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable payload truncated while reading u16"))?;
    *cursor = end;

    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u8(input: &[u8], cursor: &mut usize) -> std::io::Result<u8> {
    let end = cursor
        .checked_add(size_of::<u8>())
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable payload truncated while reading u8"))?;
    *cursor = end;
    Ok(bytes[0])
}

fn read_slice<'a>(input: &'a [u8], cursor: &mut usize, len: usize) -> std::io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| invalid_data("SSTable decode cursor overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("SSTable payload field exceeds bounds"))?;
    *cursor = end;
    Ok(bytes)
}
