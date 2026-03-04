use std::{
    ops::Bound,
    path::{Path, PathBuf},
    sync::Arc,
};

use snap::raw::Decoder as SnappyDecoder;

use crate::{
    error::Result,
    types::{InternalEntry, Key, ScanBounds},
};

use super::{
    BlockEncodingKind, DATA_BLOCK_HEADER_LEN, FOOTER_TRAILER_LEN, FooterCompressionCodec,
    SST_MAGIC, SST_VERSION, SsTableMeta,
    block::{decode_block, find_in_block_dispatch},
    bloom::BloomFilter,
    decode_data_block_header, decode_footer, decode_footer_trailer, decode_index_entries,
    invalid_data,
    io::{ReadBackend, ReadBytes},
};

pub struct SsTableReader {
    file_path: PathBuf,
    backend: Arc<ReadBackend>,
    meta: SsTableMeta,
    index: Vec<super::IndexEntry>,
    bloom: BloomFilter,
    block_encoding: BlockEncodingKind,
    compression_codec: FooterCompressionCodec,
}

pub struct SsTableRangeIter {
    backend: Arc<ReadBackend>,
    bounds: ScanBounds,
    visible_seq: u64,
    block_handles: Vec<super::BlockHandle>,
    block_index_cursor: usize,
    in_block_entries: Vec<InternalEntry>,
    in_block_cursor: usize,
    block_encoding: BlockEncodingKind,
    compression_codec: FooterCompressionCodec,
}

enum DecodedBlock<'a> {
    Borrowed(&'a [u8]),
    Owned(Arc<[u8]>),
}

impl DecodedBlock<'_> {
    #[must_use]
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes.as_ref(),
        }
    }
}

impl SsTableReader {
    /// Opens an existing `SSTable` and loads footer, index, and bloom metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if file IO fails, the footer format is invalid, or metadata mismatches.
    pub fn open(path: &Path, meta: SsTableMeta, enable_mmap_reads: bool) -> Result<Self> {
        let backend = Arc::new(ReadBackend::open(path, enable_mmap_reads)?);
        let file_len = backend.len();

        let trailer_len_u64 = u64::try_from(FOOTER_TRAILER_LEN)
            .map_err(|_| invalid_data("SSTable trailer size not supported"))?;
        if file_len < trailer_len_u64 {
            return Err(invalid_data("SSTable file is too short for footer trailer").into());
        }

        let trailer = backend.read_range(
            file_len - trailer_len_u64,
            u32::try_from(FOOTER_TRAILER_LEN)
                .map_err(|_| invalid_data("SSTable trailer size not supported"))?,
        )?;

        let (footer_len_u32, version, magic) = decode_footer_trailer(&trailer)?;
        if magic != SST_MAGIC {
            return Err(invalid_data("SSTable magic mismatch").into());
        }
        if version != SST_VERSION {
            return Err(invalid_data("SSTable version is not supported").into());
        }

        let footer = read_footer(&backend, footer_len_u32)?;

        if footer.entry_count() == 0 {
            return Err(invalid_data("SSTable footer entry count must be positive").into());
        }
        if footer.min_key() != &meta.min_key
            || footer.max_key() != &meta.max_key
            || footer.max_seq() != meta.max_seq
        {
            return Err(invalid_data("SSTable metadata does not match footer contents").into());
        }

        let index_raw = read_handle_payload(
            &backend,
            footer.index_handle().offset,
            footer.index_handle().len,
        )?;
        let index = decode_index_entries(&index_raw)?;
        if index.is_empty() {
            return Err(invalid_data("SSTable index must contain at least one block").into());
        }

        let bloom_raw = read_handle_payload(
            &backend,
            footer.bloom_handle().offset,
            footer.bloom_handle().len,
        )?;
        let bloom = BloomFilter::decode(&bloom_raw)?;
        let metadata = footer.metadata();
        let block_encoding = metadata.block_encoding_kind;
        let compression_codec = metadata.default_compression_codec;

        Ok(Self {
            file_path: path.to_path_buf(),
            backend,
            meta,
            index,
            bloom,
            block_encoding,
            compression_codec,
        })
    }

    /// Loads table metadata from the `SSTable` footer.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the footer is invalid.
    pub fn load_meta(path: &Path, table_id: u64, level: u8) -> Result<SsTableMeta> {
        let backend = ReadBackend::open(path, false)?;
        let file_len = backend.len();
        let trailer_len_u64 = u64::try_from(FOOTER_TRAILER_LEN)
            .map_err(|_| invalid_data("SSTable trailer size not supported"))?;
        if file_len < trailer_len_u64 {
            return Err(invalid_data("SSTable file is too short for footer trailer").into());
        }

        let trailer = backend.read_range(
            file_len - trailer_len_u64,
            u32::try_from(FOOTER_TRAILER_LEN)
                .map_err(|_| invalid_data("SSTable trailer size not supported"))?,
        )?;
        let (footer_len_u32, version, magic) = decode_footer_trailer(&trailer)?;
        if magic != SST_MAGIC {
            return Err(invalid_data("SSTable magic mismatch").into());
        }
        if version != SST_VERSION {
            return Err(invalid_data("SSTable version is not supported").into());
        }

        let footer = read_footer(&backend, footer_len_u32)?;

        Ok(SsTableMeta {
            table_id,
            level,
            min_key: footer.min_key().clone(),
            max_key: footer.max_key().clone(),
            max_seq: footer.max_seq(),
        })
    }

    /// Looks up a key inside this `SSTable`.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or decoding the selected data block fails.
    pub fn get(&self, key: &[u8]) -> Result<Option<InternalEntry>> {
        if key < self.meta.min_key.as_ref() || key > self.meta.max_key.as_ref() {
            return Ok(None);
        }

        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        let Some(index_pos) = locate_candidate_block(&self.index, key) else {
            return Ok(None);
        };

        let handle = self.index[index_pos].handle;
        let block = read_data_block_payload(&self.backend, handle, self.compression_codec)?;
        let block_bytes = block.as_slice();
        find_in_block_dispatch(block_bytes, key, self.block_encoding)
    }

    /// Reads and decodes all entries in this table in key order.
    ///
    /// # Errors
    ///
    /// Returns an error if any block cannot be read or decoded.
    pub fn scan_all_entries(&self) -> Result<Vec<InternalEntry>> {
        let mut all_entries = Vec::new();

        for index_entry in &self.index {
            let block =
                read_data_block_payload(&self.backend, index_entry.handle, self.compression_codec)?;
            let block_bytes = block.as_slice();
            let mut entries = decode_block(block_bytes, self.block_encoding)?;
            all_entries.append(&mut entries);
        }

        Ok(all_entries)
    }

    /// Creates an iterator over entries within `bounds` visible up to `visible_seq`.
    ///
    /// # Errors
    ///
    /// Returns an error if selected table block reads fail.
    pub fn iter_range(&self, bounds: &ScanBounds, visible_seq: u64) -> Result<SsTableRangeIter> {
        if !table_overlaps_bounds(&self.meta, bounds) {
            return Ok(SsTableRangeIter {
                backend: Arc::clone(&self.backend),
                bounds: clone_scan_bounds(bounds),
                visible_seq,
                block_handles: Vec::new(),
                block_index_cursor: 0,
                in_block_entries: Vec::new(),
                in_block_cursor: 0,
                block_encoding: self.block_encoding,
                compression_codec: self.compression_codec,
            });
        }

        let block_handles = block_handles_for_bounds(&self.index, bounds);

        Ok(SsTableRangeIter {
            backend: Arc::clone(&self.backend),
            bounds: clone_scan_bounds(bounds),
            visible_seq,
            block_handles,
            block_index_cursor: 0,
            in_block_entries: Vec::new(),
            in_block_cursor: 0,
            block_encoding: self.block_encoding,
            compression_codec: self.compression_codec,
        })
    }

    /// Creates an iterator over all entries in this table.
    ///
    /// # Errors
    ///
    /// Returns an error if selected table block reads fail.
    pub fn iter_all(&self) -> Result<SsTableRangeIter> {
        self.iter_range(
            &ScanBounds {
                start: Bound::Unbounded,
                end: Bound::Unbounded,
            },
            u64::MAX,
        )
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.file_path
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn is_mmap_backend(&self) -> bool {
        self.backend.is_mmap()
    }
}

impl SsTableRangeIter {
    /// Advances this range iterator and returns the next matching entry.
    ///
    /// # Errors
    ///
    /// Returns an error if a selected table block cannot be read or decoded.
    pub fn next_entry(&mut self) -> Result<Option<InternalEntry>> {
        loop {
            while self.in_block_cursor < self.in_block_entries.len() {
                let index = self.in_block_cursor;
                self.in_block_cursor = self.in_block_cursor.saturating_add(1);
                let entry = self.in_block_entries[index].clone();
                if entry.seq > self.visible_seq {
                    continue;
                }
                if !key_in_bounds(entry.key.as_ref(), &self.bounds) {
                    continue;
                }
                return Ok(Some(entry));
            }

            if self.block_index_cursor >= self.block_handles.len() {
                return Ok(None);
            }

            let handle = self.block_handles[self.block_index_cursor];
            self.block_index_cursor = self.block_index_cursor.saturating_add(1);
            let block = read_data_block_payload(&self.backend, handle, self.compression_codec)?;
            let block_bytes = block.as_slice();
            self.in_block_entries = decode_block(block_bytes, self.block_encoding)?;
            self.in_block_cursor = 0;
        }
    }
}

fn locate_candidate_block(index_entries: &[super::IndexEntry], key: &[u8]) -> Option<usize> {
    match index_entries.binary_search_by(|entry| entry.last_key.as_ref().cmp(key)) {
        Ok(pos) => Some(pos),
        Err(pos) => {
            if pos < index_entries.len() {
                Some(pos)
            } else {
                None
            }
        }
    }
}

fn block_handles_for_bounds(
    index_entries: &[super::IndexEntry],
    bounds: &ScanBounds,
) -> Vec<super::BlockHandle> {
    if index_entries.is_empty() {
        return Vec::new();
    }

    let start_index = match &bounds.start {
        Bound::Unbounded => 0,
        Bound::Included(key) | Bound::Excluded(key) => {
            let Some(pos) = locate_candidate_block(index_entries, key.as_ref()) else {
                return Vec::new();
            };
            pos
        }
    };

    let end_index = match &bounds.end {
        Bound::Unbounded => index_entries.len().saturating_sub(1),
        Bound::Included(key) | Bound::Excluded(key) => {
            locate_candidate_block(index_entries, key.as_ref())
                .unwrap_or_else(|| index_entries.len().saturating_sub(1))
        }
    };

    if start_index > end_index {
        return Vec::new();
    }

    index_entries[start_index..=end_index]
        .iter()
        .map(|entry| entry.handle)
        .collect()
}

fn table_overlaps_bounds(meta: &SsTableMeta, bounds: &ScanBounds) -> bool {
    let above_start = match &bounds.start {
        Bound::Included(start) => meta.max_key.as_ref() >= start.as_ref(),
        Bound::Excluded(start) => meta.max_key.as_ref() > start.as_ref(),
        Bound::Unbounded => true,
    };
    let below_end = match &bounds.end {
        Bound::Included(end) => meta.min_key.as_ref() <= end.as_ref(),
        Bound::Excluded(end) => meta.min_key.as_ref() < end.as_ref(),
        Bound::Unbounded => true,
    };
    above_start && below_end
}

fn key_in_bounds(key: &[u8], bounds: &ScanBounds) -> bool {
    let above_start = match &bounds.start {
        Bound::Included(start) => key >= start.as_ref(),
        Bound::Excluded(start) => key > start.as_ref(),
        Bound::Unbounded => true,
    };
    let below_end = match &bounds.end {
        Bound::Included(end) => key <= end.as_ref(),
        Bound::Excluded(end) => key < end.as_ref(),
        Bound::Unbounded => true,
    };
    above_start && below_end
}

fn clone_bound(bound: &Bound<Key>) -> Bound<Key> {
    match bound {
        Bound::Included(key) => Bound::Included(key.clone()),
        Bound::Excluded(key) => Bound::Excluded(key.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn clone_scan_bounds(bounds: &ScanBounds) -> ScanBounds {
    ScanBounds {
        start: clone_bound(&bounds.start),
        end: clone_bound(&bounds.end),
    }
}

fn read_footer(backend: &ReadBackend, footer_len_u32: u32) -> Result<super::TableFooter> {
    let footer_len = usize::try_from(footer_len_u32)
        .map_err(|_| invalid_data("SSTable footer size not supported on this platform"))?;
    let trailer_len_u64 = u64::try_from(FOOTER_TRAILER_LEN)
        .map_err(|_| invalid_data("SSTable trailer size not supported"))?;
    let file_len = backend.len();
    let footer_total = trailer_len_u64
        .checked_add(u64::from(footer_len_u32))
        .ok_or_else(|| invalid_data("SSTable footer length overflow"))?;
    if footer_total > file_len {
        return Err(invalid_data("SSTable footer exceeds file length").into());
    }

    let footer_offset = file_len - footer_total;
    let footer_raw = backend.read_range(
        footer_offset,
        u32::try_from(footer_len)
            .map_err(|_| invalid_data("SSTable footer size not supported on this platform"))?,
    )?;
    decode_footer(&footer_raw)
}

fn read_handle_payload(backend: &ReadBackend, offset: u64, len: u32) -> Result<Vec<u8>> {
    backend.read_range(offset, len)
}

fn read_data_block_payload(
    backend: &ReadBackend,
    handle: super::BlockHandle,
    compression_codec: FooterCompressionCodec,
) -> Result<DecodedBlock<'_>> {
    let raw = backend.read_range_bytes(handle.offset, handle.len)?;
    decode_data_block_payload(raw, compression_codec)
}

fn decode_data_block_payload(
    raw_block: ReadBytes<'_>,
    compression_codec: FooterCompressionCodec,
) -> Result<DecodedBlock<'_>> {
    if raw_block.as_slice().len() < DATA_BLOCK_HEADER_LEN {
        return Err(invalid_data("SSTable v2 block is smaller than header").into());
    }
    let (is_compressed, raw_len_u32, stored_len_u32) =
        decode_data_block_header(&raw_block.as_slice()[..DATA_BLOCK_HEADER_LEN])?;
    let stored_len = usize::try_from(stored_len_u32)
        .map_err(|_| invalid_data("SSTable block stored length not supported on this platform"))?;
    let raw_len = usize::try_from(raw_len_u32)
        .map_err(|_| invalid_data("SSTable block raw length not supported on this platform"))?;
    if raw_len > super::io::MAX_SECTION_BYTES {
        return Err(invalid_data("SSTable block raw length exceeds safety limit").into());
    }

    match raw_block {
        ReadBytes::Borrowed(raw) => {
            let payload = raw
                .get(DATA_BLOCK_HEADER_LEN..)
                .ok_or_else(|| invalid_data("SSTable v2 block payload truncated"))?;
            if payload.len() != stored_len {
                return Err(invalid_data("SSTable v2 block length metadata mismatch").into());
            }

            if !is_compressed {
                if raw_len != stored_len {
                    return Err(
                        invalid_data("SSTable v2 raw block length metadata mismatch").into(),
                    );
                }
                return Ok(DecodedBlock::Borrowed(payload));
            }

            let decoded = match compression_codec {
                FooterCompressionCodec::None => {
                    return Err(invalid_data(
                        "SSTable v2 block is marked compressed with codec none",
                    )
                    .into());
                }
                FooterCompressionCodec::Snappy => SnappyDecoder::new()
                    .decompress_vec(payload)
                    .map_err(|_| invalid_data("SSTable snappy block decompression failed"))?,
                FooterCompressionCodec::Zstd => zstd::bulk::decompress(payload, raw_len)
                    .map_err(|_| invalid_data("SSTable zstd block decompression failed"))?,
            };
            if decoded.len() != raw_len {
                return Err(invalid_data("SSTable v2 block decoded length mismatch").into());
            }
            Ok(DecodedBlock::Owned(Arc::<[u8]>::from(decoded)))
        }
        ReadBytes::Owned(raw) => {
            let payload = raw
                .get(DATA_BLOCK_HEADER_LEN..)
                .ok_or_else(|| invalid_data("SSTable v2 block payload truncated"))?;
            if payload.len() != stored_len {
                return Err(invalid_data("SSTable v2 block length metadata mismatch").into());
            }

            if !is_compressed {
                if raw_len != stored_len {
                    return Err(
                        invalid_data("SSTable v2 raw block length metadata mismatch").into(),
                    );
                }
                return Ok(DecodedBlock::Owned(Arc::<[u8]>::from(payload.to_vec())));
            }

            let decoded = match compression_codec {
                FooterCompressionCodec::None => {
                    return Err(invalid_data(
                        "SSTable v2 block is marked compressed with codec none",
                    )
                    .into());
                }
                FooterCompressionCodec::Snappy => SnappyDecoder::new()
                    .decompress_vec(payload)
                    .map_err(|_| invalid_data("SSTable snappy block decompression failed"))?,
                FooterCompressionCodec::Zstd => zstd::bulk::decompress(payload, raw_len)
                    .map_err(|_| invalid_data("SSTable zstd block decompression failed"))?,
            };
            if decoded.len() != raw_len {
                return Err(invalid_data("SSTable v2 block decoded length mismatch").into());
            }
            Ok(DecodedBlock::Owned(Arc::<[u8]>::from(decoded)))
        }
    }
}

#[cfg(test)]
mod tests {
    use snap::raw::Encoder as SnappyEncoder;

    use super::super::encode_data_block_header;
    use super::{DecodedBlock, FooterCompressionCodec, ReadBytes, decode_data_block_payload};

    #[test]
    fn decode_uncompressed_borrowed_block_returns_borrowed_payload() {
        let payload = b"hello-zero-copy";
        let payload_len_u32 = match u32::try_from(payload.len()) {
            Ok(len) => len,
            Err(err) => panic!("payload length must fit in u32: {err}"),
        };
        let mut raw = Vec::new();
        raw.extend_from_slice(&encode_data_block_header(
            false,
            payload_len_u32,
            payload_len_u32,
        ));
        raw.extend_from_slice(payload);

        let decoded = match decode_data_block_payload(
            ReadBytes::Borrowed(&raw),
            FooterCompressionCodec::None,
        ) {
            Ok(decoded) => decoded,
            Err(err) => panic!("decode should succeed: {err}"),
        };

        match decoded {
            DecodedBlock::Borrowed(bytes) => assert_eq!(bytes, payload),
            DecodedBlock::Owned(_) => {
                panic!("expected borrowed payload for uncompressed mmap-like block")
            }
        }
    }

    #[test]
    fn decode_compressed_borrowed_block_returns_owned_payload() {
        let payload = b"hello-compressed-zero-copy";
        let payload_len_u32 = match u32::try_from(payload.len()) {
            Ok(len) => len,
            Err(err) => panic!("payload length must fit in u32: {err}"),
        };
        let compressed = match SnappyEncoder::new().compress_vec(payload) {
            Ok(compressed) => compressed,
            Err(err) => panic!("snappy compression should succeed: {err}"),
        };
        let compressed_len_u32 = match u32::try_from(compressed.len()) {
            Ok(len) => len,
            Err(err) => panic!("compressed length must fit in u32: {err}"),
        };
        let mut raw = Vec::new();
        raw.extend_from_slice(&encode_data_block_header(
            true,
            payload_len_u32,
            compressed_len_u32,
        ));
        raw.extend_from_slice(&compressed);

        let decoded = match decode_data_block_payload(
            ReadBytes::Borrowed(&raw),
            FooterCompressionCodec::Snappy,
        ) {
            Ok(decoded) => decoded,
            Err(err) => panic!("decode should succeed: {err}"),
        };

        match decoded {
            DecodedBlock::Borrowed(_) => panic!("expected owned payload for compressed block"),
            DecodedBlock::Owned(bytes) => assert_eq!(bytes.as_ref(), payload),
        }
    }
}
