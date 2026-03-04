use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use bytes::Bytes;
use snap::raw::Encoder as SnappyEncoder;

use crate::{error::Result, types::InternalEntry};

use super::{
    BlockEncodingKind, BlockHandle, DATA_BLOCK_HEADER_LEN, DEFAULT_BLOCK_TARGET_BYTES,
    FooterCompressionCodec, FooterMetadata, IndexEntry, SST_VERSION, SsTableMeta, TableFooter,
    block::{BlockBuilder, BlockEncoder, PrefixBlockBuilder},
    bloom::BloomFilterBuilder,
    encode_data_block_header, encode_footer, encode_footer_trailer, encode_index_entries,
    invalid_data, invalid_input,
};

const DEFAULT_BLOOM_BITS_PER_KEY: u32 = 10;
const DEFAULT_MIN_COMPRESS_SIZE_BYTES: usize = 1024;
const ZSTD_DEFAULT_LEVEL: i32 = 0;

pub struct SsTableWriter {
    file: File,
    table_id: u64,
    level: u8,
    block_target_bytes: usize,
    current_offset: u64,
    current_block: BlockEncoder,
    index_entries: Vec<IndexEntry>,
    bloom_keys: Vec<Bytes>,
    min_key: Option<Bytes>,
    max_key: Option<Bytes>,
    max_seq: u64,
    entry_count: u64,
    last_added_key: Option<Bytes>,
    footer_metadata: FooterMetadata,
    min_compress_size_bytes: usize,
}

impl SsTableWriter {
    /// Creates a new `SSTable` file writer.
    ///
    /// # Errors
    ///
    /// Returns an error when the table file cannot be created or positioned.
    pub fn create(path: &Path, table_id: u64, level: u8) -> Result<Self> {
        Self::create_with_block_target(path, table_id, level, DEFAULT_BLOCK_TARGET_BYTES)
    }

    /// Creates a new `SSTable` file writer with an explicit block target size.
    ///
    /// # Errors
    ///
    /// Returns an error when the table file cannot be created or positioned.
    pub fn create_with_block_target(
        path: &Path,
        table_id: u64,
        level: u8,
        block_target_bytes: usize,
    ) -> Result<Self> {
        Self::create_with_block_target_and_options(
            path,
            table_id,
            level,
            block_target_bytes,
            FooterMetadata::default(),
            DEFAULT_MIN_COMPRESS_SIZE_BYTES,
        )
    }

    /// Creates a new `SSTable` file writer with explicit block encoding and compression options.
    ///
    /// # Errors
    ///
    /// Returns an error when the table file cannot be created or positioned.
    pub(crate) fn create_with_block_target_and_options(
        path: &Path,
        table_id: u64,
        level: u8,
        block_target_bytes: usize,
        footer_metadata: FooterMetadata,
        min_compress_size_bytes: usize,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::Start(0))?;

        let current_block = if footer_metadata.block_encoding_kind == BlockEncodingKind::Prefix {
            BlockEncoder::Prefix(PrefixBlockBuilder::new(
                footer_metadata.prefix_restart_interval,
            ))
        } else {
            BlockEncoder::Plain(BlockBuilder::new())
        };

        Ok(Self {
            file,
            table_id,
            level,
            block_target_bytes: block_target_bytes.max(1),
            current_offset: 0,
            current_block,
            index_entries: Vec::new(),
            bloom_keys: Vec::new(),
            min_key: None,
            max_key: None,
            max_seq: 0,
            entry_count: 0,
            last_added_key: None,
            footer_metadata,
            min_compress_size_bytes,
        })
    }

    /// Adds an entry to the current `SSTable`, enforcing strict key ordering.
    ///
    /// # Errors
    ///
    /// Returns an error if keys are not strictly increasing or block/file writes fail.
    pub fn add_entry(&mut self, entry: &InternalEntry) -> Result<()> {
        self.validate_sorted_order(entry.key.as_ref())?;

        let entry_size_with_block = self.current_block.encoded_len_with(entry);
        if !self.current_block.is_empty() && entry_size_with_block > self.block_target_bytes {
            self.flush_current_block()?;
        }

        self.current_block.push(entry.clone());
        self.bloom_keys.push(entry.key.clone());

        match self.min_key {
            None => self.min_key = Some(entry.key.clone()),
            Some(_) => self.max_key = Some(entry.key.clone()),
        }

        if self.max_key.is_none() {
            self.max_key = Some(entry.key.clone());
        }

        self.max_seq = self.max_seq.max(entry.seq);
        self.entry_count = self
            .entry_count
            .checked_add(1)
            .ok_or_else(|| invalid_data("SSTable entry count overflow"))?;
        self.last_added_key = Some(entry.key.clone());

        Ok(())
    }

    /// Finalizes block/index/bloom/footer sections and persists the table to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the table is empty or any encode/write/sync step fails.
    pub fn finish(mut self) -> Result<SsTableMeta> {
        if self.entry_count == 0 {
            return Err(invalid_input("SSTable cannot be finalized without entries").into());
        }

        self.flush_current_block()?;

        let index_raw = encode_index_entries(&self.index_entries)?;
        let index_handle = self.write_section(&index_raw)?;

        let expected_keys = usize::try_from(self.entry_count)
            .map_err(|_| invalid_data("SSTable entry count not supported on this platform"))?;
        let mut bloom_builder = BloomFilterBuilder::new(expected_keys, DEFAULT_BLOOM_BITS_PER_KEY);
        for key in &self.bloom_keys {
            bloom_builder.add_key(key.as_ref());
        }
        let bloom_raw = bloom_builder.finish();
        let bloom_handle = self.write_section(&bloom_raw)?;

        let min_key = self
            .min_key
            .take()
            .ok_or_else(|| invalid_data("SSTable missing min key on finalize"))?;
        let max_key = self
            .max_key
            .take()
            .ok_or_else(|| invalid_data("SSTable missing max key on finalize"))?;

        let footer = TableFooter {
            index: index_handle,
            bloom: bloom_handle,
            min_key: min_key.clone(),
            max_key: max_key.clone(),
            entry_count: self.entry_count,
            max_seq: self.max_seq,
            metadata: self.footer_metadata,
        };

        let footer_payload = encode_footer(&footer)?;
        let footer_len = u32::try_from(footer_payload.len())
            .map_err(|_| invalid_data("SSTable footer payload exceeds u32 length"))?;

        self.file.write_all(&footer_payload)?;
        self.current_offset = checked_advance_offset(self.current_offset, footer_payload.len())?;

        let trailer = encode_footer_trailer(footer_len, SST_VERSION);
        self.file.write_all(&trailer)?;
        self.current_offset = checked_advance_offset(self.current_offset, trailer.len())?;

        self.file.sync_all()?;

        Ok(SsTableMeta {
            table_id: self.table_id,
            level: self.level,
            min_key,
            max_key,
            max_seq: self.max_seq,
        })
    }

    fn validate_sorted_order(&self, key: &[u8]) -> Result<()> {
        if let Some(previous_key) = &self.last_added_key {
            match previous_key.as_ref().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(
                        invalid_input("SSTable writer requires strictly increasing keys").into(),
                    );
                }
                std::cmp::Ordering::Greater => {
                    return Err(invalid_input("SSTable writer received out-of-order key").into());
                }
            }
        }

        Ok(())
    }

    fn flush_current_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let last_key = self
            .current_block
            .last_key()
            .ok_or_else(|| invalid_data("SSTable block missing last key"))?
            .to_vec();

        let replacement = if self.footer_metadata.block_encoding_kind == BlockEncodingKind::Prefix {
            BlockEncoder::Prefix(PrefixBlockBuilder::new(
                self.footer_metadata.prefix_restart_interval,
            ))
        } else {
            BlockEncoder::Plain(BlockBuilder::new())
        };
        let block = std::mem::replace(&mut self.current_block, replacement).finish()?;
        let block = self.prepare_data_block(&block)?;
        let handle = self.write_section(&block)?;

        self.index_entries.push(IndexEntry {
            last_key: Bytes::from(last_key),
            handle,
        });

        Ok(())
    }

    fn prepare_data_block(&self, raw_block: &[u8]) -> Result<Vec<u8>> {
        let raw_len_u32 = u32::try_from(raw_block.len())
            .map_err(|_| invalid_data("SSTable data block raw length exceeds u32"))?;
        let (is_compressed, payload) = self.maybe_compress_payload(raw_block)?;
        let stored_len_u32 = u32::try_from(payload.len())
            .map_err(|_| invalid_data("SSTable data block stored length exceeds u32"))?;

        let mut out = Vec::with_capacity(DATA_BLOCK_HEADER_LEN.saturating_add(payload.len()));
        out.extend_from_slice(&encode_data_block_header(
            is_compressed,
            raw_len_u32,
            stored_len_u32,
        ));
        out.extend_from_slice(&payload);
        Ok(out)
    }

    fn maybe_compress_payload(&self, raw_block: &[u8]) -> Result<(bool, Vec<u8>)> {
        let codec = self.footer_metadata.default_compression_codec;
        if raw_block.len() < self.min_compress_size_bytes || codec == FooterCompressionCodec::None {
            return Ok((false, raw_block.to_vec()));
        }

        let compressed = match codec {
            FooterCompressionCodec::None => raw_block.to_vec(),
            FooterCompressionCodec::Snappy => SnappyEncoder::new()
                .compress_vec(raw_block)
                .map_err(|_| invalid_data("SSTable snappy block compression failed"))?,
            FooterCompressionCodec::Zstd => zstd::bulk::compress(raw_block, ZSTD_DEFAULT_LEVEL)
                .map_err(|_| invalid_data("SSTable zstd block compression failed"))?,
        };
        if compressed.len() < raw_block.len() {
            Ok((true, compressed))
        } else {
            Ok((false, raw_block.to_vec()))
        }
    }

    fn write_section(&mut self, bytes: &[u8]) -> Result<BlockHandle> {
        let len = u32::try_from(bytes.len())
            .map_err(|_| invalid_data("SSTable section exceeds u32 length"))?;

        let handle = BlockHandle {
            offset: self.current_offset,
            len,
        };

        self.file.write_all(bytes)?;
        self.current_offset = checked_advance_offset(self.current_offset, bytes.len())?;

        Ok(handle)
    }
}

fn checked_advance_offset(offset: u64, delta: usize) -> std::io::Result<u64> {
    let delta_u64 = u64::try_from(delta)
        .map_err(|_| invalid_data("SSTable section length does not fit u64"))?;
    offset
        .checked_add(delta_u64)
        .ok_or_else(|| invalid_data("SSTable file offset overflow"))
}

#[cfg(test)]
mod tests {
    use std::{fs, time::SystemTime};

    use bytes::Bytes;

    use super::super::{FooterCompressionCodec, decode_data_block_header, reader::SsTableReader};
    use super::*;
    use crate::types::{InternalEntry, ValueEntry};

    fn temp_sstable_path(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        std::env::temp_dir().join(format!("aether-{prefix}-{nanos}.sst"))
    }

    fn put(seq: u64, key: &'static [u8], value: &'static [u8]) -> InternalEntry {
        InternalEntry {
            seq,
            key: Bytes::from_static(key),
            value: ValueEntry::Put(Bytes::from_static(value)),
        }
    }

    fn put_owned(seq: u64, key: &[u8], value: Vec<u8>) -> InternalEntry {
        InternalEntry {
            seq,
            key: Bytes::copy_from_slice(key),
            value: ValueEntry::Put(Bytes::from(value)),
        }
    }

    fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed.max(1);
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.push((state & 0xFF) as u8);
        }
        out
    }

    #[test]
    fn writer_and_reader_roundtrip_prefix_blocks() {
        let path = temp_sstable_path("sst-prefix-roundtrip");
        let entries = vec![
            put(1, b"alpha_001", b"v1"),
            put(2, b"alpha_002", b"v2"),
            put(3, b"alpha_003", b"v3"),
            put(4, b"bravo_001", b"v4"),
            put(5, b"charlie_001", b"v5"),
        ];
        let mut writer = match SsTableWriter::create_with_block_target_and_options(
            &path,
            42,
            0,
            64,
            FooterMetadata {
                block_encoding_kind: BlockEncodingKind::Prefix,
                default_compression_codec: FooterCompressionCodec::None,
                prefix_restart_interval: 2,
            },
            DEFAULT_MIN_COMPRESS_SIZE_BYTES,
        ) {
            Ok(writer) => writer,
            Err(err) => panic!("writer create should work: {err}"),
        };
        for entry in &entries {
            if let Err(err) = writer.add_entry(entry) {
                panic!("add entry should work: {err}");
            }
        }
        let meta = match writer.finish() {
            Ok(meta) => meta,
            Err(err) => panic!("finish should work: {err}"),
        };

        let reader = match SsTableReader::open(&path, meta, false) {
            Ok(reader) => reader,
            Err(err) => panic!("open should work: {err}"),
        };

        let all = match reader.scan_all_entries() {
            Ok(entries) => entries,
            Err(err) => panic!("scan should work: {err}"),
        };
        assert_eq!(all, entries);

        let found = match reader.get(b"bravo_001") {
            Ok(found) => found,
            Err(err) => panic!("get should work: {err}"),
        };
        assert_eq!(found, Some(entries[3].clone()));

        let missing = match reader.get(b"delta_001") {
            Ok(found) => found,
            Err(err) => panic!("get should work: {err}"),
        };
        assert!(missing.is_none());

        if let Err(err) = fs::remove_file(&path) {
            panic!("cleanup should work: {err}");
        }
    }

    #[test]
    fn writer_marks_compressed_blocks_when_beneficial() {
        let path = temp_sstable_path("sst-block-compress");
        let value = vec![b'a'; 8 * 1024];
        let entry = put_owned(1, b"compressible", value);
        let mut writer = match SsTableWriter::create_with_block_target_and_options(
            &path,
            43,
            0,
            64 * 1024,
            FooterMetadata {
                block_encoding_kind: BlockEncodingKind::Plain,
                default_compression_codec: FooterCompressionCodec::Snappy,
                prefix_restart_interval: 16,
            },
            1,
        ) {
            Ok(writer) => writer,
            Err(err) => panic!("writer create should work: {err}"),
        };
        if let Err(err) = writer.add_entry(&entry) {
            panic!("add entry should work: {err}");
        }
        let meta = match writer.finish() {
            Ok(meta) => meta,
            Err(err) => panic!("finish should work: {err}"),
        };

        let reader = match SsTableReader::open(&path, meta, false) {
            Ok(reader) => reader,
            Err(err) => panic!("open should work: {err}"),
        };
        let found = match reader.get(b"compressible") {
            Ok(found) => found,
            Err(err) => panic!("get should work: {err}"),
        };
        assert_eq!(found, Some(entry));

        let table_bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) => panic!("read should work: {err}"),
        };
        let (is_compressed, _, _) =
            match decode_data_block_header(&table_bytes[..DATA_BLOCK_HEADER_LEN]) {
                Ok(header) => header,
                Err(err) => panic!("decode header should work: {err}"),
            };
        assert!(is_compressed);

        if let Err(err) = fs::remove_file(&path) {
            panic!("cleanup should work: {err}");
        }
    }

    #[test]
    fn writer_marks_zstd_blocks_compressed_when_beneficial() {
        let path = temp_sstable_path("sst-block-zstd-compress");
        let value = vec![b'b'; 8 * 1024];
        let entry = put_owned(1, b"compressible-zstd", value);
        let mut writer = match SsTableWriter::create_with_block_target_and_options(
            &path,
            45,
            0,
            64 * 1024,
            FooterMetadata {
                block_encoding_kind: BlockEncodingKind::Plain,
                default_compression_codec: FooterCompressionCodec::Zstd,
                prefix_restart_interval: 16,
            },
            1,
        ) {
            Ok(writer) => writer,
            Err(err) => panic!("writer create should work: {err}"),
        };
        if let Err(err) = writer.add_entry(&entry) {
            panic!("add entry should work: {err}");
        }
        let meta = match writer.finish() {
            Ok(meta) => meta,
            Err(err) => panic!("finish should work: {err}"),
        };

        let reader = match SsTableReader::open(&path, meta, false) {
            Ok(reader) => reader,
            Err(err) => panic!("open should work: {err}"),
        };
        let found = match reader.get(b"compressible-zstd") {
            Ok(found) => found,
            Err(err) => panic!("get should work: {err}"),
        };
        assert_eq!(found, Some(entry));

        let table_bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) => panic!("read should work: {err}"),
        };
        let (is_compressed, _, _) =
            match decode_data_block_header(&table_bytes[..DATA_BLOCK_HEADER_LEN]) {
                Ok(header) => header,
                Err(err) => panic!("decode header should work: {err}"),
            };
        assert!(is_compressed);

        if let Err(err) = fs::remove_file(&path) {
            panic!("cleanup should work: {err}");
        }
    }

    #[test]
    fn writer_keeps_raw_blocks_when_compression_is_not_smaller() {
        let path = temp_sstable_path("sst-block-raw");
        let value = pseudo_random_bytes(8 * 1024, 0xA57A_17CE);
        let entry = put_owned(1, b"incompressible-ish", value);
        let mut writer = match SsTableWriter::create_with_block_target_and_options(
            &path,
            44,
            0,
            64 * 1024,
            FooterMetadata {
                block_encoding_kind: BlockEncodingKind::Plain,
                default_compression_codec: FooterCompressionCodec::Snappy,
                prefix_restart_interval: 16,
            },
            1,
        ) {
            Ok(writer) => writer,
            Err(err) => panic!("writer create should work: {err}"),
        };
        if let Err(err) = writer.add_entry(&entry) {
            panic!("add entry should work: {err}");
        }
        let meta = match writer.finish() {
            Ok(meta) => meta,
            Err(err) => panic!("finish should work: {err}"),
        };

        let reader = match SsTableReader::open(&path, meta, false) {
            Ok(reader) => reader,
            Err(err) => panic!("open should work: {err}"),
        };
        let found = match reader.get(b"incompressible-ish") {
            Ok(found) => found,
            Err(err) => panic!("get should work: {err}"),
        };
        assert_eq!(found, Some(entry));

        let table_bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) => panic!("read should work: {err}"),
        };
        let (is_compressed, _, _) =
            match decode_data_block_header(&table_bytes[..DATA_BLOCK_HEADER_LEN]) {
                Ok(header) => header,
                Err(err) => panic!("decode header should work: {err}"),
            };
        assert!(!is_compressed);

        if let Err(err) = fs::remove_file(&path) {
            panic!("cleanup should work: {err}");
        }
    }
}
