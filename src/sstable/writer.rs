use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use bytes::Bytes;

use crate::{error::Result, types::InternalEntry};

use super::{
    BlockHandle, DEFAULT_BLOCK_TARGET_BYTES, IndexEntry, SsTableMeta, TableFooter,
    block::BlockBuilder, bloom::BloomFilterBuilder, encode_footer, encode_footer_trailer,
    encode_index_entries, invalid_data, invalid_input,
};

const DEFAULT_BLOOM_BITS_PER_KEY: u32 = 10;

pub struct SsTableWriter {
    file: File,
    table_id: u64,
    level: u8,
    block_target_bytes: usize,
    current_offset: u64,
    current_block: BlockBuilder,
    index_entries: Vec<IndexEntry>,
    bloom_keys: Vec<Bytes>,
    min_key: Option<Bytes>,
    max_key: Option<Bytes>,
    max_seq: u64,
    entry_count: u64,
    last_added_key: Option<Bytes>,
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
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::Start(0))?;

        Ok(Self {
            file,
            table_id,
            level,
            block_target_bytes: block_target_bytes.max(1),
            current_offset: 0,
            current_block: BlockBuilder::new(),
            index_entries: Vec::new(),
            bloom_keys: Vec::new(),
            min_key: None,
            max_key: None,
            max_seq: 0,
            entry_count: 0,
            last_added_key: None,
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
        };

        let footer_payload = encode_footer(&footer)?;
        let footer_len = u32::try_from(footer_payload.len())
            .map_err(|_| invalid_input("SSTable footer payload exceeds u32 length"))?;

        self.file.write_all(&footer_payload)?;
        self.current_offset = checked_advance_offset(self.current_offset, footer_payload.len())?;

        let trailer = encode_footer_trailer(footer_len);
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
        let block = std::mem::take(&mut self.current_block).finish()?;
        let handle = self.write_section(&block)?;

        self.index_entries.push(IndexEntry {
            last_key: Bytes::from(last_key),
            handle,
        });

        Ok(())
    }

    fn write_section(&mut self, bytes: &[u8]) -> Result<BlockHandle> {
        let len = u32::try_from(bytes.len())
            .map_err(|_| invalid_input("SSTable section exceeds u32 length"))?;

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
