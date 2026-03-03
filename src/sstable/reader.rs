use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    ops::Bound,
    path::{Path, PathBuf},
};

use parking_lot::Mutex;

use crate::{
    error::Result,
    types::{InternalEntry, Key, ScanBounds},
};

use super::{
    FOOTER_TRAILER_LEN, SST_MAGIC, SST_VERSION, SsTableMeta, block::find_in_block,
    bloom::BloomFilter, decode_footer, decode_footer_trailer, decode_index_entries, invalid_data,
};

pub struct SsTableReader {
    file_path: PathBuf,
    file: Mutex<File>,
    file_len: u64,
    meta: SsTableMeta,
    index: Vec<super::IndexEntry>,
    bloom: BloomFilter,
}

pub struct SsTableRangeIter {
    file: File,
    file_len: u64,
    bounds: ScanBounds,
    visible_seq: u64,
    block_handles: Vec<super::BlockHandle>,
    block_index_cursor: usize,
    in_block_entries: Vec<InternalEntry>,
    in_block_cursor: usize,
}

const MAX_SECTION_BYTES: usize = 128 * 1024 * 1024;

impl SsTableReader {
    /// Opens an existing `SSTable` and loads footer, index, and bloom metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if file IO fails, the footer format is invalid, or metadata mismatches.
    pub fn open(path: &Path, meta: SsTableMeta) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        let trailer_len_u64 = u64::try_from(FOOTER_TRAILER_LEN)
            .map_err(|_| invalid_data("SSTable trailer size not supported"))?;
        if file_len < trailer_len_u64 {
            return Err(invalid_data("SSTable file is too short for footer trailer").into());
        }

        file.seek(SeekFrom::Start(file_len - trailer_len_u64))?;
        let mut trailer = vec![0_u8; FOOTER_TRAILER_LEN];
        file.read_exact(&mut trailer)?;

        let (footer_len_u32, version, magic) = decode_footer_trailer(&trailer)?;
        if magic != SST_MAGIC {
            return Err(invalid_data("SSTable magic mismatch").into());
        }
        if version != SST_VERSION {
            return Err(invalid_data("SSTable version mismatch").into());
        }

        let footer = read_footer(&mut file, file_len, footer_len_u32)?;

        if footer.entry_count == 0 {
            return Err(invalid_data("SSTable footer entry count must be positive").into());
        }
        if footer.min_key != meta.min_key
            || footer.max_key != meta.max_key
            || footer.max_seq != meta.max_seq
        {
            return Err(invalid_data("SSTable metadata does not match footer contents").into());
        }

        let index_raw =
            read_handle_payload(&mut file, file_len, footer.index.offset, footer.index.len)?;
        let index = decode_index_entries(&index_raw)?;
        if index.is_empty() {
            return Err(invalid_data("SSTable index must contain at least one block").into());
        }

        let bloom_raw =
            read_handle_payload(&mut file, file_len, footer.bloom.offset, footer.bloom.len)?;
        let bloom = BloomFilter::decode(&bloom_raw)?;

        Ok(Self {
            file_path: path.to_path_buf(),
            file: Mutex::new(file),
            file_len,
            meta,
            index,
            bloom,
        })
    }

    /// Loads table metadata from the `SSTable` footer.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the footer is invalid.
    pub fn load_meta(path: &Path, table_id: u64, level: u8) -> Result<SsTableMeta> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let trailer_len_u64 = u64::try_from(FOOTER_TRAILER_LEN)
            .map_err(|_| invalid_data("SSTable trailer size not supported"))?;
        if file_len < trailer_len_u64 {
            return Err(invalid_data("SSTable file is too short for footer trailer").into());
        }

        file.seek(SeekFrom::Start(file_len - trailer_len_u64))?;
        let mut trailer = vec![0_u8; FOOTER_TRAILER_LEN];
        file.read_exact(&mut trailer)?;
        let (footer_len_u32, version, magic) = decode_footer_trailer(&trailer)?;
        if magic != SST_MAGIC {
            return Err(invalid_data("SSTable magic mismatch").into());
        }
        if version != SST_VERSION {
            return Err(invalid_data("SSTable version mismatch").into());
        }

        let footer = read_footer(&mut file, file_len, footer_len_u32)?;

        Ok(SsTableMeta {
            table_id,
            level,
            min_key: footer.min_key,
            max_key: footer.max_key,
            max_seq: footer.max_seq,
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
        let block = {
            let mut file = self.file.lock();
            read_handle_payload(&mut file, self.file_len, handle.offset, handle.len)?
        };
        find_in_block(&block, key)
    }

    /// Reads and decodes all entries in this table in key order.
    ///
    /// # Errors
    ///
    /// Returns an error if any block cannot be read or decoded.
    pub fn scan_all_entries(&self) -> Result<Vec<InternalEntry>> {
        let mut all_entries = Vec::new();

        for index_entry in &self.index {
            let block = {
                let mut file = self.file.lock();
                read_handle_payload(
                    &mut file,
                    self.file_len,
                    index_entry.handle.offset,
                    index_entry.handle.len,
                )?
            };
            let mut entries = super::block::decode_block_entries(&block)?;
            all_entries.append(&mut entries);
        }

        Ok(all_entries)
    }

    /// Creates an iterator over entries within `bounds` visible up to `visible_seq`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying table file handle cannot be cloned.
    pub fn iter_range(&self, bounds: &ScanBounds, visible_seq: u64) -> Result<SsTableRangeIter> {
        if !table_overlaps_bounds(&self.meta, bounds) {
            let file = {
                let file = self.file.lock();
                file.try_clone()?
            };
            return Ok(SsTableRangeIter {
                file,
                file_len: self.file_len,
                bounds: clone_scan_bounds(bounds),
                visible_seq,
                block_handles: Vec::new(),
                block_index_cursor: 0,
                in_block_entries: Vec::new(),
                in_block_cursor: 0,
            });
        }

        let block_handles = block_handles_for_bounds(&self.index, bounds);
        let file = {
            let file = self.file.lock();
            file.try_clone()?
        };

        Ok(SsTableRangeIter {
            file,
            file_len: self.file_len,
            bounds: clone_scan_bounds(bounds),
            visible_seq,
            block_handles,
            block_index_cursor: 0,
            in_block_entries: Vec::new(),
            in_block_cursor: 0,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.file_path
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
            let block =
                read_handle_payload(&mut self.file, self.file_len, handle.offset, handle.len)?;
            self.in_block_entries = super::block::decode_block_entries(&block)?;
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

fn read_footer(file: &mut File, file_len: u64, footer_len_u32: u32) -> Result<super::TableFooter> {
    let footer_len = usize::try_from(footer_len_u32)
        .map_err(|_| invalid_data("SSTable footer size not supported on this platform"))?;
    let trailer_len_u64 = u64::try_from(FOOTER_TRAILER_LEN)
        .map_err(|_| invalid_data("SSTable trailer size not supported"))?;
    let footer_total = trailer_len_u64
        .checked_add(u64::from(footer_len_u32))
        .ok_or_else(|| invalid_data("SSTable footer length overflow"))?;
    if footer_total > file_len {
        return Err(invalid_data("SSTable footer exceeds file length").into());
    }

    let footer_offset = file_len - footer_total;
    file.seek(SeekFrom::Start(footer_offset))?;
    let mut footer_raw = vec![0_u8; footer_len];
    file.read_exact(&mut footer_raw)?;
    decode_footer(&footer_raw)
}

fn read_handle_payload(file: &mut File, file_len: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
    let len_usize = usize::try_from(len)
        .map_err(|_| invalid_data("SSTable section length not supported on this platform"))?;
    if len_usize > MAX_SECTION_BYTES {
        return Err(invalid_data("SSTable section length exceeds safety limit").into());
    }
    let end = offset
        .checked_add(u64::from(len))
        .ok_or_else(|| invalid_data("SSTable section bounds overflow"))?;
    if end > file_len {
        return Err(invalid_data("SSTable section exceeds file bounds").into());
    }

    let mut raw = vec![0_u8; len_usize];

    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut raw)?;

    Ok(raw)
}
