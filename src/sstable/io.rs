//! `SSTable` read IO backends.
//!
//! The main crate uses this module to load on-disk byte ranges while keeping
//! memory-mapping unsafe internals inside the `sstable-mmap` helper crate.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use tracing::warn;

use crate::error::Result;

use super::invalid_data;

pub(super) const MAX_SECTION_BYTES: usize = 128 * 1024 * 1024;

pub(super) enum ReadBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl ReadBytes<'_> {
    #[must_use]
    pub(super) const fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes.as_slice(),
        }
    }

    #[must_use]
    pub(super) fn into_owned(self) -> Vec<u8> {
        match self {
            Self::Borrowed(bytes) => bytes.to_vec(),
            Self::Owned(bytes) => bytes,
        }
    }
}

pub(super) enum ReadBackend {
    File(FileBackend),
    Mmap(MmapBackend),
}

pub(super) struct FileBackend {
    file: File,
    file_len: u64,
}

pub(super) struct MmapBackend {
    map: sstable_mmap::ReadOnlyMmap,
    file_len: u64,
}

impl ReadBackend {
    /// Opens a backend for `SSTable` reads.
    ///
    /// When `enable_mmap_reads` is true, mmap is attempted first and this
    /// function falls back to file reads if mapping fails.
    ///
    /// # Errors
    ///
    /// Returns an error when the backing file cannot be opened.
    pub(super) fn open(path: &Path, enable_mmap_reads: bool) -> Result<Self> {
        if enable_mmap_reads {
            match sstable_mmap::ReadOnlyMmap::open(path) {
                Ok(map) => {
                    let file_len = u64::try_from(map.len())
                        .map_err(|_| invalid_data("SSTable file length not supported"))?;
                    return Ok(Self::Mmap(MmapBackend { map, file_len }));
                }
                Err(err) => {
                    warn!(
                        path = %path.display(),
                        error = %err,
                        "failed to open mmap backend; falling back to file backend"
                    );
                }
            }
        }

        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        Ok(Self::File(FileBackend { file, file_len }))
    }

    #[must_use]
    pub(super) const fn len(&self) -> u64 {
        match self {
            Self::File(backend) => backend.file_len,
            Self::Mmap(backend) => backend.file_len,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(super) const fn is_mmap(&self) -> bool {
        matches!(self, Self::Mmap(_))
    }

    /// Reads a checked byte range from the selected backend.
    ///
    /// # Errors
    ///
    /// Returns an error if bounds are invalid or if the range cannot be read.
    pub(super) fn read_range(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.read_range_bytes(offset, len)
            .map(ReadBytes::into_owned)
    }

    /// Reads a checked byte range from the selected backend and may return
    /// borrowed bytes for mmap-backed files.
    ///
    /// # Errors
    ///
    /// Returns an error if bounds are invalid or if the range cannot be read.
    pub(super) fn read_range_bytes(&self, offset: u64, len: u32) -> Result<ReadBytes<'_>> {
        let len_usize = usize::try_from(len)
            .map_err(|_| invalid_data("SSTable section length not supported on this platform"))?;
        if len_usize > MAX_SECTION_BYTES {
            return Err(invalid_data("SSTable section length exceeds safety limit").into());
        }

        let file_len = self.len();
        let end = offset
            .checked_add(u64::from(len))
            .ok_or_else(|| invalid_data("SSTable section bounds overflow"))?;
        if end > file_len {
            return Err(invalid_data("SSTable section exceeds file bounds").into());
        }

        match self {
            Self::File(backend) => {
                let mut out = vec![0_u8; len_usize];
                // Clone the descriptor per read to avoid a shared seek cursor lock
                // serializing all file-backed table reads.
                let mut file = backend.file.try_clone()?;
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut out)?;
                Ok(ReadBytes::Owned(out))
            }
            Self::Mmap(backend) => {
                let offset_usize = usize::try_from(offset)
                    .map_err(|_| invalid_data("SSTable offset not supported on this platform"))?;
                let range = backend.map.slice(offset_usize, len_usize)?;
                Ok(ReadBytes::Borrowed(range))
            }
        }
    }
}
