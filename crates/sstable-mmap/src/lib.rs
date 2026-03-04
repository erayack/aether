use std::{fs::File, io, path::Path};

use memmap2::Mmap;

/// Read-only memory map with checked slicing.
pub struct ReadOnlyMmap {
    _file: File,
    map: Mmap,
}

impl ReadOnlyMmap {
    /// Opens a read-only memory mapping for the provided file path.
    ///
    /// # Errors
    ///
    /// Returns an IO error if the file cannot be opened or mapped.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: `file` is opened read-only and stored in `Self` so the backing
        // descriptor outlives the map for the full mapping lifetime.
        let map = unsafe { Mmap::map(&file)? };

        Ok(Self { _file: file, map })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns a borrowed subslice from the mapped file with bounds validation.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` when `offset + len` overflows or exceeds map bounds.
    pub fn slice(&self, offset: usize, len: usize) -> io::Result<&[u8]> {
        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "mmap slice offset overflow")
        })?;

        self.map.get(offset..end).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "mmap slice exceeds mapped file bounds",
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::ReadOnlyMmap;

    fn temp_file_path(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        path.push(format!("{prefix}-{nanos}.sst"));
        path
    }

    #[test]
    fn mmap_open_len_and_slice_roundtrip() -> io::Result<()> {
        let path = temp_file_path("aether-mmap-open");
        fs::write(&path, b"abcdef")?;

        let mmap = ReadOnlyMmap::open(&path)?;
        assert_eq!(mmap.len(), 6);
        assert_eq!(mmap.slice(1, 3)?, b"bcd");

        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn mmap_slice_oob_returns_error() -> io::Result<()> {
        let path = temp_file_path("aether-mmap-oob");
        fs::write(&path, b"abc")?;

        let mmap = ReadOnlyMmap::open(&path)?;
        assert!(mmap.slice(2, 2).is_err());

        fs::remove_file(path)?;
        Ok(())
    }
}
