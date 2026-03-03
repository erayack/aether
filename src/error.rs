use thiserror::Error;

pub type Result<T> = std::result::Result<T, AetherError>;

#[derive(Debug, Error)]
pub enum AetherError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SSTable is corrupt: {0}")]
    CorruptSstable(&'static str),

    #[error("SSTable key ordering violation: {0}")]
    InvalidSstableOrder(&'static str),

    #[error("Unsupported SSTable version: {0}")]
    UnsupportedSstableVersion(u32),
}
