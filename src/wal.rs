use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    mem::size_of,
    path::Path,
    time::{Duration, Instant},
};

use bytes::Bytes;
use crc32fast::hash;
use tracing::info;

use crate::{
    error::Result,
    types::{FsyncPolicy, InternalEntry, ValueEntry},
};

const CRC_LEN: usize = size_of::<u32>();
const RECORD_LEN_LEN: usize = size_of::<u32>();
const HEADER_LEN: usize = CRC_LEN + RECORD_LEN_LEN;

const SEQ_LEN: usize = size_of::<u64>();
const OP_LEN: usize = size_of::<u8>();
const FIELD_LEN_LEN: usize = size_of::<u32>();
const BATCH_COUNT_LEN: usize = size_of::<u32>();
const MIN_RECORD_PAYLOAD_LEN: usize = SEQ_LEN + OP_LEN + BATCH_COUNT_LEN;
const MIN_SINGLE_OP_PAYLOAD_LEN: usize = SEQ_LEN + OP_LEN + FIELD_LEN_LEN + FIELD_LEN_LEN;

const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_BATCH: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalBatchOp<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalOp<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
    Batch { ops: &'a [WalBatchOp<'a>] },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalAppendResult {
    pub seq: u64,
    pub file_offset: u64,
}

pub struct WalWriter {
    file: File,
    writer: BufWriter<File>,
    next_seq: u64,
    fsync_policy: FsyncPolicy,
    last_sync_at: Option<Instant>,
    pending_bytes: usize,
    write_offset: u64,
}

impl WalWriter {
    /// Opens an existing WAL or creates a new WAL file and prepares append state.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, scanned, or repaired.
    pub fn open(path: &Path, policy: FsyncPolicy) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        let scan_outcome = scan_wal(&mut file)?;

        let mut writer_file = file.try_clone()?;
        writer_file.seek(SeekFrom::Start(scan_outcome.valid_end_offset))?;
        file.seek(SeekFrom::Start(scan_outcome.valid_end_offset))?;

        Ok(Self::new_writer(
            file,
            writer_file,
            scan_outcome.next_seq,
            policy,
            scan_outcome.valid_end_offset,
        ))
    }

    /// Opens an existing WAL and guarantees the writer sequence starts at least at `min_seq`.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL cannot be opened, scanned, or repaired.
    pub fn open_with_min_seq(path: &Path, policy: FsyncPolicy, min_seq: u64) -> Result<Self> {
        let mut writer = Self::open(path, policy)?;
        if writer.next_seq < min_seq {
            writer.next_seq = min_seq;
        }
        Ok(writer)
    }

    /// Creates a new WAL file by truncating any previous contents.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or positioned.
    pub fn create_fresh(path: &Path, policy: FsyncPolicy) -> Result<Self> {
        Self::create_fresh_with_start_seq(path, policy, 0)
    }

    /// Creates a new WAL file and starts sequence assignment from `start_seq`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or positioned.
    pub fn create_fresh_with_start_seq(
        path: &Path,
        policy: FsyncPolicy,
        start_seq: u64,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::Start(0))?;

        let mut writer_file = file.try_clone()?;
        writer_file.seek(SeekFrom::Start(0))?;

        Ok(Self::new_writer(file, writer_file, start_seq, policy, 0))
    }

    /// Appends a single WAL operation and returns its sequence and file offset.
    ///
    /// # Errors
    ///
    /// Returns an error when record encoding, buffered writes, or fsync fail.
    pub fn append(&mut self, op: WalOp<'_>) -> Result<WalAppendResult> {
        let seq = self.next_seq;
        let following_seq = seq
            .checked_add(1)
            .ok_or_else(|| invalid_data("WAL sequence number overflow"))?;

        let record = encode_record(seq, op)?;
        let record_len = record.len();
        let file_offset = self.write_offset;

        self.writer.write_all(&record)?;

        self.write_offset = checked_add_offset(self.write_offset, record_len)?;
        self.pending_bytes = self
            .pending_bytes
            .checked_add(record_len)
            .ok_or_else(|| invalid_data("WAL pending-bytes overflow"))?;
        self.next_seq = following_seq;

        if self.should_sync() {
            self.sync()?;
        }

        Ok(WalAppendResult { seq, file_offset })
    }

    /// Flushes the buffered WAL writer and issues `sync_data` on the WAL file.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing buffers or syncing to disk fails.
    pub fn sync(&mut self) -> Result<()> {
        let synced_bytes = self.pending_bytes;
        self.writer.flush()?;
        self.file.sync_data()?;
        self.last_sync_at = Some(Instant::now());
        self.pending_bytes = 0;
        info!(
            event = "wal.sync",
            synced_bytes,
            write_offset = self.write_offset,
            ?self.fsync_policy,
            "WAL sync completed"
        );
        Ok(())
    }

    /// Returns the sequence number that will be assigned to the next append.
    #[must_use]
    pub const fn next_sequence_number(&self) -> u64 {
        self.next_seq
    }

    /// Returns the current valid WAL end offset.
    #[must_use]
    pub const fn write_offset(&self) -> u64 {
        self.write_offset
    }

    fn should_sync(&self) -> bool {
        if self.pending_bytes == 0 {
            return false;
        }

        match self.fsync_policy {
            FsyncPolicy::Always => true,
            FsyncPolicy::EveryMillis(interval_millis) => {
                let interval = Duration::from_millis(interval_millis);
                self.last_sync_at
                    .is_none_or(|last_sync| last_sync.elapsed() >= interval)
            }
            FsyncPolicy::NeverForBenchOnly => false,
        }
    }

    fn new_writer(
        file: File,
        writer_file: File,
        next_seq: u64,
        fsync_policy: FsyncPolicy,
        write_offset: u64,
    ) -> Self {
        Self {
            file,
            writer: BufWriter::new(writer_file),
            next_seq,
            fsync_policy,
            last_sync_at: None,
            pending_bytes: 0,
            write_offset,
        }
    }
}

/// Replays WAL records into internal entries and truncates a torn/corrupt tail.
///
/// # Errors
///
/// Returns an error if opening, reading, or truncating the WAL file fails.
pub fn replay_wal(path: &Path) -> Result<Vec<InternalEntry>> {
    let mut file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };

    let scan_outcome = scan_wal(&mut file)?;
    Ok(scan_outcome.entries)
}

struct ScanOutcome {
    entries: Vec<InternalEntry>,
    valid_end_offset: u64,
    next_seq: u64,
}

fn scan_wal(file: &mut File) -> Result<ScanOutcome> {
    file.seek(SeekFrom::Start(0))?;
    let initial_len = file.metadata()?.len();

    let mut entries = Vec::new();
    let mut last_seq: Option<u64> = None;
    let mut last_valid_offset = 0_u64;

    loop {
        let record_start = file.stream_position()?;

        let mut header = [0_u8; HEADER_LEN];
        let header_bytes = read_fully(file, &mut header)?;

        if header_bytes == 0 {
            break;
        }
        if header_bytes < HEADER_LEN {
            break;
        }

        let expected_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let record_len_u32 = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let Ok(record_len) = usize::try_from(record_len_u32) else {
            break;
        };

        if record_len < MIN_RECORD_PAYLOAD_LEN {
            break;
        }

        let payload_start = file.stream_position()?;
        let remaining_bytes = initial_len.saturating_sub(payload_start);
        if u64::from(record_len_u32) > remaining_bytes {
            break;
        }

        let mut payload = vec![0_u8; record_len];
        let payload_bytes = read_fully(file, &mut payload)?;
        if payload_bytes < record_len {
            break;
        }

        if hash(&payload) != expected_crc {
            break;
        }

        let Ok(decoded) = decode_payload(&payload) else {
            break;
        };

        if let Some(previous_seq) = last_seq
            && decoded.seq <= previous_seq
        {
            break;
        }

        last_seq = Some(decoded.seq);
        entries.extend(decoded.entries);
        last_valid_offset = file.stream_position()?;
        debug_assert!(last_valid_offset >= record_start);
    }

    truncate_tail(file, last_valid_offset, initial_len)?;

    let next_seq = match last_seq {
        Some(seq) => seq
            .checked_add(1)
            .ok_or_else(|| invalid_data("WAL sequence number overflow during replay"))?,
        None => 0,
    };

    Ok(ScanOutcome {
        entries,
        valid_end_offset: last_valid_offset,
        next_seq,
    })
}

fn encode_record(seq: u64, op: WalOp<'_>) -> Result<Vec<u8>> {
    let payload = match op {
        WalOp::Put { key, value } => encode_single_payload(seq, OP_PUT, key, value)?,
        WalOp::Delete { key } => encode_single_payload(seq, OP_DELETE, key, &[])?,
        WalOp::Batch { ops } => encode_batch_payload(seq, ops)?,
    };
    let payload_len_u32 = u32::try_from(payload.len())
        .map_err(|_| invalid_input("WAL record payload exceeds framing limit"))?;

    let crc = hash(&payload);
    let mut framed = Vec::with_capacity(HEADER_LEN + payload.len());
    framed.extend_from_slice(&crc.to_le_bytes());
    framed.extend_from_slice(&payload_len_u32.to_le_bytes());
    framed.extend_from_slice(&payload);

    Ok(framed)
}

fn encode_single_payload(seq: u64, op_code: u8, key: &[u8], value: &[u8]) -> io::Result<Vec<u8>> {
    let key_len = u32::try_from(key.len()).map_err(|_| invalid_input("WAL key is too large"))?;
    let value_len =
        u32::try_from(value.len()).map_err(|_| invalid_input("WAL value is too large"))?;
    let payload_len = MIN_SINGLE_OP_PAYLOAD_LEN
        .checked_add(key.len())
        .and_then(|len| len.checked_add(value.len()))
        .ok_or_else(|| invalid_input("WAL record payload length overflow"))?;

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&seq.to_le_bytes());
    payload.push(op_code);
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&value_len.to_le_bytes());
    payload.extend_from_slice(value);
    Ok(payload)
}

fn encode_batch_payload(seq: u64, ops: &[WalBatchOp<'_>]) -> io::Result<Vec<u8>> {
    let count_u32 =
        u32::try_from(ops.len()).map_err(|_| invalid_input("WAL batch is too large"))?;
    let mut payload_len = MIN_RECORD_PAYLOAD_LEN;
    for op in ops {
        let (key, value) = match op {
            WalBatchOp::Put { key, value } => (*key, *value),
            WalBatchOp::Delete { key } => (*key, &[][..]),
        };
        payload_len = payload_len
            .checked_add(OP_LEN)
            .and_then(|len| len.checked_add(FIELD_LEN_LEN))
            .and_then(|len| len.checked_add(key.len()))
            .and_then(|len| len.checked_add(FIELD_LEN_LEN))
            .and_then(|len| len.checked_add(value.len()))
            .ok_or_else(|| invalid_input("WAL batch payload length overflow"))?;
    }

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&seq.to_le_bytes());
    payload.push(OP_BATCH);
    payload.extend_from_slice(&count_u32.to_le_bytes());
    for op in ops {
        match op {
            WalBatchOp::Put { key, value } => {
                encode_batch_entry(&mut payload, OP_PUT, key, value)?;
            }
            WalBatchOp::Delete { key } => {
                encode_batch_entry(&mut payload, OP_DELETE, key, &[])?;
            }
        }
    }
    Ok(payload)
}

fn encode_batch_entry(
    payload: &mut Vec<u8>,
    op_code: u8,
    key: &[u8],
    value: &[u8],
) -> io::Result<()> {
    let key_len = u32::try_from(key.len()).map_err(|_| invalid_input("WAL key is too large"))?;
    let value_len =
        u32::try_from(value.len()).map_err(|_| invalid_input("WAL value is too large"))?;

    payload.push(op_code);
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&value_len.to_le_bytes());
    payload.extend_from_slice(value);
    Ok(())
}

struct DecodedRecord {
    seq: u64,
    entries: Vec<InternalEntry>,
}

fn decode_payload(payload: &[u8]) -> io::Result<DecodedRecord> {
    let mut cursor = 0_usize;

    let seq = read_u64(payload, &mut cursor)?;
    let op = read_u8(payload, &mut cursor)?;

    let entries = match op {
        OP_PUT | OP_DELETE => {
            let entry = decode_single_entry_payload(payload, &mut cursor, seq, op)?;
            vec![entry]
        }
        OP_BATCH => decode_batch_payload(payload, &mut cursor, seq)?,
        _ => return Err(invalid_data("WAL record has unknown operation code")),
    };

    if cursor != payload.len() {
        return Err(invalid_data("WAL record has unexpected trailing bytes"));
    }

    Ok(DecodedRecord { seq, entries })
}

fn decode_single_entry_payload(
    payload: &[u8],
    cursor: &mut usize,
    seq: u64,
    op: u8,
) -> io::Result<InternalEntry> {
    let key_len = read_len(payload, cursor)?;
    let key = read_slice(payload, cursor, key_len)?;
    let value_len = read_len(payload, cursor)?;
    let value = read_slice(payload, cursor, value_len)?;

    let value_entry = decode_value_entry(op, value)?;

    Ok(InternalEntry {
        seq,
        key: Bytes::copy_from_slice(key),
        value: value_entry,
    })
}

fn decode_batch_payload(
    payload: &[u8],
    cursor: &mut usize,
    seq: u64,
) -> io::Result<Vec<InternalEntry>> {
    let count = read_len(payload, cursor)?;
    let remaining_payload = payload.len().saturating_sub(*cursor);
    let min_entry_len = OP_LEN
        .saturating_add(FIELD_LEN_LEN)
        .saturating_add(FIELD_LEN_LEN);
    let max_entries_by_size = if min_entry_len == 0 {
        0
    } else {
        remaining_payload / min_entry_len
    };
    if count > max_entries_by_size {
        return Err(invalid_data("WAL batch entry count exceeds payload bounds"));
    }

    let mut entries = Vec::with_capacity(count);

    for _ in 0..count {
        let op = read_u8(payload, cursor)?;
        if op != OP_PUT && op != OP_DELETE {
            return Err(invalid_data("WAL batch entry has unknown operation code"));
        }

        let key_len = read_len(payload, cursor)?;
        let key = read_slice(payload, cursor, key_len)?;
        let value_len = read_len(payload, cursor)?;
        let value = read_slice(payload, cursor, value_len)?;

        entries.push(InternalEntry {
            seq,
            key: Bytes::copy_from_slice(key),
            value: decode_value_entry(op, value)?,
        });
    }

    Ok(entries)
}

fn decode_value_entry(op: u8, value: &[u8]) -> io::Result<ValueEntry> {
    match op {
        OP_PUT => Ok(ValueEntry::Put(Bytes::copy_from_slice(value))),
        OP_DELETE => {
            if !value.is_empty() {
                return Err(invalid_data("WAL delete record must have empty value"));
            }
            Ok(ValueEntry::Tombstone)
        }
        _ => Err(invalid_data("WAL record has unknown operation code")),
    }
}

fn read_u64(payload: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor
        .checked_add(SEQ_LEN)
        .ok_or_else(|| invalid_data("WAL record cursor overflow"))?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("WAL record truncated while decoding sequence number"))?;
    *cursor = end;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn read_u8(payload: &[u8], cursor: &mut usize) -> io::Result<u8> {
    let end = cursor
        .checked_add(OP_LEN)
        .ok_or_else(|| invalid_data("WAL record cursor overflow"))?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("WAL record truncated while decoding operation code"))?;
    *cursor = end;
    Ok(bytes[0])
}

fn read_len(payload: &[u8], cursor: &mut usize) -> io::Result<usize> {
    let end = cursor
        .checked_add(FIELD_LEN_LEN)
        .ok_or_else(|| invalid_data("WAL record cursor overflow"))?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("WAL record truncated while decoding field length"))?;
    *cursor = end;

    let len_u32 = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    usize::try_from(len_u32).map_err(|_| invalid_data("WAL field length exceeds platform limits"))
}

fn read_slice<'a>(payload: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| invalid_data("WAL record cursor overflow"))?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("WAL record field length exceeds payload bounds"))?;
    *cursor = end;
    Ok(bytes)
}

fn read_fully(file: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    let mut total_read = 0_usize;

    while total_read < buffer.len() {
        match file.read(&mut buffer[total_read..]) {
            Ok(0) => break,
            Ok(read_count) => {
                total_read = total_read
                    .checked_add(read_count)
                    .ok_or_else(|| invalid_data("WAL read cursor overflow"))?;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }

    Ok(total_read)
}

fn truncate_tail(file: &mut File, truncate_to: u64, original_len: u64) -> Result<()> {
    if truncate_to < original_len {
        file.set_len(truncate_to)?;
        file.seek(SeekFrom::Start(truncate_to))?;
    }
    Ok(())
}

fn checked_add_offset(offset: u64, delta: usize) -> io::Result<u64> {
    let delta_u64 =
        u64::try_from(delta).map_err(|_| invalid_data("WAL offset increment overflow"))?;
    offset
        .checked_add(delta_u64)
        .ok_or_else(|| invalid_data("WAL offset overflow"))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::{Seek, SeekFrom, Write},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{WalBatchOp, WalOp, WalWriter, encode_record, replay_wal};
    use crate::types::{FsyncPolicy, ValueEntry};

    fn temp_wal_path(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("aether-{prefix}-{nanos}.wal"))
    }

    #[test]
    fn replay_truncates_partial_tail_record() {
        let wal_path = temp_wal_path("wal-partial-tail");

        {
            let mut writer = match WalWriter::create_fresh(&wal_path, FsyncPolicy::Always) {
                Ok(writer) => writer,
                Err(err) => panic!("wal create should succeed: {err}"),
            };
            if let Err(err) = writer.append(WalOp::Put {
                key: b"k1",
                value: b"v1",
            }) {
                panic!("append should succeed: {err}");
            }
            if let Err(err) = writer.sync() {
                panic!("sync should succeed: {err}");
            }
        }

        let valid_len = match fs::metadata(&wal_path) {
            Ok(metadata) => metadata.len(),
            Err(err) => panic!("metadata should succeed: {err}"),
        };

        {
            let mut file = match OpenOptions::new().append(true).open(&wal_path) {
                Ok(file) => file,
                Err(err) => panic!("open append should succeed: {err}"),
            };
            if let Err(err) = file.write_all(&[0xDE, 0xAD, 0xBE]) {
                panic!("tail write should succeed: {err}");
            }
            if let Err(err) = file.sync_data() {
                panic!("file sync should succeed: {err}");
            }
        }

        let entries = match replay_wal(&wal_path) {
            Ok(entries) => entries,
            Err(err) => panic!("replay should succeed: {err}"),
        };
        assert_eq!(entries.len(), 1);
        match &entries[0].value {
            ValueEntry::Put(value) => assert_eq!(value.as_ref(), b"v1"),
            ValueEntry::Tombstone => panic!("unexpected tombstone"),
        }

        let repaired_len = match fs::metadata(&wal_path) {
            Ok(metadata) => metadata.len(),
            Err(err) => panic!("metadata should succeed: {err}"),
        };
        assert_eq!(repaired_len, valid_len);

        if let Err(err) = fs::remove_file(&wal_path) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn replay_truncates_corrupt_crc_tail_record() {
        let wal_path = temp_wal_path("wal-corrupt-tail");
        let second_offset = {
            let mut writer = match WalWriter::create_fresh(&wal_path, FsyncPolicy::Always) {
                Ok(writer) => writer,
                Err(err) => panic!("wal create should succeed: {err}"),
            };
            if let Err(err) = writer.append(WalOp::Put {
                key: b"k1",
                value: b"v1",
            }) {
                panic!("append should succeed: {err}");
            }
            let second = match writer.append(WalOp::Put {
                key: b"k2",
                value: b"v2",
            }) {
                Ok(result) => result,
                Err(err) => panic!("append should succeed: {err}"),
            };
            if let Err(err) = writer.sync() {
                panic!("sync should succeed: {err}");
            }
            second.file_offset
        };

        {
            let mut file = match OpenOptions::new().read(true).write(true).open(&wal_path) {
                Ok(file) => file,
                Err(err) => panic!("open for mutation should succeed: {err}"),
            };
            let corrupt_pos = second_offset + 8;
            if let Err(err) = file.seek(SeekFrom::Start(corrupt_pos)) {
                panic!("seek should succeed: {err}");
            }
            if let Err(err) = file.write_all(&[0xFF]) {
                panic!("corrupt write should succeed: {err}");
            }
            if let Err(err) = file.sync_data() {
                panic!("file sync should succeed: {err}");
            }
        }

        let entries = match replay_wal(&wal_path) {
            Ok(entries) => entries,
            Err(err) => panic!("replay should succeed: {err}"),
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.as_ref(), b"k1");

        let repaired_len = match fs::metadata(&wal_path) {
            Ok(metadata) => metadata.len(),
            Err(err) => panic!("metadata should succeed: {err}"),
        };
        assert_eq!(repaired_len, second_offset);

        if let Err(err) = fs::remove_file(&wal_path) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn replay_batch_record_keeps_shared_sequence() {
        let wal_path = temp_wal_path("wal-batch-replay");

        {
            let mut writer = match WalWriter::create_fresh(&wal_path, FsyncPolicy::Always) {
                Ok(writer) => writer,
                Err(err) => panic!("wal create should succeed: {err}"),
            };
            let ops = [
                WalBatchOp::Put {
                    key: b"a",
                    value: b"1",
                },
                WalBatchOp::Delete { key: b"b" },
            ];
            if let Err(err) = writer.append(WalOp::Batch { ops: &ops }) {
                panic!("append should succeed: {err}");
            }
            if let Err(err) = writer.sync() {
                panic!("sync should succeed: {err}");
            }
        }

        let entries = match replay_wal(&wal_path) {
            Ok(entries) => entries,
            Err(err) => panic!("replay should succeed: {err}"),
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, entries[1].seq);
        assert_eq!(entries[0].key.as_ref(), b"a");
        assert_eq!(entries[1].key.as_ref(), b"b");
        assert!(matches!(entries[1].value, ValueEntry::Tombstone));

        if let Err(err) = fs::remove_file(&wal_path) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn replay_drops_torn_tail_batch_record_atomically() {
        let wal_path = temp_wal_path("wal-batch-torn-tail");

        {
            let mut writer = match WalWriter::create_fresh(&wal_path, FsyncPolicy::Always) {
                Ok(writer) => writer,
                Err(err) => panic!("wal create should succeed: {err}"),
            };
            if let Err(err) = writer.append(WalOp::Put {
                key: b"stable",
                value: b"ok",
            }) {
                panic!("append should succeed: {err}");
            }
            if let Err(err) = writer.sync() {
                panic!("sync should succeed: {err}");
            }
        }

        let valid_len = match fs::metadata(&wal_path) {
            Ok(metadata) => metadata.len(),
            Err(err) => panic!("metadata should succeed: {err}"),
        };

        {
            let mut file = match OpenOptions::new().append(true).open(&wal_path) {
                Ok(file) => file,
                Err(err) => panic!("open append should succeed: {err}"),
            };
            let ops = [
                WalBatchOp::Put {
                    key: b"a",
                    value: b"1",
                },
                WalBatchOp::Put {
                    key: b"b",
                    value: b"2",
                },
            ];
            let record = match encode_record(1, WalOp::Batch { ops: &ops }) {
                Ok(record) => record,
                Err(err) => panic!("encode should succeed: {err}"),
            };
            let torn_len = record.len().saturating_sub(3);
            if let Err(err) = file.write_all(&record[..torn_len]) {
                panic!("tail write should succeed: {err}");
            }
            if let Err(err) = file.sync_data() {
                panic!("file sync should succeed: {err}");
            }
        }

        let entries = match replay_wal(&wal_path) {
            Ok(entries) => entries,
            Err(err) => panic!("replay should succeed: {err}"),
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.as_ref(), b"stable");
        match &entries[0].value {
            ValueEntry::Put(value) => assert_eq!(value.as_ref(), b"ok"),
            ValueEntry::Tombstone => panic!("unexpected tombstone"),
        }

        let repaired_len = match fs::metadata(&wal_path) {
            Ok(metadata) => metadata.len(),
            Err(err) => panic!("metadata should succeed: {err}"),
        };
        assert_eq!(repaired_len, valid_len);

        if let Err(err) = fs::remove_file(&wal_path) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn replay_truncates_non_monotonic_batch_record() {
        let wal_path = temp_wal_path("wal-batch-non-monotonic");

        {
            let mut file = match OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&wal_path)
            {
                Ok(file) => file,
                Err(err) => panic!("open should succeed: {err}"),
            };

            let first = match encode_record(
                7,
                WalOp::Put {
                    key: b"k1",
                    value: b"v1",
                },
            ) {
                Ok(record) => record,
                Err(err) => panic!("encode should succeed: {err}"),
            };
            let first_len = first.len() as u64;
            let second_ops = [
                WalBatchOp::Put {
                    key: b"k2",
                    value: b"v2",
                },
                WalBatchOp::Delete { key: b"k3" },
            ];
            let second = match encode_record(7, WalOp::Batch { ops: &second_ops }) {
                Ok(record) => record,
                Err(err) => panic!("encode should succeed: {err}"),
            };
            if let Err(err) = file.write_all(&first) {
                panic!("write should succeed: {err}");
            }
            if let Err(err) = file.write_all(&second) {
                panic!("write should succeed: {err}");
            }
            if let Err(err) = file.sync_data() {
                panic!("file sync should succeed: {err}");
            }

            let len = match file.metadata() {
                Ok(metadata) => metadata.len(),
                Err(err) => panic!("metadata should succeed: {err}"),
            };
            assert!(len > first_len);
        }

        let entries = match replay_wal(&wal_path) {
            Ok(entries) => entries,
            Err(err) => panic!("replay should succeed: {err}"),
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.as_ref(), b"k1");

        let repaired_len = match fs::metadata(&wal_path) {
            Ok(metadata) => metadata.len(),
            Err(err) => panic!("metadata should succeed: {err}"),
        };
        let expected_len = match encode_record(
            7,
            WalOp::Put {
                key: b"k1",
                value: b"v1",
            },
        ) {
            Ok(record) => record.len() as u64,
            Err(err) => panic!("encode should succeed: {err}"),
        };
        assert_eq!(repaired_len, expected_len);

        if let Err(err) = fs::remove_file(&wal_path) {
            panic!("cleanup should succeed: {err}");
        }
    }
}
