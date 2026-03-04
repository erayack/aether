use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
};

use crate::{error::Result, sstable::SsTableMeta};

const MANIFEST_FILE_NAME: &str = "MANIFEST";
const MANIFEST_TMP_FILE_NAME: &str = "MANIFEST.tmp";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ManifestState {
    pub next_sequence_number: u64,
    pub next_table_id: u64,
    pub wal_generation: u64,
    pub wal_durable_checkpoint_offset: u64,
    pub levels: Vec<Vec<SsTableMeta>>,
    pub pending_deletes: Vec<PendingDeleteEntry>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PendingDeleteEntry {
    pub table_id: u64,
    pub level: u8,
    pub max_seq: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedManifestState {
    next_sequence_number: u64,
    next_table_id: u64,
    wal_generation: u64,
    wal_durable_checkpoint_offset: u64,
    levels: Vec<Vec<PersistedSsTableMeta>>,
    #[serde(default)]
    pending_deletes: Vec<PersistedPendingDeleteEntry>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedSsTableMeta {
    table_id: u64,
    level: u8,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    max_seq: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedPendingDeleteEntry {
    table_id: u64,
    level: u8,
    max_seq: u64,
}

pub struct ManifestStore;

impl ManifestStore {
    /// Loads the durable manifest state or creates a new default manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be read, parsed, or persisted.
    pub fn load_or_create(db_dir: &Path) -> Result<ManifestState> {
        let manifest_path = manifest_path(db_dir);
        match fs::read(&manifest_path) {
            Ok(raw) => parse_manifest_state(&raw),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let initial_state = ManifestState::default();
                Self::commit(db_dir, &initial_state)?;
                Ok(initial_state)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Atomically commits a full snapshot of manifest state.
    ///
    /// # Errors
    ///
    /// Returns an error if temporary write, sync, rename, or directory sync fails.
    pub fn commit(db_dir: &Path, state: &ManifestState) -> Result<()> {
        fs::create_dir_all(db_dir)?;

        let tmp_path = manifest_tmp_path(db_dir);
        let final_path = manifest_path(db_dir);
        let payload = serialize_manifest_state(state)?;

        let mut tmp_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        tmp_file.write_all(&payload)?;
        tmp_file.sync_data()?;
        drop(tmp_file);

        fs::rename(&tmp_path, &final_path)?;
        sync_directory(db_dir)?;

        Ok(())
    }
}

impl ManifestState {
    pub fn enqueue_pending_delete(&mut self, entry: PendingDeleteEntry) {
        if self
            .pending_deletes
            .iter()
            .any(|queued| queued.table_id == entry.table_id && queued.level == entry.level)
        {
            return;
        }
        self.pending_deletes.push(entry);
    }

    pub fn dequeue_pending_delete(&mut self, table_id: u64, level: u8) -> bool {
        let before = self.pending_deletes.len();
        self.pending_deletes
            .retain(|entry| !(entry.table_id == table_id && entry.level == level));
        before != self.pending_deletes.len()
    }
}

fn serialize_manifest_state(state: &ManifestState) -> Result<Vec<u8>> {
    let persisted = PersistedManifestState::from(state);
    serde_json::to_vec(&persisted).map_err(json_error)
}

fn parse_manifest_state(raw: &[u8]) -> Result<ManifestState> {
    let persisted: PersistedManifestState = serde_json::from_slice(raw).map_err(json_error)?;
    Ok(ManifestState::from(persisted))
}

fn manifest_path(db_dir: &Path) -> std::path::PathBuf {
    db_dir.join(MANIFEST_FILE_NAME)
}

fn manifest_tmp_path(db_dir: &Path) -> std::path::PathBuf {
    db_dir.join(MANIFEST_TMP_FILE_NAME)
}

fn json_error(err: serde_json::Error) -> crate::error::AetherError {
    io::Error::new(io::ErrorKind::InvalidData, err).into()
}

fn sync_directory(db_dir: &Path) -> Result<()> {
    match File::open(db_dir) {
        Ok(dir) => match dir.sync_data() {
            Ok(()) => Ok(()),
            Err(err) if should_ignore_dir_sync_error(&err) => Ok(()),
            Err(err) => Err(err.into()),
        },
        Err(err) if should_ignore_dir_sync_error(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn should_ignore_dir_sync_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::PermissionDenied | io::ErrorKind::InvalidInput | io::ErrorKind::Other
    )
}

impl From<&ManifestState> for PersistedManifestState {
    fn from(value: &ManifestState) -> Self {
        Self {
            next_sequence_number: value.next_sequence_number,
            next_table_id: value.next_table_id,
            wal_generation: value.wal_generation,
            wal_durable_checkpoint_offset: value.wal_durable_checkpoint_offset,
            levels: value
                .levels
                .iter()
                .map(|level| level.iter().map(PersistedSsTableMeta::from).collect())
                .collect(),
            pending_deletes: value
                .pending_deletes
                .iter()
                .map(PersistedPendingDeleteEntry::from)
                .collect(),
        }
    }
}

impl From<PersistedManifestState> for ManifestState {
    fn from(value: PersistedManifestState) -> Self {
        Self {
            next_sequence_number: value.next_sequence_number,
            next_table_id: value.next_table_id,
            wal_generation: value.wal_generation,
            wal_durable_checkpoint_offset: value.wal_durable_checkpoint_offset,
            levels: value
                .levels
                .into_iter()
                .map(|level| level.into_iter().map(SsTableMeta::from).collect())
                .collect(),
            pending_deletes: value
                .pending_deletes
                .into_iter()
                .map(PendingDeleteEntry::from)
                .collect(),
        }
    }
}

impl From<&SsTableMeta> for PersistedSsTableMeta {
    fn from(value: &SsTableMeta) -> Self {
        Self {
            table_id: value.table_id,
            level: value.level,
            min_key: value.min_key.to_vec(),
            max_key: value.max_key.to_vec(),
            max_seq: value.max_seq,
        }
    }
}

impl From<PersistedSsTableMeta> for SsTableMeta {
    fn from(value: PersistedSsTableMeta) -> Self {
        Self {
            table_id: value.table_id,
            level: value.level,
            min_key: value.min_key.into(),
            max_key: value.max_key.into(),
            max_seq: value.max_seq,
        }
    }
}

impl From<&PendingDeleteEntry> for PersistedPendingDeleteEntry {
    fn from(value: &PendingDeleteEntry) -> Self {
        Self {
            table_id: value.table_id,
            level: value.level,
            max_seq: value.max_seq,
        }
    }
}

impl From<PersistedPendingDeleteEntry> for PendingDeleteEntry {
    fn from(value: PersistedPendingDeleteEntry) -> Self {
        Self {
            table_id: value.table_id,
            level: value.level,
            max_seq: value.max_seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use bytes::Bytes;

    use super::{ManifestState, ManifestStore, PendingDeleteEntry};
    use crate::sstable::SsTableMeta;

    fn temp_db_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("aether-manifest-{prefix}-{nanos}"))
    }

    #[test]
    fn load_or_create_initializes_default_state() {
        let db_dir = temp_db_dir("create");

        let state = match ManifestStore::load_or_create(&db_dir) {
            Ok(state) => state,
            Err(err) => panic!("manifest load/create should succeed: {err}"),
        };

        assert_eq!(state, ManifestState::default());

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn commit_roundtrips_manifest_state() {
        let db_dir = temp_db_dir("roundtrip");

        let state = ManifestState {
            next_sequence_number: 42,
            next_table_id: 7,
            wal_generation: 3,
            wal_durable_checkpoint_offset: 1024,
            levels: vec![vec![SsTableMeta {
                table_id: 6,
                level: 0,
                min_key: Bytes::from_static(b"a"),
                max_key: Bytes::from_static(b"z"),
                max_seq: 41,
            }]],
            pending_deletes: vec![PendingDeleteEntry {
                table_id: 5,
                level: 1,
                max_seq: 40,
            }],
        };

        if let Err(err) = ManifestStore::commit(&db_dir, &state) {
            panic!("manifest commit should succeed: {err}");
        }

        let loaded = match ManifestStore::load_or_create(&db_dir) {
            Ok(state) => state,
            Err(err) => panic!("manifest reload should succeed: {err}"),
        };

        assert_eq!(loaded, state);

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn commit_cleans_up_temp_file_on_success() {
        let db_dir = temp_db_dir("tmp-cleanup");
        let state = ManifestState::default();

        if let Err(err) = ManifestStore::commit(&db_dir, &state) {
            panic!("manifest commit should succeed: {err}");
        }

        let tmp_path = db_dir.join("MANIFEST.tmp");
        assert!(!tmp_path.exists(), "temporary manifest should not remain");

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn load_manifest_without_pending_deletes_defaults_to_empty_queue() {
        let db_dir = temp_db_dir("pending-default");
        if let Err(err) = fs::create_dir_all(&db_dir) {
            panic!("db dir create should succeed: {err}");
        }

        let manifest_payload = r#"{
            "next_sequence_number": 9,
            "next_table_id": 3,
            "wal_generation": 1,
            "wal_durable_checkpoint_offset": 128,
            "levels": []
        }"#;
        if let Err(err) = fs::write(db_dir.join("MANIFEST"), manifest_payload.as_bytes()) {
            panic!("manifest write should succeed: {err}");
        }

        let state = match ManifestStore::load_or_create(&db_dir) {
            Ok(state) => state,
            Err(err) => panic!("manifest load should succeed: {err}"),
        };
        assert!(
            state.pending_deletes.is_empty(),
            "pending deletes should default to empty when field is missing"
        );

        if let Err(err) = fs::remove_dir_all(&db_dir) {
            panic!("cleanup should succeed: {err}");
        }
    }

    #[test]
    fn enqueue_and_dequeue_pending_delete_deduplicates_entries() {
        let mut state = ManifestState::default();
        state.enqueue_pending_delete(PendingDeleteEntry {
            table_id: 11,
            level: 0,
            max_seq: 99,
        });
        state.enqueue_pending_delete(PendingDeleteEntry {
            table_id: 11,
            level: 0,
            max_seq: 99,
        });
        assert_eq!(
            state.pending_deletes.len(),
            1,
            "duplicate should be ignored"
        );
        assert!(
            state.dequeue_pending_delete(11, 0),
            "existing entry should be removed"
        );
        assert!(
            !state.dequeue_pending_delete(11, 0),
            "missing entry should report no removal"
        );
    }
}
