use crate::engine::{
    DocumentVersion, EngineError, EngineResult, Lsn, NoSqlEngine, RawKey, RawValue, ReadOptions,
    WalCorruptionKind,
};
use mfs_core::Operation;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

pub(crate) const RAW_WAL_RECORD_MAGIC: u32 = u32::from_le_bytes(*b"MFSW");
pub const RAW_WAL_FORMAT_VERSION: u16 = 2;

const RAW_WAL_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const RAW_WAL_MAX_FIELD_BYTES: usize = 64 * 1024 * 1024;
const HEADER_LEN: u64 = 8;
const CHECKSUM_LEN: u64 = 4;
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawWalRecord {
    pub lsn: Lsn,
    pub version: DocumentVersion,
    pub collection: String,
    pub op: Operation,
    pub key: RawKey,
    pub value: Option<RawValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawWalReplayStats {
    pub records: usize,
    pub last_lsn: Lsn,
}

pub struct RawWalSegmentWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    next_lsn: u64,
    record_versions: HashMap<(String, RawKey), DocumentVersion>,
    scratch: Vec<u8>,
}

pub struct RawWalSegmentReader;

impl RawWalSegmentWriter {
    pub fn open(path: impl AsRef<Path>) -> EngineResult<Self> {
        let path = path.as_ref().to_path_buf();
        let append_state = append_state_for_path(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|e| wal_io("open raw WAL segment", e))?;
        let file_len = file
            .metadata()
            .map_err(|e| wal_io("stat raw WAL segment", e))?
            .len();
        if append_state.last_good_offset < file_len {
            file.set_len(append_state.last_good_offset)
                .map_err(|e| wal_io("truncate raw WAL torn tail", e))?;
            file.sync_data()
                .map_err(|e| wal_io("sync raw WAL torn-tail truncation", e))?;
        }

        Ok(Self {
            path,
            writer: BufWriter::new(file),
            next_lsn: append_state.next_lsn,
            record_versions: append_state.record_versions,
            scratch: Vec::with_capacity(256),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append_put(
        &mut self,
        collection: &str,
        key: &RawKey,
        value: &RawValue,
    ) -> EngineResult<Lsn> {
        let version = self.next_record_version(collection, key);
        self.append_record(collection, Operation::Put, key, Some(value), version, true)
    }

    pub fn append_delete(&mut self, collection: &str, key: &RawKey) -> EngineResult<Lsn> {
        let version = self.next_record_version(collection, key);
        self.append_record(collection, Operation::Delete, key, None, version, true)
    }

    pub(crate) fn append_put_versioned(
        &mut self,
        collection: &str,
        key: &RawKey,
        value: &RawValue,
        version: DocumentVersion,
    ) -> EngineResult<Lsn> {
        self.append_record(collection, Operation::Put, key, Some(value), version, false)
    }

    pub(crate) fn append_delete_versioned(
        &mut self,
        collection: &str,
        key: &RawKey,
        version: DocumentVersion,
    ) -> EngineResult<Lsn> {
        self.append_record(collection, Operation::Delete, key, None, version, false)
    }

    pub fn flush(&mut self) -> EngineResult<()> {
        self.writer
            .flush()
            .map_err(|e| wal_io("flush raw WAL segment", e))
    }

    pub fn sync_now(&mut self) -> EngineResult<()> {
        self.writer
            .flush()
            .map_err(|e| wal_io("flush raw WAL segment", e))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| wal_io("sync raw WAL segment", e))
    }

    fn append_record(
        &mut self,
        collection: &str,
        op: Operation,
        key: &RawKey,
        value: Option<&RawValue>,
        version: DocumentVersion,
        track_version: bool,
    ) -> EngineResult<Lsn> {
        let lsn = Lsn::new(self.next_lsn);
        self.next_lsn = self.next_lsn.saturating_add(1);

        encode_payload(&mut self.scratch, lsn, version, collection, op, key, value)?;
        let payload_len =
            u32::try_from(self.scratch.len()).map_err(|_| EngineError::WalCorruption {
                offset: 0,
                kind: WalCorruptionKind::PayloadTooLarge,
            })?;

        let mut crc = crc32c::crc32c(&RAW_WAL_RECORD_MAGIC.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &payload_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.scratch);

        self.writer
            .write_all(&RAW_WAL_RECORD_MAGIC.to_le_bytes())
            .map_err(|e| wal_io("write raw WAL magic", e))?;
        self.writer
            .write_all(&payload_len.to_le_bytes())
            .map_err(|e| wal_io("write raw WAL length", e))?;
        self.writer
            .write_all(&self.scratch)
            .map_err(|e| wal_io("write raw WAL payload", e))?;
        self.writer
            .write_all(&crc.to_le_bytes())
            .map_err(|e| wal_io("write raw WAL checksum", e))?;

        if track_version {
            self.record_versions
                .insert((collection.to_string(), key.clone()), version);
        }

        Ok(lsn)
    }

    fn next_record_version(&self, collection: &str, key: &RawKey) -> DocumentVersion {
        let current = self
            .record_versions
            .get(&(collection.to_string(), key.clone()))
            .copied()
            .unwrap_or(DocumentVersion::ZERO);
        DocumentVersion::new(current.get() + 1)
    }
}

impl RawWalSegmentReader {
    pub fn read_records(path: impl AsRef<Path>) -> EngineResult<Vec<RawWalRecord>> {
        let mut records = Vec::new();
        scan_records(path, |record| {
            records.push(record);
            Ok(())
        })?;
        Ok(records)
    }

    pub fn replay_into(
        path: impl AsRef<Path>,
        engine: &NoSqlEngine,
    ) -> EngineResult<RawWalReplayStats> {
        let mut stats = RawWalReplayStats {
            records: 0,
            last_lsn: Lsn::ZERO,
        };

        scan_records(path, |record| {
            apply_raw_record(engine, &record)?;
            stats.records += 1;
            stats.last_lsn = record.lsn;
            Ok(())
        })?;

        Ok(stats)
    }

    pub fn replay_after(
        path: impl AsRef<Path>,
        engine: &NoSqlEngine,
        after_lsn: Lsn,
    ) -> EngineResult<RawWalReplayStats> {
        let mut stats = RawWalReplayStats {
            records: 0,
            last_lsn: Lsn::ZERO,
        };

        scan_records(path, |record| {
            if record.lsn > after_lsn {
                apply_raw_record(engine, &record)?;
                stats.records += 1;
                stats.last_lsn = record.lsn;
            }
            Ok(())
        })?;

        Ok(stats)
    }
}

pub fn replay_raw_wal(
    path: impl AsRef<Path>,
    engine: &NoSqlEngine,
) -> EngineResult<RawWalReplayStats> {
    RawWalSegmentReader::replay_into(path, engine)
}

pub fn replay_raw_wal_after(
    path: impl AsRef<Path>,
    engine: &NoSqlEngine,
    after_lsn: Lsn,
) -> EngineResult<RawWalReplayStats> {
    RawWalSegmentReader::replay_after(path, engine, after_lsn)
}

struct RawWalAppendState {
    next_lsn: u64,
    last_good_offset: u64,
    record_versions: HashMap<(String, RawKey), DocumentVersion>,
}

fn append_state_for_path(path: &Path) -> EngineResult<RawWalAppendState> {
    let mut last_lsn = Lsn::ZERO;
    let mut record_versions = HashMap::new();
    if !path.exists() {
        return Ok(RawWalAppendState {
            next_lsn: 1,
            last_good_offset: 0,
            record_versions,
        });
    }

    let scan = scan_records_with_offsets(path, |record| {
        last_lsn = record.lsn;
        record_versions.insert(
            (record.collection.clone(), record.key.clone()),
            record.version,
        );
        Ok(())
    })?;
    Ok(RawWalAppendState {
        next_lsn: last_lsn.get().saturating_add(1).max(1),
        last_good_offset: scan.last_good_offset,
        record_versions,
    })
}

struct RawWalScan {
    stats: RawWalReplayStats,
    last_good_offset: u64,
}

fn scan_records(
    path: impl AsRef<Path>,
    on_record: impl FnMut(RawWalRecord) -> EngineResult<()>,
) -> EngineResult<RawWalReplayStats> {
    scan_records_with_offsets(path, on_record).map(|scan| scan.stats)
}

fn scan_records_with_offsets(
    path: impl AsRef<Path>,
    mut on_record: impl FnMut(RawWalRecord) -> EngineResult<()>,
) -> EngineResult<RawWalScan> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(RawWalScan {
            stats: RawWalReplayStats {
                records: 0,
                last_lsn: Lsn::ZERO,
            },
            last_good_offset: 0,
        });
    }

    let file = File::open(path).map_err(|e| wal_io("open raw WAL segment", e))?;
    let total_len = file
        .metadata()
        .map_err(|e| wal_io("stat raw WAL segment", e))?
        .len();
    let mut reader = BufReader::new(file);
    let mut stats = RawWalReplayStats {
        records: 0,
        last_lsn: Lsn::ZERO,
    };
    let mut last_good_offset = 0;

    loop {
        let record_start = reader
            .stream_position()
            .map_err(|e| wal_io("seek raw WAL segment", e))?;
        if record_start == total_len {
            break;
        }
        if total_len.saturating_sub(record_start) < HEADER_LEN {
            break;
        }

        let mut header = [0u8; HEADER_LEN as usize];
        reader
            .read_exact(&mut header)
            .map_err(|e| wal_io("read raw WAL header", e))?;
        let magic = u32::from_le_bytes(header[0..4].try_into().expect("magic length"));
        if magic != RAW_WAL_RECORD_MAGIC {
            return Err(corrupt(record_start, WalCorruptionKind::BadMagic));
        }

        let payload_len = u32::from_le_bytes(header[4..8].try_into().expect("length")) as usize;
        let record_len = HEADER_LEN + payload_len as u64 + CHECKSUM_LEN;
        if record_start.saturating_add(record_len) > total_len {
            break;
        }
        if payload_len > RAW_WAL_MAX_PAYLOAD_BYTES {
            return Err(corrupt(record_start, WalCorruptionKind::PayloadTooLarge));
        }

        let mut payload = vec![0u8; payload_len];
        reader
            .read_exact(&mut payload)
            .map_err(|e| wal_io("read raw WAL payload", e))?;
        let mut checksum = [0u8; CHECKSUM_LEN as usize];
        reader
            .read_exact(&mut checksum)
            .map_err(|e| wal_io("read raw WAL checksum", e))?;
        let stored_crc = u32::from_le_bytes(checksum);

        let mut crc = crc32c::crc32c(&RAW_WAL_RECORD_MAGIC.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &(payload_len as u32).to_le_bytes());
        crc = crc32c::crc32c_append(crc, &payload);
        if crc != stored_crc {
            let record_end = record_start + record_len;
            if record_end == total_len {
                break;
            }
            return Err(corrupt(record_start, WalCorruptionKind::ChecksumMismatch));
        }

        let record = decode_payload(&payload, record_start)?;
        if record.lsn <= stats.last_lsn {
            return Err(corrupt(record_start, WalCorruptionKind::NonMonotonicLsn));
        }
        stats.records += 1;
        stats.last_lsn = record.lsn;
        on_record(record)?;
        last_good_offset = record_start + record_len;
    }

    Ok(RawWalScan {
        stats,
        last_good_offset,
    })
}

fn apply_raw_record(engine: &NoSqlEngine, record: &RawWalRecord) -> EngineResult<()> {
    ensure_raw_collection(engine, &record.collection)?;
    match record.op {
        Operation::Put => {
            let value = record
                .value
                .clone()
                .ok_or_else(|| corrupt(record.lsn.get(), WalCorruptionKind::MalformedRecord))?;
            engine.apply_raw_replay_record(
                &record.collection,
                record.key.clone(),
                Some(value),
                record.version,
            )?;
        }
        Operation::Delete => {
            engine.apply_raw_replay_record(
                &record.collection,
                record.key.clone(),
                None,
                record.version,
            )?;
        }
    }
    Ok(())
}

fn ensure_raw_collection(engine: &NoSqlEngine, collection: &str) -> EngineResult<()> {
    match engine.create_raw_collection(collection) {
        Ok(_) => Ok(()),
        Err(EngineError::CollectionAlreadyExists { .. }) => Ok(()),
        Err(EngineError::CollectionLimitExceeded { .. }) => {
            let probe = RawKey::from(&b"__mfs_collection_probe__"[..]);
            match engine.get_raw(collection, &probe, ReadOptions::default()) {
                Ok(_) => Ok(()),
                Err(_) => Err(EngineError::CollectionLimitExceeded {
                    max_collections: engine.config().max_collections,
                }),
            }
        }
        Err(err) => Err(err),
    }
}

fn encode_payload(
    out: &mut Vec<u8>,
    lsn: Lsn,
    version: DocumentVersion,
    collection: &str,
    op: Operation,
    key: &RawKey,
    value: Option<&RawValue>,
) -> EngineResult<()> {
    let collection_bytes = collection.as_bytes();
    validate_field_len(collection_bytes.len())?;
    validate_field_len(key.as_bytes().len())?;
    if let Some(value) = value {
        validate_field_len(value.as_bytes().len())?;
    }

    out.clear();
    out.extend_from_slice(&RAW_WAL_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&lsn.get().to_le_bytes());
    out.extend_from_slice(&version.get().to_le_bytes());
    write_len_prefixed(collection_bytes, out);
    out.push(op_code(op));
    write_len_prefixed(key.as_bytes(), out);
    match (op, value) {
        (Operation::Put, Some(value)) => write_len_prefixed(value.as_bytes(), out),
        (Operation::Delete, None) => out.extend_from_slice(&0u32.to_le_bytes()),
        _ => return Err(corrupt(0, WalCorruptionKind::MalformedRecord)),
    }
    Ok(())
}

fn decode_payload(payload: &[u8], record_start: u64) -> EngineResult<RawWalRecord> {
    let mut cursor = 0usize;
    let version = read_u16(payload, &mut cursor, record_start)?;
    if version != RAW_WAL_FORMAT_VERSION {
        return Err(corrupt(
            record_start,
            WalCorruptionKind::UnknownFormatVersion,
        ));
    }

    let lsn = Lsn::new(read_u64(payload, &mut cursor, record_start)?);
    let record_version = DocumentVersion::new(read_u64(payload, &mut cursor, record_start)?);
    let collection_bytes = read_bytes(payload, &mut cursor, record_start)?;
    let collection = String::from_utf8(collection_bytes.to_vec())
        .map_err(|_| corrupt(record_start, WalCorruptionKind::MalformedRecord))?;
    if collection.is_empty() {
        return Err(corrupt(record_start, WalCorruptionKind::MalformedRecord));
    }

    let op = match read_u8(payload, &mut cursor, record_start)? {
        OP_PUT => Operation::Put,
        OP_DELETE => Operation::Delete,
        _ => return Err(corrupt(record_start, WalCorruptionKind::UnknownOperation)),
    };
    let key = RawKey::from(read_bytes(payload, &mut cursor, record_start)?.to_vec());
    let value_bytes = read_bytes(payload, &mut cursor, record_start)?;
    let value = match op {
        Operation::Put => Some(RawValue::from(value_bytes.to_vec())),
        Operation::Delete if value_bytes.is_empty() => None,
        Operation::Delete => return Err(corrupt(record_start, WalCorruptionKind::MalformedRecord)),
    };

    if cursor != payload.len() {
        return Err(corrupt(record_start, WalCorruptionKind::MalformedRecord));
    }

    Ok(RawWalRecord {
        lsn,
        version: record_version,
        collection,
        op,
        key,
        value,
    })
}

fn validate_field_len(len: usize) -> EngineResult<()> {
    if len > RAW_WAL_MAX_FIELD_BYTES || len > u32::MAX as usize {
        return Err(corrupt(0, WalCorruptionKind::FieldTooLarge));
    }
    Ok(())
}

fn write_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) {
    let len = u32::try_from(bytes.len()).expect("field length checked");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

fn read_u8(payload: &[u8], cursor: &mut usize, offset: u64) -> EngineResult<u8> {
    if payload.len().saturating_sub(*cursor) < 1 {
        return Err(corrupt(offset, WalCorruptionKind::MalformedRecord));
    }
    let value = payload[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u16(payload: &[u8], cursor: &mut usize, offset: u64) -> EngineResult<u16> {
    Ok(u16::from_le_bytes(read_exact::<2>(
        payload, cursor, offset,
    )?))
}

fn read_u32(payload: &[u8], cursor: &mut usize, offset: u64) -> EngineResult<u32> {
    Ok(u32::from_le_bytes(read_exact::<4>(
        payload, cursor, offset,
    )?))
}

fn read_u64(payload: &[u8], cursor: &mut usize, offset: u64) -> EngineResult<u64> {
    Ok(u64::from_le_bytes(read_exact::<8>(
        payload, cursor, offset,
    )?))
}

fn read_bytes<'a>(payload: &'a [u8], cursor: &mut usize, offset: u64) -> EngineResult<&'a [u8]> {
    let len = read_u32(payload, cursor, offset)? as usize;
    if len > RAW_WAL_MAX_FIELD_BYTES {
        return Err(corrupt(offset, WalCorruptionKind::FieldTooLarge));
    }
    if payload.len().saturating_sub(*cursor) < len {
        return Err(corrupt(offset, WalCorruptionKind::MalformedRecord));
    }
    let out = &payload[*cursor..*cursor + len];
    *cursor += len;
    Ok(out)
}

fn read_exact<const N: usize>(
    payload: &[u8],
    cursor: &mut usize,
    offset: u64,
) -> EngineResult<[u8; N]> {
    if payload.len().saturating_sub(*cursor) < N {
        return Err(corrupt(offset, WalCorruptionKind::MalformedRecord));
    }
    let out = payload[*cursor..*cursor + N]
        .try_into()
        .expect("fixed field length");
    *cursor += N;
    Ok(out)
}

fn op_code(op: Operation) -> u8 {
    match op {
        Operation::Put => OP_PUT,
        Operation::Delete => OP_DELETE,
    }
}

fn corrupt(offset: u64, kind: WalCorruptionKind) -> EngineError {
    EngineError::WalCorruption { offset, kind }
}

fn wal_io(operation: &'static str, error: io::Error) -> EngineError {
    EngineError::WalIo {
        operation,
        message: error.to_string(),
    }
}
