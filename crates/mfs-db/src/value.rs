//! Redis-like value model for the object-store API.
//!
//! This is intentionally separate from the dense writers. Dense writers are
//! limited to 8-byte `Copy` values; `MfsValue` represents variable-sized data
//! and is meant to be stored behind `Arc` by the object-store writers.

use mfs_core::durability::WalCodec;
use std::collections::{BTreeMap, BTreeSet};
use std::io;

pub const MAX_ENCODED_VALUE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_COLLECTION_ITEMS: usize = 1_048_576;
pub const MAX_BLOB_BYTES: usize = 64 * 1024 * 1024;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueTag {
    Bytes = 0,
    String = 1,
    Integer = 2,
    List = 3,
    Hash = 4,
    Set = 5,
    SortedSet = 6,
    Json = 7,
    Stream = 8,
    Null = 9,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetEntry {
    pub score: f64,
    pub member: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub millis: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MfsValue {
    Bytes(Vec<u8>),
    String(String),
    Integer(i64),
    List(Vec<Vec<u8>>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(BTreeSet<Vec<u8>>),
    SortedSet(Vec<SortedSetEntry>),
    Json(Vec<u8>),
    Stream(Vec<StreamEntry>),
    Null,
}

impl MfsValue {
    pub fn tag(&self) -> ValueTag {
        match self {
            Self::Bytes(_) => ValueTag::Bytes,
            Self::String(_) => ValueTag::String,
            Self::Integer(_) => ValueTag::Integer,
            Self::List(_) => ValueTag::List,
            Self::Hash(_) => ValueTag::Hash,
            Self::Set(_) => ValueTag::Set,
            Self::SortedSet(_) => ValueTag::SortedSet,
            Self::Json(_) => ValueTag::Json,
            Self::Stream(_) => ValueTag::Stream,
            Self::Null => ValueTag::Null,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MfsValueCodec;

impl WalCodec<Vec<u8>, MfsValue> for MfsValueCodec {
    fn encode_key(&self, key: &Vec<u8>, out: &mut Vec<u8>) {
        out.extend_from_slice(key);
    }

    fn encode_value(&self, value: &MfsValue, out: &mut Vec<u8>) {
        encode_value(value, out);
    }

    fn decode_key(&self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }

    fn decode_value(&self, bytes: &[u8]) -> io::Result<MfsValue> {
        decode_value(bytes)
    }
}

pub fn encode_value(value: &MfsValue, out: &mut Vec<u8>) {
    out.push(value.tag() as u8);
    match value {
        MfsValue::Bytes(bytes) | MfsValue::Json(bytes) => encode_bytes(bytes, out),
        MfsValue::String(value) => encode_bytes(value.as_bytes(), out),
        MfsValue::Integer(value) => out.extend_from_slice(&value.to_le_bytes()),
        MfsValue::List(values) => {
            encode_len(values.len(), out);
            for value in values {
                encode_bytes(value, out);
            }
        }
        MfsValue::Hash(fields) => {
            encode_len(fields.len(), out);
            for (field, value) in fields {
                encode_bytes(field, out);
                encode_bytes(value, out);
            }
        }
        MfsValue::Set(values) => {
            encode_len(values.len(), out);
            for value in values {
                encode_bytes(value, out);
            }
        }
        MfsValue::SortedSet(entries) => {
            encode_len(entries.len(), out);
            for entry in entries {
                out.extend_from_slice(&entry.score.to_le_bytes());
                encode_bytes(&entry.member, out);
            }
        }
        MfsValue::Stream(entries) => {
            encode_len(entries.len(), out);
            for entry in entries {
                out.extend_from_slice(&entry.id.millis.to_le_bytes());
                out.extend_from_slice(&entry.id.sequence.to_le_bytes());
                encode_len(entry.fields.len(), out);
                for (field, value) in &entry.fields {
                    encode_bytes(field, out);
                    encode_bytes(value, out);
                }
            }
        }
        MfsValue::Null => {}
    }
}

pub fn decode_value(bytes: &[u8]) -> io::Result<MfsValue> {
    if bytes.len() > MAX_ENCODED_VALUE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MfsValue payload exceeds decoder limit",
        ));
    }
    let mut cursor = 0usize;
    let tag = read_u8(bytes, &mut cursor)?;
    match tag {
        0 => Ok(MfsValue::Bytes(read_bytes(bytes, &mut cursor)?.to_vec())),
        1 => {
            let value = String::from_utf8(read_bytes(bytes, &mut cursor)?.to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(MfsValue::String(value))
        }
        2 => Ok(MfsValue::Integer(read_i64(bytes, &mut cursor)?)),
        3 => {
            let count = read_count(bytes, &mut cursor, 4)?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(read_bytes(bytes, &mut cursor)?.to_vec());
            }
            Ok(MfsValue::List(values))
        }
        4 => {
            let mut fields = BTreeMap::new();
            for _ in 0..read_count(bytes, &mut cursor, 8)? {
                let field = read_bytes(bytes, &mut cursor)?.to_vec();
                let value = read_bytes(bytes, &mut cursor)?.to_vec();
                fields.insert(field, value);
            }
            Ok(MfsValue::Hash(fields))
        }
        5 => {
            let mut values = BTreeSet::new();
            for _ in 0..read_count(bytes, &mut cursor, 4)? {
                values.insert(read_bytes(bytes, &mut cursor)?.to_vec());
            }
            Ok(MfsValue::Set(values))
        }
        6 => {
            let count = read_count(bytes, &mut cursor, 12)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let score = f64::from_le_bytes(read_exact::<8>(bytes, &mut cursor)?);
                if !score.is_finite() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "sorted set score must be finite",
                    ));
                }
                let member = read_bytes(bytes, &mut cursor)?.to_vec();
                entries.push(SortedSetEntry { score, member });
            }
            Ok(MfsValue::SortedSet(entries))
        }
        7 => Ok(MfsValue::Json(read_bytes(bytes, &mut cursor)?.to_vec())),
        8 => {
            let count = read_count(bytes, &mut cursor, 20)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let millis = u64::from_le_bytes(read_exact::<8>(bytes, &mut cursor)?);
                let sequence = u64::from_le_bytes(read_exact::<8>(bytes, &mut cursor)?);
                let mut fields = BTreeMap::new();
                for _ in 0..read_count(bytes, &mut cursor, 8)? {
                    let field = read_bytes(bytes, &mut cursor)?.to_vec();
                    let value = read_bytes(bytes, &mut cursor)?.to_vec();
                    fields.insert(field, value);
                }
                entries.push(StreamEntry {
                    id: StreamId { millis, sequence },
                    fields,
                });
            }
            Ok(MfsValue::Stream(entries))
        }
        9 => Ok(MfsValue::Null),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown MfsValue tag",
        )),
    }
    .and_then(|value| {
        if cursor == bytes.len() {
            Ok(value)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing MfsValue bytes",
            ))
        }
    })
}

fn encode_len(len: usize, out: &mut Vec<u8>) {
    let len = u32::try_from(len).expect("MfsValue collection too large");
    out.extend_from_slice(&len.to_le_bytes());
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    encode_len(bytes.len(), out);
    out.extend_from_slice(bytes);
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> io::Result<u8> {
    if bytes.len().saturating_sub(*cursor) < 1 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "tag truncated",
        ));
    }
    let value = bytes[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> io::Result<i64> {
    Ok(i64::from_le_bytes(read_exact::<8>(bytes, cursor)?))
}

fn read_len(bytes: &[u8], cursor: &mut usize) -> io::Result<usize> {
    Ok(u32::from_le_bytes(read_exact::<4>(bytes, cursor)?) as usize)
}

fn read_count(bytes: &[u8], cursor: &mut usize, min_bytes_per_item: usize) -> io::Result<usize> {
    let count = read_len(bytes, cursor)?;
    if count > MAX_COLLECTION_ITEMS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MfsValue collection exceeds item limit",
        ));
    }
    let remaining = bytes.len().saturating_sub(*cursor);
    if min_bytes_per_item > 0 && count > remaining / min_bytes_per_item {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "MfsValue collection length exceeds remaining payload",
        ));
    }
    Ok(count)
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> io::Result<&'a [u8]> {
    let len = read_len(bytes, cursor)?;
    if len > MAX_BLOB_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MfsValue byte field exceeds decoder limit",
        ));
    }
    if bytes.len().saturating_sub(*cursor) < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "bytes truncated",
        ));
    }
    let out = &bytes[*cursor..*cursor + len];
    *cursor += len;
    Ok(out)
}

fn read_exact<const N: usize>(bytes: &[u8], cursor: &mut usize) -> io::Result<[u8; N]> {
    if bytes.len().saturating_sub(*cursor) < N {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "fixed field truncated",
        ));
    }
    let out = bytes[*cursor..*cursor + N]
        .try_into()
        .expect("fixed length");
    *cursor += N;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mfs_core::{FlushBackend, FlushRecord, Operation};

    #[test]
    fn codec_round_trips_all_value_shapes() {
        let mut hash = BTreeMap::new();
        hash.insert(b"name".to_vec(), b"Ada".to_vec());
        let mut set = BTreeSet::new();
        set.insert(b"member".to_vec());
        let stream = StreamEntry {
            id: StreamId {
                millis: 1,
                sequence: 2,
            },
            fields: hash.clone(),
        };
        let values = [
            MfsValue::Bytes(b"raw".to_vec()),
            MfsValue::String("hello".to_string()),
            MfsValue::Integer(-42),
            MfsValue::List(vec![b"a".to_vec(), b"b".to_vec()]),
            MfsValue::Hash(hash),
            MfsValue::Set(set),
            MfsValue::SortedSet(vec![SortedSetEntry {
                score: 1.5,
                member: b"z".to_vec(),
            }]),
            MfsValue::Json(br#"{"a":1}"#.to_vec()),
            MfsValue::Stream(vec![stream]),
            MfsValue::Null,
        ];
        for value in values {
            let mut encoded = Vec::new();
            encode_value(&value, &mut encoded);
            assert_eq!(decode_value(&encoded).unwrap(), value);
        }
    }

    #[test]
    fn wal_codec_round_trips_object_records() {
        let mut path = std::env::temp_dir();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("mfs_value_codec_{ts}.wal"));

        let mut hash = BTreeMap::new();
        hash.insert(b"field".to_vec(), b"value".to_vec());
        {
            let mut wal = mfs_core::durability::WalBackend::open(
                &path,
                MfsValueCodec,
                mfs_core::durability::WalConfig::default(),
            )
            .unwrap();
            wal.flush(&[
                FlushRecord {
                    key: b"a".to_vec(),
                    value: Some(std::sync::Arc::new(MfsValue::String("hello".to_string()))),
                    version: 1,
                    op: Operation::Put,
                },
                FlushRecord {
                    key: b"b".to_vec(),
                    value: Some(std::sync::Arc::new(MfsValue::Hash(hash.clone()))),
                    version: 2,
                    op: Operation::Put,
                },
            ])
            .unwrap();
            wal.sync_now().unwrap();
        }

        let mut seen = Vec::new();
        mfs_core::durability::WalBackend::<Vec<u8>, MfsValue, MfsValueCodec>::replay(
            &path,
            &MfsValueCodec,
            |record| seen.push((record.key, record.value, record.version, record.op)),
        )
        .unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            seen[0],
            (
                b"a".to_vec(),
                Some(MfsValue::String("hello".to_string())),
                1,
                Operation::Put
            )
        );
        assert_eq!(
            seen[1],
            (b"b".to_vec(), Some(MfsValue::Hash(hash)), 2, Operation::Put)
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn decoder_rejects_malformed_payloads() {
        assert!(decode_value(&[]).is_err());
        assert!(decode_value(&[255]).is_err());
        assert!(decode_value(&[ValueTag::String as u8, 10, 0, 0, 0, b'a']).is_err());
        assert!(decode_value(&[ValueTag::String as u8, 1, 0, 0, 0, 0xff]).is_err());
        let mut trailing = Vec::new();
        encode_value(&MfsValue::Integer(1), &mut trailing);
        trailing.push(0);
        assert!(decode_value(&trailing).is_err());
    }

    #[test]
    fn decoder_rejects_oversized_lengths_before_allocating() {
        let mut huge_list = vec![ValueTag::List as u8];
        huge_list.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_value(&huge_list).is_err());

        let mut huge_bytes = vec![ValueTag::Bytes as u8];
        huge_bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_value(&huge_bytes).is_err());
    }

    #[test]
    fn decoder_rejects_non_finite_sorted_set_scores() {
        let mut encoded = vec![ValueTag::SortedSet as u8];
        encoded.extend_from_slice(&1u32.to_le_bytes());
        encoded.extend_from_slice(&f64::NAN.to_le_bytes());
        encoded.extend_from_slice(&1u32.to_le_bytes());
        encoded.push(b'a');
        assert!(decode_value(&encoded).is_err());
    }
}
