//! WAL physical record format — mirrors RocksDB's log_format.h exactly.
//!
//! # Block layout
//!
//! The WAL is divided into 32 KiB blocks. Records are embedded within blocks;
//! a record that spans a block boundary is fragmented into First/Middle/Last pieces.
//!
//! ```text
//! Block (32 768 bytes):
//! ┌─────────────────────────────────────────────────────┐
//! │  Record 0: [header][payload]                        │
//! │  Record 1: [header][payload]                        │
//! │  ...                                                │
//! │  [zero padding to fill the block]                   │
//! └─────────────────────────────────────────────────────┘
//!
//! Standard header (7 bytes):
//!   CRC32c  [4 bytes LE]   — checksum over [type_byte ++ payload]
//!   Length  [2 bytes LE]   — payload length (max 32 761 bytes)
//!   Type    [1 byte]       — RecordType discriminant
//!
//! Recyclable header (11 bytes, used when recyclable=true):
//!   CRC32c     [4 bytes LE]
//!   Length     [2 bytes LE]
//!   Type       [1 byte]
//!   LogNumber  [4 bytes LE]  — identifies log generation for recycled files
//! ```

/// Block size: 32 KiB. Records are never split across this boundary except via
/// the standard First/Middle/Last fragmentation mechanism.
pub const BLOCK_SIZE: usize = 32_768;

/// Standard (non-recyclable) record header size in bytes.
pub const HEADER_SIZE: usize = 7;

/// Recyclable record header size in bytes. Adds a 4-byte log number field.
pub const RECYCLABLE_HEADER_SIZE: usize = 11;

/// Minimum bytes remaining in a block before we zero-pad and start a new one.
/// Must be ≥ HEADER_SIZE to fit even a zero-payload record.
pub const MIN_REMAINING: usize = HEADER_SIZE;

/// Record type discriminant (1 byte).
/// Recyclable variants (≥5) are used when `WalWriter::recyclable = true`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordType {
    /// Entire record fits in one physical record.
    Full = 1,
    /// First fragment of a multi-block record.
    First = 2,
    /// Middle fragment.
    Middle = 3,
    /// Last fragment.
    Last = 4,
    /// Full record in recyclable format.
    RecyclableFull = 5,
    /// First fragment, recyclable.
    RecyclableFirst = 6,
    /// Middle fragment, recyclable.
    RecyclableMiddle = 7,
    /// Last fragment, recyclable.
    RecyclableLast = 8,
}

impl RecordType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Full),
            2 => Some(Self::First),
            3 => Some(Self::Middle),
            4 => Some(Self::Last),
            5 => Some(Self::RecyclableFull),
            6 => Some(Self::RecyclableFirst),
            7 => Some(Self::RecyclableMiddle),
            8 => Some(Self::RecyclableLast),
            _ => None,
        }
    }

    pub fn is_recyclable(self) -> bool {
        (self as u8) >= 5
    }

    pub fn header_size(self) -> usize {
        if self.is_recyclable() {
            RECYCLABLE_HEADER_SIZE
        } else {
            HEADER_SIZE
        }
    }
}
