//! Byte-stable value types for dense 8-byte maps.

mod sealed {
    pub trait Sealed {}
}

/// Values that can be stored losslessly in one dense `AtomicU64` slot.
///
/// This trait is sealed because raw `Copy + size_of::<T>() == 8` is not enough:
/// references, pointers, and structs with padding can have invalid or
/// uninitialised bit patterns. Implementations are limited to primitive values
/// with well-defined 64-bit representations.
pub trait DenseValue: sealed::Sealed + Copy + Send + Sync + 'static {
    fn into_u64(self) -> u64;
    fn from_u64(raw: u64) -> Self;
}

impl sealed::Sealed for u64 {}
impl DenseValue for u64 {
    #[inline]
    fn into_u64(self) -> u64 {
        self
    }

    #[inline]
    fn from_u64(raw: u64) -> Self {
        raw
    }
}

impl sealed::Sealed for i64 {}
impl DenseValue for i64 {
    #[inline]
    fn into_u64(self) -> u64 {
        self as u64
    }

    #[inline]
    fn from_u64(raw: u64) -> Self {
        raw as i64
    }
}

impl sealed::Sealed for f64 {}
impl DenseValue for f64 {
    #[inline]
    fn into_u64(self) -> u64 {
        self.to_bits()
    }

    #[inline]
    fn from_u64(raw: u64) -> Self {
        Self::from_bits(raw)
    }
}

impl sealed::Sealed for [u8; 8] {}
impl DenseValue for [u8; 8] {
    #[inline]
    fn into_u64(self) -> u64 {
        u64::from_ne_bytes(self)
    }

    #[inline]
    fn from_u64(raw: u64) -> Self {
        raw.to_ne_bytes()
    }
}
