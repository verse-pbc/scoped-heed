use crate::ScopedDbError;
use heed::{BytesDecode, BytesEncode};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

/// Adapter to convert `RangeBounds<&[u8]>` to `RangeBounds<[u8]>` for heed's Bytes codec.
pub struct HeedRangeAdapter<'a, R: RangeBounds<&'a [u8]>>(&'a R, PhantomData<&'a ()>);

impl<'a, R: RangeBounds<&'a [u8]>> HeedRangeAdapter<'a, R> {
    pub fn new(range: &'a R) -> Self {
        HeedRangeAdapter(range, PhantomData)
    }
}

impl<'a, R: RangeBounds<&'a [u8]>> RangeBounds<[u8]> for HeedRangeAdapter<'a, R> {
    fn start_bound(&self) -> Bound<&[u8]> {
        match self.0.start_bound() {
            Bound::Included(&k) => Bound::Included(k),
            Bound::Excluded(&k) => Bound::Excluded(k),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        match self.0.end_bound() {
            Bound::Included(&k) => Bound::Included(k),
            Bound::Excluded(&k) => Bound::Excluded(k),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

/// Specialized codec for byte-based scoped keys with optimized binary layout.
///
/// This codec provides an efficient encoding for scoped keys with the following binary structure:
/// ```text
/// [scope_hash_le: 4 bytes][key_len_le: 8 bytes][original_key_data]
/// ```
/// where:
/// - `scope_hash_le`: 32-bit xxHash of the scope name (little-endian)
/// - `key_len_le`: 64-bit length of the original key (little-endian)
/// - `original_key_data`: The original key bytes
///
/// This format was chosen to:
/// 1. Be efficiently decodable without full deserialization
/// 2. Maintain proper lexicographic ordering by scope_hash first, then by key
/// 3. Allow for efficient ranged operations with partial keys
/// 4. Support byte slices of any length
///
/// The 8-byte key length field matches bincode's encoding for byte slices
/// and provides sufficient capacity for even very large keys.
pub enum ScopedBytesCodec {}

impl ScopedBytesCodec {
    #[inline]
    pub fn encode(scope_hash: u32, key: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(12 + key.len());

        // Scope hash as u32 little-endian (4 bytes)
        output.extend_from_slice(&scope_hash.to_le_bytes());

        // Key length as u64 little-endian (8 bytes) - matches bincode format
        let key_len = key.len() as u64;
        output.extend_from_slice(&key_len.to_le_bytes());
        output.extend_from_slice(key);

        output
    }

    #[inline]
    pub fn decode(bytes: &[u8]) -> Result<(u32, &[u8]), ScopedDbError> {
        if bytes.len() < 12 {
            return Err(ScopedDbError::Encoding(
                "Not enough bytes to decode scoped key".into(),
            ));
        }

        // Extract scope hash from first 4 bytes
        let scope_hash = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // Extract key length from next 8 bytes
        let key_len_bytes = &bytes[4..12];
        let key_len = u64::from_le_bytes(key_len_bytes.try_into().unwrap());
        let key_start = 12;
        let key_end = key_start + key_len as usize;
        if bytes.len() < key_end {
            return Err(ScopedDbError::Encoding("Not enough bytes for key".into()));
        }
        let key = &bytes[key_start..key_end];

        Ok((scope_hash, key))
    }
}

impl<'a> BytesEncode<'a> for ScopedBytesCodec {
    type EItem = (u32, &'a [u8]);

    fn bytes_encode(
        (scope_hash, key): &Self::EItem,
    ) -> Result<std::borrow::Cow<'a, [u8]>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(std::borrow::Cow::Owned(Self::encode(*scope_hash, key)))
    }
}

impl<'a> BytesDecode<'a> for ScopedBytesCodec {
    type DItem = (u32, &'a [u8]);

    fn bytes_decode(
        bytes: &'a [u8],
    ) -> Result<Self::DItem, Box<dyn std::error::Error + Send + Sync>> {
        Self::decode(bytes).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Get a default key value for range bound construction.
///
/// This function creates a default value of type K for use in range bounds and other
/// database operations. Currently it just calls K::default(), but is kept as a separate
/// function to allow for future optimizations or alternate implementations.
///
/// It's used in methods like `clear()`, `range()`, and `iter()` to create the minimum 
/// viable range bounds when working with scoped databases.
pub fn get_key_default<K>() -> K
where
    K: Default,
{
    K::default()
}
