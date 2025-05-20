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

/// Specialized codec for byte-based scoped keys to match bincode encoding
#[doc(hidden)]
pub enum ScopedBytesCodec {}

impl ScopedBytesCodec {
    #[inline]
    pub fn encode(scope_hash: u32, key: &[u8]) -> Vec<u8> {
        // Total size: 4 (u32) + 8 (u64 length) + key.len()
        let mut output = Vec::with_capacity(12 + key.len());

        // 1. Encode scope_hash as u32 little-endian (4 bytes)
        output.extend_from_slice(&scope_hash.to_le_bytes());

        // 2. Encode key length as u64 little-endian (8 bytes) - to match bincode
        let key_len = key.len() as u64;
        output.extend_from_slice(&key_len.to_le_bytes());

        // 3. Encode key bytes
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

        // 1. Decode scope_hash
        let scope_hash = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // 2. Decode key length
        let key_len_bytes = &bytes[4..12];
        let key_len = u64::from_le_bytes(key_len_bytes.try_into().unwrap());

        // 3. Extract key
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

/// Get a default-constructed K value for creating range bounds
/// Used by ScopedDatabase.clear to construct minimum viable range bounds
pub fn get_default_or_clone_first<K>() -> K
where
    K: Default,
{
    // Create a default instance of K
    K::default()
    
    // Note: This approach works for all common types (String, Vec, numeric types, etc.)
    // which all implement Default. For more complex scenarios, additional strategies
    // could be developed.    
}