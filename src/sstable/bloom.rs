use crc32fast::Hasher;

const SEED_MULTIPLIER: u32 = 0x9E37_79B9;
const MIN_BITS: usize = 64;
const BITS_PER_BYTE: usize = 8;

#[derive(Debug)]
pub(super) struct BloomFilterBuilder {
    k_hashes: u32,
    bitset: Vec<u8>,
}

impl BloomFilterBuilder {
    #[must_use]
    pub(super) fn new(expected_keys: usize, bits_per_key: u32) -> Self {
        let bits_per_key = bits_per_key.max(1);
        let raw_bits = expected_keys
            .saturating_mul(usize::try_from(bits_per_key).unwrap_or(1))
            .max(MIN_BITS);
        let byte_len = raw_bits.div_ceil(BITS_PER_BYTE);
        let k_hashes = optimal_k(bits_per_key);

        Self {
            k_hashes,
            bitset: vec![0_u8; byte_len],
        }
    }

    pub(super) fn add_key(&mut self, key: &[u8]) {
        if self.bitset.is_empty() {
            return;
        }

        let bit_len = self.bitset.len().saturating_mul(BITS_PER_BYTE);
        let h1 = hash_with_seed(key, 0);
        let h2 = hash_with_seed(key, SEED_MULTIPLIER);

        for i in 0..self.k_hashes {
            let combined = h1.wrapping_add(i.wrapping_mul(h2));
            let index = usize::try_from(combined).unwrap_or(0) % bit_len;
            set_bit(&mut self.bitset, index);
        }
    }

    #[must_use]
    pub(super) fn finish(self) -> Vec<u8> {
        let bit_len_u32 =
            u32::try_from(self.bitset.len().saturating_mul(BITS_PER_BYTE)).unwrap_or(u32::MAX);
        let mut out = Vec::with_capacity(8 + self.bitset.len());
        out.extend_from_slice(&bit_len_u32.to_le_bytes());
        out.extend_from_slice(&self.k_hashes.to_le_bytes());
        out.extend_from_slice(&self.bitset);
        out
    }
}

#[derive(Clone, Debug)]
pub(super) struct BloomFilter {
    k_hashes: u32,
    bitset: Vec<u8>,
}

impl BloomFilter {
    pub(super) fn decode(raw: &[u8]) -> crate::error::Result<Self> {
        if raw.len() < 8 {
            return Err(invalid_data("Bloom filter payload too short").into());
        }

        let bit_len_u32 = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
        let k_hashes = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
        if k_hashes == 0 {
            return Err(invalid_data("Bloom filter uses zero hash functions").into());
        }

        let bitset = raw[8..].to_vec();
        let expected_bit_len = bitset.len().saturating_mul(BITS_PER_BYTE);
        let requested_bit_len = usize::try_from(bit_len_u32)
            .map_err(|_| invalid_data("Bloom filter bit length not supported on this platform"))?;

        if requested_bit_len != expected_bit_len {
            return Err(invalid_data("Bloom filter bit length does not match payload size").into());
        }

        Ok(Self { k_hashes, bitset })
    }

    #[must_use]
    pub(super) fn may_contain(&self, key: &[u8]) -> bool {
        if self.bitset.is_empty() {
            return false;
        }

        let bit_len = self.bitset.len().saturating_mul(BITS_PER_BYTE);
        let h1 = hash_with_seed(key, 0);
        let h2 = hash_with_seed(key, SEED_MULTIPLIER);

        for i in 0..self.k_hashes {
            let combined = h1.wrapping_add(i.wrapping_mul(h2));
            let index = usize::try_from(combined).unwrap_or(0) % bit_len;
            if !is_bit_set(&self.bitset, index) {
                return false;
            }
        }

        true
    }
}

fn optimal_k(bits_per_key: u32) -> u32 {
    let numerator = bits_per_key.saturating_mul(69_315);
    let scaled = numerator / 100_000;
    scaled.clamp(1, 30)
}

fn hash_with_seed(data: &[u8], seed: u32) -> u32 {
    let mut hasher = Hasher::new_with_initial(seed);
    hasher.update(data);
    hasher.finalize()
}

fn set_bit(bitset: &mut [u8], index: usize) {
    let byte_index = index / BITS_PER_BYTE;
    let bit_index = index % BITS_PER_BYTE;
    bitset[byte_index] |= 1_u8 << bit_index;
}

fn is_bit_set(bitset: &[u8], index: usize) -> bool {
    let byte_index = index / BITS_PER_BYTE;
    let bit_index = index % BITS_PER_BYTE;
    (bitset[byte_index] & (1_u8 << bit_index)) != 0
}

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::{BloomFilter, BloomFilterBuilder};

    #[test]
    fn bloom_roundtrip_and_membership() {
        let mut builder = BloomFilterBuilder::new(3, 10);
        builder.add_key(b"apple");
        builder.add_key(b"banana");
        builder.add_key(b"cherry");
        let encoded = builder.finish();

        let bloom = match BloomFilter::decode(&encoded) {
            Ok(bloom) => bloom,
            Err(err) => panic!("decode should work: {err}"),
        };
        assert!(bloom.may_contain(b"apple"));
        assert!(bloom.may_contain(b"banana"));
        assert!(bloom.may_contain(b"cherry"));
    }
}
