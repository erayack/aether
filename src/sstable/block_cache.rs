use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::metrics::MetricsHandle;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct BlockCacheKey {
    pub table_id: u64,
    pub block_offset: u64,
    pub block_len: u32,
    pub codec: u8,
    pub encoding: u8,
}

pub struct BlockCache {
    capacity_bytes: usize,
    metrics: MetricsHandle,
    state: Mutex<BlockCacheState>,
}

#[derive(Default)]
struct BlockCacheState {
    entries: HashMap<BlockCacheKey, BlockCacheEntry>,
    lru: VecDeque<(BlockCacheKey, u64)>,
    current_bytes: usize,
    tick: u64,
}

struct BlockCacheEntry {
    block: Arc<[u8]>,
    size_bytes: usize,
    stamp: u64,
}

impl BlockCache {
    #[must_use]
    pub fn new(capacity_bytes: usize, metrics: MetricsHandle) -> Self {
        metrics.set_block_cache_bytes_current(0);
        Self {
            capacity_bytes,
            metrics,
            state: Mutex::new(BlockCacheState::default()),
        }
    }

    #[must_use]
    pub fn get(&self, key: &BlockCacheKey) -> Option<Arc<[u8]>> {
        let mut state = self.state.lock();
        let stamp = state.next_stamp();

        let found = if let Some(entry) = state.entries.get_mut(key) {
            entry.stamp = stamp;
            let block = Arc::clone(&entry.block);
            state.lru.push_back((key.clone(), stamp));
            state.maybe_compact_lru();
            Some(block)
        } else {
            None
        };
        drop(state);

        if found.is_some() {
            self.metrics.record_block_cache_hit();
        } else {
            self.metrics.record_block_cache_miss();
        }

        found
    }

    pub fn insert(&self, key: BlockCacheKey, block: Arc<[u8]>) {
        if self.capacity_bytes == 0 {
            return;
        }

        let mut state = self.state.lock();
        let stamp = state.next_stamp();
        let size_bytes = block.len();

        if let Some(previous) = state.entries.insert(
            key.clone(),
            BlockCacheEntry {
                block,
                size_bytes,
                stamp,
            },
        ) {
            state.current_bytes = state.current_bytes.saturating_sub(previous.size_bytes);
        }

        state.current_bytes = state.current_bytes.saturating_add(size_bytes);
        state.lru.push_back((key, stamp));
        state.maybe_compact_lru();
        let evicted = state.enforce_capacity(self.capacity_bytes);
        let current_bytes = state.current_bytes;
        drop(state);

        for _ in 0..evicted {
            self.metrics.record_block_cache_eviction();
        }
        self.metrics.set_block_cache_bytes_current(current_bytes);
    }
}

impl BlockCacheState {
    const fn next_stamp(&mut self) -> u64 {
        self.tick = self.tick.wrapping_add(1);
        self.tick
    }

    fn enforce_capacity(&mut self, capacity_bytes: usize) -> usize {
        let mut evicted = 0_usize;
        while self.current_bytes > capacity_bytes {
            let Some((candidate_key, candidate_stamp)) = self.lru.pop_front() else {
                break;
            };

            let is_current = self
                .entries
                .get(&candidate_key)
                .is_some_and(|entry| entry.stamp == candidate_stamp);
            if !is_current {
                continue;
            }

            if let Some(removed) = self.entries.remove(&candidate_key) {
                self.current_bytes = self.current_bytes.saturating_sub(removed.size_bytes);
                evicted = evicted.saturating_add(1);
            }
        }

        evicted
    }

    fn maybe_compact_lru(&mut self) {
        let entry_count = self.entries.len();
        if entry_count == 0 {
            self.lru.clear();
            return;
        }

        let max_lru_len = entry_count.saturating_mul(4).saturating_add(64);
        if self.lru.len() <= max_lru_len {
            return;
        }

        let mut compacted = VecDeque::with_capacity(entry_count);
        while let Some((candidate_key, candidate_stamp)) = self.lru.pop_front() {
            let is_current = self
                .entries
                .get(&candidate_key)
                .is_some_and(|entry| entry.stamp == candidate_stamp);
            if is_current {
                compacted.push_back((candidate_key, candidate_stamp));
            }
        }
        self.lru = compacted;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::metrics::MetricsHandle;

    use super::{BlockCache, BlockCacheKey};

    fn key(table_id: u64, block_offset: u64) -> BlockCacheKey {
        BlockCacheKey {
            table_id,
            block_offset,
            block_len: 32,
            codec: 0,
            encoding: 0,
        }
    }

    #[test]
    fn repeated_block_access_produces_cache_hits() {
        let metrics = MetricsHandle::default();
        let cache = BlockCache::new(1024, metrics.clone());

        let k1 = key(1, 0);
        cache.insert(k1.clone(), Arc::<[u8]>::from(vec![1_u8; 32]));

        for _ in 0..5 {
            assert!(cache.get(&k1).is_some());
        }

        let snap = metrics.snapshot();
        assert_eq!(snap.block_cache_hits_total, 5);
        assert_eq!(snap.block_cache_misses_total, 0);

        let missing = key(99, 99);
        assert!(cache.get(&missing).is_none());

        let snap = metrics.snapshot();
        assert_eq!(snap.block_cache_hits_total, 5);
        assert_eq!(snap.block_cache_misses_total, 1);
    }

    #[test]
    fn capacity_eviction_triggers_when_over_limit() {
        let metrics = MetricsHandle::default();
        // Capacity fits only 64 bytes; each block is 32 bytes.
        let cache = BlockCache::new(64, metrics.clone());

        let k1 = key(1, 0);
        let k2 = key(1, 100);
        let k3 = key(1, 200);

        cache.insert(k1.clone(), Arc::<[u8]>::from(vec![0_u8; 32]));
        cache.insert(k2.clone(), Arc::<[u8]>::from(vec![0_u8; 32]));
        // At 64 bytes – fits exactly.
        assert!(cache.get(&k1).is_some());
        assert!(cache.get(&k2).is_some());

        // Third insert pushes to 96 bytes; eviction must reclaim the LRU entry.
        cache.insert(k3.clone(), Arc::<[u8]>::from(vec![0_u8; 32]));

        assert!(cache.get(&k3).is_some(), "newest entry should survive");

        let snap = metrics.snapshot();
        assert!(
            snap.block_cache_evictions_total > 0,
            "eviction counter should be positive after exceeding capacity"
        );
    }

    #[test]
    fn cache_metrics_expose_hit_miss_eviction_counts() {
        let metrics = MetricsHandle::default();
        let cache = BlockCache::new(32, metrics.clone());

        // Phase 1: insert one block (32 bytes exactly at capacity).
        let k1 = key(1, 0);
        cache.insert(k1.clone(), Arc::<[u8]>::from(vec![0_u8; 32]));

        let snap = metrics.snapshot();
        assert_eq!(snap.block_cache_evictions_total, 0);
        assert_eq!(snap.block_cache_bytes_current, 32);

        // Phase 2: hit k1 twice.
        assert!(cache.get(&k1).is_some());
        assert!(cache.get(&k1).is_some());

        let snap = metrics.snapshot();
        assert_eq!(snap.block_cache_hits_total, 2);
        assert_eq!(snap.block_cache_misses_total, 0);

        // Phase 3: miss on an absent key.
        let absent = key(42, 42);
        assert!(cache.get(&absent).is_none());

        let snap = metrics.snapshot();
        assert_eq!(snap.block_cache_hits_total, 2);
        assert_eq!(snap.block_cache_misses_total, 1);

        // Phase 4: insert k2 (32 bytes) which forces eviction of k1.
        let k2 = key(2, 0);
        cache.insert(k2.clone(), Arc::<[u8]>::from(vec![0_u8; 32]));

        let snap = metrics.snapshot();
        assert!(
            snap.block_cache_evictions_total >= 1,
            "eviction must be recorded"
        );
        assert_eq!(snap.block_cache_bytes_current, 32);

        // Confirm k1 was evicted and k2 is live.
        assert!(cache.get(&k1).is_none());
        assert!(cache.get(&k2).is_some());

        let snap = metrics.snapshot();
        assert_eq!(snap.block_cache_hits_total, 3);
        assert_eq!(snap.block_cache_misses_total, 2);
    }

    #[test]
    fn compacts_stale_lru_metadata_after_repeated_hits() {
        let cache = BlockCache::new(1024 * 1024, MetricsHandle::default());
        let key = key(1, 10);
        cache.insert(key.clone(), Arc::<[u8]>::from(vec![7_u8; 32]));

        for _ in 0..5_000 {
            let hit = cache.get(&key);
            assert!(hit.is_some());
        }

        let lru_len = {
            let state = cache.state.lock();
            state.lru.len()
        };
        assert!(
            lru_len <= 68,
            "lru metadata should stay bounded for a single live key, got {lru_len}",
        );
    }
}
