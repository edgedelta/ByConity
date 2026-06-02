/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <array>
#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <Common/HashTable/Hash.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Common/ShardCache.h>
#include <sys/types.h>
#include <Poco/Logger.h>

namespace DB
{

class TTLCacheFDBIndex;

class DiskCacheTTLMeta
{
public:
    enum class State
    {
        Caching,
        Cached,
        Deleting,
    };

    DiskCacheTTLMeta(State state_, const DiskPtr & disk_, size_t size_, time_t cached_at_, time_t max_ts_, String rel_path_ = {})
        : state(state_), disk(disk_), size(size_), cached_at(cached_at_), max_timestamp(max_ts_), rel_path(std::move(rel_path_))
    {}

    State state;
    DiskPtr disk;
    size_t size;
    time_t cached_at;
    time_t max_timestamp;
    String rel_path;  // exact on-disk relative path; avoids reconstructing prefix (data/ vs meta/) at eviction time
};

struct DiskCacheTTLWeightFunction
{
    size_t operator()(const DiskCacheTTLMeta& meta) const
    {
        if (meta.state == DiskCacheTTLMeta::State::Cached)
            return meta.size;
        return 0;
    }
};

/// TTL-based disk cache
/// Evicts parts based on partition timestamp and retention window
/// Parallel to DiskCacheLRU
class DiskCacheTTL: public IDiskCache
{
public:
    using KeyType = UInt128;

    DiskCacheTTL(
        const String & name_,
        const String & table_uuid_,
        const VolumePtr & volume,
        const ThrottlerPtr & throttler,
        const DiskCacheSettings & settings,
        const IDiskCacheStrategyPtr & strategy_,
        UInt64 ttl_minutes_,
        size_t max_size_bytes_ = 0,  // 0 = use settings.ttl_cache_max_size
        IDiskCache::DataType type_ = IDiskCache::DataType::ALL);

    /// Drains in-flight async eviction tasks before members are torn down. Async eviction
    /// (get/set/updateSettings) is scheduled on a shared pool capturing `this`; without this
    /// drain a task could run against a destroyed cache on table-drop or settings-replace.
    ~DiskCacheTTL() override;

    void set(const String& seg_name, ReadBuffer& value, size_t weight_hint, bool is_preload, time_t max_time = 0) override;
    std::pair<DiskPtr, String> get(const String& seg_name) override;
    void load() override;
    size_t drop(const String & part_name) override;

    size_t getKeyCount() const override { return total_entries.load(); }
    size_t getCachedSize() const override { return total_size.load(); }
    std::filesystem::path getRelativePath(const KeyType & key, const String & seg_name, const String & prefix = {}) { return getPath(key, latest_disk_cache_dir, seg_name, prefix);}

    std::filesystem::path getPath(const KeyType & key, const String & path, const String & seg_name, const String & prefix) const;

    static KeyType hash(const String & seg_name);
    static String hexKey(const KeyType & key);

    void evictExpired();
    void evictOldestPartitionsUntilSpace(size_t needed_bytes);
    static std::optional<KeyType> unhexKey(const String & hex);

    /// Parse partition timestamp from part name
    /// Returns 0 if partition is not time-based
    static time_t parsePartitionTimestamp(const String & part_name);

    // Stats structures for observability

    // Per-partition snapshot, derived on demand from part_index in getPartitionStats().
    struct PartitionStats
    {
        String partition_id;
        size_t entry_count{0};
        size_t total_bytes{0};
        time_t partition_timestamp{0};
    };

    // Snapshot for return (plain types, copyable)
    struct TTLCacheStats
    {
        String table_uuid;
        size_t total_entries{0};
        size_t total_bytes{0};

        // TTL-specific counters
        size_t evicted_expired{0};
        size_t evicted_size_limit{0};
        size_t rejected_non_time_partition{0};
        size_t rejected_too_old{0};
        time_t last_eviction_run{0};

        // Async size-based eviction stats
        size_t async_eviction_triggered{0};
        size_t async_eviction_skipped_rate_limit{0};

        // Write source breakdown (preload vs query-triggered vs restored from FDB on startup)
        size_t cached_from_preload{0};
        size_t cached_from_query{0};
        size_t cached_bytes_preload{0};
        size_t cached_bytes_query{0};
        size_t cached_from_restored{0};
        size_t cached_bytes_restored{0};
        // Skip-index write breakdown (same events, idx extension only)
        size_t cached_idx_from_preload{0};
        size_t cached_idx_bytes_preload{0};
        size_t cached_idx_from_query{0};
        size_t cached_idx_bytes_query{0};

        // Aggregated hit/miss counts across all partitions, by segment type
        size_t total_hits{0};
        size_t total_misses{0};
        size_t data_hits{0};
        size_t data_misses{0};
        size_t idx_hits{0};
        size_t idx_misses{0};
    };

    // Internal stats with atomics
    struct TTLCacheStatsInternal
    {
        String table_uuid;

        // TTL-specific counters
        std::atomic<size_t> evicted_expired{0};
        std::atomic<size_t> evicted_size_limit{0};
        std::atomic<size_t> rejected_non_time_partition{0};
        std::atomic<size_t> rejected_too_old{0};
        std::atomic<time_t> last_eviction_run{0};

        // Async size-based eviction stats
        std::atomic<size_t> async_eviction_triggered{0};
        std::atomic<size_t> async_eviction_skipped_rate_limit{0};

        // Write source breakdown (preload vs query-triggered vs restored from FDB on startup)
        std::atomic<size_t> cached_from_preload{0};
        std::atomic<size_t> cached_from_query{0};
        std::atomic<size_t> cached_bytes_preload{0};
        std::atomic<size_t> cached_bytes_query{0};
        std::atomic<size_t> cached_from_restored{0};
        std::atomic<size_t> cached_bytes_restored{0};
        // Skip-index write breakdown
        std::atomic<size_t> cached_idx_from_preload{0};
        std::atomic<size_t> cached_idx_bytes_preload{0};
        std::atomic<size_t> cached_idx_from_query{0};
        std::atomic<size_t> cached_idx_bytes_query{0};

        // Aggregated hit/miss by segment type
        std::atomic<size_t> data_hits{0};
        std::atomic<size_t> data_misses{0};
        std::atomic<size_t> idx_hits{0};
        std::atomic<size_t> idx_misses{0};
    };

    TTLCacheStats getStats() const;
    std::vector<PartitionStats> getPartitionStats() const;

    UInt64 getTTLMinutes() const { return ttl_minutes.load(std::memory_order_relaxed); }
    size_t getMaxSizeBytes() const { return max_size_bytes.load(std::memory_order_relaxed); }
    void setFDBIndex(std::shared_ptr<TTLCacheFDBIndex> idx) { fdb_index = std::move(idx); }

    /// Update TTL / size limits at runtime. If a limit tightened, eagerly schedules eviction
    void updateSettings(UInt64 new_ttl_minutes, size_t new_max_size_bytes);

    /// Release global counter and schedule async deletion of all on-disk data for this table.
    /// Cheap to call: renames directories synchronously, deletes files in background.
    void drop();

    /// Look up whether a peer worker has this segment cached via the FDB reverse index.
    /// Returns peer RPC endpoint if found, nullopt if not found or FDB unavailable.
    /// Gated on fdb_index being set; caller is responsible for checking stealing mode.
    std::optional<String> findPeerOwner(const String & seg_name);

private:
    struct CacheEraseResult {
        String partition_id;
        time_t partition_ts{0};
        UInt64 hash_high{0};
        size_t count{0};
        size_t bytes{0};
        std::vector<std::pair<DiskPtr, String>> files;
    };

    struct PartIndexEntry {
        String partition_id;
        time_t partition_ts{0};
        std::unordered_set<KeyType, UInt128Hash> keys;
        size_t total_bytes{0};
    };

    size_t writeSegment(ReadBuffer& buffer, ReservationPtr& reservation, const String& cache_rel_path);
    bool shouldCache(time_t part_ts) const;

    static constexpr size_t NUM_SHARDS = 64;

    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<KeyType, std::shared_ptr<DiskCacheTTLMeta>, UInt128Hash> cache_map;
        std::unordered_map<UInt64, PartIndexEntry> part_index;
    };

    Shard & getShard(UInt64 hash_high) { return shards[hash_high & (NUM_SHARDS - 1)]; }

    /// Structural helpers — caller must hold shard.mutex
    void cacheInsertLocked(Shard & shard, KeyType key, std::shared_ptr<DiskCacheTTLMeta> meta, const String & precomputed_partition_id = {});
    CacheEraseResult cacheEraseLocked(Shard & shard, KeyType key);
    CacheEraseResult cacheErasePartLocked(Shard & shard, UInt64 hash_high);

    /// Apply a batch of erase results: delete files, notify FDB.
    /// Caller must NOT hold any shard mutex. Increments total_evicted by result.count for each entry.
    void applyEraseResults(std::vector<CacheEraseResult> & results, size_t & total_evicted, const char * log_tag);

    /// Schedule `task` on the shared evict pool, tracking it in inflight_async so the destructor can drain it. No-op once shutting_down is set.
    void scheduleEvictTask(std::function<void()> task);

    // NOTE: the parallel disk-walk machinery (DiskIterator + DiskCacheLoader/Migrator/Deleter)
    // was removed. Its only real purpose was rebuilding the in-memory index by scanning the
    // on-disk tree on startup — pointless on instance/NVMe disk, which does not survive a
    // restart. If a persistent-disk deployment is ever introduced, re-add a startup scan
    // to recover the warm cache; see DiskCacheLRU for the parallel-iterator pattern to copy.

    /// FDB-backed index for fast startup recovery
    /// optional — null if catalog unavailable
    std::shared_ptr<TTLCacheFDBIndex> fdb_index;

    ThrottlerPtr set_rate_throttler;
    ThrottlerPtr set_throughput_throttler;
    std::atomic<bool> is_droping{false};

    /// Async-eviction lifetime guard: ~DiskCacheTTL sets shutting_down and waits for inflight_async to drain so no scheduled task outlives this object.
    std::atomic<bool> shutting_down{false};
    std::atomic<int> inflight_async{0};

    const String table_uuid;
    std::atomic<UInt64> ttl_minutes;
    std::atomic<size_t> max_size_bytes;  // 0 = unlimited

    std::array<Shard, NUM_SHARDS> shards;
    std::atomic<size_t> total_entries{0};
    std::atomic<size_t> total_size{0};

    /// Last eviction check time
    std::atomic<time_t> last_eviction_check{0};

    /// Last async size-based eviction trigger time
    std::atomic<time_t> last_size_eviction_trigger{0};

    /// Cache statistics
    TTLCacheStatsInternal cache_stats;
};

}
