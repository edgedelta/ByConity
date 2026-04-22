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

#include <atomic>
#include <filesystem>
#include <map>
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

    DiskCacheTTLMeta(State state_, const DiskPtr & disk_, size_t size_, time_t cached_at_, time_t max_ts_)
        : state(state_), disk(disk_), size(size_), cached_at(cached_at_), max_timestamp(max_ts_)
    {}

    State state;
    DiskPtr disk;
    size_t size;
    time_t cached_at;
    time_t max_timestamp;  // Max timestamp from part data (for fine-grained TTL)
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

    void set(const String& seg_name, ReadBuffer& value, size_t weight_hint, bool is_preload, time_t min_time = 0, time_t max_time = 0) override;
    std::pair<DiskPtr, String> get(const String& seg_name) override;
    void load() override;
    size_t drop(const String & part_name) override;

    size_t getKeyCount() const override { return total_entries.load(); }
    size_t getCachedSize() const override { return total_size.load(); }
    std::filesystem::path getRelativePath(const KeyType & key, const String & seg_name, const String & prefix = {}) { return getPath(key, latest_disk_cache_dir, seg_name, prefix);}

    std::filesystem::path getPath(const KeyType & key, const String & path, const String & seg_name, const String & prefix) const;

    static KeyType hash(const String & seg_name);
    static String hexKey(const KeyType & key);
    static std::optional<KeyType> unhexKey(const String & hex);

    /// Parse partition timestamp from part name
    /// Returns 0 if partition is not time-based
    static time_t parsePartitionTimestamp(const String & part_name);

    // Stats structures for observability

    // Internal stats with atomics (not copyable)
    struct PartitionStatsInternal
    {
        String partition_id;
        size_t entry_count{0};
        size_t total_bytes{0};
        time_t partition_timestamp{0};
        std::atomic<size_t> hits{0};
        std::atomic<size_t> misses{0};
    };

    // Snapshot for return (plain types, copyable)
    struct PartitionStats
    {
        String partition_id;
        size_t entry_count{0};
        size_t total_bytes{0};
        time_t partition_timestamp{0};
        size_t hits{0};
        size_t misses{0};
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
        size_t async_eviction_triggered_global{0};
        size_t async_eviction_skipped_rate_limit_global{0};

        // Write source breakdown (preload vs query-triggered)
        size_t cached_from_preload{0};
        size_t cached_from_query{0};
        size_t cached_bytes_preload{0};
        size_t cached_bytes_query{0};

        // Aggregated hit/miss counts across all partitions
        size_t total_hits{0};
        size_t total_misses{0};
    };

    // Internal stats with atomics
    struct TTLCacheStatsInternal
    {
        String table_uuid;
        std::atomic<size_t> total_entries{0};
        std::atomic<size_t> total_bytes{0};

        // TTL-specific counters
        std::atomic<size_t> evicted_expired{0};
        std::atomic<size_t> evicted_size_limit{0};
        std::atomic<size_t> rejected_non_time_partition{0};
        std::atomic<size_t> rejected_too_old{0};
        std::atomic<time_t> last_eviction_run{0};

        // Async size-based eviction stats
        std::atomic<size_t> async_eviction_triggered{0};
        std::atomic<size_t> async_eviction_skipped_rate_limit{0};
        std::atomic<size_t> async_eviction_triggered_global{0};
        std::atomic<size_t> async_eviction_skipped_rate_limit_global{0};

        // Write source breakdown (preload vs query-triggered)
        std::atomic<size_t> cached_from_preload{0};
        std::atomic<size_t> cached_from_query{0};
        std::atomic<size_t> cached_bytes_preload{0};
        std::atomic<size_t> cached_bytes_query{0};

        // Per-partition breakdown
        mutable std::shared_mutex partition_stats_mutex;
        std::unordered_map<String, PartitionStatsInternal> partition_stats;
    };

    TTLCacheStats getStats() const;
    std::vector<PartitionStats> getPartitionStats() const;

    UInt64 getTTLMinutes() const { return ttl_minutes; }
    size_t getMaxSizeBytes() const { return max_size_bytes; }
    void setFDBIndex(std::shared_ptr<TTLCacheFDBIndex> idx) { fdb_index = std::move(idx); }

private:
    size_t writeSegment(const String& seg_name, ReadBuffer& buffer, ReservationPtr& reservation);

    /// Check if segment should be cached based on TTL
    bool shouldCache(time_t part_ts) const;

    /// Evict expired segments
    void evictExpired();

    /// Evict oldest partitions until enough space for new segment
    void evictOldestPartitionsUntilSpace(size_t needed_bytes);

    /// Update partition-level stats
    void updatePartitionStats(const String & partition_id, time_t partition_ts, bool hit, size_t bytes);

    struct DiskIterator : private boost::noncopyable
    {
        explicit DiskIterator(
            const String & name_, DiskCacheTTL & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_);
        virtual ~DiskIterator() = default;

        virtual void exec(std::filesystem::path entry_path);
        virtual void iterateDirectory(std::filesystem::path rel_path, size_t depth);
        virtual void iterateFile(std::filesystem::path file_path, size_t file_size) = 0;

        String name;
        DiskCacheTTL & disk_cache;
        DiskPtr disk;
        size_t worker_per_disk{1};
        int min_depth_parallel{-1};
        int max_depth_parallel{-1};
        std::unique_ptr<ThreadPool> pool;
        ExceptionHandler handler;
        Poco::Logger * log;
    };

    struct DiskCacheLoader : DiskIterator
    {
        explicit DiskCacheLoader(
            DiskCacheTTL & cache_, DiskPtr disk_, size_t worker_per_disk, int min_depth_parallel, int max_depth_parallel);
        ~DiskCacheLoader() override;
        void iterateFile(std::filesystem::path file_path, size_t file_size) override;

        std::atomic_size_t total_loaded = 0;
    };

    struct DiskCacheMigrator : DiskIterator
    {
        explicit DiskCacheMigrator(
            DiskCacheTTL & cache_, DiskPtr disk_, size_t worker_per_disk, int min_depth_parallel, int max_depth_parallel);
        ~DiskCacheMigrator() override;
        void iterateFile(std::filesystem::path file_path, size_t file_size) override;

        std::atomic_size_t total_migrated = 0;
    };

    struct DiskCacheDeleter : DiskIterator
    {
        explicit DiskCacheDeleter(
            DiskCacheTTL & cache_, DiskPtr disk_, size_t worker_per_disk, int min_depth_parallel, int max_depth_parallel);
        ~DiskCacheDeleter() override;
        void exec(std::filesystem::path entry_path) override;
        void iterateFile(std::filesystem::path file_path, size_t file_size) override;

        size_t delete_file_size {0};
    };

    /// FDB-backed index for fast startup recovery 
    /// optional — null if catalog unavailable
    std::shared_ptr<TTLCacheFDBIndex> fdb_index;

    ThrottlerPtr set_rate_throttler;
    ThrottlerPtr set_throughput_throttler;
    std::atomic<bool> is_droping{false};

    const String table_uuid;
    UInt64 ttl_minutes;
    size_t max_size_bytes;  // 0 = unlimited

    /// Simple map-based storage (not using BucketLRUCache)
    std::mutex cache_mutex;
    std::map<KeyType, std::shared_ptr<DiskCacheTTLMeta>> cache_map;
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
