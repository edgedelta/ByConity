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
#include <Common/HashTable/Hash.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Common/ShardCache.h>
#include <sys/types.h>
#include <Poco/Logger.h>

namespace DB
{

class DiskCacheTTLMeta
{
public:
    enum class State
    {
        Caching,
        Cached,
        Deleting,
    };

    DiskCacheTTLMeta(State state_, const DiskPtr & disk_, size_t size_, time_t cached_at_, time_t part_ts_)
        : state(state_), disk(disk_), size(size_), cached_at(cached_at_), part_timestamp(part_ts_)
    {}

    State state;
    DiskPtr disk;
    size_t size;
    time_t cached_at;
    time_t part_timestamp;
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
        IDiskCache::DataType type_ = IDiskCache::DataType::ALL);

    void set(const String& seg_name, ReadBuffer& value, size_t weight_hint, bool is_preload) override;
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

private:
    size_t writeSegment(const String& seg_name, ReadBuffer& buffer, ReservationPtr& reservation);

    /// Check if segment should be cached based on TTL
    bool shouldCache(time_t part_ts) const;

    /// Evict expired segments
    void evictExpired();

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

    ThrottlerPtr set_rate_throttler;
    ThrottlerPtr set_throughput_throttler;
    std::atomic<bool> is_droping{false};

    const String table_uuid;
    UInt64 ttl_minutes;

    /// Simple map-based storage (not using BucketLRUCache)
    std::mutex cache_mutex;
    std::map<KeyType, std::shared_ptr<DiskCacheTTLMeta>> cache_map;
    std::atomic<size_t> total_entries{0};
    std::atomic<size_t> total_size{0};

    /// Last eviction check time
    std::atomic<time_t> last_eviction_check{0};
};

}
