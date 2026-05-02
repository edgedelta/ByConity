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

#include <Core/UUID.h>
#include <Storages/DiskCache/DiskCache_fwd.h>
#include <common/singleton.h>
#include <common/types.h>
#include <atomic>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <Poco/Exception.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace DB
{
class Context;
class IVolume;
class Throttler;
using VolumePtr = std::shared_ptr<IVolume>;
using ThrottlerPtr = std::shared_ptr<Throttler>;

/// Per-query cache stats accumulated on workers and surfaced via segment profiles.
struct QueryCacheStats
{
    std::atomic<size_t> cache_hit_segs{0};    // data segments served from local TTL cache
    std::atomic<size_t> cache_miss_segs{0};   // data segments not found in local cache
    std::atomic<size_t> steal_segs{0};        // segments fetched from peer via steal RPC
    std::atomic<size_t> s3_fallback_segs{0};  // data segments read directly from S3
    std::atomic<size_t> cache_bytes{0};       // bytes through cache_buffer for data (local + steal)
    std::atomic<size_t> s3_bytes{0};          // bytes through source_buffer for data (S3)
    std::atomic<uint64_t> cache_read_ms{0};
    std::atomic<uint64_t> cache_read_ms_max{0};
    std::atomic<uint64_t> cache_read_ms_min{UINT64_MAX};
    std::atomic<uint64_t> s3_read_ms{0};
    std::atomic<size_t> reader_count{0};
    // Skip-index segment counters (extension .idx)
    std::atomic<size_t> idx_hit_segs{0};
    std::atomic<size_t> idx_miss_segs{0};
    std::atomic<size_t> idx_cache_bytes{0};
    std::atomic<size_t> idx_s3_bytes{0};
    std::atomic<uint64_t> idx_cache_read_ms{0};
    std::atomic<uint64_t> idx_s3_read_ms{0};
};

/// Plain snapshot, used for local accumulation and return values.
struct QueryCacheStatsSnapshot
{
    size_t cache_hit_segs{0};
    size_t cache_miss_segs{0};
    size_t steal_segs{0};
    size_t s3_fallback_segs{0};
    size_t cache_bytes{0};
    size_t s3_bytes{0};
    uint64_t cache_read_ms{0};
    uint64_t cache_read_ms_max{0};
    uint64_t cache_read_ms_min{0};
    uint64_t s3_read_ms{0};
    size_t reader_count{0};
    // Skip-index segment counters (extension .idx)
    size_t idx_hit_segs{0};
    size_t idx_miss_segs{0};
    size_t idx_cache_bytes{0};
    size_t idx_s3_bytes{0};
    uint64_t idx_cache_read_ms{0};
    uint64_t idx_s3_read_ms{0};

    bool empty() const { return cache_hit_segs == 0 && cache_miss_segs == 0 && steal_segs == 0 && s3_fallback_segs == 0
        && idx_hit_segs == 0 && idx_miss_segs == 0; }
};

enum class DiskCacheType {
    File, // for generic file disk cache
    MergeTree,
    Hive,
    Manifest
};

std::string diskCacheTypeToString(DiskCacheType type);
DiskCacheType stringToDiskCacheType(const std::string & type);

class DiskCacheFactory : public ext::singleton<DiskCacheFactory>
{
public:
    void init(Context & context);

    /// not thread-safe, you must call it when server startup
    void registerDiskCaches(Context & global_context);

    void shutdown();

    IDiskCachePtr get(DiskCacheType type)
    {
        auto it = caches.find(type);
        if (it == caches.end())
            throw Poco::Exception("Unknown disk cache " + diskCacheTypeToString(type), ErrorCodes::BAD_ARGUMENTS);
        return it->second;
    }

    IDiskCachePtr tryGet(DiskCacheType type)
    {
        auto it = caches.find(type);
        if (it == caches.end())
            return nullptr;
        return it->second;
    }

    /// Create per-table TTL cache instance from table settings
    IDiskCachePtr createDiskCacheFromTableSettings(
        const String & table_name,
        const UUID & table_uuid,
        Context & context,
        const ThrottlerPtr & throttler,
        UInt64 ttl_minutes,
        size_t max_size_bytes = 0);

    /// Return a snapshot of all registered per-table TTL caches (UUID → cache ptr).
    std::unordered_map<UUID, IDiskCachePtr> getAllTableTTLCaches() const
    {
        std::lock_guard<std::mutex> lock(ttl_cache_registry_mutex);
        return per_table_ttl_caches;
    }

    /// Remove a per-table TTL cache entry from the registry.
    /// Called when disk_cache_ttl_hours is set to 0 so re-enabling creates a fresh object.
    void removeTableTTLCache(const UUID & table_uuid)
    {
        std::lock_guard<std::mutex> lock(ttl_cache_registry_mutex);
        per_table_ttl_caches.erase(table_uuid);
    }

    /// Global TTL cache usage tracking
    /// shared across all per-table TTL caches
    void addGlobalTTLUsage(size_t bytes) { global_ttl_cache_usage.fetch_add(bytes); }
    void releaseGlobalTTL(size_t bytes) { global_ttl_cache_usage.fetch_sub(bytes); }
    size_t getGlobalTTLUsage() const { return global_ttl_cache_usage.load(); }
    size_t getGlobalTTLLimit() const;

    /// Per-query cache stats registry.
    /// unique_lock only for first insertion, then atomic fetch_add on the fields.
    void mergeQueryCacheStats(const String & query_id, const QueryCacheStatsSnapshot & local);
    std::optional<QueryCacheStatsSnapshot> consumeQueryCacheStats(const String & query_id);

    /// Register a callback to be fired by consumeQueryCacheStats before reading stats.
    /// Used to flush the last partial segment's stats from live read buffers.
    void registerFlushCallback(const String & query_id, std::function<void()> callback);

    /// Resolve a stable worker_id (e.g. byconity-vw-vw-default-0) to its current RPC
    /// host:port by querying the Resource Manager. Result cached for 30 seconds.
    std::optional<String> resolveWorkerEndpoint(const String & worker_id);

private:
    void addNewCache(Context & context, const std::string & cache_name, bool create_default);
    std::unordered_map<DiskCacheType, IDiskCachePtr> caches;

    /// Per-table TTL cache registry (for workers)
    std::unordered_map<UUID, IDiskCachePtr> per_table_ttl_caches;
    mutable std::mutex ttl_cache_registry_mutex;

    /// Global TTL cache usage tracking
    std::atomic<size_t> global_ttl_cache_usage{0};

    /// Per-query cache stats (query_id → shared stats object)
    std::unordered_map<String, std::shared_ptr<QueryCacheStats>> query_cache_stats_map;
    /// Per-query flush callbacks fired before consume (query_id → callbacks list)
    std::unordered_map<String, std::vector<std::function<void()>>> query_flush_callbacks_map;
    mutable std::shared_mutex query_cache_stats_mutex;

    /// Worker endpoint resolution: worker_id → host:port, refreshed every 30s from RM.
    std::function<std::unordered_map<String, String>()> worker_endpoint_resolver;
    mutable std::mutex worker_endpoint_cache_mutex;
    std::unordered_map<String, String> worker_endpoint_cache;
    time_t worker_endpoint_cache_refresh_time{0};
    static constexpr int WORKER_ENDPOINT_CACHE_TTL_SEC = 30;
};
}
