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
#include <unordered_map>
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

private:
    void addNewCache(Context & context, const std::string & cache_name, bool create_default);
    std::unordered_map<DiskCacheType, IDiskCachePtr> caches;

    /// Per-table TTL cache registry (for workers)
    std::unordered_map<UUID, IDiskCachePtr> per_table_ttl_caches;
    std::mutex ttl_cache_registry_mutex;

    /// Global TTL cache usage tracking
    std::atomic<size_t> global_ttl_cache_usage{0};
};
}
