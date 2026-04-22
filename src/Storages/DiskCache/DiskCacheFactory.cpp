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

#include "DiskCacheFactory.h"
#include <cstddef>
#include <memory>

#include <Catalog/Catalog.h>
#include <Common/HostWithPorts.h>
#include <Core/UUID.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <Storages/DiskCache/TTLCacheFDBIndex.h>
#include <common/logger_useful.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/DiskCache/IDiskCache.h>

namespace DB
{

void DiskCacheFactory::init(Context & context)
{
    if (!caches.empty())
        throw Exception("Can't repeat register DiskCache!", DB::ErrorCodes::LOGICAL_ERROR);
    const auto & config = context.getConfigRef();

    /// init pool
    IDiskCache::init(context);
    Poco::Logger * log{&Poco::Logger::get("DiskCacheFactory")};

    // build disk cache for each type
    if (config.has(DiskCacheSettings::root))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(DiskCacheSettings::root, keys);
        for (const auto & key : keys)
            addNewCache(context, key, false);
    }

    // create dafault cache for MergeTree Diskcache
    if (caches.find(DiskCacheType::MergeTree) == caches.end())
    {
        LOG_TRACE(log, "Creating default DiskCache of {}", diskCacheTypeToString(DiskCacheType::MergeTree));
        addNewCache(context, diskCacheTypeToString(DiskCacheType::MergeTree), true);
    }

    // create default manifest file cache if it does not exists
    if (caches.find(DiskCacheType::Manifest) == caches.end())
    {
        LOG_TRACE(log, "Creating default DiskCache of {}", diskCacheTypeToString(DiskCacheType::Manifest));
        addNewCache(context, diskCacheTypeToString(DiskCacheType::Manifest), true);
    }
}

std::string diskCacheTypeToString(const DiskCacheType type)
{
    switch (type)
    {
        case DiskCacheType::File:
            return "File";
        case DiskCacheType::MergeTree:
            return "MergeTree";
        case DiskCacheType::Hive:
            return "Hive";
        case DiskCacheType::Manifest:
            return "Manifest";
    }

    return "InvalidDiskCacheType";
}

DiskCacheType stringToDiskCacheType(const std::string & type)
{
    if (type == "File")
        return DiskCacheType::File;

    if (type == "simple" || type == "MergeTree") // `simple` for compatible with old config
        return DiskCacheType::MergeTree;

    if (type == "parquet" || type == "Hive") // `parquet` for compatible with old config
        return DiskCacheType::Hive;

    if (type == "Manifest")
        return DiskCacheType::Manifest;

    throw Poco::Exception("Invalid strategy name: " + type + " should be `simple`, `parquet`, `File`, `MergeTree`, `Hive`", ErrorCodes::BAD_ARGUMENTS);
}


void DiskCacheFactory::shutdown()
{
    for (const auto & disk_cache : caches)
    {
        if (disk_cache.second)
            disk_cache.second->shutdown();
    }
    IDiskCache::close();
}

size_t DiskCacheFactory::getGlobalTTLLimit() const
{
    auto it = caches.find(DiskCacheType::MergeTree);
    if (it != caches.end() && it->second)
        return it->second->getSettings().ttl_cache_max_size;
    return 0;
}

IDiskCachePtr DiskCacheFactory::createDiskCacheFromTableSettings(
    const String & table_name,
    const UUID & table_uuid,
    Context & context,
    const ThrottlerPtr & throttler,
    UInt64 ttl_minutes,
    size_t max_size_bytes)
{
    Poco::Logger * log = &Poco::Logger::get("DiskCacheFactory");

    // Check registry first (for worker reuse).
    // If settings changed (ttl_minutes or max_size_bytes), evict the stale entry and fall through to recreate.
    {
        std::lock_guard<std::mutex> lock(ttl_cache_registry_mutex);
        auto reg_it = per_table_ttl_caches.find(table_uuid);
        if (reg_it != per_table_ttl_caches.end())
        {
            auto existing = static_pointer_cast<DiskCacheTTL>(reg_it->second);
            if (existing->getTTLMinutes() == ttl_minutes && existing->getMaxSizeBytes() == max_size_bytes)
            {
                LOG_TRACE(log, "Reusing existing TTL cache for {} (UUID: {})", table_name, UUIDHelpers::UUIDToString(table_uuid));
                return reg_it->second;
            }
            LOG_INFO(log, "TTL cache settings changed for {} (UUID: {}), recreating (ttl: {}->{}min, max_size: {}->{}bytes)",
                table_name, UUIDHelpers::UUIDToString(table_uuid),
                existing->getTTLMinutes(), ttl_minutes,
                existing->getMaxSizeBytes(), max_size_bytes);
            per_table_ttl_caches.erase(reg_it);
        }
    }

    // Get global cache settings as base
    DiskCacheSettings cache_settings;
    auto it = caches.find(DiskCacheType::MergeTree);
    if (it != caches.end() && it->second)
    {
        cache_settings = it->second->getSettings();
    }

    // Get volume from ttl_disk_policy 
    // defaults to disk_policy if not set
    VolumePtr volume = context.getStoragePolicy(cache_settings.ttl_disk_policy)->getVolumeByName("local", true);

    // Per-table size limit: explicit setting or constrained by global limit
    size_t effective_max_size = max_size_bytes;
    if (effective_max_size == 0)
    {
        LOG_DEBUG(log, "TTL cache for {} has no per-table limit, constrained only by global limit",
                 table_name);
    }

    // Per-table cache is always TTL-based
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(cache_settings);
    auto cache = std::make_shared<DiskCacheTTL>(
        table_name, UUIDHelpers::UUIDToString(table_uuid), volume, throttler, cache_settings, strategy, ttl_minutes, effective_max_size);

    if (effective_max_size > 0)
    {
        LOG_INFO(log, "Created per-table TTL cache for {} (UUID: {}, TTL: {} minutes, max_size: {}GB, policy: {})",
            table_name, UUIDHelpers::UUIDToString(table_uuid), ttl_minutes, effective_max_size / (1024*1024*1024), cache_settings.ttl_disk_policy);
    }
    else
    {
        LOG_INFO(log, "Created per-table TTL cache for {} (UUID: {}, TTL: {} minutes, max_size: unlimited, policy: {})",
            table_name, UUIDHelpers::UUIDToString(table_uuid), ttl_minutes, cache_settings.ttl_disk_policy);
    }

    if (auto catalog = context.getCnchCatalog())
    {
        try
        {
            auto metastore = catalog->getMetastore();
            String ns = context.getCnchConfigRef().getString("catalog.name_space", "default");
            String worker_id = getWorkerID(context.shared_from_this());
            String uuid_str = UUIDHelpers::UUIDToString(table_uuid);
            String own_endpoint = context.getHostWithPorts().getRPCAddress();
            auto fdb_idx = std::make_shared<TTLCacheFDBIndex>(metastore, ns, worker_id, uuid_str, own_endpoint);
            static_pointer_cast<DiskCacheTTL>(cache)->setFDBIndex(std::move(fdb_idx));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to create TTLCacheFDBIndex, cache will use disk scan on restart");
        }
    }

    // Insert into registry with re-check: if another thread won the race, discard ours.
    // load() is called only on the winner so only one disk scan runs per table UUID.
    {
        std::lock_guard<std::mutex> lock(ttl_cache_registry_mutex);
        auto [it, inserted] = per_table_ttl_caches.emplace(table_uuid, cache);
        if (!inserted)
        {
            LOG_TRACE(log, "Reusing TTL cache created concurrently for {} (UUID: {})", table_name, UUIDHelpers::UUIDToString(table_uuid));
            return it->second;
        }
    }

    // Schedule disk scan only for the winning cache object.
    auto & thread_pool = IDiskCache::getThreadPool();
    thread_pool.scheduleOrThrowOnError([cache] { cache->load(); });

    return cache;
}

void DiskCacheFactory::addNewCache(Context & context, const std::string & cache_name, bool create_default)
{
    Poco::Logger * log{&Poco::Logger::get("DiskCacheFactory")};

    DiskCacheSettings cache_settings;
    auto throttler = context.getDiskCacheThrottler();

    const auto & config = context.getConfigRef();
    cache_settings.loadFromConfig(config, cache_name);

    VolumePtr disk_cache_volume = context.getStoragePolicy(cache_settings.disk_policy)->getVolumeByName("local", true);

    auto total_space_unlimited = disk_cache_volume->getTotalSpace(true);
    auto total_space_limited = disk_cache_volume->getTotalSpace(false);

    if (create_default)
    {
        cache_settings.lru_max_size = std::min(
            static_cast<size_t>(total_space_unlimited.bytes * (cache_settings.lru_max_percent * 1.0 / 100)), cache_settings.lru_max_size);
        cache_settings.lru_max_nums = std::min(
            static_cast<size_t>(total_space_unlimited.inodes * (cache_settings.lru_max_percent * 1.0 / 100)), cache_settings.lru_max_nums);
    }
    else
    {
        cache_settings.lru_max_size = std::min(
            total_space_limited.bytes,
            std::min(
                static_cast<size_t>(total_space_unlimited.bytes * (cache_settings.lru_max_percent * 1.0 / 100)),
                cache_settings.lru_max_size));
        cache_settings.lru_max_nums = std::min(
            total_space_limited.inodes,
            std::min(
                static_cast<size_t>(total_space_unlimited.inodes * (cache_settings.lru_max_percent * 1.0 / 100)),
                cache_settings.lru_max_nums));
    }

    // Resolve global TTL cache limit (like LRU pattern)
    cache_settings.ttl_cache_max_size = (cache_settings.ttl_cache_max_size > 0)
        ? cache_settings.ttl_cache_max_size
        : static_cast<size_t>(total_space_unlimited.bytes * (cache_settings.ttl_cache_max_percent / 100.0));

    LOG_INFO(log, "{} cache: TTL global limit {}GB",
             cache_name, cache_settings.ttl_cache_max_size / (1024*1024*1024));

    // Global cache always uses LRU (TTL cache is per-table only)
    if (!cache_settings.meta_cache_size_ratio)
    {
        auto disk_cache = std::make_shared<DiskCacheLRU>(
            cache_name, disk_cache_volume, throttler, cache_settings, std::make_shared<DiskCacheSimpleStrategy>(cache_settings));
        caches.emplace(stringToDiskCacheType(cache_name), disk_cache);
        LOG_DEBUG(log, fmt::format("Registered `{}` single disk cache", cache_name));
    }
    else
    {
        auto strategy = std::make_shared<DiskCacheSimpleStrategy>(cache_settings);

        auto meta_disk_cache = std::make_shared<DiskCacheLRU>(
            cache_name, disk_cache_volume, throttler, cache_settings, strategy, IDiskCache::DataType::META);
        auto data_disk_cache = std::make_shared<DiskCacheLRU>(
            cache_name, disk_cache_volume, throttler, cache_settings, strategy, IDiskCache::DataType::DATA);
        caches.emplace(
            stringToDiskCacheType(cache_name),
            std::make_shared<MultiDiskCache>(
                cache_name, disk_cache_volume, throttler, cache_settings, strategy, meta_disk_cache, data_disk_cache));
        LOG_DEBUG(log, fmt::format("Registered `{}` multi disk cache", cache_name));
    }
}

}
