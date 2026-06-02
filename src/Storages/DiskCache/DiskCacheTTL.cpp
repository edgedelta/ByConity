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

#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/TTLCacheFDBIndex.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <fmt/core.h>
#include <sys/stat.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include "Common/Exception.h"
#include "Common/hex.h"
#include "common/logger_useful.h"
#include <Common/Throttler.h>
#include <Common/setThreadName.h>
#include <IO/OpenedFileCache.h>
#include "Interpreters/Context.h"
#include "Storages/DiskCache/DiskCache_fwd.h"
#include "Storages/DiskCache/IDiskCache.h"
#include <common/errnoToString.h>
#include <Disks/IVolume.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric DiskCacheEvictQueueLength;
}

namespace ProfileEvents
{
    extern const Event DiskCacheGetMetaMicroSeconds;
    extern const Event DiskCacheGetTotalOps;
    extern const Event DiskCacheSetTotalOps;
    extern const Event DiskCacheSetTotalBytes;
    extern const Event DiskCacheDataHits;
    extern const Event DiskCacheDataMisses;
    extern const Event DiskCacheIdxHits;
    extern const Event DiskCacheIdxMisses;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

static constexpr auto DISK_CACHE_TEMP_FILE_SUFFIX = ".temp";
static constexpr auto META_DISK_CACHE_DIR_PREFIX = "meta";
static constexpr auto DATA_DISK_CACHE_DIR_PREFIX = "data";

// On a size-eviction we deliberately free the overflow PLUS this fraction of the cap, so a
// cache sitting right at the limit under steady writes doesn't evict-on-every-set. Combined
// with the 10s trigger rate-limit, this bounds eviction churn.
static constexpr double SIZE_EVICTION_HEADROOM_FRACTION = 0.10;

namespace
{
    constexpr size_t HEX_KEY_LEN = sizeof(DiskCacheTTL::KeyType) * 2;

    // Atomic subtract that never wraps below zero. Defense-in-depth for total_size/total_entries:
    // a future accounting bug that subtracts more than was added would otherwise underflow size_t
    // and permanently poison size-based eviction.
    void atomicSubClamped(std::atomic<size_t> & counter, size_t v)
    {
        size_t cur = counter.load(std::memory_order_relaxed);
        while (!counter.compare_exchange_weak(cur, cur > v ? cur - v : 0, std::memory_order_relaxed))
            ;
    }

    // Extract UUID from segment/part name (format: uuid/part_name/...)
    String extractUUID(const String & seg_name)
    {
        size_t first_slash = seg_name.find('/');
        if (first_slash == std::string::npos)
            return seg_name;

        return seg_name.substr(0, first_slash);
    }

    // Extract part_name from segment name (format: uuid/part_name/column_segment.ext)
    String extractPartName(const String & seg_name)
    {
        size_t first_slash = seg_name.find('/');
        if (first_slash == std::string::npos)
            return seg_name;

        size_t second_slash = seg_name.find('/', first_slash + 1);
        if (second_slash == std::string::npos)
            return seg_name.substr(first_slash + 1);  // Return everything after uuid/

        return seg_name.substr(first_slash + 1, second_slash - first_slash - 1);
    }

    // Extract partition_id from part_name (format: 20240315_1_100_2 → 20240315)
    String extractPartitionId(const String & part_name)
    {
        size_t underscore_pos = part_name.find('_');
        if (underscore_pos == std::string::npos)
            return part_name;

        return part_name.substr(0, underscore_pos);
    }

    // Get relative path for part with new structure
    // Structure: prefix/uuid/partition/3char/hash_part/
    // Example: data/a1b2c3.../20240315/abc/abc123def456/
    fs::path getRelativePathForPart(const String & uuid, const String & part_name, const String & prefix)
    {
        String partition_id = extractPartitionId(part_name);
        auto hash_part = sipHash64(part_name.data(), part_name.size());
        String hex_hash(HEX_KEY_LEN / 2, '\0');
        writeHexUIntLowercase(hash_part, hex_hash.data());

        return fs::path(prefix) / uuid / partition_id / hex_hash.substr(0, 3) / hex_hash / "";
    }

    String formatPartitionId(time_t ts)
    {
        struct tm t;
        gmtime_r(&ts, &t);
        return fmt::format("{:04d}{:02d}{:02d}", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    }

    bool isHexKey(const String & hex_key)
    {
        if (hex_key.size() != HEX_KEY_LEN)
            return false;

        for (char c : hex_key)
        {
            if (!(isNumericASCII(c) || (c >= 'a' && c <= 'f')))
                return false;
        }

        return true;
    }

}

DiskCacheTTL::DiskCacheTTL(
    const String & name_,
    const String & table_uuid_,
    const VolumePtr & volume_,
    const ThrottlerPtr & throttler_,
    const DiskCacheSettings & settings_,
    const IDiskCacheStrategyPtr & strategy_,
    UInt64 ttl_minutes_,
    size_t max_size_bytes_,
    IDiskCache::DataType type_)
    : IDiskCache(name_, volume_, throttler_, settings_, strategy_, false, type_)
    , set_rate_throttler(settings_.cache_set_rate_limit == 0 ? nullptr : std::make_shared<Throttler>(settings_.cache_set_rate_limit))
    , set_throughput_throttler(settings_.cache_set_throughput_limit == 0 ? nullptr : std::make_shared<Throttler>(settings_.cache_set_throughput_limit))
    , table_uuid(table_uuid_)
    , ttl_minutes(ttl_minutes_)
    // Single source of truth for the "0 = use worker-level cap" contract: resolve it here so every
    // caller; factory or direct gets the same effective cap.
    , max_size_bytes(max_size_bytes_ > 0 ? max_size_bytes_ : settings_.ttl_cache_max_size)
{
    cache_stats.table_uuid = table_uuid_;
    LOG_INFO(log, "Initialized TTL cache for table {} with ttl_minutes={}, max_size_bytes={} ({}GB)",
             table_uuid_, ttl_minutes_, max_size_bytes, max_size_bytes / (1024*1024*1024));
    if (settings.cache_load_dispatcher_drill_down_level < -1)
    {
        throw Exception(fmt::format("Load dispatcher's drill down level {} invalid, "
            "must be positive or -1", settings.cache_load_dispatcher_drill_down_level),
            ErrorCodes::BAD_ARGUMENTS);
    }
    // load() is called by the factory after this object wins the registry race,
    // so only one disk scan runs per table UUID.
}

DiskCacheTTL::~DiskCacheTTL()
{
    // Stop scheduling new async eviction and wait for any in-flight task to finish before members are destroyed
    shutting_down.store(true);
    while (inflight_async.load() > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

void DiskCacheTTL::scheduleEvictTask(std::function<void()> task)
{
    // Reserve a slot before checking the flag so a concurrent destructor either sees this
    // count and waits, or we observe shutting_down and back out.
    inflight_async.fetch_add(1);
    if (shutting_down.load())
    {
        inflight_async.fetch_sub(1);
        return;
    }
    try
    {
        IDiskCache::getEvictPool().scheduleOrThrow([this, task = std::move(task)] {
            SCOPE_EXIT({ inflight_async.fetch_sub(1); });
            task();
        });
    }
    catch (...)
    {
        inflight_async.fetch_sub(1);
        throw;
    }
}

DiskCacheTTL::KeyType DiskCacheTTL::hash(const String & seg_key)
{
    // seg_key format: "uuid/part_name/column.bin/offset_0"
    // hash_high = hash(part_name only) for grouping all segments of a part
    // hash_low = hash(column + segment) for unique segment identification

    size_t first_slash = seg_key.find('/');
    if (first_slash == std::string::npos)
        throw Exception("Invalid seg key: " + seg_key, ErrorCodes::LOGICAL_ERROR);

    size_t second_slash = seg_key.find('/', first_slash + 1);
    if (second_slash == std::string::npos)
        throw Exception("Invalid seg key: " + seg_key, ErrorCodes::LOGICAL_ERROR);

    // hash_high = hash(part_name) - all segments in same part share this
    auto high = sipHash64(seg_key.data() + first_slash + 1, second_slash - first_slash - 1);

    // hash_low = hash(column/segment) - unique per segment
    auto low = sipHash64(seg_key.data() + second_slash + 1, seg_key.size() - second_slash - 1);

    return {high, low};
}

String DiskCacheTTL::hexKey(const KeyType & key)
{
    std::string res(HEX_KEY_LEN, '\0');
    writeHexUIntLowercase(key, res.data());
    return res;
}

std::optional<DiskCacheTTL::KeyType> DiskCacheTTL::unhexKey(const String & hex_key)
{
    if (!isHexKey(hex_key))
        return {};

    auto low = unhexUInt<UInt64>(hex_key.data());
    auto high = unhexUInt<UInt64>(hex_key.data() + HEX_KEY_LEN / 2);

    return UInt128{high, low};
}

fs::path DiskCacheTTL::getPath(const DiskCacheTTL::KeyType & hash_key, const String & path, const String & seg_name, const String & prefix) const
{
    // New structure: uuid/partition/3char/hash_part/hash_low
    // Example: a1b2c3d4.../20240315/abc/abc123def456/567890abcd

    String hex_key = hexKey(hash_key);
    std::string_view view(hex_key);
    std::string_view hex_key_low = view.substr(0, HEX_KEY_LEN / 2);
    std::string_view hex_key_high = view.substr(HEX_KEY_LEN / 2, HEX_KEY_LEN);

    String part_name = extractPartName(seg_name);
    String partition_id = extractPartitionId(part_name);
    String data_prefix = endsWith(seg_name, DATA_FILE_EXTENSION) ? DATA_DISK_CACHE_DIR_PREFIX : META_DISK_CACHE_DIR_PREFIX;

    // Structure: prefix/uuid/partition/3char/hash_high/hash_low
    return fs::path(path) / (prefix.empty() ? data_prefix : prefix)
           / table_uuid / partition_id
           / hex_key_high.substr(0, 3) / hex_key_high / hex_key_low;
}

// Parse YYYYMM, YYYYMMDD, or YYYYMMDDHH into a unix timestamp. Returns 0 on failure.
static time_t numericPartitionIdToTimestamp(const String & partition_id)
{
    if (partition_id.size() < 6 || !std::all_of(partition_id.begin(), partition_id.end(), ::isdigit))
        return 0;
    try
    {
        int year  = std::stoi(partition_id.substr(0, 4));
        int month = std::stoi(partition_id.substr(4, 2));
        int day   = partition_id.size() >= 8  ? std::stoi(partition_id.substr(6, 2)) : 1;
        int hour  = partition_id.size() >= 10 ? std::stoi(partition_id.substr(8, 2)) : 0;

        struct tm t = {};
        t.tm_year = year - 1900;
        t.tm_mon  = month - 1;
        t.tm_mday = day;
        t.tm_hour = hour;
        t.tm_isdst = -1;
        return mktime(&t);
    }
    catch (...) { return 0; }
}

time_t DiskCacheTTL::parsePartitionTimestamp(const String & part_name)
{
    try
    {
        // Extract part name from segment path (format: uuid/part_name/segment_name)
        size_t first_slash = part_name.find('/');
        if (first_slash == std::string::npos)
            return 0;

        size_t second_slash = part_name.find('/', first_slash + 1);
        String actual_part_name;
        if (second_slash != std::string::npos)
            actual_part_name = part_name.substr(first_slash + 1, second_slash - first_slash - 1);
        else
            actual_part_name = part_name.substr(first_slash + 1);

        // Parse partition_id from part name
        MergeTreePartInfo info;
        if (!MergeTreePartInfo::tryParsePartName(actual_part_name, &info, MergeTreeDataFormatVersion(1)))
            return 0;

        const String & partition_id = info.partition_id;
        if (partition_id.empty())
            return 0;

        return numericPartitionIdToTimestamp(partition_id);
    }
    catch (...)
    {
        return 0;
    }
}

bool DiskCacheTTL::shouldCache(time_t part_ts) const
{
    // TTL cache only for time-based partitions
    if (part_ts == 0)
        return false; // Non-time partitions are not cached

    // TTL disabled, defensively not cache
    if (ttl_minutes == 0)
        return false;

    time_t now = time(nullptr);
    time_t age_seconds = now - part_ts;
    time_t ttl_seconds = ttl_minutes * 60;

    return age_seconds <= ttl_seconds;
}

void DiskCacheTTL::cacheInsertLocked(Shard & shard, KeyType key, std::shared_ptr<DiskCacheTTLMeta> meta, const String & precomputed_partition_id)
{
    shard.cache_map[key] = meta;
    UInt64 hash_high = key.items[0];
    auto & entry = shard.part_index[hash_high];
    if (entry.partition_id.empty())
    {
        entry.partition_id = precomputed_partition_id.empty()
            ? formatPartitionId(meta->max_timestamp)
            : precomputed_partition_id;
        entry.partition_ts = meta->max_timestamp;
    }
    entry.keys.insert(key);
    entry.total_bytes += meta->size;
}

DiskCacheTTL::CacheEraseResult DiskCacheTTL::cacheEraseLocked(Shard & shard, KeyType key)
{
    CacheEraseResult result;
    auto it = shard.cache_map.find(key);
    if (it == shard.cache_map.end())
        return result;

    size_t bytes = it->second->size;
    shard.cache_map.erase(it);

    UInt64 hash_high = key.items[0];
    auto pit = shard.part_index.find(hash_high);
    if (pit != shard.part_index.end())
    {
        result.partition_id = pit->second.partition_id;
        result.partition_ts = pit->second.partition_ts;
        result.count = 1;
        result.bytes = bytes;

        pit->second.total_bytes -= bytes;
        pit->second.keys.erase(key);
        if (pit->second.keys.empty())
            shard.part_index.erase(pit);
    }
    return result;
}

DiskCacheTTL::CacheEraseResult DiskCacheTTL::cacheErasePartLocked(Shard & shard, UInt64 hash_high)
{
    CacheEraseResult result;
    auto pit = shard.part_index.find(hash_high);
    if (pit == shard.part_index.end())
        return result;

    result.partition_id = pit->second.partition_id;
    result.partition_ts = pit->second.partition_ts;
    result.hash_high = hash_high;
    result.count = pit->second.keys.size();
    result.bytes = pit->second.total_bytes;

    for (const auto & key : pit->second.keys)
    {
        auto it = shard.cache_map.find(key);
        if (it != shard.cache_map.end())
        {
            if (it->second->disk && !it->second->rel_path.empty())
                result.files.emplace_back(it->second->disk, it->second->rel_path);
            shard.cache_map.erase(it);
        }
    }

    shard.part_index.erase(pit);
    return result;
}

void DiskCacheTTL::applyEraseResults(std::vector<CacheEraseResult> & results, size_t & total_evicted, const char * log_tag)
{
    for (auto & result : results)
    {
        for (const auto & [disk, rel_path] : result.files)
        {
            try { disk->removeFileIfExists(rel_path); }
            catch (...) { tryLogCurrentException(log, log_tag); }
        }
        if (fdb_index)
            fdb_index->evictPart(result.partition_id, result.hash_high);
        total_evicted += result.count;
    }
}

void DiskCacheTTL::set(const String& seg_name, ReadBuffer& value, size_t weight_hint, bool is_preload, time_t max_time)
{
    if (is_droping)
    {
        LOG_WARNING(log, fmt::format("skip write disk cache for droping disk cache is running"));
        return;
    }

    if (weight_hint == 0)
        return;

    // Use provided max_time if available, else parse from partition_id
    time_t part_ts = (max_time > 0) ? max_time : parsePartitionTimestamp(seg_name);
    if (!shouldCache(part_ts))
    {
        if (part_ts == 0)
            cache_stats.rejected_non_time_partition++;
        else
            cache_stats.rejected_too_old++;
        LOG_TRACE(log, "Skipping cache for expired partition: {}", seg_name);
        return;
    }

    if (set_rate_throttler)
    {
        set_rate_throttler->add(1);
    }

    ProfileEvents::increment(ProfileEvents::DiskCacheSetTotalOps, 1, Metrics::MetricType::Rate, {{"type", (is_preload ? "preload": "query")}});

    auto key = hash(seg_name);
    String part_name = extractPartName(seg_name);
    String partition_id = extractPartitionId(part_name);
    bool is_idx_seg = endsWith(seg_name, INDEX_FILE_EXTENSION);
    time_t cached_at = time(nullptr);

    auto & shard = getShard(key.items[0]);

    // First lock: check if already exists, reserve slot
    {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        if (shard.cache_map.find(key) != shard.cache_map.end())
            return;

        shard.cache_map[key] = std::make_shared<DiskCacheTTLMeta>(
            DiskCacheTTLMeta::State::Caching, nullptr, 0, cached_at, part_ts
        );
    }

    ReservationPtr reserved_space = nullptr;
    try
    {
        reserved_space = volume->reserve(weight_hint);
        if (reserved_space == nullptr)
        {
            throw Exception("Failed to reserve space", ErrorCodes::BAD_ARGUMENTS);
        }

        String cache_rel_path = getRelativePath(key, seg_name).string();
        size_t weight = writeSegment(value, reserved_space, cache_rel_path);
        ProfileEvents::increment(ProfileEvents::DiskCacheSetTotalBytes, weight, Metrics::MetricType::Rate, {{"type", (is_preload ? "preload": "query")}});

        {
            std::unique_lock<std::shared_mutex> lock(shard.mutex);
            auto meta = std::make_shared<DiskCacheTTLMeta>(
                DiskCacheTTLMeta::State::Cached, reserved_space->getDisk(), weight, cached_at, part_ts, cache_rel_path
            );
            cacheInsertLocked(shard, key, meta, partition_id);
            total_entries++;
            total_size += weight;

            // Track write source (preload vs query), split by segment type
            if (is_preload)
            {
                if (is_idx_seg) { cache_stats.cached_idx_from_preload++; cache_stats.cached_idx_bytes_preload += weight; }
                else { cache_stats.cached_from_preload++; cache_stats.cached_bytes_preload += weight; }
            }
            else
            {
                if (is_idx_seg) { cache_stats.cached_idx_from_query++; cache_stats.cached_idx_bytes_query += weight; }
                else { cache_stats.cached_from_query++; cache_stats.cached_bytes_query += weight; }
            }

            // Update global TTL usage
            DiskCacheFactory::instance().addGlobalTTLUsage(weight);
        }

        if (fdb_index)
            fdb_index->onSet(key, partition_id);

        // Async size-based eviction once the hard cap is exceeded.
        // max_size_bytes is always set (factory falls back to global limit when no per-table limit
        // is configured), so one check suffices. Done after updatePartitionStats so the
        // just-added partition is visible to evictOldestPartitionsUntilSpace.
        if (max_size_bytes > 0 && total_size.load() > max_size_bytes)
        {
            time_t now = time(nullptr);
            time_t last_trigger = last_size_eviction_trigger.load();

            // Rate limit: at most once per 10 seconds
            if (now - last_trigger > 10)
            {
                if (last_size_eviction_trigger.compare_exchange_strong(last_trigger, now))
                {
                    // Re-load once and floor: a concurrent eviction may have dropped total_size
                    // below the cap since the guard above, and (cur - max) would underflow size_t
                    // to a huge value, causing us to evict the entire cache.
                    size_t cur = total_size.load();
                    size_t excess = cur > max_size_bytes ? cur - max_size_bytes : 0;
                    size_t target_free = excess + max_size_bytes * SIZE_EVICTION_HEADROOM_FRACTION;
                    cache_stats.async_eviction_triggered++;
                    LOG_DEBUG(log, "Table cache {}% full, scheduling async eviction to free {} bytes",
                             (total_size.load() * 100 / max_size_bytes), target_free);

                    scheduleEvictTask([this, target_free] {
                        Stopwatch watch;
                        evictOldestPartitionsUntilSpace(target_free);
                        LOG_INFO(log, "Async size-based eviction freed space in {} ms",
                                watch.elapsedMilliseconds());
                    });
                }
            }
            else
            {
                cache_stats.async_eviction_skipped_rate_limit++;
            }
        }
    }
    catch (...)
    {
        // Catch everything, not just DB::Exception: writeSegment can throw std::bad_alloc,
        // std::filesystem_error, etc. If any of those escaped, the State::Caching placeholder
        // inserted under the first lock would never be erased, permanently poisoning this key
        // (get() always misses on it, set() early-returns on the "already present" guard).
        String local_disk_path = reserved_space == nullptr ? "" : reserved_space->getDisk()->getPath();
        tryLogCurrentException(log, fmt::format("Failed to write key {} "
            "to local, disk path: {}, weight: {}", seg_name, local_disk_path, weight_hint));

        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        cacheEraseLocked(shard, key);  // also cleans up part_index reservation slot
    }
}

std::pair<DiskPtr, String> DiskCacheTTL::get(const String & seg_name)
{
    ProfileEvents::increment(ProfileEvents::DiskCacheGetTotalOps);
    Stopwatch watch;
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::DiskCacheGetMetaMicroSeconds,
        watch.elapsedMicroseconds());});

    // Periodic eviction check
    time_t now = time(nullptr);
    time_t last_check = last_eviction_check.load();
    if (now - last_check > 60)
    {
        if (last_eviction_check.compare_exchange_strong(last_check, now))
        {
            // Trigger eviction asynchronously
            scheduleEvictTask([this] { evictExpired(); });
        }
    }

    auto key = hash(seg_name);
    bool is_idx_seg = endsWith(seg_name, INDEX_FILE_EXTENSION);

    DiskPtr disk;
    String rel_path;
    CacheEraseResult erase_result;

    auto & shard = getShard(key.items[0]);
    {
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        auto it = shard.cache_map.find(key);
        if (it == shard.cache_map.end() || it->second->state != DiskCacheTTLMeta::State::Cached)
        {
            if (is_idx_seg) { cache_stats.idx_misses++; ProfileEvents::increment(ProfileEvents::DiskCacheIdxMisses); }
            else { cache_stats.data_misses++; ProfileEvents::increment(ProfileEvents::DiskCacheDataMisses); }
        }
        else if (unlikely(it->second->disk == nullptr))
        {
            // Corrupted entry: upgrade to exclusive lock to erase
            lock.unlock();
            std::unique_lock<std::shared_mutex> ulock(shard.mutex);
            auto it2 = shard.cache_map.find(key);
            if (it2 != shard.cache_map.end() && it2->second->disk == nullptr)
            {
                LOG_ERROR(log, "Cached entry {} has null disk — corrupted meta, evicting", seg_name);
                erase_result = cacheEraseLocked(shard, key);
                if (erase_result.count > 0)
                {
                    atomicSubClamped(total_entries, erase_result.count);
                    atomicSubClamped(total_size, erase_result.bytes);
                    DiskCacheFactory::instance().releaseGlobalTTL(erase_result.bytes);
                }
            }
            if (is_idx_seg) { cache_stats.idx_misses++; ProfileEvents::increment(ProfileEvents::DiskCacheIdxMisses); }
            else { cache_stats.data_misses++; ProfileEvents::increment(ProfileEvents::DiskCacheDataMisses); }
        }
        else
        {
            if (is_idx_seg) { cache_stats.idx_hits++; ProfileEvents::increment(ProfileEvents::DiskCacheIdxHits); }
            else { cache_stats.data_hits++; ProfileEvents::increment(ProfileEvents::DiskCacheDataHits); }
            disk = it->second->disk;
            rel_path = it->second->rel_path;
        }
    }

    return {disk, rel_path};
}

size_t DiskCacheTTL::writeSegment(ReadBuffer& buffer, ReservationPtr& reservation, const String& cache_rel_path)
{
    DiskPtr disk = reservation->getDisk();
    String temp_cache_rel_path = cache_rel_path + ".temp";

    try
    {
        disk->createDirectories(fs::path(cache_rel_path).parent_path());

        size_t written_size = 0;
        {
            WriteBufferFromFile to(
                fs::path(disk->getPath()) / temp_cache_rel_path, DBMS_DEFAULT_BUFFER_SIZE, -1, 0666, nullptr, 0, set_throughput_throttler);
            copyData(buffer, to, reservation.get());
            to.finalize();
            written_size = to.count();
        }

        disk->replaceFile(temp_cache_rel_path, cache_rel_path);

        if (written_size == 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "cached {} write produced 0 bytes — refusing to cache empty file",
                cache_rel_path);

        if (disk->getFileSize(cache_rel_path) != written_size)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "cached {} file size {} doesn't match written size {}",
                cache_rel_path,
                disk->getFileSize(cache_rel_path),
                written_size);

        return written_size;
    }
    catch (...)
    {
        disk->removeFileIfExists(temp_cache_rel_path);
        disk->removeFileIfExists(cache_rel_path);
        throw;
    }
}

void DiskCacheTTL::evictExpired()
{
    // a shared-lock scan that doesn't block concurrent get()/set().
    // We only upgrade to the exclusive lock for shards that actually have expired parts, and re-validate there.
    std::vector<CacheEraseResult> erase_results;
    size_t evicted_bytes = 0;

    auto is_expired = [this](const Shard & shard, const PartIndexEntry & entry) {
        if (entry.keys.empty()) return false;
        auto sample = shard.cache_map.find(*entry.keys.begin());
        return sample != shard.cache_map.end() && !shouldCache(sample->second->max_timestamp);
    };

    for (auto & shard : shards)
    {
        // read-locked scan.
        std::vector<UInt64> expired_hash_highs;
        {
            std::shared_lock<std::shared_mutex> lock(shard.mutex);
            for (const auto & [hash_high, entry] : shard.part_index)
                if (is_expired(shard, entry))
                    expired_hash_highs.push_back(hash_high);
        }
        if (expired_hash_highs.empty())
            continue;

        // write-locked erase, only for shards with work. Re-check under the lock.
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        for (UInt64 hash_high : expired_hash_highs)
        {
            auto pit = shard.part_index.find(hash_high);
            if (pit == shard.part_index.end() || !is_expired(shard, pit->second))
                continue;  // gone or refreshed since the read scan
            auto result = cacheErasePartLocked(shard, hash_high);
            if (result.count > 0)
            {
                atomicSubClamped(total_entries, result.count);
                atomicSubClamped(total_size, result.bytes);
                evicted_bytes += result.bytes;
                erase_results.push_back(std::move(result));
            }
        }
    }

    if (erase_results.empty())
    {
        cache_stats.last_eviction_run = time(nullptr);
        return;
    }

    size_t total_evicted = 0;
    applyEraseResults(erase_results, total_evicted, "Failed to evict expired segment");
    cache_stats.evicted_expired += total_evicted;
    DiskCacheFactory::instance().releaseGlobalTTL(evicted_bytes);

    LOG_INFO(log, "Evicted {} expired segments, freed {} bytes", total_evicted, evicted_bytes);
    cache_stats.last_eviction_run = time(nullptr);
}

void DiskCacheTTL::evictOldestPartitionsUntilSpace(size_t needed_bytes)
{
    size_t cur = total_size.load();
    size_t target_size = cur > needed_bytes ? cur - needed_bytes : 0;
    if (cur <= target_size)
        return;

    LOG_DEBUG(log, "Size eviction: current={}, needed={}, target={}", cur, needed_bytes, target_size);

    // Snapshot every part as (partition_ts, shard, hash_high) under short per-shard read
    // locks. partition_ts is per-part, so sorting on it directly gives a globally correct
    // oldest-first order with no representative-timestamp guesswork.
    struct PartRef { time_t ts; size_t shard; UInt64 hash_high; };
    std::vector<PartRef> parts;
    for (size_t s = 0; s < shards.size(); ++s)
    {
        std::shared_lock<std::shared_mutex> lock(shards[s].mutex);
        parts.reserve(parts.size() + shards[s].part_index.size());
        for (const auto & [hash_high, entry] : shards[s].part_index)
            parts.push_back({entry.partition_ts, s, hash_high});
    }
    std::sort(parts.begin(), parts.end(), [](const PartRef & a, const PartRef & b) { return a.ts < b.ts; });

    // Evict oldest parts one at a time until we'd be under target. cacheErasePartLocked does
    // all the per-part bookkeeping (cache_map + part_index erase, exact file list); a part
    // removed between snapshot and now returns count==0 and is skipped.
    std::vector<CacheEraseResult> erased;
    size_t freed = 0;
    for (const auto & p : parts)
    {
        if (cur - freed <= target_size)
            break;
        std::unique_lock<std::shared_mutex> lock(shards[p.shard].mutex);
        auto r = cacheErasePartLocked(shards[p.shard], p.hash_high);
        if (r.count > 0)
        {
            atomicSubClamped(total_entries, r.count);
            atomicSubClamped(total_size, r.bytes);
            freed += r.bytes;
            erased.push_back(std::move(r));
        }
    }
    if (erased.empty())
        return;

    // Remove the exact files, evict each part from FDB, and decrement partition stats —
    // all via the shared helper used by evictExpired/drop (per-file removeFileIfExists, so a
    // concurrent set() in the same partition dir is never collateral-deleted; subtractFrom-
    // PartitionStats drops a partition's stats row once its last part is evicted).
    size_t total_evicted = 0;
    applyEraseResults(erased, total_evicted, "Failed to evict segment for size limit");

    cache_stats.evicted_size_limit += total_evicted;
    DiskCacheFactory::instance().releaseGlobalTTL(freed);
    LOG_INFO(log, "Size eviction: freed {} bytes ({} segments across {} parts)", freed, total_evicted, erased.size());
}

void DiskCacheTTL::updateSettings(UInt64 new_ttl_minutes, size_t new_max_size_bytes)
{
    UInt64 old_ttl = ttl_minutes.exchange(new_ttl_minutes, std::memory_order_relaxed);
    size_t old_max = max_size_bytes.exchange(new_max_size_bytes, std::memory_order_relaxed);

    // ttl_minutes==0 means "disabled", treated as a tighten
    // from any nonzero ttl; max_size==0 means "no per-table cap", never a size tighten.
    bool ttl_tightened  = new_ttl_minutes != 0 && (old_ttl == 0 || new_ttl_minutes < old_ttl);
    bool size_tightened = new_max_size_bytes != 0 && new_max_size_bytes < old_max;
    if (!ttl_tightened && !size_tightened)
        return;

    try
    {
        scheduleEvictTask([this, ttl_tightened, size_tightened] {
            if (ttl_tightened)
                evictExpired();
            if (size_tightened)
            {
                size_t cap = max_size_bytes.load();
                size_t cur = total_size.load();
                if (cap > 0 && cur > cap)
                    evictOldestPartitionsUntilSpace((cur - cap) + cap * SIZE_EVICTION_HEADROOM_FRACTION);
            }
        });
    }
    catch (...)
    {
        // Evict pool saturated; the shrink will still be applied lazily by set()/the periodic
        // tick. Don't let a settings update fail because of a transient scheduling error.
        tryLogCurrentException(log, "updateSettings: failed to schedule eager eviction from ttl cache settings change");
    }
}

void DiskCacheTTL::load()
{
    // Instance-disk deployment: the local NVMe cache directory does not survive a restart, so
    // there is nothing to restore — no FDB reconcile, no disk scan. We always start cold.
    // Defensively wipe any directory that did survive (non-instance disk), because an in-memory
    // index that starts empty would never learn about those files, i.e. leaked disk forever.
    for (const auto & disk : volume->getDisks())
    {
        try
        {
            if (disk->exists(latest_disk_cache_dir))
                disk->removeRecursive(latest_disk_cache_dir);
            for (const auto & prev : previous_disk_cache_dirs)
                if (disk->exists(prev))
                    disk->removeRecursive(prev);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("TTL cache for {}: failed to clear stale cache dir on load", table_uuid));
        }
    }
    LOG_INFO(log, "TTL disk cache for {} started cold (instance disk: nothing to restore)", table_uuid);
}

size_t DiskCacheTTL::drop(const String & part_base_path)
{
    // New structure: uuid/partition/3char/hash_part/
    // part_base_path format: "uuid/part_name"
    fs::path meta_path, data_path;

    if (part_base_path.empty())
    {
        // Drop entire cache for this table
        if (type == DataType::ALL || type == DataType::META)
            meta_path = fs::path(latest_disk_cache_dir) / META_DISK_CACHE_DIR_PREFIX / table_uuid;
        if (type == DataType::ALL || type == DataType::DATA)
            data_path = fs::path(latest_disk_cache_dir) / DATA_DISK_CACHE_DIR_PREFIX / table_uuid;
    }
    else
    {
        // Drop specific part: extract uuid and part_name from part_base_path
        String uuid = extractUUID(part_base_path);
        String part_name = extractPartName(part_base_path);

        if (type == DataType::ALL || type == DataType::META)
            meta_path = fs::path(latest_disk_cache_dir) / getRelativePathForPart(uuid, part_name, META_DISK_CACHE_DIR_PREFIX);
        if (type == DataType::ALL || type == DataType::DATA)
            data_path = fs::path(latest_disk_cache_dir) / getRelativePathForPart(uuid, part_name, DATA_DISK_CACHE_DIR_PREFIX);
    }

    LOG_TRACE(log, "Dropping cache for part {} (meta: {}, data: {})", part_base_path, meta_path.string(), data_path.string());

    const Disks & disks = volume->getDisks();

    for (const auto & disk : disks)
    {
        try
        {
            if (!meta_path.empty() && disk->exists(meta_path))
                disk->removeRecursive(meta_path);
            if (!data_path.empty() && disk->exists(data_path))
                disk->removeRecursive(data_path);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to remove cache dir on drop: " + part_base_path);
        }
    }

    if (part_base_path.empty())
    {
        size_t dropped_bytes = total_size.load();
        for (auto & shard : shards)
        {
            std::unique_lock<std::shared_mutex> lock(shard.mutex);
            shard.cache_map.clear();
            shard.part_index.clear();
        }
        total_entries.store(0);
        total_size.store(0);
        DiskCacheFactory::instance().releaseGlobalTTL(dropped_bytes);
    }
    else
    {
        String part_name_only = extractPartName(part_base_path);
        UInt64 hash_high = sipHash64(part_name_only.data(), part_name_only.size());

        auto & shard = getShard(hash_high);
        CacheEraseResult result;
        {
            std::unique_lock<std::shared_mutex> lock(shard.mutex);
            result = cacheErasePartLocked(shard, hash_high);
            if (result.count > 0)
            {
                atomicSubClamped(total_entries, result.count);
                atomicSubClamped(total_size, result.bytes);
                DiskCacheFactory::instance().releaseGlobalTTL(result.bytes);
            }
        }
        if (result.count > 0)
        {
            if (fdb_index)
                fdb_index->evictPart(result.partition_id, result.hash_high);
        }
    }

    LOG_TRACE(log, "Dropped cache for part {}", part_base_path);
    return 0;
}

void DiskCacheTTL::drop()
{
    is_droping.store(true);

    // Release global counter + clear in-memory state
    size_t dropped_bytes = total_size.load();
    for (auto & shard : shards)
    {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        shard.cache_map.clear();
        shard.part_index.clear();
    }
    total_entries.store(0);
    total_size.store(0);
    DiskCacheFactory::instance().releaseGlobalTTL(dropped_bytes);

    // Atomically rename table dirs (cheap), then delete asynchronously
    String ts = std::to_string(time(nullptr));
    std::vector<std::pair<DiskPtr, String>> dirs_to_delete;
    for (const auto & disk : volume->getDisks())
    {
        for (const char * prefix : {META_DISK_CACHE_DIR_PREFIX, DATA_DISK_CACHE_DIR_PREFIX})
        {
            String old_path = (fs::path(latest_disk_cache_dir) / prefix / table_uuid).string();
            if (!disk->exists(old_path))
                continue;
            String drop_path = (fs::path(latest_disk_cache_dir) / prefix / (".drop_" + table_uuid + "_" + ts)).string();
            try
            {
                disk->moveDirectory(old_path, drop_path);
                dirs_to_delete.emplace_back(disk, drop_path);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to rename for async drop: " + old_path);
            }
        }
    }

    LOG_INFO(log, "TTL cache drop: released {} bytes, {} dirs queued for async deletion", dropped_bytes, dirs_to_delete.size());

    auto fdb = fdb_index;
    getEvictPool().scheduleOrThrow([dirs = std::move(dirs_to_delete), fdb] {
        for (auto & [disk, path] : dirs)
        {
            try { disk->removeRecursive(path); }
            catch (...) { tryLogCurrentException(&Poco::Logger::get("DiskCacheTTL"), "Async drop failed: " + path); }
        }
        if (fdb)
            fdb->evictTable();
    });
}

DiskCacheTTL::TTLCacheStats DiskCacheTTL::getStats() const
{
    TTLCacheStats stats;
    stats.table_uuid = cache_stats.table_uuid;
    stats.total_entries = total_entries.load();
    stats.total_bytes = total_size.load();
    stats.evicted_expired = cache_stats.evicted_expired.load();
    stats.evicted_size_limit = cache_stats.evicted_size_limit.load();
    stats.rejected_non_time_partition = cache_stats.rejected_non_time_partition.load();
    stats.rejected_too_old = cache_stats.rejected_too_old.load();
    stats.last_eviction_run = cache_stats.last_eviction_run.load();
    stats.async_eviction_triggered = cache_stats.async_eviction_triggered.load();
    stats.async_eviction_skipped_rate_limit = cache_stats.async_eviction_skipped_rate_limit.load();
    stats.cached_from_preload = cache_stats.cached_from_preload.load();
    stats.cached_from_query = cache_stats.cached_from_query.load();
    stats.cached_bytes_preload = cache_stats.cached_bytes_preload.load();
    stats.cached_bytes_query = cache_stats.cached_bytes_query.load();
    stats.cached_from_restored = cache_stats.cached_from_restored.load();
    stats.cached_bytes_restored = cache_stats.cached_bytes_restored.load();
    stats.cached_idx_from_preload = cache_stats.cached_idx_from_preload.load();
    stats.cached_idx_bytes_preload = cache_stats.cached_idx_bytes_preload.load();
    stats.cached_idx_from_query = cache_stats.cached_idx_from_query.load();
    stats.cached_idx_bytes_query = cache_stats.cached_idx_bytes_query.load();
    stats.data_hits = cache_stats.data_hits.load();
    stats.data_misses = cache_stats.data_misses.load();
    stats.idx_hits = cache_stats.idx_hits.load();
    stats.idx_misses = cache_stats.idx_misses.load();
    stats.total_hits = stats.data_hits + stats.idx_hits;
    stats.total_misses = stats.data_misses + stats.idx_misses;
    return stats;
}

std::vector<DiskCacheTTL::PartitionStats> DiskCacheTTL::getPartitionStats() const
{
    // Derived on demand from part_index rather than maintained incrementally: O(parts) read-locked scan.
    std::unordered_map<String, PartitionStats> agg;
    for (const auto & shard : shards)
    {
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        for (const auto & [hash_high, entry] : shard.part_index)
        {
            auto & p = agg[entry.partition_id];
            p.partition_id = entry.partition_id;
            p.entry_count += entry.keys.size();
            p.total_bytes += entry.total_bytes;
            // Representative timestamp = oldest part in the partition.
            if (p.partition_timestamp == 0 || (entry.partition_ts > 0 && entry.partition_ts < p.partition_timestamp))
                p.partition_timestamp = entry.partition_ts;
        }
    }

    std::vector<PartitionStats> result;
    result.reserve(agg.size());
    for (auto & [pid, snapshot] : agg)
        result.push_back(std::move(snapshot));
    return result;
}

std::optional<String> DiskCacheTTL::findPeerOwner(const String & seg_name)
{
    if (!fdb_index)
        return std::nullopt;

    auto key = hash(seg_name);
    String part_name = extractPartName(seg_name);
    String partition_id = extractPartitionId(part_name);

    auto maybe_worker_id = fdb_index->findPeerOwner(key, partition_id);
    if (!maybe_worker_id)
        return std::nullopt;

    return DiskCacheFactory::instance().resolveWorkerEndpoint(*maybe_worker_id);
}

}

