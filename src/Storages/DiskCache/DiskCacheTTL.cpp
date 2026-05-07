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
#include <filesystem>
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

namespace
{
    constexpr size_t HEX_KEY_LEN = sizeof(DiskCacheTTL::KeyType) * 2;

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

    bool isHexHalf(const String & s)
    {
        if (s.size() != HEX_KEY_LEN / 2)
            return false;
        for (char c : s)
            if (!(isNumericASCII(c) || (c >= 'a' && c <= 'f')))
                return false;
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
    , max_size_bytes(max_size_bytes_)  // Already calculated by factory
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

        // Try to parse as date/datetime
        // Common formats: YYYYMMDD, YYYYMMDDHH, YYYYMM
        if (partition_id.size() >= 8 && std::all_of(partition_id.begin(), partition_id.end(), ::isdigit))
        {
            // Parse as YYYYMMDD
            int year = std::stoi(partition_id.substr(0, 4));
            int month = std::stoi(partition_id.substr(4, 2));
            int day = partition_id.size() >= 8 ? std::stoi(partition_id.substr(6, 2)) : 1;

            struct tm tm_info = {};
            tm_info.tm_year = year - 1900;
            tm_info.tm_mon = month - 1;
            tm_info.tm_mday = day;
            tm_info.tm_hour = 0;
            tm_info.tm_min = 0;
            tm_info.tm_sec = 0;
            tm_info.tm_isdst = -1;

            return mktime(&tm_info);
        }

        return 0;
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

void DiskCacheTTL::addToPartitionStats(const String & partition_id, time_t partition_ts, size_t bytes, size_t count)
{
    {
        std::shared_lock<std::shared_mutex> lk(cache_stats.partition_stats_mutex);
        auto it = cache_stats.partition_stats.find(partition_id);
        if (it != cache_stats.partition_stats.end())
        {
            it->second.entry_count += count;
            it->second.total_bytes += bytes;
            return;
        }
    }
    std::unique_lock<std::shared_mutex> lk(cache_stats.partition_stats_mutex);
    auto [it, inserted] = cache_stats.partition_stats.try_emplace(partition_id);
    if (inserted)
    {
        it->second.partition_id = partition_id;
        it->second.partition_timestamp = partition_ts;
    }
    it->second.entry_count += count;
    it->second.total_bytes += bytes;
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
        subtractFromPartitionStats(result);
        total_evicted += result.count;
    }
}

void DiskCacheTTL::subtractFromPartitionStats(const CacheEraseResult & result)
{
    if (result.count == 0)
        return;
    // shared_lock suffices: we only decrement existing atomics, no map insert/rehash.
    std::shared_lock<std::shared_mutex> lk(cache_stats.partition_stats_mutex);
    auto it = cache_stats.partition_stats.find(result.partition_id);
    if (it == cache_stats.partition_stats.end())
        return;
    auto & ps = it->second;
    ps.entry_count -= result.count;
    ps.total_bytes -= result.bytes;
}

void DiskCacheTTL::set(const String& seg_name, ReadBuffer& value, size_t weight_hint, bool is_preload, time_t /*min_time*/, time_t max_time)
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
        Stopwatch wait_sw;
        std::lock_guard<std::mutex> lock(shard.mutex);
        if (wait_sw.elapsedMicroseconds() > 1000)
            LOG_WARNING(log, "[ttl-perf] set() first lock waited {} us", wait_sw.elapsedMicroseconds());

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
            Stopwatch wait_sw;
            std::lock_guard<std::mutex> lock(shard.mutex);
            if (wait_sw.elapsedMicroseconds() > 1000)
                LOG_WARNING(log, "[ttl-perf] set() second lock waited {} us", wait_sw.elapsedMicroseconds());

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

        // Update partition stats outside shard mutex to avoid lock ordering with partition_stats_mutex
        addToPartitionStats(partition_id, part_ts, weight);

        if (fdb_index)
            fdb_index->onSet(key, seg_name, weight, part_ts);

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
                    size_t excess = total_size.load() - max_size_bytes;
                    size_t target_free = excess + max_size_bytes * 0.10;
                    cache_stats.async_eviction_triggered++;
                    LOG_DEBUG(log, "Table cache {}% full, scheduling async eviction to free {} bytes",
                             (total_size.load() * 100 / max_size_bytes), target_free);

                    auto & thread_pool = IDiskCache::getEvictPool();
                    thread_pool.scheduleOrThrow([this, target_free] {
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
    catch(const Exception & e)
    {
        String local_disk_path = reserved_space == nullptr ? "" : reserved_space->getDisk()->getPath();
        tryLogCurrentException(log, fmt::format("Failed to write key {} "
            "to local, disk path: {}, weight: {}, fail: {}", seg_name, local_disk_path, weight_hint, e.message()));

        std::lock_guard<std::mutex> lock(shard.mutex);
        cacheEraseLocked(shard, key);  // also cleans up part_index reservation slot
    }
}

std::pair<DiskPtr, String> DiskCacheTTL::get(const String & seg_name)
{
    ProfileEvents::increment(ProfileEvents::DiskCacheGetTotalOps);
    Stopwatch watch;
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::DiskCacheGetMetaMicroSeconds,
        watch.elapsedMicroseconds());});

    // Periodic eviction check (every 5 minutes)
    time_t now = time(nullptr);
    time_t last_check = last_eviction_check.load();
    if (now - last_check > 300)
    {
        if (last_eviction_check.compare_exchange_strong(last_check, now))
        {
            // Trigger eviction asynchronously
            auto & thread_pool = IDiskCache::getEvictPool();
            thread_pool.scheduleOrThrow([this] { evictExpired(); });
        }
    }

    auto key = hash(seg_name);
    bool is_idx_seg = endsWith(seg_name, INDEX_FILE_EXTENSION);

    DiskPtr disk;
    String rel_path;
    CacheEraseResult erase_result;

    auto & shard = getShard(key.items[0]);
    {
        Stopwatch wait_sw;
        std::lock_guard<std::mutex> lock(shard.mutex);
        if (wait_sw.elapsedMicroseconds() > 1000)
            LOG_WARNING(log, "[ttl-perf] get() lock waited {} us, shard_size={}", wait_sw.elapsedMicroseconds(), shard.cache_map.size());

        auto it = shard.cache_map.find(key);
        if (it == shard.cache_map.end() || it->second->state != DiskCacheTTLMeta::State::Cached)
        {
            if (is_idx_seg) { cache_stats.idx_misses++; ProfileEvents::increment(ProfileEvents::DiskCacheIdxMisses); }
            else { cache_stats.data_misses++; ProfileEvents::increment(ProfileEvents::DiskCacheDataMisses); }
        }
        else if (unlikely(it->second->disk == nullptr))
        {
            LOG_ERROR(log, "Cached entry {} has null disk — corrupted meta, evicting", seg_name);
            erase_result = cacheEraseLocked(shard, key);
            if (erase_result.count > 0)
            {
                total_entries--;
                total_size -= erase_result.bytes;
                DiskCacheFactory::instance().releaseGlobalTTL(erase_result.bytes);
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

    if (erase_result.count > 0)
        subtractFromPartitionStats(erase_result);

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
    // Single lock: scan + erase in one critical section — no disk I/O happens inside.
    // Collect expired hash_highs first, then erase in a second pass
    // to avoid iterator invalidation from cacheErasePartLocked.
    std::vector<CacheEraseResult> erase_results;
    size_t evicted_bytes = 0;

    for (auto & shard : shards)
    {
        std::lock_guard<std::mutex> lock(shard.mutex);

        std::vector<UInt64> expired_hash_highs;
        for (const auto & [hash_high, entry] : shard.part_index)
        {
            if (entry.keys.empty()) continue;
            auto sample = shard.cache_map.find(*entry.keys.begin());
            if (sample != shard.cache_map.end() && !shouldCache(sample->second->max_timestamp))
                expired_hash_highs.push_back(hash_high);
        }

        for (UInt64 hash_high : expired_hash_highs)
        {
            auto result = cacheErasePartLocked(shard, hash_high);
            if (result.count > 0)
            {
                total_entries -= result.count;
                total_size -= result.bytes;
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

    LOG_DEBUG(log, "Size eviction: current={}, needed={}, target={}", cur, needed_bytes, target_size);

    size_t total_evicted = 0;
    size_t evicted_bytes = 0;

    for (auto & shard : shards)
    {
        if (total_size.load() <= target_size)
            break;

        // Snapshot part timestamps under a short lock — no allocations, just push_backs.
        std::vector<std::pair<time_t, UInt64>> by_ts; // (partition_ts, hash_high)
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            by_ts.reserve(shard.part_index.size());
            for (const auto & [hash_high, entry] : shard.part_index)
                by_ts.emplace_back(entry.partition_ts, hash_high);
        }

        // Sort oldest-first outside the lock.
        std::sort(by_ts.begin(), by_ts.end());

        // Evict under a second lock. Parts may have been removed between the two locks;
        // cacheErasePartLocked returns count=0 for missing entries and is skipped.
        std::vector<CacheEraseResult> erase_results;
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            size_t current_size = total_size.load();
            for (auto & [ts, hash_high] : by_ts)
            {
                if (current_size <= target_size)
                    break;

                auto result = cacheErasePartLocked(shard, hash_high);
                if (result.count > 0)
                {
                    total_entries -= result.count;
                    total_size -= result.bytes;
                    current_size -= result.bytes;
                    evicted_bytes += result.bytes;
                    erase_results.push_back(std::move(result));
                }
            }
        }

        applyEraseResults(erase_results, total_evicted, "Failed to evict segment for size limit");
    }

    if (total_evicted > 0)
    {
        cache_stats.evicted_size_limit += total_evicted;
        DiskCacheFactory::instance().releaseGlobalTTL(evicted_bytes);
        LOG_INFO(log, "Evicted {} segments from oldest parts for size limit, freed {} bytes",
                 total_evicted, evicted_bytes);
    }
}

void DiskCacheTTL::load()
{
    if (fdb_index)
    {
        auto result = fdb_index->reconcile(
            volume,
            [this](UInt128 key, const String & seg_name) { return getRelativePath(key, seg_name); },
            [this](time_t ts) { return shouldCache(ts); },
            [this](TTLCacheFDBIndex::ReconcileBatch & batch) {
                // Group by shard: one lock per shard instead of one per entry.
                std::array<std::vector<std::pair<UInt128, std::shared_ptr<DiskCacheTTLMeta>>>, NUM_SHARDS> by_shard;
                for (auto & [key, meta] : batch)
                    by_shard[key.items[0] & (NUM_SHARDS - 1)].emplace_back(key, meta);
                for (size_t i = 0; i < NUM_SHARDS; ++i)
                {
                    if (by_shard[i].empty())
                        continue;
                    std::lock_guard<std::mutex> lock(shards[i].mutex);
                    for (auto & [key, meta] : by_shard[i])
                        cacheInsertLocked(shards[i], key, meta);
                }

                // Batch stats update: one lock per unique partition instead of one per entry.
                std::unordered_map<String, std::tuple<time_t, size_t, size_t>> stats_acc; // pid -> (ts, bytes, count)
                for (auto & [key, meta] : batch)
                {
                    auto pid = formatPartitionId(meta->max_timestamp);
                    auto & [ts, bytes, count] = stats_acc[pid];
                    ts = meta->max_timestamp;
                    bytes += meta->size;
                    count++;
                }
                for (auto & [pid, tbc] : stats_acc)
                {
                    auto & [ts, bytes, count] = tbc;
                    addToPartitionStats(pid, ts, bytes, count);
                    cache_stats.cached_from_restored += count;
                    cache_stats.cached_bytes_restored += bytes;
                }
            });

        if (result)
        {
            auto [entries, bytes] = *result;
            // fetch_add: concurrent set() calls may have already bumped these counters between cache registration and now
            total_entries.fetch_add(entries, std::memory_order_relaxed);
            total_size.fetch_add(bytes, std::memory_order_relaxed);

            LOG_INFO(log, "TTL cache for {} recovered from FDB index: {} entries, {} bytes",
                table_uuid, entries, bytes);
            return;
        }
        // reconcile() already logged the per-entry summary (restored/stale counts)
        LOG_WARNING(log, "TTL cache for {}: FDB index had no restorable entries, falling back to disk scan", table_uuid);
    }
    else
    {
        LOG_WARNING(log, "TTL cache for {}: no FDB index available, loading from disk scan", table_uuid);
    }

    LOG_INFO(log, "Loading TTL disk cache from disk scan for {}...", table_uuid);

    for (const auto & disk : volume->getDisks())
    {
        DiskCacheLoader loader(*this, disk, settings.cache_loader_per_disk,
            settings.cache_load_dispatcher_drill_down_level,
            settings.cache_load_dispatcher_drill_down_level);

        for (const auto & dir_path : previous_disk_cache_dirs)
        {
            if (disk->exists(dir_path))
                loader.exec(dir_path);
        }

        if (disk->exists(latest_disk_cache_dir))
            loader.exec(latest_disk_cache_dir);

        LOG_INFO(log, "Loaded {} segments from disk {}", loader.total_loaded, disk->getName());
    }

    LOG_INFO(log, "TTL disk cache load complete. Total: {} segments, {} bytes", total_entries.load(), total_size.load());

    // Post-scan eviction: trigger synchronously now that partition_stats are fully populated.
    // This handles the deadlock where a disk that was overfull before restart has all subsequent
    // set() calls fail at volume->reserve() before reaching the eviction check in the write path,
    // leaving the cache stuck full with no way to self-recover via normal writes.
    // max_size_bytes is always set (factory falls back to global limit), so one check suffices.
    // Use hard cap (not 90%) — max_size_bytes already encodes the configured percent of disk.
    if (max_size_bytes > 0 && total_size.load() > max_size_bytes)
    {
        size_t excess = total_size.load() - max_size_bytes;
        size_t target_free = excess + max_size_bytes * 0.10;
        LOG_INFO(log, "Post-scan eviction triggered: total_size={}, max={}, freeing {} bytes",
                 total_size.load(), max_size_bytes, target_free);
        evictOldestPartitionsUntilSpace(target_free);
    }
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
    size_t delete_file_size = 0;

    for (const auto & disk : disks)
    {
        if (!meta_path.empty() && disk->exists(meta_path))
        {
            DiskCacheDeleter deleter(*this, disk, 1, -1, -1);
            deleter.exec(meta_path);
            delete_file_size += deleter.delete_file_size;
        }

        if (!data_path.empty() && disk->exists(data_path))
        {
            DiskCacheDeleter deleter(*this, disk, 1, -1, -1);
            deleter.exec(data_path);
            delete_file_size += deleter.delete_file_size;
        }
    }

    if (part_base_path.empty())
    {
        size_t dropped_bytes = total_size.load();
        for (auto & shard : shards)
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            shard.cache_map.clear();
            shard.part_index.clear();
        }
        total_entries.store(0);
        total_size.store(0);
        DiskCacheFactory::instance().releaseGlobalTTL(dropped_bytes);

        std::unique_lock<std::shared_mutex> lk(cache_stats.partition_stats_mutex);
        cache_stats.partition_stats.clear();
    }
    else
    {
        String part_name_only = extractPartName(part_base_path);
        UInt64 hash_high = sipHash64(part_name_only.data(), part_name_only.size());

        auto & shard = getShard(hash_high);
        CacheEraseResult result;
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            result = cacheErasePartLocked(shard, hash_high);
            if (result.count > 0)
            {
                total_entries -= result.count;
                total_size -= result.bytes;
                DiskCacheFactory::instance().releaseGlobalTTL(result.bytes);
            }
        }
        if (result.count > 0)
        {
            subtractFromPartitionStats(result);
            if (fdb_index)
                fdb_index->evictPart(result.partition_id, result.hash_high);
        }
    }

    LOG_TRACE(log, "Dropped {} bytes of cache for part {}", delete_file_size, part_base_path);
    return delete_file_size;
}

// DiskIterator implementations
DiskCacheTTL::DiskIterator::DiskIterator(
    const String & name_, DiskCacheTTL & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_)
    : name(name_), disk_cache(cache_), disk(disk_), worker_per_disk(worker_per_disk_),
      min_depth_parallel(min_depth_parallel_), max_depth_parallel(max_depth_parallel_)
{
    log = &Poco::Logger::get(name);

    if (worker_per_disk > 1)
        pool = std::make_unique<ThreadPool>(worker_per_disk);
}

void DiskCacheTTL::DiskIterator::exec(std::filesystem::path entry_path)
{
    iterateDirectory(entry_path, 0);

    if (pool)
        pool->wait();
}

void DiskCacheTTL::DiskIterator::iterateDirectory(std::filesystem::path rel_path, size_t depth)
{
    if (!disk->exists(rel_path))
        return;

    for (auto it = disk->iterateDirectory(rel_path); it->isValid(); it->next())
    {
        auto entry_path = rel_path / it->name();

        if (disk->isDirectory(entry_path))
        {
            iterateDirectory(entry_path, depth + 1);
        }
        else if (disk->isFile(entry_path))
        {
            iterateFile(entry_path, disk->getFileSize(entry_path));
        }
    }
}

// DiskCacheLoader
DiskCacheTTL::DiskCacheLoader::DiskCacheLoader(
    DiskCacheTTL & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_)
    : DiskIterator("DiskCacheTTLLoader", cache_, disk_, worker_per_disk_, min_depth_parallel_, max_depth_parallel_)
{
}

DiskCacheTTL::DiskCacheLoader::~DiskCacheLoader()
{
}

void DiskCacheTTL::DiskCacheLoader::iterateFile(std::filesystem::path file_path, size_t file_size)
{
    String filename = file_path.filename();

    // Skip temp files
    if (endsWith(filename, DISK_CACHE_TEMP_FILE_SUFFIX))
    {
        disk->removeFileIfExists(file_path);
        return;
    }

    // Skip and clean up 0-byte files — they indicate an interrupted or empty write
    // and would cause false cache HITs returning empty content.
    if (file_size == 0)
    {
        disk->removeFileIfExists(file_path);
        return;
    }

    // Path structure: {cache_dir}/{data|meta}/{uuid}/{partition}/{3char}/{hash_high}/{hash_low}
    // The filename is hash_low (low 64 bits of the key) and the parent dir is hash_high.
    // Parse each half as a hex UInt64 to reconstruct the full UInt128 key.
    if (!isHexHalf(filename))
    {
        LOG_WARNING(log, "Invalid cache file (hash_low): {}", file_path.string());
        return;
    }
    UInt64 low = unhexUInt<UInt64>(filename.data());

    // New structure: data/uuid/partition/3char/hash_high/hash_low
    // Extract partition from path hierarchy
    auto hash_high_dir = file_path.parent_path().filename().string();  // hash_high
    auto partition_dir = file_path.parent_path().parent_path().parent_path().filename().string();  // partition_id

    if (!isHexHalf(hash_high_dir))
    {
        LOG_WARNING(log, "Invalid cache directory (hash_high): {}", file_path.string());
        return;
    }
    UInt64 high = unhexUInt<UInt64>(hash_high_dir.data());

    // Build full key matching UInt128{high, low} as returned by unhexKey
    UInt128 key = {high, low};

    // Parse timestamp from partition_id (e.g., "20240315")
    time_t part_ts = 0;
    if (partition_dir.size() >= 8 && std::all_of(partition_dir.begin(), partition_dir.end(), ::isdigit))
    {
        try
        {
            int year = std::stoi(partition_dir.substr(0, 4));
            int month = std::stoi(partition_dir.substr(4, 2));
            int day = std::stoi(partition_dir.substr(6, 2));

            struct tm tm_info = {};
            tm_info.tm_year = year - 1900;
            tm_info.tm_mon = month - 1;
            tm_info.tm_mday = day;
            tm_info.tm_isdst = -1;

            part_ts = mktime(&tm_info);
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to parse partition timestamp from: {}", partition_dir);
        }
    }

    // Skip expired or non-time-based segments; delete the stale file so it
    // doesn't accumulate on disk across restarts.
    if (!disk_cache.shouldCache(part_ts))
    {
        disk->removeFileIfExists(file_path);
        return;
    }

    String file_path_str = file_path.string();
    {
        auto & shard = disk_cache.getShard(high);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto meta = std::make_shared<DiskCacheTTLMeta>(
            DiskCacheTTLMeta::State::Cached, disk, file_size, time(nullptr), part_ts, std::move(file_path_str)
        );
        disk_cache.cacheInsertLocked(shard, key, meta, partition_dir);
        disk_cache.total_entries++;
        disk_cache.total_size += file_size;
        DiskCacheFactory::instance().addGlobalTTLUsage(file_size);
    }

    // Update partition stats outside shard mutex to avoid lock ordering with partition_stats_mutex
    disk_cache.addToPartitionStats(partition_dir, part_ts, file_size);
    disk_cache.cache_stats.cached_from_restored++;
    disk_cache.cache_stats.cached_bytes_restored += file_size;

    total_loaded++;
}

// DiskCacheMigrator (stub)
DiskCacheTTL::DiskCacheMigrator::DiskCacheMigrator(
    DiskCacheTTL & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_)
    : DiskIterator("DiskCacheTTLMigrator", cache_, disk_, worker_per_disk_, min_depth_parallel_, max_depth_parallel_)
{
}

DiskCacheTTL::DiskCacheMigrator::~DiskCacheMigrator()
{
}

void DiskCacheTTL::DiskCacheMigrator::iterateFile(std::filesystem::path, size_t)
{
}

// DiskCacheDeleter (stub)
DiskCacheTTL::DiskCacheDeleter::DiskCacheDeleter(
    DiskCacheTTL & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_)
    : DiskIterator("DiskCacheTTLDeleter", cache_, disk_, worker_per_disk_, min_depth_parallel_, max_depth_parallel_)
{
}

DiskCacheTTL::DiskCacheDeleter::~DiskCacheDeleter()
{
}

void DiskCacheTTL::DiskCacheDeleter::exec(std::filesystem::path entry_path)
{
    disk->removeRecursive(entry_path);
}

void DiskCacheTTL::DiskCacheDeleter::iterateFile(std::filesystem::path, size_t)
{
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
    std::vector<PartitionStats> result;
    std::shared_lock<std::shared_mutex> lock(cache_stats.partition_stats_mutex);
    result.reserve(cache_stats.partition_stats.size());
    for (const auto & [partition_id, internal_stats] : cache_stats.partition_stats)
    {
        PartitionStats snapshot;
        snapshot.partition_id = internal_stats.partition_id;
        snapshot.entry_count = internal_stats.entry_count.load();
        snapshot.total_bytes = internal_stats.total_bytes.load();
        snapshot.partition_timestamp = internal_stats.partition_timestamp;
        result.push_back(snapshot);
    }
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

