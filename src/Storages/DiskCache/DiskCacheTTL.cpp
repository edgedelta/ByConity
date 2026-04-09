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
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <fmt/core.h>
#include <sys/stat.h>
#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
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
static constexpr auto TMP_SUFFIX_LEN = std::char_traits<char>::length(DISK_CACHE_TEMP_FILE_SUFFIX);
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

    UInt64 unhex16(const char * data)
    {
        UInt64 res = 0;
        for (size_t i = 0; i < sizeof(UInt64) * 2; ++i, ++data)
        {
            res <<= 4;
            res += static_cast<UInt64>(unhex(*data));
        }
        return res;
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

    auto & thread_pool = IDiskCache::getThreadPool();
    thread_pool.scheduleOrThrowOnError([this] { load(); });
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

    auto low = unhex16(hex_key.data());
    auto high = unhex16(hex_key.data() + HEX_KEY_LEN / 2);

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

    // TTL disabled - cache all time-based partitions
    if (ttl_minutes == 0)
        return true;

    time_t now = time(nullptr);
    time_t age_seconds = now - part_ts;
    time_t ttl_seconds = ttl_minutes * 60;

    return age_seconds <= ttl_seconds;
}

void DiskCacheTTL::set(const String& seg_name, ReadBuffer& value, size_t weight_hint, bool is_preload, time_t min_time, time_t max_time)
{
    if (is_droping)
    {
        LOG_WARNING(log, fmt::format("skip write disk cache for droping disk cache is running"));
        return;
    }

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

    // Async size-based eviction: if cache >90% full, schedule background cleanup
    if (max_size_bytes > 0 && total_size.load() > max_size_bytes * 0.90)
    {
        time_t now = time(nullptr);
        time_t last_trigger = last_size_eviction_trigger.load();

        // Rate limit: at most once per minute
        if (now - last_trigger > 60)
        {
            if (last_size_eviction_trigger.compare_exchange_strong(last_trigger, now))
            {
                size_t target_free = max_size_bytes * 0.10;  // Free 10%
                cache_stats.async_eviction_triggered++;

                LOG_DEBUG(log, "Cache {}% full, scheduling async eviction to free {} bytes",
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

    auto key = hash(seg_name);

    // Check if already exists
    {
        std::lock_guard<std::mutex> lock(cache_mutex);
        if (cache_map.find(key) != cache_map.end())
            return;

        // Reserve slot
        cache_map[key] = std::make_shared<DiskCacheTTLMeta>(
            DiskCacheTTLMeta::State::Caching, nullptr, 0, time(nullptr), part_ts
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

        size_t weight = writeSegment(seg_name, value, reserved_space);
        ProfileEvents::increment(ProfileEvents::DiskCacheSetTotalBytes, weight, Metrics::MetricType::Rate, {{"type", (is_preload ? "preload": "query")}});

        // Update to Cached state
        {
            std::lock_guard<std::mutex> lock(cache_mutex);
            cache_map[key] = std::make_shared<DiskCacheTTLMeta>(
                DiskCacheTTLMeta::State::Cached, reserved_space->getDisk(), weight, time(nullptr), part_ts
            );
            total_entries++;
            total_size += weight;

            cache_stats.total_entries++;
            cache_stats.total_bytes += weight;

            // Track write source (preload vs query)
            if (is_preload)
            {
                cache_stats.cached_from_preload++;
                cache_stats.cached_bytes_preload += weight;
            }
            else
            {
                cache_stats.cached_from_query++;
                cache_stats.cached_bytes_query += weight;
            }
        }

        // Update partition stats
        String part_name = extractPartName(seg_name);
        String partition_id = extractPartitionId(part_name);
        updatePartitionStats(partition_id, part_ts, false, weight);
    }
    catch(const Exception & e)
    {
        String local_disk_path = reserved_space == nullptr ? "" : reserved_space->getDisk()->getPath();
        tryLogCurrentException(log, fmt::format("Failed to write key {} "
            "to local, disk path: {}, weight: {}, fail: {}", seg_name, local_disk_path, weight_hint, e.message()));

        std::lock_guard<std::mutex> lock(cache_mutex);
        cache_map.erase(key);
    }
}

std::pair<DiskPtr, String> DiskCacheTTL::get(const String & seg_name)
{
    ProfileEvents::increment(ProfileEvents::DiskCacheGetTotalOps);
    Stopwatch watch;
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::DiskCacheGetMetaMicroSeconds,
        watch.elapsedMicroseconds());});

    // Periodic eviction check (every hour)
    time_t now = time(nullptr);
    time_t last_check = last_eviction_check.load();
    if (now - last_check > 3600)
    {
        if (last_eviction_check.compare_exchange_strong(last_check, now))
        {
            // Trigger eviction asynchronously
            auto & thread_pool = IDiskCache::getEvictPool();
            thread_pool.scheduleOrThrow([this] { evictExpired(); });
        }
    }

    auto key = hash(seg_name);

    String part_name = extractPartName(seg_name);
    String partition_id = extractPartitionId(part_name);

    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache_map.find(key);
    if (it == cache_map.end() || it->second->state != DiskCacheTTLMeta::State::Cached)
    {
        updatePartitionStats(partition_id, 0, false, 0);
        return {};
    }

    if (unlikely(it->second->disk == nullptr))
    {
        cache_map.erase(it);
        updatePartitionStats(partition_id, 0, false, 0);
        return {};
    }

    // Check TTL on read
    time_t part_ts = it->second->max_timestamp;
    if (!shouldCache(part_ts))
    {
        // Expired, return miss
        updatePartitionStats(partition_id, part_ts, false, 0);
        return {};
    }

    updatePartitionStats(partition_id, part_ts, true, 0);
    return {it->second->disk, getRelativePath(key, seg_name)};
}

size_t DiskCacheTTL::writeSegment(const String& seg_key, ReadBuffer& buffer, ReservationPtr& reservation)
{
    DiskPtr disk = reservation->getDisk();
    String cache_rel_path = getRelativePath(hash(seg_key), seg_key);
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
    // Group segments by part (using hash_high) for efficient batch eviction
    std::map<UInt64, std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheTTLMeta>>>> parts;

    {
        std::lock_guard<std::mutex> lock(cache_mutex);
        for (const auto & [key, meta] : cache_map)
        {
            if (meta->state == DiskCacheTTLMeta::State::Cached)
            {
                // Group by part: hash_high uniquely identifies the part
                UInt64 part_hash = key.items[0];
                parts[part_hash].push_back({key, meta});
            }
        }
    }

    // Check TTL once per part (all segments in same part share same max_timestamp)
    std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheTTLMeta>>> to_evict;

    for (const auto & [part_hash, segments] : parts)
    {
        if (!segments.empty() && !shouldCache(segments[0].second->max_timestamp))
        {
            // Entire part is expired - evict all its segments
            to_evict.insert(to_evict.end(), segments.begin(), segments.end());
        }
    }

    // Remove from cache_map
    {
        std::lock_guard<std::mutex> lock(cache_mutex);
        for (const auto & [key, meta] : to_evict)
        {
            auto it = cache_map.find(key);
            if (it != cache_map.end())
            {
                total_size -= it->second->size;
                total_entries--;
                cache_map.erase(it);
            }
        }
    }

    // Delete files
    for (const auto & [key, meta] : to_evict)
    {
        try
        {
            // Build path from key + partition timestamp
            String hex_key = hexKey(key);
            std::string_view view(hex_key);
            std::string_view hex_key_low = view.substr(0, HEX_KEY_LEN / 2);
            std::string_view hex_key_high = view.substr(HEX_KEY_LEN / 2, HEX_KEY_LEN);

            // Convert timestamp to partition_id
            struct tm tm_time;
            gmtime_r(&meta->max_timestamp, &tm_time);
            String partition_id = fmt::format("{:04d}{:02d}{:02d}",
                tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday);

            String prefix = type == IDiskCache::DataType::META ? META_DISK_CACHE_DIR_PREFIX : DATA_DISK_CACHE_DIR_PREFIX;

            // Structure: prefix/uuid/partition/3char/hash_high/hash_low
            auto rel_path = fs::path(latest_disk_cache_dir) / prefix / table_uuid / partition_id
                           / hex_key_high.substr(0, 3) / hex_key_high / hex_key_low;

            if (meta->disk && meta->disk->exists(rel_path))
            {
                meta->disk->removeFile(rel_path);
                LOG_TRACE(log, "Evicted expired segment: {}", rel_path.string());
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to evict expired segment");
        }
    }

    if (!to_evict.empty())
    {
        size_t evicted_count = to_evict.size();
        size_t evicted_bytes = 0;
        for (const auto & [key, meta] : to_evict)
            evicted_bytes += meta->size;

        cache_stats.evicted_expired += evicted_count;
        cache_stats.total_entries -= evicted_count;
        cache_stats.total_bytes -= evicted_bytes;

        LOG_INFO(log, "Evicted {} expired segments, freed {} bytes", evicted_count, evicted_bytes);
    }

    cache_stats.last_eviction_run = time(nullptr);
}

void DiskCacheTTL::evictOldestPartitionsUntilSpace(size_t needed_bytes)
{
    // Check if eviction needed
    if (max_size_bytes == 0 || total_size.load() + needed_bytes <= max_size_bytes)
        return;

    size_t target_size = max_size_bytes > needed_bytes ? max_size_bytes - needed_bytes : 0;

    LOG_DEBUG(log, "Size limit reached: current={}, needed={}, max={}, target={}",
              total_size.load(), needed_bytes, max_size_bytes, target_size);

    // 1. Sort partitions by timestamp (oldest first) - only sort 10-100 partitions
    std::vector<std::pair<time_t, String>> sorted_partitions;
    {
        std::shared_lock<std::shared_mutex> stats_lock(cache_stats.partition_stats_mutex);
        for (const auto & [partition_id, stats] : cache_stats.partition_stats)
        {
            if (stats.total_bytes > 0)
                sorted_partitions.emplace_back(stats.partition_timestamp, partition_id);
        }
    }
    std::sort(sorted_partitions.begin(), sorted_partitions.end());

    // 2. Evict parts from oldest partitions until target reached
    std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheTTLMeta>>> to_evict;
    size_t evicted_bytes = 0;

    for (const auto & [partition_ts, partition_id] : sorted_partitions)
    {
        if (total_size.load() - evicted_bytes <= target_size)
            break;  // Early exit - freed enough space

        // Collect all parts in this partition, grouped by part
        std::map<UInt64, std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheTTLMeta>>>> parts_in_partition;

        {
            std::lock_guard<std::mutex> lock(cache_mutex);
            for (auto it = cache_map.begin(); it != cache_map.end(); ++it)
            {
                if (it->second->state == DiskCacheTTLMeta::State::Cached
                    && it->second->max_timestamp == partition_ts)
                {
                    // Group by part: hash_high = hash(part_name)
                    UInt64 part_hash_high = it->first.items[0];
                    parts_in_partition[part_hash_high].push_back(*it);
                }
            }
        }

        // Evict parts one by one from this partition until enough space
        for (auto & [part_hash_high, segments] : parts_in_partition)
        {
            if (total_size.load() - evicted_bytes <= target_size)
                break;

            size_t part_bytes = 0;

            // Evict all segments in this part
            {
                std::lock_guard<std::mutex> lock(cache_mutex);
                for (const auto & [key, meta] : segments)
                {
                    to_evict.push_back({key, meta});
                    evicted_bytes += meta->size;
                    part_bytes += meta->size;
                    total_size -= meta->size;
                    total_entries--;
                    cache_map.erase(key);
                }
            }

            LOG_DEBUG(log, "Evicted part (hash_high={}) from partition {} for size limit, freed {} bytes",
                     part_hash_high, partition_id, part_bytes);
        }
    }

    // Delete files
    for (const auto & [key, meta] : to_evict)
    {
        try
        {
            String hex_key = hexKey(key);
            std::string_view view(hex_key);
            std::string_view hex_key_low = view.substr(0, HEX_KEY_LEN / 2);
            std::string_view hex_key_high = view.substr(HEX_KEY_LEN / 2, HEX_KEY_LEN);

            struct tm tm_time;
            gmtime_r(&meta->max_timestamp, &tm_time);
            String partition_id = fmt::format("{:04d}{:02d}{:02d}",
                tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday);

            String prefix = type == IDiskCache::DataType::META ? META_DISK_CACHE_DIR_PREFIX : DATA_DISK_CACHE_DIR_PREFIX;

            auto rel_path = fs::path(latest_disk_cache_dir) / prefix / table_uuid / partition_id
                           / hex_key_high.substr(0, 3) / hex_key_high / hex_key_low;

            if (meta->disk && meta->disk->exists(rel_path))
                meta->disk->removeFile(rel_path);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to evict segment for size limit");
        }
    }

    if (!to_evict.empty())
    {
        cache_stats.evicted_size_limit += to_evict.size();
        cache_stats.total_entries -= to_evict.size();
        cache_stats.total_bytes -= evicted_bytes;

        LOG_INFO(log, "Evicted {} segments from oldest parts for size limit, freed {} bytes",
                 to_evict.size(), evicted_bytes);
    }
}

void DiskCacheTTL::load()
{
    LOG_INFO(log, "Loading TTL disk cache from disk...");

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

    // Note: We don't clean up cache_map here (same as LRU behavior)
    // - cache_map stores hash keys, can't efficiently reverse-match to part_name
    // - Stale entries are harmless: get() returns path, caller handles missing files gracefully
    // - evictExpired() will eventually remove stale entries based on disk existence checks
    // - Memory overhead is negligible compared to I/O cost of scanning cache_map

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

    // Validate hash_low filename
    auto key_low = DiskCacheTTL::unhexKey(filename);
    if (!key_low.has_value())
    {
        LOG_WARNING(log, "Invalid cache file (hash_low): {}", file_path.string());
        return;
    }

    // New structure: data/uuid/partition/3char/hash_high/hash_low
    // Extract partition from path hierarchy
    auto hash_high_dir = file_path.parent_path().filename().string();  // hash_high
    auto partition_dir = file_path.parent_path().parent_path().parent_path().filename().string();  // partition_id

    // Validate hash_high
    auto key_high = DiskCacheTTL::unhexKey(hash_high_dir);
    if (!key_high.has_value())
    {
        LOG_WARNING(log, "Invalid cache directory (hash_high): {}", file_path.string());
        return;
    }

    // Build full key
    UInt128 key = {key_high.value(), key_low.value()};

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

    std::lock_guard<std::mutex> lock(disk_cache.cache_mutex);
    disk_cache.cache_map[key] = std::make_shared<DiskCacheTTLMeta>(
        DiskCacheTTLMeta::State::Cached, disk, file_size, time(nullptr), part_ts
    );
    disk_cache.total_entries++;
    disk_cache.total_size += file_size;
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
    stats.total_entries = cache_stats.total_entries.load();
    stats.total_bytes = cache_stats.total_bytes.load();
    stats.evicted_expired = cache_stats.evicted_expired.load();
    stats.evicted_size_limit = cache_stats.evicted_size_limit.load();
    stats.rejected_non_time_partition = cache_stats.rejected_non_time_partition.load();
    stats.rejected_too_old = cache_stats.rejected_too_old.load();
    stats.last_eviction_run = cache_stats.last_eviction_run.load();
    stats.async_eviction_triggered = cache_stats.async_eviction_triggered.load();
    stats.async_eviction_skipped_rate_limit = cache_stats.async_eviction_skipped_rate_limit.load();
    return stats;
}

std::vector<DiskCacheTTL::PartitionStats> DiskCacheTTL::getPartitionStats() const
{
    std::vector<PartitionStats> result;
    std::shared_lock<std::shared_mutex> lock(cache_stats.partition_stats_mutex);
    result.reserve(cache_stats.partition_stats.size());
    for (const auto & [partition_id, stats] : cache_stats.partition_stats)
    {
        result.push_back(stats);
    }
    return result;
}

void DiskCacheTTL::updatePartitionStats(const String & partition_id, time_t partition_ts, bool hit, size_t bytes)
{
    std::unique_lock<std::shared_mutex> lock(cache_stats.partition_stats_mutex);
    auto & pstats = cache_stats.partition_stats[partition_id];

    if (pstats.partition_id.empty())
    {
        pstats.partition_id = partition_id;
        pstats.partition_timestamp = partition_ts;
    }

    if (hit)
        pstats.hits++;
    else
        pstats.misses++;

    if (bytes > 0)
    {
        pstats.entry_count++;
        pstats.total_bytes += bytes;
    }
}

}
