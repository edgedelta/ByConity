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

#include <filesystem>
#include <gtest/gtest.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>

namespace fs = std::filesystem;

namespace DB
{

class DiskCacheTTLTest : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
        Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
        Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));
        Poco::Logger::root().setLevel("trace");
        Poco::Logger::root().setChannel(channel);

        ctx = getContext().context;
    }

    static void TearDownTestCase()
    {
        ctx->shutdown();
    }

    void SetUp() override
    {
        fs::remove_all("tmp/");
        fs::create_directories("tmp/");
        fs::create_directory("tmp/ttl_cache/");
        UnitTest::initLogger();
        DB::IDiskCache::init(*getContext().context);
    }

    void TearDown() override
    {
        fs::remove_all("tmp/");
        DB::IDiskCache::close();
    }

    VolumePtr createTestVolume()
    {
        fs::create_directory("tmp/ttl_disk/");
        auto disk = std::make_shared<DiskLocal>("ttl_disk", "tmp/ttl_disk/", 0);
        return std::make_shared<SingleDiskVolume>("ttl_volume", std::move(disk), 0);
    }

    static std::shared_ptr<Context> ctx;
};

std::shared_ptr<Context> DiskCacheTTLTest::ctx = nullptr;

// Test parsing partition timestamps from part names
TEST_F(DiskCacheTTLTest, ParsePartitionTimestamp)
{
    // YYYYMMDD format (20240315)
    {
        String part_name = "20240315_1_100_2";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(part_name);
        ASSERT_GT(ts, 0);

        struct tm tm_time;
        gmtime_r(&ts, &tm_time);
        ASSERT_EQ(tm_time.tm_year + 1900, 2024);
        ASSERT_EQ(tm_time.tm_mon + 1, 3);
        ASSERT_EQ(tm_time.tm_mday, 15);
    }

    // YYYYMMDDHH format (2024031523)
    {
        String part_name = "2024031523_1_100_2";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(part_name);
        ASSERT_GT(ts, 0);

        struct tm tm_time;
        gmtime_r(&ts, &tm_time);
        ASSERT_EQ(tm_time.tm_year + 1900, 2024);
        ASSERT_EQ(tm_time.tm_mon + 1, 3);
        ASSERT_EQ(tm_time.tm_mday, 15);
        ASSERT_EQ(tm_time.tm_hour, 23);
    }

    // YYYYMM format (202403)
    {
        String part_name = "202403_1_100_2";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(part_name);
        ASSERT_GT(ts, 0);

        struct tm tm_time;
        gmtime_r(&ts, &tm_time);
        ASSERT_EQ(tm_time.tm_year + 1900, 2024);
        ASSERT_EQ(tm_time.tm_mon + 1, 3);
        ASSERT_EQ(tm_time.tm_mday, 1);
    }

    // Non-time partition (string partition)
    {
        String part_name = "some_partition_1_100_2";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(part_name);
        ASSERT_EQ(ts, 0);
    }

    // Invalid format
    {
        String part_name = "999_1_100_2";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(part_name);
        ASSERT_EQ(ts, 0);
    }

    // Empty partition
    {
        String part_name = "_1_100_2";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(part_name);
        ASSERT_EQ(ts, 0);
    }
}

// Test shouldCache decision logic
TEST_F(DiskCacheTTLTest, ShouldCacheLogic)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache("test_ttl", volume, nullptr, settings, strategy, ttl_minutes);

    time_t now = time(nullptr);

    // Recent partition (30 minutes old) - should cache
    {
        time_t recent_ts = now - (30 * 60);
        ASSERT_TRUE(cache.shouldCache(recent_ts));
    }

    // Old partition (2 hours old) - should not cache
    {
        time_t old_ts = now - (2 * 60 * 60);
        ASSERT_FALSE(cache.shouldCache(old_ts));
    }

    // Exact TTL boundary - should cache
    {
        time_t boundary_ts = now - (60 * 60);
        ASSERT_TRUE(cache.shouldCache(boundary_ts));
    }

    // Just outside TTL - should not cache
    {
        time_t outside_ts = now - (60 * 60 + 1);
        ASSERT_FALSE(cache.shouldCache(outside_ts));
    }

    // Zero part_ts (non-time partition) - always cache
    {
        ASSERT_TRUE(cache.shouldCache(0));
    }

    // Future timestamp - should cache
    {
        time_t future_ts = now + (30 * 60);
        ASSERT_TRUE(cache.shouldCache(future_ts));
    }
}

// Test TTL disabled (ttl_minutes = 0)
TEST_F(DiskCacheTTLTest, TTLDisabled)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 0; // TTL disabled
    DiskCacheTTL cache("test_no_ttl", volume, nullptr, settings, strategy, ttl_minutes);

    time_t now = time(nullptr);

    // All partitions should be cached when TTL is disabled
    ASSERT_TRUE(cache.shouldCache(now));
    ASSERT_TRUE(cache.shouldCache(now - (365 * 24 * 60 * 60))); // 1 year old
    ASSERT_TRUE(cache.shouldCache(0));
    ASSERT_TRUE(cache.shouldCache(now + (30 * 60)));
}

// Test basic set/get operations with TTL filtering
TEST_F(DiskCacheTTLTest, BasicOperations)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache("test_basic", volume, nullptr, settings, strategy, ttl_minutes);

    time_t now = time(nullptr);

    // Create recent partition name (should be cached)
    struct tm tm_recent;
    gmtime_r(&now, &tm_recent);
    String recent_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_recent.tm_year + 1900, tm_recent.tm_mon + 1, tm_recent.tm_mday);

    // Create old partition name (should not be cached)
    time_t old_time = now - (2 * 60 * 60); // 2 hours ago
    struct tm tm_old;
    gmtime_r(&old_time, &tm_old);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);

    // Try to set recent part - should succeed
    {
        IDiskCacheSegmentsVector segments;
        auto seg = std::make_shared<RemoteDiskCacheSegment>(
            recent_part, "offset_123", 0, 1024, IDiskCache::DataType::DATA);
        segments.push_back(seg);

        size_t cached = cache.cacheSegmentsToLocalDisk(segments);
        ASSERT_GT(cached, 0);
    }

    // Try to set old part - should be rejected
    {
        IDiskCacheSegmentsVector segments;
        auto seg = std::make_shared<RemoteDiskCacheSegment>(
            old_part, "offset_456", 0, 1024, IDiskCache::DataType::DATA);
        segments.push_back(seg);

        size_t cached = cache.cacheSegmentsToLocalDisk(segments);
        ASSERT_EQ(cached, 0);
    }

    // Recent part should be in cache
    {
        auto [disk, path] = cache.get(recent_part);
        ASSERT_FALSE(path.empty());
    }

    // Old part should not be in cache
    {
        auto [disk, path] = cache.get(old_part);
        ASSERT_TRUE(path.empty());
    }
}

// Test eviction of expired entries
TEST_F(DiskCacheTTLTest, EvictExpired)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache("test_evict", volume, nullptr, settings, strategy, ttl_minutes);

    // Manually add entries to cache with different ages
    time_t now = time(nullptr);

    // Add recent entry (should survive eviction)
    String recent_key = "recent_part";
    time_t recent_ts = now - (30 * 60); // 30 minutes ago
    {
        std::unique_lock lock(cache.cache_mutex);
        auto meta = std::make_shared<DiskCacheTTL::DiskCacheTTLMeta>();
        meta->partition_timestamp = recent_ts;
        meta->size = 1024;
        cache.cache_map[recent_key] = meta;
        cache.cache_stats.updateCacheSize(1024);
    }

    // Add old entry (should be evicted)
    String old_key = "old_part";
    time_t old_ts = now - (2 * 60 * 60); // 2 hours ago
    {
        std::unique_lock lock(cache.cache_mutex);
        auto meta = std::make_shared<DiskCacheTTL::DiskCacheTTLMeta>();
        meta->partition_timestamp = old_ts;
        meta->size = 1024;
        cache.cache_map[old_key] = meta;
        cache.cache_stats.updateCacheSize(1024);
    }

    // Add non-time partition entry (should survive eviction)
    String nontime_key = "nontime_part";
    {
        std::unique_lock lock(cache.cache_mutex);
        auto meta = std::make_shared<DiskCacheTTL::DiskCacheTTLMeta>();
        meta->partition_timestamp = 0;
        meta->size = 1024;
        cache.cache_map[nontime_key] = meta;
        cache.cache_stats.updateCacheSize(1024);
    }

    // Verify all entries exist
    ASSERT_EQ(cache.cache_map.size(), 3);

    // Run eviction
    cache.evictExpired();

    // Verify old entry was evicted
    {
        std::unique_lock lock(cache.cache_mutex);
        ASSERT_EQ(cache.cache_map.count(recent_key), 1);
        ASSERT_EQ(cache.cache_map.count(old_key), 0);
        ASSERT_EQ(cache.cache_map.count(nontime_key), 1);
        ASSERT_EQ(cache.cache_map.size(), 2);
    }
}

// Test periodic eviction check (hourly)
TEST_F(DiskCacheTTLTest, PeriodicEvictionCheck)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test_periodic", volume, nullptr, settings, strategy, ttl_minutes);

    time_t initial_check = cache.last_eviction_check.load();
    ASSERT_EQ(initial_check, 0);

    // First set should trigger eviction check
    {
        IDiskCacheSegmentsVector segments;
        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

        auto seg = std::make_shared<RemoteDiskCacheSegment>(
            part, "offset_1", 0, 1024, IDiskCache::DataType::DATA);
        segments.push_back(seg);

        cache.cacheSegmentsToLocalDisk(segments);

        time_t after_first_check = cache.last_eviction_check.load();
        ASSERT_GT(after_first_check, initial_check);
    }

    // Subsequent sets within same hour should not trigger eviction
    time_t first_check_time = cache.last_eviction_check.load();
    {
        IDiskCacheSegmentsVector segments;
        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_2_200_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

        auto seg = std::make_shared<RemoteDiskCacheSegment>(
            part, "offset_2", 0, 1024, IDiskCache::DataType::DATA);
        segments.push_back(seg);

        cache.cacheSegmentsToLocalDisk(segments);

        time_t after_second_set = cache.last_eviction_check.load();
        ASSERT_EQ(after_second_set, first_check_time);
    }

    // Manually advance time and verify eviction check runs
    cache.last_eviction_check.store(time(nullptr) - 3601); // Over 1 hour ago
    {
        IDiskCacheSegmentsVector segments;
        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_3_300_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

        auto seg = std::make_shared<RemoteDiskCacheSegment>(
            part, "offset_3", 0, 1024, IDiskCache::DataType::DATA);
        segments.push_back(seg);

        cache.cacheSegmentsToLocalDisk(segments);

        time_t after_hour_passed = cache.last_eviction_check.load();
        ASSERT_GT(after_hour_passed, first_check_time);
    }
}

} // namespace DB
