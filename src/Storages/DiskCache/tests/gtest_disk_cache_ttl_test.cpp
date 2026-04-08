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
#include <thread>
#include <atomic>
#include <gtest/gtest.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/VolumeJBOD.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <IO/ReadBufferFromString.h>
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

    VolumePtr createDualDiskVolume()
    {
        fs::create_directory("tmp/ttl_disk1/");
        fs::create_directory("tmp/ttl_disk2/");
        Disks disks;
        disks.emplace_back(std::make_shared<DiskLocal>("ttl_disk1", "tmp/ttl_disk1/", DiskStats{}));
        disks.emplace_back(std::make_shared<DiskLocal>("ttl_disk2", "tmp/ttl_disk2/", DiskStats{}));
        return std::make_shared<VolumeJBOD>("ttl_dual_volume", disks, disks.front()->getName(), 0, false);
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
    DiskCacheTTL cache("test_ttl", "test-uuid-0000-0000-0000-000000000001", volume, nullptr, settings, strategy, ttl_minutes);

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

    // Zero part_ts (non-time partition) - never cache
    {
        ASSERT_FALSE(cache.shouldCache(0));
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
    DiskCacheTTL cache("test_no_ttl", "test-uuid-0000-0000-0000-000000000002", volume, nullptr, settings, strategy, ttl_minutes);

    time_t now = time(nullptr);

    // When TTL is disabled, all time-based partitions should be cached
    ASSERT_TRUE(cache.shouldCache(now));
    ASSERT_TRUE(cache.shouldCache(now - (365 * 24 * 60 * 60))); // 1 year old
    ASSERT_TRUE(cache.shouldCache(now + (30 * 60))); // future
    // Non-time partitions still rejected even with TTL disabled
    ASSERT_FALSE(cache.shouldCache(0));
}

// Test non-time partitions are rejected
TEST_F(DiskCacheTTLTest, RejectNonTimePartitions)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test_nontime", "test-uuid-0000-0000-0000-000000000003", volume, nullptr, settings, strategy, ttl_minutes);

    // String partition (non-time)
    String nontime_part = "string_partition_1_100_2";
    String nontime_seg = fmt::format("test_uuid/{}/column.bin/offset_0", nontime_part);

    // Numeric but invalid date partition
    String invalid_part = "999_1_100_2";
    String invalid_seg = fmt::format("test_uuid/{}/column.bin/offset_1", invalid_part);

    // Try to cache non-time partitions - should be rejected
    {
        String data = "test data";
        ReadBufferFromString buf1(data);
        ReadBufferFromString buf2(data);
        cache.set(nontime_seg, buf1, data.size(), false);
        cache.set(invalid_seg, buf2, data.size(), false);

        // Should not be cached
        auto [disk1, path1] = cache.get(nontime_seg);
        auto [disk2, path2] = cache.get(invalid_seg);
        ASSERT_TRUE(path1.empty());
        ASSERT_TRUE(path2.empty());
    }

    ASSERT_EQ(cache.getKeyCount(), 0);
}

// Test basic set/get operations with TTL filtering
TEST_F(DiskCacheTTLTest, BasicOperations)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache("test_basic", "test-uuid-0000-0000-0000-000000000004", volume, nullptr, settings, strategy, ttl_minutes);

    time_t now = time(nullptr);

    // Create recent segment name (should be cached)
    struct tm tm_recent;
    gmtime_r(&now, &tm_recent);
    String recent_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_recent.tm_year + 1900, tm_recent.tm_mon + 1, tm_recent.tm_mday);
    String recent_seg = fmt::format("test_uuid/{}/column.bin/offset_123", recent_part);

    // Create old segment name (should not be cached)
    time_t old_time = now - (2 * 60 * 60); // 2 hours ago
    struct tm tm_old;
    gmtime_r(&old_time, &tm_old);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);
    String old_seg = fmt::format("test_uuid/{}/column.bin/offset_456", old_part);

    // Try to set recent segment - should succeed
    {
        String test_data = "test data content";
        ReadBufferFromString buffer(test_data);
        cache.set(recent_seg, buffer, test_data.size(), false);

        auto [disk, path] = cache.get(recent_seg);
        ASSERT_FALSE(path.empty());
        ASSERT_TRUE(disk != nullptr);
    }

    // Try to set old segment - should be rejected (not cached due to TTL)
    {
        String test_data = "old data content";
        ReadBufferFromString buffer(test_data);
        cache.set(old_seg, buffer, test_data.size(), false);

        auto [disk, path] = cache.get(old_seg);
        ASSERT_TRUE(path.empty()); // Should not be cached
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
    DiskCacheTTL cache("test_evict", "test-uuid-0000-0000-0000-000000000005", volume, nullptr, settings, strategy, ttl_minutes);

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

    // Verify both entries exist
    ASSERT_EQ(cache.cache_map.size(), 2);

    // Run eviction
    cache.evictExpired();

    // Verify old entry was evicted, recent remains
    {
        std::unique_lock lock(cache.cache_mutex);
        ASSERT_EQ(cache.cache_map.count(recent_key), 1);
        ASSERT_EQ(cache.cache_map.count(old_key), 0);
        ASSERT_EQ(cache.cache_map.size(), 1);
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
    DiskCacheTTL cache("test_periodic", "test-uuid-0000-0000-0000-000000000006", volume, nullptr, settings, strategy, ttl_minutes);

    time_t initial_check = cache.last_eviction_check.load();
    ASSERT_EQ(initial_check, 0);

    // First set should trigger eviction check
    {
        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
        String seg_name = fmt::format("test_uuid/{}/column.bin/offset_1", part);

        String test_data = "test data 1";
        ReadBufferFromString buffer(test_data);
        cache.set(seg_name, buffer, test_data.size(), false);

        time_t after_first_check = cache.last_eviction_check.load();
        ASSERT_GT(after_first_check, initial_check);
    }

    // Subsequent sets within same hour should not trigger eviction
    time_t first_check_time = cache.last_eviction_check.load();
    {
        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_2_200_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
        String seg_name = fmt::format("test_uuid/{}/column.bin/offset_2", part);

        String test_data = "test data 2";
        ReadBufferFromString buffer(test_data);
        cache.set(seg_name, buffer, test_data.size(), false);

        time_t after_second_set = cache.last_eviction_check.load();
        ASSERT_EQ(after_second_set, first_check_time);
    }

    // Manually advance time and verify eviction check runs
    cache.last_eviction_check.store(time(nullptr) - 3601); // Over 1 hour ago
    {
        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_3_300_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
        String seg_name = fmt::format("test_uuid/{}/column.bin/offset_3", part);

        String test_data = "test data 3";
        ReadBufferFromString buffer(test_data);
        cache.set(seg_name, buffer, test_data.size(), false);

        time_t after_hour_passed = cache.last_eviction_check.load();
        ASSERT_GT(after_hour_passed, first_check_time);
    }
}

// Test concurrent set/get operations
TEST_F(DiskCacheTTLTest, ConcurrentAccess)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 10 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test_concurrent", "test-uuid-0000-0000-0000-000000000007", volume, nullptr, settings, strategy, ttl_minutes);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Multiple threads writing different segments
    for (int i = 0; i < 10; i++)
    {
        threads.emplace_back([&, i]() {
            String seg_name = fmt::format("test_uuid/{}/col.bin/offset_{}", part, i);
            String data = fmt::format("data_{}", i);
            ReadBufferFromString buffer(data);
            cache.set(seg_name, buffer, data.size(), false);

            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            auto [disk, path] = cache.get(seg_name);
            if (!path.empty())
                success_count++;
        });
    }

    for (auto& t : threads)
        t.join();

    ASSERT_EQ(success_count.load(), 10);
    ASSERT_EQ(cache.getKeyCount(), 10);
}

// Test drop() method removes part segments
TEST_F(DiskCacheTTLTest, DropPart)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test_drop", "test-uuid-0000-0000-0000-000000000008", volume, nullptr, settings, strategy, ttl_minutes);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part1 = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
    String part2 = fmt::format("{:04d}{:02d}{:02d}_2_200_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    // Add segments for two parts
    for (int i = 0; i < 3; i++)
    {
        String seg1 = fmt::format("test_uuid/{}/col.bin/offset_{}", part1, i);
        String seg2 = fmt::format("test_uuid/{}/col.bin/offset_{}", part2, i);

        String data = "test data";
        ReadBufferFromString buf1(data);
        ReadBufferFromString buf2(data);
        cache.set(seg1, buf1, data.size(), false);
        cache.set(seg2, buf2, data.size(), false);
    }

    size_t initial_count = cache.getKeyCount();
    ASSERT_EQ(initial_count, 6);

    // Drop part1
    size_t dropped = cache.drop(part1);
    ASSERT_EQ(dropped, 3);
    ASSERT_EQ(cache.getKeyCount(), 3);

    // Verify part1 gone, part2 remains
    String seg1_check = fmt::format("test_uuid/{}/col.bin/offset_0", part1);
    String seg2_check = fmt::format("test_uuid/{}/col.bin/offset_0", part2);

    auto [disk1, path1] = cache.get(seg1_check);
    auto [disk2, path2] = cache.get(seg2_check);

    ASSERT_TRUE(path1.empty());
    ASSERT_FALSE(path2.empty());
}

// Test cache stats
TEST_F(DiskCacheTTLTest, CacheStats)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test_stats", "test-uuid-0000-0000-0000-000000000009", volume, nullptr, settings, strategy, ttl_minutes);

    ASSERT_EQ(cache.getKeyCount(), 0);
    ASSERT_EQ(cache.getCachedSize(), 0);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    // Add entries
    for (int i = 0; i < 5; i++)
    {
        String seg_name = fmt::format("test_uuid/{}/col.bin/offset_{}", part, i);
        String data = String(100, 'a');
        ReadBufferFromString buffer(data);
        cache.set(seg_name, buffer, data.size(), false);
    }

    ASSERT_EQ(cache.getKeyCount(), 5);
    ASSERT_GT(cache.getCachedSize(), 0);
}

// Test multi-disk volume
TEST_F(DiskCacheTTLTest, MultiDiskVolume)
{
    auto volume = createDualDiskVolume();
    DiskCacheSettings settings;
    settings.lru_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test_multidisk", "test-uuid-0000-0000-0000-00000000000a", volume, nullptr, settings, strategy, ttl_minutes);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    // Add multiple segments to trigger distribution across disks
    for (int i = 0; i < 10; i++)
    {
        String seg_name = fmt::format("test_uuid/{}/col.bin/offset_{}", part, i);
        String data = String(1000, 'a');
        ReadBufferFromString buffer(data);
        cache.set(seg_name, buffer, data.size(), false);
    }

    ASSERT_EQ(cache.getKeyCount(), 10);

    // Verify all can be retrieved
    for (int i = 0; i < 10; i++)
    {
        String seg_name = fmt::format("test_uuid/{}/col.bin/offset_{}", part, i);
        auto [disk, path] = cache.get(seg_name);
        ASSERT_FALSE(path.empty());
        ASSERT_TRUE(disk != nullptr);
    }
}

} // namespace DB
