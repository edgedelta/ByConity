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
#include <fmt/core.h>
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
        auto disk = std::make_shared<DiskLocal>("ttl_disk", "tmp/ttl_disk/", DiskStats{});
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

// Test TTL behavior through set/get operations (tests shouldCache indirectly)
TEST_F(DiskCacheTTLTest, TTLBehaviorThroughOperations)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache("test_ttl", "test-uuid-0000-0000-0000-000000000001", volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);

    // Recent partition (30 minutes old) - should cache
    {
        struct tm tm_time;
        time_t recent_time = now - (30 * 60);
        gmtime_r(&recent_time, &tm_time);
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday);
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000001/{}/col.bin/offset_0", part);

        String data = "test";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);

        auto [disk, path] = cache.get(seg);
        ASSERT_FALSE(path.empty()); // Should be cached
    }

    // Old partition (2 hours old) - should not cache
    {
        struct tm tm_time;
        time_t old_time = now - (2 * 60 * 60);
        gmtime_r(&old_time, &tm_time);
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday);
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000001/{}/col.bin/offset_1", part);

        String data = "test";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);

        auto [disk, path] = cache.get(seg);
        ASSERT_TRUE(path.empty()); // Should NOT be cached
    }
}

// Test TTL disabled (ttl_minutes = 0) - all time-based partitions cached
TEST_F(DiskCacheTTLTest, TTLDisabled)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 0; // TTL disabled
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);

    // Very old partition (1 year old) should be cached when TTL disabled
    struct tm tm_time;
    time_t old_time = now - (365 * 24 * 60 * 60);
    gmtime_r(&old_time, &tm_time);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday);
    String seg = fmt::format("test-uuid-0000-0000-0000-000000000002/{}/col.bin/offset_0", part);

    String data = "test";
    ReadBufferFromString buf(data);
    cache.set(seg, buf, data.size(), false);

    auto [disk, path] = cache.get(seg);
    ASSERT_FALSE(path.empty()); // Should be cached even though very old
}

// Test non-time partitions are rejected
TEST_F(DiskCacheTTLTest, RejectNonTimePartitions)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);

    // Create recent partition (30 minutes old - should survive)
    struct tm tm_recent;
    time_t recent_time = now - (30 * 60);
    gmtime_r(&recent_time, &tm_recent);
    String recent_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_recent.tm_year + 1900, tm_recent.tm_mon + 1, tm_recent.tm_mday);
    String recent_seg = fmt::format("test-uuid-0000-0000-0000-000000000005/{}/column.bin/offset_0", recent_part);

    // Create old partition (2 hours old - should be evicted)
    struct tm tm_old;
    time_t old_time = now - (2 * 60 * 60);
    gmtime_r(&old_time, &tm_old);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);
    String old_seg = fmt::format("test-uuid-0000-0000-0000-000000000005/{}/column.bin/offset_1", old_part);

    // Add both segments
    String data = "test data";
    ReadBufferFromString buf1(data);
    ReadBufferFromString buf2(data);
    cache.set(recent_seg, buf1, data.size(), false);
    cache.set(old_seg, buf2, data.size(), false);

    // Verify both exist initially
    size_t initial_count = cache.getKeyCount();
    ASSERT_EQ(initial_count, 2);

    // Wait a moment for potential background eviction
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Old partition should be evicted, recent should remain
    ASSERT_EQ(cache.getKeyCount(), 1);

    auto [disk1, path1] = cache.get(recent_seg);
    auto [disk2, path2] = cache.get(old_seg);

    ASSERT_FALSE(path1.empty());  // Recent still cached
    ASSERT_TRUE(path2.empty());   // Old evicted
}

// Periodic eviction is tested indirectly through EvictExpired test
// (eviction happens automatically every hour during get() operations)

// Test concurrent set/get operations
TEST_F(DiskCacheTTLTest, ConcurrentAccess)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 10 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

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

// Test detailed statistics collection
TEST_F(DiskCacheTTLTest, DetailedStats)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 10 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);

    // Add recent entries (should be cached)
    struct tm tm_recent;
    time_t recent_time = now - (30 * 60);
    gmtime_r(&recent_time, &tm_recent);
    String recent_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_recent.tm_year + 1900, tm_recent.tm_mon + 1, tm_recent.tm_mday);

    for (int i = 0; i < 5; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000b/{}/col.bin/offset_{}", recent_part, i);
        String data = String(100, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    // Try to add old entries (should be rejected)
    struct tm tm_old;
    time_t old_time = now - (2 * 60 * 60);
    gmtime_r(&old_time, &tm_old);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);

    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000b/{}/col.bin/offset_{}", old_part, i);
        String data = String(100, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    // Try to add non-time partition (should be rejected)
    String nontime_part = "string_partition_1_100_2";
    for (int i = 0; i < 2; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000b/{}/col.bin/offset_{}", nontime_part, i);
        String data = String(100, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    // Check global stats
    auto stats = cache.getStats();
    ASSERT_EQ(stats.table_uuid, "test-uuid-0000-0000-0000-00000000000b");
    ASSERT_EQ(stats.total_entries.load(), 5); // Only recent entries
    ASSERT_GT(stats.total_bytes.load(), 0);
    ASSERT_EQ(stats.rejected_too_old.load(), 3); // Old entries rejected
    ASSERT_EQ(stats.rejected_non_time_partition.load(), 2); // Non-time rejected

    // Perform gets (hits)
    for (int i = 0; i < 5; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000b/{}/col.bin/offset_{}", recent_part, i);
        auto [disk, path] = cache.get(seg);
        ASSERT_FALSE(path.empty());
    }

    // Perform gets (misses)
    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000b/{}/col.bin/offset_{}", old_part, i);
        auto [disk, path] = cache.get(seg);
        ASSERT_TRUE(path.empty());
    }

    // Check partition stats
    auto partition_stats = cache.getPartitionStats();
    ASSERT_GE(partition_stats.size(), 1);

    // Find recent partition stats
    String recent_partition_id = fmt::format("{:04d}{:02d}{:02d}",
        tm_recent.tm_year + 1900, tm_recent.tm_mon + 1, tm_recent.tm_mday);

    bool found_recent = false;
    for (const auto & ps : partition_stats)
    {
        if (ps.partition_id == recent_partition_id)
        {
            found_recent = true;
            ASSERT_EQ(ps.entry_count, 5);
            ASSERT_GT(ps.total_bytes, 0);
            ASSERT_EQ(ps.hits.load(), 5);
            ASSERT_EQ(ps.misses.load(), 1); // Initial set counts as miss
            break;
        }
    }
    ASSERT_TRUE(found_recent);
}

// Test stats after eviction
TEST_F(DiskCacheTTLTest, StatsAfterEviction)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);

    // Add recent entries
    struct tm tm_recent;
    time_t recent_time = now - (30 * 60);
    gmtime_r(&recent_time, &tm_recent);
    String recent_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_recent.tm_year + 1900, tm_recent.tm_mon + 1, tm_recent.tm_mday);

    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000c/{}/col.bin/offset_{}", recent_part, i);
        String data = "test data";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    // Add old entries (will be cached initially but evicted later)
    struct tm tm_old;
    time_t old_time = now - (2 * 60 * 60);
    gmtime_r(&old_time, &tm_old);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);

    for (int i = 0; i < 2; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000c/{}/col.bin/offset_{}", old_part, i);
        String data = "test data";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    auto stats_before = cache.getStats();
    size_t entries_before = stats_before.total_entries.load();

    // Wait for eviction
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto stats_after = cache.getStats();

    // Old entries should be evicted
    ASSERT_LT(stats_after.total_entries.load(), entries_before);
    ASSERT_GT(stats_after.evicted_expired.load(), 0);
    ASSERT_GT(stats_after.last_eviction_run.load(), 0);
}

// Test per-partition hit rate calculation
TEST_F(DiskCacheTTLTest, PartitionHitRate)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache(\1, volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
    String partition_id = fmt::format("{:04d}{:02d}{:02d}",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    // Add 10 segments
    for (int i = 0; i < 10; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000d/{}/col.bin/offset_{}", part, i);
        String data = "test";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    // Hit 7 segments, miss 3
    for (int i = 0; i < 7; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000d/{}/col.bin/offset_{}", part, i);
        auto [disk, path] = cache.get(seg);
        ASSERT_FALSE(path.empty());
    }

    for (int i = 10; i < 13; i++) // Non-existent segments
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000d/{}/col.bin/offset_{}", part, i);
        auto [disk, path] = cache.get(seg);
        ASSERT_TRUE(path.empty());
    }

    auto partition_stats = cache.getPartitionStats();
    bool found = false;
    for (const auto & ps : partition_stats)
    {
        if (ps.partition_id == partition_id)
        {
            found = true;
            size_t hits = ps.hits.load();
            size_t misses = ps.misses.load();

            // Hits should include successful gets
            ASSERT_EQ(hits, 7);

            // Misses include: initial set (10) + failed gets (3)
            ASSERT_EQ(misses, 13);

            double hit_rate = static_cast<double>(hits) / (hits + misses);
            ASSERT_GT(hit_rate, 0.0);
            ASSERT_LT(hit_rate, 1.0);
            break;
        }
    }
    ASSERT_TRUE(found);
}

// Test async size-based eviction
TEST_F(DiskCacheTTLTest, AsyncSizeBasedEviction)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;  // 1MB limit
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test_async_eviction", "test-uuid-0000-0000-0000-00000000000e", volume, nullptr, settings, strategy, ttl_minutes, 1024 * 1024);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);

    // Fill cache to ~95% (trigger async eviction threshold of 90%)
    size_t segment_size = 100 * 1024;  // 100KB per segment
    int segments_to_add = 10;  // 1MB total

    for (int i = 0; i < segments_to_add; i++)
    {
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000e/{}/col.bin/offset_{}", part, i);

        String data = String(segment_size, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    auto stats_before = cache.getStats();
    ASSERT_GT(stats_before.total_bytes.load(), settings.ttl_cache_max_size * 0.90);
    ASSERT_EQ(stats_before.async_eviction_triggered.load(), 0);

    // Add one more segment - should trigger async eviction
    {
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000e/{}/col.bin/offset_trigger", part);

        String data = String(segment_size, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    // Check that async eviction was triggered
    auto stats_after = cache.getStats();
    ASSERT_EQ(stats_after.async_eviction_triggered.load(), 1);

    // Wait for async eviction to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Verify some space was freed
    auto stats_final = cache.getStats();
    ASSERT_GT(stats_final.evicted_size_limit.load(), 0);

    // Try adding another segment immediately - should be rate limited
    {
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000e/{}/col.bin/offset_rate_limit", part);

        String data = String(segment_size, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    // Should be rate limited (still 1 trigger, but skipped counter increased)
    auto stats_rate_limit = cache.getStats();
    ASSERT_EQ(stats_rate_limit.async_eviction_triggered.load(), 1);
    ASSERT_GT(stats_rate_limit.async_eviction_skipped_rate_limit.load(), 0);
}

// Test explicit min/max time parameters override partition_id parsing
TEST_F(DiskCacheTTLTest, ExplicitTimestamps)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache("test_ttl", "test-uuid-0000-0000-0000-000000000010", volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);

    // Use old partition_id (2 hours ago) that would be rejected by partition parsing
    time_t old_time = now - (2 * 60 * 60);
    struct tm tm_old;
    gmtime_r(&old_time, &tm_old);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);

    // Test 1: Without explicit timestamps - should be rejected (partition is old)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000010/{}/col.bin/offset_0", old_part);
        String data = "test1";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);

        auto [disk, path] = cache.get(seg);
        ASSERT_TRUE(path.empty()); // Should NOT be cached (old partition)
    }

    // Test 2: With explicit max_time (recent) - should be cached despite old partition_id
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000010/{}/col.bin/offset_1", old_part);
        String data = "test2";
        ReadBufferFromString buf(data);

        time_t recent_max_time = now - (30 * 60); // 30 minutes ago (within TTL)
        cache.set(seg, buf, data.size(), false, 0, recent_max_time);

        auto [disk, path] = cache.get(seg);
        ASSERT_FALSE(path.empty()); // Should BE cached (explicit max_time is recent)
    }

    // Test 3: With explicit max_time (old) - should be rejected
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000010/{}/col.bin/offset_2", old_part);
        String data = "test3";
        ReadBufferFromString buf(data);

        time_t old_max_time = now - (90 * 60); // 90 minutes ago (outside TTL)
        cache.set(seg, buf, data.size(), false, 0, old_max_time);

        auto [disk, path] = cache.get(seg);
        ASSERT_TRUE(path.empty()); // Should NOT be cached (explicit max_time is old)
    }
}

// Test preload vs query stats tracking
TEST_F(DiskCacheTTLTest, PreloadQueryStats)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60; // 1 hour TTL
    DiskCacheTTL cache("test_ttl", "test-uuid-0000-0000-0000-000000000011", volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    // Cache with preload=false (query-triggered)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000011/{}/col.bin/offset_0", part);
        String data = String(100, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false); // is_preload=false

        auto stats = cache.getStats();
        ASSERT_EQ(stats.cached_from_query.load(), 1);
        ASSERT_EQ(stats.cached_bytes_query.load(), 100);
        ASSERT_EQ(stats.cached_from_preload.load(), 0);
        ASSERT_EQ(stats.cached_bytes_preload.load(), 0);
    }

    // Cache with preload=true (background preload)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000011/{}/col.bin/offset_1", part);
        String data = String(200, 'b');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), true); // is_preload=true

        auto stats = cache.getStats();
        ASSERT_EQ(stats.cached_from_query.load(), 1);
        ASSERT_EQ(stats.cached_bytes_query.load(), 100);
        ASSERT_EQ(stats.cached_from_preload.load(), 1);
        ASSERT_EQ(stats.cached_bytes_preload.load(), 200);
    }

    // Cache more query-triggered segments
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000011/{}/col.bin/offset_2", part);
        String data = String(50, 'c');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false); // is_preload=false

        auto stats = cache.getStats();
        ASSERT_EQ(stats.cached_from_query.load(), 2);
        ASSERT_EQ(stats.cached_bytes_query.load(), 150);
        ASSERT_EQ(stats.cached_from_preload.load(), 1);
        ASSERT_EQ(stats.cached_bytes_preload.load(), 200);
    }
}

// Test auto-sizing by percent when max_size_bytes=0
TEST_F(DiskCacheTTLTest, AutoSizeByPercent)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 0;  // Don't use worker-level limit
    settings.ttl_cache_max_percent = 50;  // Use 50% of disk
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;

    // Pass max_size_bytes=0 to trigger auto-sizing
    DiskCacheTTL cache("test_auto_size", "test-uuid-0000-0000-0000-000000000012", volume, nullptr, settings, strategy, ttl_minutes, 0);

    // Get disk capacity
    auto total_space = volume->getTotalSpace(true);
    size_t expected_max_size = static_cast<size_t>(total_space.bytes * 0.50);

    ASSERT_GT(expected_max_size, 0);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    // Fill cache beyond 50% of disk - should trigger size-based eviction
    size_t segment_size = expected_max_size / 8;  // Each segment = 12.5% of limit
    int segments_added = 0;

    for (int i = 0; i < 10; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000012/{}/col.bin/offset_{}", part, i);
        String data = String(segment_size, 'x');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
        segments_added++;

        // Should trigger async eviction around 90% (7.2 segments)
        if (segments_added >= 8)
        {
            auto stats = cache.getStats();
            if (stats.async_eviction_triggered.load() > 0)
                break;
        }
    }

    // Verify async eviction was triggered due to percent-based size limit
    auto stats = cache.getStats();
    ASSERT_GT(stats.async_eviction_triggered.load(), 0);

    // Wait for eviction to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Verify eviction happened
    auto final_stats = cache.getStats();
    ASSERT_GT(final_stats.evicted_size_limit.load(), 0);
}

// Test 3-tier precedence: per-table max_size_bytes > worker ttl_cache_max_size > auto-size
TEST_F(DiskCacheTTLTest, SizeLimitPrecedence)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 10 * 1024 * 1024;  // 10MB worker-level limit
    settings.ttl_cache_max_percent = 80;  // Would be larger than 10MB
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;

    // Test 1: Per-table limit (1MB) overrides worker-level (10MB)
    {
        DiskCacheTTL cache("test_per_table", "test-uuid-0000-0000-0000-000000000013",
                          volume, nullptr, settings, strategy, ttl_minutes, 1024 * 1024);

        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

        // Fill to 95% of 1MB (should trigger at 90%)
        size_t segment_size = 100 * 1024;  // 100KB per segment
        for (int i = 0; i < 10; i++)
        {
            String seg = fmt::format("test-uuid-0000-0000-0000-000000000013/{}/col.bin/offset_{}", part, i);
            String data = String(segment_size, 'a');
            ReadBufferFromString buf(data);
            cache.set(seg, buf, data.size(), false);
        }

        // Should trigger eviction at 1MB limit, not 10MB
        auto stats = cache.getStats();
        ASSERT_GT(stats.async_eviction_triggered.load(), 0);
    }

    // Test 2: Worker-level limit (10MB) used when per-table = 0
    {
        DiskCacheTTL cache("test_worker_level", "test-uuid-0000-0000-0000-000000000014",
                          volume, nullptr, settings, strategy, ttl_minutes, 0);

        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

        // Fill to 95% of 10MB
        size_t segment_size = 1024 * 1024;  // 1MB per segment
        for (int i = 0; i < 10; i++)
        {
            String seg = fmt::format("test-uuid-0000-0000-0000-000000000014/{}/col.bin/offset_{}", part, i);
            String data = String(segment_size, 'b');
            ReadBufferFromString buf(data);
            cache.set(seg, buf, data.size(), false);
        }

        // Should trigger eviction at 10MB limit
        auto stats = cache.getStats();
        ASSERT_GT(stats.async_eviction_triggered.load(), 0);
    }

    // Test 3: Auto-size by percent when both per-table and worker-level = 0
    {
        DiskCacheSettings settings_no_limit;
        settings_no_limit.ttl_cache_max_size = 0;
        settings_no_limit.ttl_cache_max_percent = 10;  // Small percent for faster test
        auto strategy_no_limit = std::make_shared<DiskCacheSimpleStrategy>(settings_no_limit);

        DiskCacheTTL cache("test_auto_size", "test-uuid-0000-0000-0000-000000000015",
                          volume, nullptr, settings_no_limit, strategy_no_limit, ttl_minutes, 0);

        auto total_space = volume->getTotalSpace(true);
        size_t expected_limit = static_cast<size_t>(total_space.bytes * 0.10);

        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

        // Fill beyond auto-sized limit
        size_t segment_size = expected_limit / 8;
        for (int i = 0; i < 10; i++)
        {
            String seg = fmt::format("test-uuid-0000-0000-0000-000000000015/{}/col.bin/offset_{}", part, i);
            String data = String(segment_size, 'c');
            ReadBufferFromString buf(data);
            cache.set(seg, buf, data.size(), false);
        }

        // Should trigger eviction based on auto-sized limit
        auto stats = cache.getStats();
        ASSERT_GT(stats.async_eviction_triggered.load(), 0);
    }
}

} // namespace DB
