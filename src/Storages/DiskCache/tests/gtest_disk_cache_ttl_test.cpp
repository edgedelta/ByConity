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
#include <map>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/VolumeJBOD.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <Storages/DiskCache/TTLCacheFDBIndex.h>
#include <Catalog/IMetastore.h>
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

// ttl_minutes=0 means "cache nothing" — all writes are rejected
TEST_F(DiskCacheTTLTest, TTLZeroRejectsAll)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, 0, 0);

    time_t now = time(nullptr);
    struct tm tm_time;
    gmtime_r(&now, &tm_time);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday);
    String seg = fmt::format("test-uuid-0000-0000-0000-000000000002/{}/col.bin/offset_0", part);

    String data = "test";
    ReadBufferFromString buf(data);
    cache.set(seg, buf, data.size(), false);

    auto [disk, path] = cache.get(seg);
    ASSERT_TRUE(path.empty()); // ttl_minutes=0 rejects all writes
    ASSERT_EQ(cache.getStats().rejected_too_old, 1u);
}

// Test non-time partitions are rejected
TEST_F(DiskCacheTTLTest, RejectNonTimePartitions)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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

    ASSERT_EQ(success_count, 10);
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
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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

    // Drop part1 — path must include the UUID prefix used in segment names
    cache.drop("test_uuid/" + part1);
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
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    ASSERT_EQ(stats.total_entries, 5); // Only recent entries
    ASSERT_GT(stats.total_bytes, 0);
    ASSERT_EQ(stats.rejected_too_old, 3); // Old entries rejected
    ASSERT_EQ(stats.rejected_non_time_partition, 2); // Non-time rejected

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
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
    size_t entries_before = stats_before.total_entries;

    // Wait for eviction
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto stats_after = cache.getStats();

    // Old entries should be evicted
    ASSERT_LT(stats_after.total_entries, entries_before);
    ASSERT_GT(stats_after.evicted_expired, 0);
    ASSERT_GT(stats_after.last_eviction_run, 0);
}

// Test per-partition hit rate calculation
TEST_F(DiskCacheTTLTest, PartitionHitRate)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
            ASSERT_GT(ps.entry_count, 0);
            ASSERT_GT(ps.total_bytes, 0);
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
    ASSERT_GT(stats_before.total_bytes, settings.ttl_cache_max_size * 0.90);
    ASSERT_EQ(stats_before.async_eviction_triggered, 0);

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
    ASSERT_EQ(stats_after.async_eviction_triggered, 1);

    // Wait for async eviction to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Verify some space was freed
    auto stats_final = cache.getStats();
    ASSERT_GT(stats_final.evicted_size_limit, 0);

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
    ASSERT_EQ(stats_rate_limit.async_eviction_triggered, 1);
    ASSERT_GT(stats_rate_limit.async_eviction_skipped_rate_limit, 0);
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
        ASSERT_EQ(stats.cached_from_query, 1);
        ASSERT_EQ(stats.cached_bytes_query, 100);
        ASSERT_EQ(stats.cached_from_preload, 0);
        ASSERT_EQ(stats.cached_bytes_preload, 0);
    }

    // Cache with preload=true (background preload)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000011/{}/col.bin/offset_1", part);
        String data = String(200, 'b');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), true); // is_preload=true

        auto stats = cache.getStats();
        ASSERT_EQ(stats.cached_from_query, 1);
        ASSERT_EQ(stats.cached_bytes_query, 100);
        ASSERT_EQ(stats.cached_from_preload, 1);
        ASSERT_EQ(stats.cached_bytes_preload, 200);
    }

    // Cache more query-triggered segments
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000011/{}/col.bin/offset_2", part);
        String data = String(50, 'c');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false); // is_preload=false

        auto stats = cache.getStats();
        ASSERT_EQ(stats.cached_from_query, 2);
        ASSERT_EQ(stats.cached_bytes_query, 150);
        ASSERT_EQ(stats.cached_from_preload, 1);
        ASSERT_EQ(stats.cached_bytes_preload, 200);
    }
}

// Test unlimited per-table cache (constrained only by global limit)
TEST_F(DiskCacheTTLTest, UnlimitedPerTable)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 0;  // No worker-level per-table default
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;

    // Pass max_size_bytes=0 → unlimited per-table (constrained by global)
    DiskCacheTTL cache("test_unlimited", "test-uuid-0000-0000-0000-000000000012", volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    // Cache some data - no per-table limit check
    for (int i = 0; i < 5; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000012/{}/col.bin/offset_{}", part, i);
        String data = String(1024, 'x');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    // Verify cached (no per-table eviction triggered)
    auto stats = cache.getStats();
    ASSERT_EQ(stats.async_eviction_triggered, 0);  // No local eviction
    ASSERT_EQ(stats.total_entries, 5);
}

// Test 2-tier precedence: per-table max_size_bytes > worker ttl_cache_max_size > unlimited (0)
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
        ASSERT_GT(stats.async_eviction_triggered, 0);
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
        ASSERT_GT(stats.async_eviction_triggered, 0);
    }

    // Test 3: Unlimited when both per-table and worker-level = 0
    {
        DiskCacheSettings settings_no_limit;
        settings_no_limit.ttl_cache_max_size = 0;
        auto strategy_no_limit = std::make_shared<DiskCacheSimpleStrategy>(settings_no_limit);

        DiskCacheTTL cache("test_unlimited", "test-uuid-0000-0000-0000-000000000015",
                          volume, nullptr, settings_no_limit, strategy_no_limit, ttl_minutes, 0);

        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

        // Cache data - no per-table limit
        for (int i = 0; i < 5; i++)
        {
            String seg = fmt::format("test-uuid-0000-0000-0000-000000000015/{}/col.bin/offset_{}", part, i);
            String data = String(1024, 'c');
            ReadBufferFromString buf(data);
            cache.set(seg, buf, data.size(), false);
        }

        // No per-table eviction (unlimited, only constrained by global)
        auto stats = cache.getStats();
        ASSERT_EQ(stats.async_eviction_triggered, 0);
        ASSERT_EQ(stats.total_entries, 5);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static String makeSegKey(const String & uuid, const String & part, const String & col, const String & ext)
{
    return fmt::format("{}/{}/{}#0{}", uuid, part, col, ext);
}

static String todayPart()
{
    time_t now = time(nullptr);
    struct tm t;
    gmtime_r(&now, &t);
    return fmt::format("{:04d}{:02d}{:02d}_1_100_2", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
}

static String expiredPart()
{
    time_t ts = time(nullptr) - 2 * 24 * 3600;
    struct tm t;
    gmtime_r(&ts, &t);
    return fmt::format("{:04d}{:02d}{:02d}_1_100_2", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
}

// ---------------------------------------------------------------------------
// In-memory IMetaStore mock
// ---------------------------------------------------------------------------

class MockMetaStore : public Catalog::IMetaStore
{
public:
    struct MockIterator : public Iterator
    {
        std::vector<std::pair<String, String>> entries;
        int pos = -1;
        bool next() override { return ++pos < static_cast<int>(entries.size()); }
        String key()   override { return entries[pos].first; }
        String value() override { return entries[pos].second; }
    };

    void put(const String & key, const String & value, bool = false) override { store[key] = value; }
    std::pair<bool, String> putCAS(const String &, const String &, const String &, bool) override { return {false, {}}; }
    uint64_t get(const String & key, String & value) override
    {
        auto it = store.find(key);
        if (it == store.end()) return 0;
        value = it->second;
        return 1;
    }
    std::vector<std::pair<String, UInt64>> multiGet(const std::vector<String> &) override { return {}; }
    bool batchWrite(const Catalog::BatchCommitRequest &, Catalog::BatchCommitResponse &) override { return true; }
    void drop(const String & key, const UInt64 &) override { store.erase(key); }
    void drop(const String & key, const String &)  override { store.erase(key); }
    IteratorPtr getAll() override { return getByPrefix(""); }
    IteratorPtr getByPrefix(const String & prefix, const size_t & = 0, uint32_t = 0, const String & = "") override
    {
        auto iter = std::make_shared<MockIterator>();
        for (auto & [k, v] : store)
            if (k.starts_with(prefix))
                iter->entries.emplace_back(k, v);
        return iter;
    }
    IteratorPtr getByRange(const String &, const String &, bool, bool) override { return std::make_shared<MockIterator>(); }
    void clean(const String & prefix) override
    {
        for (auto it = store.begin(); it != store.end(); )
            it = it->first.starts_with(prefix) ? store.erase(it) : std::next(it);
    }
    void close() override {}
    uint32_t getMaxBatchSize() override { return 1000; }
    uint32_t getMaxKVSize()    override { return 1024 * 1024; }

    std::map<String, String> store;
};

// ---------------------------------------------------------------------------
// Parameterized: set / get / evict for .bin, .mrk, .idx
// ---------------------------------------------------------------------------

struct SegCase { const char * ext; const char * expected_prefix; };

class SegmentPrefixTest : public DiskCacheTTLTest,
                          public ::testing::WithParamInterface<SegCase> {};

INSTANTIATE_TEST_SUITE_P(AllTypes, SegmentPrefixTest, ::testing::Values(
    SegCase{".bin", "data/"},
    SegCase{".mrk", "meta/"},
    SegCase{".idx", "meta/"}
));

TEST_P(SegmentPrefixTest, SetGoesToCorrectDir)
{
    auto p = GetParam();
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 64 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, 60, 0);

    String seg = makeSegKey("aaaa-bbbb", todayPart(), "col", p.ext);
    String data = "payload";
    ReadBufferFromString buf(data);
    cache.set(seg, buf, data.size(), false);

    auto [disk, path] = cache.get(seg);
    ASSERT_FALSE(path.empty()) << "segment not found after set: " << seg;
    EXPECT_NE(path.find(p.expected_prefix), String::npos)
        << p.ext << " should be under " << p.expected_prefix << " but path=" << path;
    // Also verify the file actually exists at the returned path
    ASSERT_TRUE(disk);
    EXPECT_TRUE(disk->exists(path)) << "file missing on disk at: " << path;
}

TEST_P(SegmentPrefixTest, GetReturnsExistingFile)
{
    auto p = GetParam();
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 64 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, 60, 0);

    String seg = makeSegKey("aaaa-bbbb", todayPart(), "col", p.ext);
    String data = "payload";
    ReadBufferFromString buf(data);
    cache.set(seg, buf, data.size(), false);

    // get() must return a path that actually contains the prefix and the file
    auto [disk, path] = cache.get(seg);
    ASSERT_TRUE(disk) << "no disk for " << seg;
    EXPECT_TRUE(disk->exists(path)) << "file not on disk: " << path;
    EXPECT_NE(path.find(p.expected_prefix), String::npos)
        << p.ext << " get() returned wrong prefix: " << path;
}

TEST_P(SegmentPrefixTest, EvictRemovesFromDisk)
{
    auto p = GetParam();
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 64 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);
    // 1-minute TTL — 2-day-old part is expired
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, 1, 0);

    time_t old_ts = time(nullptr) - 2 * 24 * 3600;
    String seg = makeSegKey("aaaa-bbbb", expiredPart(), "col", p.ext);
    String data = "payload";
    ReadBufferFromString buf(data);
    cache.set(seg, buf, data.size(), false, 0, old_ts);

    ASSERT_EQ(cache.getKeyCount(), 1);
    auto [disk, path] = cache.get(seg);
    ASSERT_TRUE(disk && disk->exists(path)) << "file should exist before eviction: " << path;

    cache.evictExpired();

    EXPECT_EQ(cache.getKeyCount(), 0);
    EXPECT_FALSE(disk->exists(path))
        << p.ext << " file still on disk after eviction — rel_path prefix bug? path=" << path;
}

// ---------------------------------------------------------------------------
// Reconcile: FDB entries for all three types restore with correct rel_path
// ---------------------------------------------------------------------------

TEST_F(DiskCacheTTLTest, ReconcileRestoresAllTypesWithCorrectRelPath)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 64 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, 60, 0);

    const String uuid = "aaaa-bbbb-cccc-dddd";
    const String part = todayPart();
    const time_t now  = time(nullptr);

    struct SegInfo { String ext; String expected_prefix; };
    SegInfo cases[] = {{".bin", "data/"}, {".mrk", "meta/"}, {".idx", "meta/"}};

    // Write all three types to disk so reconcile can verify file existence
    std::map<String, String> seg_to_path;
    for (auto & c : cases)
    {
        String seg = makeSegKey(uuid, part, "col", c.ext);
        String data = "payload";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, 0, now);
        auto [disk, path] = cache.get(seg);
        ASSERT_FALSE(path.empty()) << "failed to cache: " << seg;
        EXPECT_NE(path.find(c.expected_prefix), String::npos)
            << "wrong write prefix for " << c.ext << ": " << path;
        seg_to_path[seg] = path;
    }

    // Build mock FDB store — key_prefix = "{ns}_DCI_{worker}_{uuid}"
    // Seed one entry per segment using encodeValue; the key just needs the prefix.
    const String ns = "byconity";
    const String worker = "test-worker";
    const String key_prefix = fmt::format("{}_DCI_{}_{}", ns, worker, uuid);
    auto mock_store = std::make_shared<MockMetaStore>();
    int i = 0;
    for (auto & [seg, path] : seg_to_path)
    {
        String fdb_key = fmt::format("{}_{:04d}", key_prefix, i++);
        mock_store->store[fdb_key] = fmt::format("{}:{}:{}", static_cast<int64_t>(now), 7, seg);
    }

    // Reconcile into a fresh cache_map
    TTLCacheFDBIndex fdb_idx(mock_store, ns, worker, uuid, worker);
    std::map<UInt128, std::shared_ptr<DiskCacheTTLMeta>> cache_map;
    auto get_rel_path = [&cache](UInt128 key, const String & seg_name) -> std::filesystem::path
    {
        return cache.getRelativePath(key, seg_name);
    };

    fdb_idx.reconcile(
        volume,
        get_rel_path,
        [](time_t) { return true; },
        [&cache_map](UInt128 key, std::shared_ptr<DiskCacheTTLMeta> meta) { cache_map[key] = meta; }
    );

    ASSERT_EQ(cache_map.size(), 3u) << "expected 3 entries restored";

    for (auto & [seg, expected_path] : seg_to_path)
    {
        auto key = DiskCacheTTL::hash(seg);
        auto it = cache_map.find(key);
        ASSERT_NE(it, cache_map.end()) << "segment not restored: " << seg;
        EXPECT_EQ(it->second->rel_path, expected_path)
            << "rel_path mismatch for " << seg
            << "\n  got:  " << it->second->rel_path
            << "\n  want: " << expected_path;
    }
}

// Verify drop() decrements partition_stats correctly
TEST_F(DiskCacheTTLTest, DropUpdatesPartitionStats)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    DiskCacheTTL cache("test_drop_pstats", "test-uuid-drop", volume, nullptr, settings, strategy, 60 * 24 * 365, 0);

    time_t now = time(nullptr);
    struct tm tm;
    gmtime_r(&now, &tm);
    String part1 = fmt::format("{:04d}{:02d}{:02d}_1_100_2", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    String part2 = fmt::format("{:04d}{:02d}{:02d}_2_200_2", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    String partition_id = fmt::format("{:04d}{:02d}{:02d}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    const String uuid = "test-uuid-drop";

    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, part1, i);
        String data(100, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }
    for (int i = 0; i < 2; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, part2, i);
        String data(100, 'b');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }
    ASSERT_EQ(cache.getKeyCount(), 5);

    {
        auto pstats = cache.getPartitionStats();
        bool found = false;
        for (const auto & ps : pstats)
        {
            if (ps.partition_id == partition_id)
            {
                found = true;
                ASSERT_EQ(ps.entry_count, 5u);
                ASSERT_EQ(ps.total_bytes, 500u);
            }
        }
        ASSERT_TRUE(found) << "partition not found before drop: " << partition_id;
    }

    cache.drop(uuid + "/" + part1);

    ASSERT_EQ(cache.getKeyCount(), 2);
    ASSERT_EQ(cache.getCachedSize(), 200u);
    {
        auto gstats = cache.getStats();
        ASSERT_EQ(gstats.total_entries, 2u);
        ASSERT_EQ(gstats.total_bytes,   200u);
    }

    {
        auto pstats = cache.getPartitionStats();
        bool found = false;
        for (const auto & ps : pstats)
        {
            if (ps.partition_id == partition_id)
            {
                found = true;
                ASSERT_EQ(ps.entry_count, 2u);
                ASSERT_EQ(ps.total_bytes, 200u);
            }
        }
        ASSERT_TRUE(found) << "partition not found after drop: " << partition_id;
    }
}

// Verify evictExpired() is a no-op on fresh entries and leaves partition_stats intact
TEST_F(DiskCacheTTLTest, EvictExpiredNoOpKeepsPartitionStats)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    UInt64 ttl_minutes = 60;
    DiskCacheTTL cache("test_evict_noop", "test-uuid-evict", volume, nullptr, settings, strategy, ttl_minutes, 0);

    time_t now = time(nullptr);
    struct tm tm;
    gmtime_r(&now, &tm);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    String partition_id = fmt::format("{:04d}{:02d}{:02d}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    const String uuid = "test-uuid-evict";

    for (int i = 0; i < 4; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, part, i);
        String data(100, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }
    ASSERT_EQ(cache.getKeyCount(), 4);

    cache.evictExpired();  // nothing should be evicted — entries are within TTL

    ASSERT_EQ(cache.getKeyCount(), 4);
    {
        auto gstats = cache.getStats();
        ASSERT_EQ(gstats.total_entries,   4u);
        ASSERT_EQ(gstats.total_bytes,   400u);
        ASSERT_EQ(gstats.evicted_expired, 0u);
    }

    auto pstats = cache.getPartitionStats();
    bool found = false;
    for (const auto & ps : pstats)
    {
        if (ps.partition_id == partition_id)
        {
            found = true;
            ASSERT_EQ(ps.entry_count, 4u);
            ASSERT_EQ(ps.total_bytes, 400u);
        }
    }
    ASSERT_TRUE(found) << "partition disappeared after no-op evictExpired: " << partition_id;
}

// Verify evictOldestPartitionsUntilSpace() decrements partition_stats for evicted partition
TEST_F(DiskCacheTTLTest, SizeLimitEvictionUpdatesPartitionStats)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    // ttl_minutes=0: no TTL rejection so we can use different-day partitions freely
    DiskCacheTTL cache("test_size_pstats", "test-uuid-size", volume, nullptr, settings, strategy, 0, 100 * 1024 * 1024);

    time_t now = time(nullptr);
    time_t yesterday = now - 25 * 3600;  // definitely the previous calendar day
    struct tm tm_now, tm_yest;
    gmtime_r(&now, &tm_now);
    gmtime_r(&yesterday, &tm_yest);

    String today_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2", tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
    String yest_part  = fmt::format("{:04d}{:02d}{:02d}_1_100_2", tm_yest.tm_year + 1900, tm_yest.tm_mon + 1, tm_yest.tm_mday);

    String today_pid = fmt::format("{:04d}{:02d}{:02d}", tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
    String yest_pid  = fmt::format("{:04d}{:02d}{:02d}", tm_yest.tm_year + 1900, tm_yest.tm_mon + 1, tm_yest.tm_mday);

    if (today_pid == yest_pid)
        GTEST_SKIP() << "test requires two distinct calendar days (running at midnight boundary)";

    const String uuid = "test-uuid-size";
    const size_t seg_size = 1024;

    for (int i = 0; i < 4; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, yest_part, i);
        String data(seg_size, 'y');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }
    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, today_part, i);
        String data(seg_size, 't');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }

    ASSERT_EQ(cache.getKeyCount(), 7);

    {
        auto pstats = cache.getPartitionStats();
        bool fy = false, ft = false;
        for (const auto & ps : pstats)
        {
            if (ps.partition_id == yest_pid)  { fy = true; ASSERT_EQ(ps.entry_count, 4u); }
            if (ps.partition_id == today_pid) { ft = true; ASSERT_EQ(ps.entry_count, 3u); }
        }
        ASSERT_TRUE(fy) << "yesterday partition missing: " << yest_pid;
        ASSERT_TRUE(ft) << "today partition missing: "     << today_pid;
    }

    // Free exactly 4 * seg_size bytes → should evict yesterday's 4 segments
    cache.evictOldestPartitionsUntilSpace(4 * seg_size);

    ASSERT_EQ(cache.getKeyCount(), 3);
    {
        auto gstats = cache.getStats();
        ASSERT_EQ(gstats.total_entries,     3u);
        ASSERT_EQ(gstats.total_bytes,       3 * seg_size);
        ASSERT_EQ(gstats.evicted_size_limit, 4u);
        ASSERT_EQ(gstats.evicted_expired,    0u);  // TTL eviction was NOT used
    }

    {
        auto pstats = cache.getPartitionStats();
        for (const auto & ps : pstats)
        {
            if (ps.partition_id == yest_pid)
            {
                ASSERT_EQ(ps.entry_count, 0u) << "yesterday partition should be empty after eviction";
                ASSERT_EQ(ps.total_bytes,  0u);
            }
            if (ps.partition_id == today_pid)
            {
                ASSERT_EQ(ps.entry_count, 3u) << "today partition should be untouched";
            }
        }
    }
}

// Verify part_index is correctly rebuilt after drop + re-add; also tests that
// cache_stats and partition_stats stay consistent across the full cycle.
TEST_F(DiskCacheTTLTest, PartIndexRebuildAfterDrop)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    DiskCacheTTL cache("test_part_idx", "test-uuid-idx", volume, nullptr, settings, strategy, 60 * 24 * 365, 0);

    const String uuid = "test-uuid-idx";
    time_t now = time(nullptr);
    struct tm tm;
    gmtime_r(&now, &tm);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    String partition_id = fmt::format("{:04d}{:02d}{:02d}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    const size_t seg_bytes = 64;

    // Phase 1: add 3 segments for 'part'
    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, part, i);
        String data(seg_bytes, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }
    ASSERT_EQ(cache.getKeyCount(), 3u);
    {
        auto gstats = cache.getStats();
        ASSERT_EQ(gstats.total_entries, 3u);
        ASSERT_EQ(gstats.total_bytes,   3 * seg_bytes);
    }

    // Phase 2: drop clears part_index entry for 'part'
    cache.drop(uuid + "/" + part);
    ASSERT_EQ(cache.getKeyCount(), 0u);
    {
        auto gstats = cache.getStats();
        ASSERT_EQ(gstats.total_entries, 0u);
        ASSERT_EQ(gstats.total_bytes,   0u);
    }
    {
        auto pstats = cache.getPartitionStats();
        for (const auto & ps : pstats)
            if (ps.partition_id == partition_id)
            {
                ASSERT_EQ(ps.entry_count, 0u);
                ASSERT_EQ(ps.total_bytes, 0u);
            }
    }

    // Phase 3: re-add 2 segments — part_index must be re-populated from scratch
    for (int i = 0; i < 2; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, part, i);
        String data(seg_bytes, 'b');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false);
    }
    ASSERT_EQ(cache.getKeyCount(), 2u);
    {
        auto gstats = cache.getStats();
        ASSERT_EQ(gstats.total_entries, 2u);
        ASSERT_EQ(gstats.total_bytes,   2 * seg_bytes);
    }

    // All 2 segments must be retrievable
    for (int i = 0; i < 2; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, part, i);
        auto [disk, path] = cache.get(seg);
        ASSERT_FALSE(path.empty()) << "segment " << i << " not found after re-add";
    }

    // Phase 4: second drop — part_index entry removed again, stats zeroed
    cache.drop(uuid + "/" + part);
    ASSERT_EQ(cache.getKeyCount(), 0u);
    {
        auto gstats = cache.getStats();
        ASSERT_EQ(gstats.total_entries, 0u);
        ASSERT_EQ(gstats.total_bytes,   0u);
    }
}

// ---------------------------------------------------------------------------
// drop() must evict FDB forward + reverse entries for the dropped part
// ---------------------------------------------------------------------------

TEST_F(DiskCacheTTLTest, DropEvictsFDBEntries)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    // No underscores in these strings so Catalog::escapeString is a no-op
    const String uuid   = "test-uuid-fdb";
    const String ns     = "byconity";
    const String worker = "test-worker";
    const String key_prefix     = ns + "_DCI_" + worker + "_" + uuid;
    const String rev_key_prefix = ns + "_DCIREV_" + uuid;

    auto mock_store = std::make_shared<MockMetaStore>();
    auto fdb_idx = std::make_shared<TTLCacheFDBIndex>(mock_store, ns, worker, uuid, worker);

    DiskCacheTTL cache("test_fdb_drop", uuid, volume, nullptr, settings, strategy, 60 * 24 * 365, 0);
    cache.setFDBIndex(fdb_idx);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2", tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
    String partition_id = fmt::format("{:04d}{:02d}{:02d}", tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    const size_t seg_bytes = 64;
    const int num_segs = 3;

    for (int i = 0; i < num_segs; i++)
    {
        String seg = makeSegKey(uuid, part, fmt::format("col{}", i), ".bin");
        String data(seg_bytes, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, 0, now);

        // Seed FDB store manually (batchWrite in MockMetaStore is a no-op).
        // hexKey layout: first 16 chars = hex(items[1]=low), last 16 = hex(items[0]=high).
        auto key  = DiskCacheTTL::hash(seg);
        auto hex  = DiskCacheTTL::hexKey(key);
        String high_hex = hex.substr(16, 16);   // items[0] = sipHash64(part_name)
        String low_hex  = hex.substr(0, 16);    // items[1] = sipHash64(column)
        mock_store->store[fmt::format("{}_{}_{}_{}",  key_prefix,     partition_id, high_hex, low_hex)]
            = fmt::format("{}:{}:{}", static_cast<int64_t>(now), seg_bytes, seg);
        mock_store->store[fmt::format("{}_{}_{}_{}",  rev_key_prefix, partition_id, high_hex, low_hex)]
            = worker;
    }

    ASSERT_EQ(cache.getKeyCount(), static_cast<size_t>(num_segs));
    ASSERT_EQ(mock_store->store.size(), static_cast<size_t>(num_segs * 2));  // fwd + rev per segment

    cache.drop(uuid + "/" + part);
    ASSERT_EQ(cache.getKeyCount(), 0u);

    // Flush pending evictPart ops: detach fdb_idx from cache then destroy it.
    // The destructor sets stopped=true, drains the queue, and joins the bg thread.
    cache.setFDBIndex(nullptr);
    fdb_idx.reset();

    EXPECT_TRUE(mock_store->store.empty())
        << "FDB entries not cleaned after drop(); remaining=" << mock_store->store.size();
}

} // namespace DB
