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
    // parsePartitionTimestamp expects a full segment key: uuid/part_name/col.bin/offset

    // YYYYMMDD format (20240315)
    {
        String seg = "uuid/20240315_1_100_2/col.bin/offset_0";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(seg);
        ASSERT_GT(ts, 0);

        struct tm tm_time;
        gmtime_r(&ts, &tm_time);
        ASSERT_EQ(tm_time.tm_year + 1900, 2024);
        ASSERT_EQ(tm_time.tm_mon + 1, 3);
        ASSERT_EQ(tm_time.tm_mday, 15);
    }

    // YYYYMMDDHH format (2024031523)
    {
        String seg = "uuid/2024031523_1_100_2/col.bin/offset_0";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(seg);
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
        String seg = "uuid/202403_1_100_2/col.bin/offset_0";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(seg);
        ASSERT_GT(ts, 0);

        struct tm tm_time;
        gmtime_r(&ts, &tm_time);
        ASSERT_EQ(tm_time.tm_year + 1900, 2024);
        ASSERT_EQ(tm_time.tm_mon + 1, 3);
        ASSERT_EQ(tm_time.tm_mday, 1);
    }

    // Non-time partition (string partition)
    {
        String seg = "uuid/some_partition_1_100_2/col.bin/offset_0";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(seg);
        ASSERT_EQ(ts, 0);
    }

    // Invalid format
    {
        String seg = "uuid/999_1_100_2/col.bin/offset_0";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(seg);
        ASSERT_EQ(ts, 0);
    }

    // Empty partition
    {
        String seg = "uuid/_1_100_2/col.bin/offset_0";
        time_t ts = DiskCacheTTL::parsePartitionTimestamp(seg);
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
        cache.set(seg, buf, data.size(), false, recent_time);

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
        cache.set(recent_seg, buffer, test_data.size(), false, now);

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

    // Large TTL so we can insert entries with old max_time, then shrink to test eviction
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, 60 * 24 * 365, 0);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);

    // Different _N_N_N suffixes → different part_name hashes (different hash_highs)
    // so evictExpired targets them independently
    String recent_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_2_200_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    String recent_seg = fmt::format("test-uuid-0000-0000-0000-000000000005/{}/column.bin/offset_0", recent_part);
    String old_seg    = fmt::format("test-uuid-0000-0000-0000-000000000005/{}/column.bin/offset_0", old_part);

    {
        String data = "test data";
        ReadBufferFromString buf(data);
        cache.set(recent_seg, buf, data.size(), false, now);
    }
    {
        String data = "test data";
        ReadBufferFromString buf(data);
        cache.set(old_seg, buf, data.size(), false, now - 7200);
    }

    ASSERT_EQ(cache.getKeyCount(), 2);

    // Shrink TTL to 60 min: old entry's max_time (now-7200 > 3600s) is now expired
    cache.updateSettings(60, 0);
    cache.evictExpired();

    ASSERT_EQ(cache.getKeyCount(), 1);

    auto [disk1, path1] = cache.get(recent_seg);
    auto [disk2, path2] = cache.get(old_seg);
    ASSERT_FALSE(path1.empty());
    ASSERT_TRUE(path2.empty());
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
            cache.set(seg_name, buffer, data.size(), false, now);

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
        cache.set(seg1, buf1, data.size(), false, now);
        cache.set(seg2, buf2, data.size(), false, now);
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
        cache.set(seg_name, buffer, data.size(), false, now);
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
        cache.set(seg_name, buffer, data.size(), false, now);
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
    DiskCacheTTL cache("test-cache", "test-uuid-0000-0000-0000-00000000000b", volume, nullptr, settings, strategy, ttl_minutes, 0);

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
        cache.set(seg, buf, data.size(), false, recent_time);
    }

    // Try to add old entries. Pass an explicit old max_time, exactly as production does
    struct tm tm_old;
    time_t old_time = now - (48 * 60 * 60);
    gmtime_r(&old_time, &tm_old);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);

    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000b/{}/col.bin/offset_{}", old_part, i);
        String data = String(100, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, old_time);
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

    // Large TTL so we can insert entries with old max_time, then shrink to test eviction
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, 60 * 24 * 365, 0);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);

    // Different suffixes → different hash_highs so evictExpired targets independently
    String recent_part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
    String old_part = fmt::format("{:04d}{:02d}{:02d}_2_200_2",
        tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000c/{}/col.bin/offset_{}", recent_part, i);
        String data = "test data";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, now - 1800);
    }

    for (int i = 0; i < 2; i++)
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000c/{}/col.bin/offset_{}", old_part, i);
        String data = "test data";
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, now - 7200);
    }

    auto stats_before = cache.getStats();
    ASSERT_EQ(stats_before.total_entries, 5u);

    // Shrink TTL to 60 min: old entries' max_time (now-7200 > 3600s) are now expired
    cache.updateSettings(60, 0);
    cache.evictExpired();

    auto stats_after = cache.getStats();
    ASSERT_LT(stats_after.total_entries, stats_before.total_entries);
    ASSERT_GT(stats_after.evicted_expired, 0u);
    ASSERT_GT(stats_after.last_eviction_run, 0u);
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
        cache.set(seg, buf, data.size(), false, now);
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
        cache.set(seg, buf, data.size(), false, now);
    }

    auto stats_before = cache.getStats();
    ASSERT_GT(stats_before.total_bytes, settings.ttl_cache_max_size * 0.90);
    ASSERT_EQ(stats_before.async_eviction_triggered, 0);

    // Add one more segment - pushes over the cap and triggers async eviction.
    {
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000e/{}/col.bin/offset_trigger", part);

        String data = String(segment_size, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, now);
    }

    // Check that async eviction was triggered
    auto stats_after = cache.getStats();
    ASSERT_EQ(stats_after.async_eviction_triggered, 1);

    // Wait for the async eviction to complete, then verify it freed space.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto stats_final = cache.getStats();
    ASSERT_GT(stats_final.evicted_size_limit, 0);

    // Now exercise the 10s trigger rate-limit deterministically: re-fill past the cap again. We're
    // still well within 10s of the first trigger, so each set that finds total_size>cap must be
    // counted as skipped (rate-limited), NOT as a second trigger. (Doing this after eviction has
    // settled avoids racing the evict pool — with a single part it frees everything in ~0ms.)
    for (int i = 0; i < segments_to_add + 1; i++)
    {
        String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2",
            tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
        String seg = fmt::format("test-uuid-0000-0000-0000-00000000000e/{}/col.bin/offset_refill_{}", part, i);

        String data = String(segment_size, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, now);
    }

    // Still exactly one trigger; the re-fill over the cap was rate-limited, not re-triggered.
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
        cache.set(seg, buf, data.size(), false, recent_max_time);

        auto [disk, path] = cache.get(seg);
        ASSERT_FALSE(path.empty()); // Should BE cached (explicit max_time is recent)
    }

    // Test 3: With explicit max_time (old) - should be rejected
    {
        String seg = fmt::format("test-uuid-0000-0000-0000-000000000010/{}/col.bin/offset_2", old_part);
        String data = "test3";
        ReadBufferFromString buf(data);

        time_t old_max_time = now - (90 * 60); // 90 minutes ago (outside TTL)
        cache.set(seg, buf, data.size(), false, old_max_time);

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
        cache.set(seg, buf, data.size(), false, now); // is_preload=false

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
        cache.set(seg, buf, data.size(), true, now); // is_preload=true

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
        cache.set(seg, buf, data.size(), false, now); // is_preload=false

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
        cache.set(seg, buf, data.size(), false, now);
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

        // Fill past the 1MB per-table cap (12 * 100KB = 1200KB > 1MB). Size eviction triggers
        // when total_size exceeds max_size_bytes, so we must exceed the cap, not just approach it.
        size_t segment_size = 100 * 1024;  // 100KB per segment
        for (int i = 0; i < 12; i++)
        {
            String seg = fmt::format("test-uuid-0000-0000-0000-000000000013/{}/col.bin/offset_{}", part, i);
            String data = String(segment_size, 'a');
            ReadBufferFromString buf(data);
            cache.set(seg, buf, data.size(), false, now);
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

        // Fill past the 10MB worker-level cap (11 * 1MB = 11MB > 10MB) to trigger size eviction.
        size_t segment_size = 1024 * 1024;  // 1MB per segment
        for (int i = 0; i < 11; i++)
        {
            String seg = fmt::format("test-uuid-0000-0000-0000-000000000014/{}/col.bin/offset_{}", part, i);
            String data = String(segment_size, 'b');
            ReadBufferFromString buf(data);
            cache.set(seg, buf, data.size(), false, now);
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
            cache.set(seg, buf, data.size(), false, now);
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
    IteratorPtr getByPrefix(const String & prefix, const size_t & limit = 0, uint32_t = 0, const String & start_key = "") override
    {
        auto iter = std::make_shared<MockIterator>();
        for (auto & [k, v] : store)
        {
            if (!k.starts_with(prefix))
                continue;
            // start_key is inclusive (FIRST_GREATER_OR_EQUAL semantics for first batch)
            if (!start_key.empty() && k < start_key)
                continue;
            iter->entries.emplace_back(k, v);
            if (limit > 0 && iter->entries.size() >= limit)
                break;
        }
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
    cache.set(seg, buf, data.size(), false, time(nullptr));

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
    cache.set(seg, buf, data.size(), false, time(nullptr));

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
    // Large TTL so we can insert old entry, then shrink to 1 min to test eviction
    DiskCacheTTL cache("test-cache", "test-uuid", volume, nullptr, settings, strategy, 60 * 24 * 365, 0);

    time_t old_ts = time(nullptr) - 2 * 24 * 3600;
    String seg = makeSegKey("aaaa-bbbb", expiredPart(), "col", p.ext);
    String data = "payload";
    ReadBufferFromString buf(data);
    cache.set(seg, buf, data.size(), false, old_ts);

    ASSERT_EQ(cache.getKeyCount(), 1);
    auto [disk, path] = cache.get(seg);
    ASSERT_TRUE(disk && disk->exists(path)) << "file should exist before eviction: " << path;

    cache.updateSettings(1, 0);  // shrink TTL to 1 min: 2-day-old entry is now expired
    cache.evictExpired();

    EXPECT_EQ(cache.getKeyCount(), 0);
    EXPECT_FALSE(disk->exists(path))
        << p.ext << " file still on disk after eviction — rel_path prefix bug? path=" << path;
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
        cache.set(seg, buf, data.size(), false, now);
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
    DiskCacheTTL cache("test_size_pstats", "test-uuid-size", volume, nullptr, settings, strategy, 60 * 24 * 2, 100 * 1024 * 1024);

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
        cache.set(seg, buf, data.size(), false, yesterday);
    }
    for (int i = 0; i < 3; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, today_part, i);
        String data(seg_size, 't');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, now);
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
        bool found_yest = false;
        bool found_today = false;
        for (const auto & ps : pstats)
        {
            if (ps.partition_id == yest_pid)  found_yest = true;
            if (ps.partition_id == today_pid) { found_today = true; ASSERT_EQ(ps.entry_count, 3u) << "today partition should be untouched"; }
        }
        ASSERT_FALSE(found_yest)  << "yesterday partition should be removed from stats after full eviction";
        ASSERT_TRUE(found_today)  << "today partition should still be present";
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
// drop() evicts the DCIREV reverse entries for the dropped part (reverse-only;
// there is no forward index).
// ---------------------------------------------------------------------------

TEST_F(DiskCacheTTLTest, DropEvictsFDBReverseEntries)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    // No underscores in these strings so Catalog::escapeString is a no-op
    const String uuid   = "test-uuid-fdb";
    const String ns     = "byconity";
    const String worker = "test-worker";
    const String rev_key_prefix = ns + "_DCIREV_" + uuid;

    auto mock_store = std::make_shared<MockMetaStore>();
    auto fdb_idx = std::make_shared<TTLCacheFDBIndex>(mock_store, ns, uuid, worker);

    DiskCacheTTL cache("test_fdb_drop", uuid, volume, nullptr, settings, strategy, 60 * 24 * 365, 0);
    cache.setFDBIndex(fdb_idx);

    time_t now = time(nullptr);
    struct tm tm_now;
    gmtime_r(&now, &tm_now);
    String part = fmt::format("{:04d}{:02d}{:02d}_1_100_2", tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);
    String partition_id = fmt::format("{:04d}{:02d}{:02d}", tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday);

    const size_t seg_bytes = 64;
    const int num_segs = 3;  // same part_name → same hash_high → one evictPart prefix covers all

    for (int i = 0; i < num_segs; i++)
    {
        String seg = makeSegKey(uuid, part, fmt::format("col{}", i), ".bin");
        String data(seg_bytes, 'a');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, now);

        // Seed the DCIREV reverse entry manually (MockMetaStore batchWrite is a no-op for puts).
        // hexKey layout: first 16 chars = hex(items[1]=low), last 16 = hex(items[0]=high).
        auto key  = DiskCacheTTL::hash(seg);
        auto hex  = DiskCacheTTL::hexKey(key);
        String high_hex = hex.substr(16, 16);   // items[0] = sipHash64(part_name)
        String low_hex  = hex.substr(0, 16);    // items[1] = sipHash64(column)
        mock_store->store[fmt::format("{}_{}_{}_{}", rev_key_prefix, partition_id, high_hex, low_hex)]
            = worker + ":1";
    }

    ASSERT_EQ(cache.getKeyCount(), static_cast<size_t>(num_segs));
    ASSERT_EQ(mock_store->store.size(), static_cast<size_t>(num_segs));  // reverse-only

    cache.drop(uuid + "/" + part);
    ASSERT_EQ(cache.getKeyCount(), 0u);

    // Flush the pending evictPart op: detach fdb_idx from cache then destroy it.
    // The destructor sets stopped=true, drains the queue, and joins the bg thread.
    cache.setFDBIndex(nullptr);
    fdb_idx.reset();

    EXPECT_TRUE(mock_store->store.empty())
        << "DCIREV entries not cleaned after drop(); remaining=" << mock_store->store.size();
}

// Verify evictOldestPartitionsUntilSpace evicts multiple partitions oldest-first
// and leaves the newest partition intact. Tests the single-pass shard scan logic.
TEST_F(DiskCacheTTLTest, SizeLimitEvictionMultiplePartitions)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 10 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    DiskCacheTTL cache("test_multi_part_evict", "test-uuid-mpe", volume, nullptr, settings, strategy, 60 * 24 * 30, 0);

    time_t now = time(nullptr);
    // Three partitions: 3 days ago, 2 days ago, today
    time_t t_old  = now - 3 * 24 * 3600;
    time_t t_mid  = now - 2 * 24 * 3600;
    time_t t_new  = now;

    struct tm tm_old, tm_mid, tm_new;
    gmtime_r(&t_old, &tm_old);
    gmtime_r(&t_mid, &tm_mid);
    gmtime_r(&t_new, &tm_new);

    String pid_old = fmt::format("{:04d}{:02d}{:02d}", tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);
    String pid_mid = fmt::format("{:04d}{:02d}{:02d}", tm_mid.tm_year + 1900, tm_mid.tm_mon + 1, tm_mid.tm_mday);
    String pid_new = fmt::format("{:04d}{:02d}{:02d}", tm_new.tm_year + 1900, tm_new.tm_mon + 1, tm_new.tm_mday);

    if (pid_old == pid_mid || pid_mid == pid_new)
        GTEST_SKIP() << "test requires three distinct calendar days (running near midnight boundary)";

    const String uuid = "test-uuid-mpe";
    const size_t seg_size = 512;

    // 2 segments in oldest partition
    for (int i = 0; i < 2; i++)
    {
        String part = fmt::format("{}_{}_200_2", pid_old, i + 1);
        String seg  = fmt::format("{}/{}/col.bin/0", uuid, part);
        String data(seg_size, 'o');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, t_old);
    }
    // 3 segments in middle partition
    for (int i = 0; i < 3; i++)
    {
        String part = fmt::format("{}_{}_200_2", pid_mid, i + 1);
        String seg  = fmt::format("{}/{}/col.bin/0", uuid, part);
        String data(seg_size, 'm');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, t_mid);
    }
    // 4 segments in newest partition
    for (int i = 0; i < 4; i++)
    {
        String part = fmt::format("{}_{}_200_2", pid_new, i + 1);
        String seg  = fmt::format("{}/{}/col.bin/0", uuid, part);
        String data(seg_size, 'n');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, t_new);
    }

    ASSERT_EQ(cache.getKeyCount(), 9u);

    // Evict enough to free the 2 oldest partitions (2 + 3 = 5 segments = 5 * seg_size bytes).
    // Request slightly more than the middle partition alone to force both old + mid to be evicted.
    cache.evictOldestPartitionsUntilSpace(5 * seg_size);

    ASSERT_EQ(cache.getKeyCount(), 4u) << "only newest partition should remain";

    auto gstats = cache.getStats();
    ASSERT_EQ(gstats.total_entries,      4u);
    ASSERT_EQ(gstats.total_bytes,        4 * seg_size);
    ASSERT_EQ(gstats.evicted_size_limit, 5u);

    auto pstats = cache.getPartitionStats();
    bool found_old = false, found_mid = false, found_new = false;
    for (const auto & ps : pstats)
    {
        if (ps.partition_id == pid_old) found_old = true;
        if (ps.partition_id == pid_mid) found_mid = true;
        if (ps.partition_id == pid_new) { found_new = true; ASSERT_EQ(ps.entry_count, 4u); }
    }
    ASSERT_FALSE(found_old) << "oldest partition should be evicted";
    ASSERT_FALSE(found_mid) << "middle partition should be evicted";
    ASSERT_TRUE(found_new)  << "newest partition should remain";
}

// Race A guard: size-eviction removes only the exact files it tracked, never the whole
// partition directory. A file that the eviction snapshot doesn't know about (e.g. one a
// concurrent set() just wrote into the same partition dir) must survive.
TEST_F(DiskCacheTTLTest, SizeEvictionPreservesUntrackedFilesInPartitionDir)
{
    auto volume = createTestVolume();
    DiskCacheSettings settings;
    settings.ttl_cache_max_size = 10 * 1024 * 1024;
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);
    const String uuid = "test-uuid-stray";
    DiskCacheTTL cache("test_evict_stray", uuid, volume, nullptr, settings, strategy, 60 * 24 * 30, 0);

    time_t now = time(nullptr);
    time_t t_old = now - 3 * 24 * 3600;
    struct tm tm_old, tm_new;
    gmtime_r(&t_old, &tm_old);
    gmtime_r(&now, &tm_new);
    String pid_old = fmt::format("{:04d}{:02d}{:02d}", tm_old.tm_year + 1900, tm_old.tm_mon + 1, tm_old.tm_mday);
    String pid_new = fmt::format("{:04d}{:02d}{:02d}", tm_new.tm_year + 1900, tm_new.tm_mon + 1, tm_new.tm_mday);
    if (pid_old == pid_new)
        GTEST_SKIP() << "test requires two distinct calendar days (running near midnight boundary)";

    const size_t seg_size = 512;

    // Old partition (one part, 2 segments) — will be evicted.
    String old_part = fmt::format("{}_1_100_2", pid_old);
    std::vector<String> old_segs;
    for (int i = 0; i < 2; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, old_part, i);
        old_segs.push_back(seg);
        String data(seg_size, 'o');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, t_old);
    }
    // New partition — kept.
    String new_part = fmt::format("{}_1_100_2", pid_new);
    for (int i = 0; i < 2; i++)
    {
        String seg = fmt::format("{}/{}/col.bin/offset_{}", uuid, new_part, i);
        String data(seg_size, 'n');
        ReadBufferFromString buf(data);
        cache.set(seg, buf, data.size(), false, now);
    }
    ASSERT_EQ(cache.getKeyCount(), 4u);

    // Locate the old partition's on-disk dir from a cached segment's path:
    // <...>/data/<uuid>/<pid_old>/<hex3>/<hexhigh>/<hexlow> → partition dir is 3 levels up.
    auto [disk, old_seg_path] = cache.get(old_segs[0]);
    ASSERT_TRUE(disk && disk->exists(old_seg_path));
    fs::path pid_dir = fs::path(old_seg_path).parent_path().parent_path().parent_path();

    // Drop an untracked file straight into the partition dir.
    String stray = (pid_dir / "stray.bin").string();
    {
        auto wb = disk->writeFile(stray);
        wb->write("stray", 5);
        wb->finalize();
    }
    ASSERT_TRUE(disk->exists(stray));

    // Evict the old partition (free its 2 segments).
    cache.evictOldestPartitionsUntilSpace(2 * seg_size);

    EXPECT_EQ(cache.getKeyCount(), 2u) << "old partition evicted, new partition kept";
    for (const auto & seg : old_segs)
    {
        auto [d, p] = cache.get(seg);
        EXPECT_TRUE(p.empty()) << "old segment should be evicted: " << seg;
    }
    // The untracked file survives — exact-file removal never touched it.
    EXPECT_TRUE(disk->exists(stray)) << "untracked file in partition dir must survive exact-file eviction";
}

} // namespace DB
