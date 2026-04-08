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

#include <Core/UUID.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <gtest/gtest.h>

namespace DB
{
TEST(DiskCache, UnhexKeyTest)
{
    String table_uuid = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    String seg_key = IDiskCacheSegment::formatSegmentName(table_uuid, "part_1", "col", 0, ".bin");

    DiskCacheLRU::KeyType key = DiskCacheLRU::hash(seg_key);
    String hex_key = DiskCacheLRU::hexKey(key);
    auto unhex = DiskCacheLRU::unhexKey(hex_key);
    EXPECT_TRUE(unhex.has_value());
    EXPECT_TRUE(unhex == key);
}

TEST(DiskCache, DiskCachePathTest)
{
    String table_uuid = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    String seg_key1 = IDiskCacheSegment::formatSegmentName(table_uuid, "part_1", "col", 0, ".bin");
    String seg_key2 = IDiskCacheSegment::formatSegmentName(table_uuid, "part_1", "col", 0, ".mrk");

    auto path1 = DiskCacheLRU::getPath(DiskCacheLRU::hash(seg_key1), "disk_cache_v1", "", "");
    auto path2 = DiskCacheLRU::getPath(DiskCacheLRU::hash(seg_key2), "disk_cache_v1", "", "");

    EXPECT_EQ(path1.parent_path(), path2.parent_path());
    EXPECT_NE(path1.filename(), path2.filename());
}

// TTL cache key tests
TEST(DiskCacheTTL, UnhexKeyTest)
{
    String table_uuid = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    String seg_key = IDiskCacheSegment::formatSegmentName(table_uuid, "20240315_1_100_2", "col", 0, ".bin");

    DiskCacheTTL::KeyType key = DiskCacheTTL::hash(seg_key);
    String hex_key = DiskCacheTTL::hexKey(key);
    auto unhex = DiskCacheTTL::unhexKey(hex_key);
    EXPECT_TRUE(unhex.has_value());
    EXPECT_EQ(unhex.value(), key);

    // Invalid hex keys
    EXPECT_FALSE(DiskCacheTTL::unhexKey("invalid").has_value());
    EXPECT_FALSE(DiskCacheTTL::unhexKey("12345").has_value());
    EXPECT_FALSE(DiskCacheTTL::unhexKey("gggggggggggggggggggggggggggggggg").has_value());
}

TEST(DiskCacheTTL, PartitionHierarchyPathTest)
{
    String table_uuid = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    String seg_key1 = IDiskCacheSegment::formatSegmentName(table_uuid, "20240315_1_100_2", "col", 0, ".bin");
    String seg_key2 = IDiskCacheSegment::formatSegmentName(table_uuid, "20240315_1_100_2", "col", 0, ".mrk");
    String seg_key3 = IDiskCacheSegment::formatSegmentName(table_uuid, "20240316_1_100_2", "col", 0, ".bin");

    auto key1 = DiskCacheTTL::hash(seg_key1);
    auto key2 = DiskCacheTTL::hash(seg_key2);
    auto key3 = DiskCacheTTL::hash(seg_key3);

    // Path structure doesn't need cache instance, just use hexKey to verify structure
    String hex1 = DiskCacheTTL::hexKey(key1);
    String hex2 = DiskCacheTTL::hexKey(key2);
    String hex3 = DiskCacheTTL::hexKey(key3);

    // Verify same part -> same hash_high (first half of hex)
    EXPECT_EQ(hex1.substr(16, 16), hex2.substr(16, 16));  // hash_high for same part
    EXPECT_NE(hex1.substr(0, 16), hex2.substr(0, 16));    // hash_low differs (different segments)

    // Different partitions -> different hash_high
    EXPECT_NE(hex1.substr(16, 16), hex3.substr(16, 16));
}

}
