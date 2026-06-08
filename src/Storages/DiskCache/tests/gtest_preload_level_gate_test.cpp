#include <Core/Settings.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>

#include <gtest/gtest.h>

using namespace DB;

/// Preload gates: which payloads a preload at a given level writes for a segment type.
/// Secondary index data is pruning metadata: MetaPreload must include it, consistent
/// with gin/bitmap index segments which fully preload at meta level.

TEST(PreloadLevelGateTest, ReadThroughCachesEverything)
{
    /// preload_level == 0 means query-time read-through: always cache both payloads.
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheData(PreloadLevelSettings::ClosePreload, SegmentType::PART_DATA));
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheData(PreloadLevelSettings::ClosePreload, SegmentType::SENCONDARY_INDEX));
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheMarks(PreloadLevelSettings::ClosePreload));
}

TEST(PreloadLevelGateTest, MetaPreloadExcludesColumnData)
{
    EXPECT_FALSE(IDiskCacheSegment::shouldCacheData(PreloadLevelSettings::MetaPreload, SegmentType::PART_DATA));
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheMarks(PreloadLevelSettings::MetaPreload));
}

TEST(PreloadLevelGateTest, MetaPreloadIncludesSecondaryIndexData)
{
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheData(PreloadLevelSettings::MetaPreload, SegmentType::SENCONDARY_INDEX));
}

TEST(PreloadLevelGateTest, DataPreloadIncludesDataExcludesMarks)
{
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheData(PreloadLevelSettings::DataPreload, SegmentType::PART_DATA));
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheData(PreloadLevelSettings::DataPreload, SegmentType::SENCONDARY_INDEX));
    EXPECT_FALSE(IDiskCacheSegment::shouldCacheMarks(PreloadLevelSettings::DataPreload));
}

TEST(PreloadLevelGateTest, AllPreloadCachesEverything)
{
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheData(PreloadLevelSettings::AllPreload, SegmentType::PART_DATA));
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheData(PreloadLevelSettings::AllPreload, SegmentType::SENCONDARY_INDEX));
    EXPECT_TRUE(IDiskCacheSegment::shouldCacheMarks(PreloadLevelSettings::AllPreload));
}
