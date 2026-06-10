#include <gtest/gtest.h>

#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>
#include <Storages/MergeTree/MarkRange.h>

using namespace DB;

namespace
{
/// Exposes the protected segment-number derivation so the test can exercise it directly.
struct ExposedStrategy : public DiskCacheSimpleStrategy
{
    using DiskCacheSimpleStrategy::DiskCacheSimpleStrategy;
    using IDiskCacheStrategy::transferRangesToSegmentNumbers;
};

std::shared_ptr<ExposedStrategy> makeStrategy(size_t segment_size)
{
    DiskCacheSettings s;
    s.segment_size = segment_size;
    return std::make_shared<ExposedStrategy>(s);
}

/// Number of skip-index marks for a part with `data_marks` data granules and a skip index of
/// the given GRANULARITY (matches MergeTreeDataPartCNCH preload: marks_file_size / mark_size,
/// i.e. one index mark per GRANULARITY data marks).
size_t indexMarks(size_t data_marks, size_t granularity)
{
    return (data_marks + granularity - 1) / granularity;
}
}

/// A skip-index cache segment is keyed by `segment_number = mark / segment_size`
/// (IDiskCacheStrategy::transferRangesToSegmentNumbers). The two code paths that touch a skip
/// index feed that primitive ranges in *different mark scales*:
///
///   PRELOAD  (MergeTreeDataPartCNCH::preload)  -> index-mark scale: MarkRange(0, skip_index_marks_count)
///   READ     (MergeTreeIndexReader)            -> data-granule scale: the query's data MarkRanges
///
/// For a skip index with GRANULARITY > 1 these scales disagree, so the segment numbers the read
/// path looks up are not the ones preload wrote -> the preloaded index segments are never found
/// (observed in prod: idx_count_preload high, idx_hits ~0). Data columns are immune because they
/// have no GRANULARITY divisor (read and preload both use data marks).
///
/// This test models the exact inputs both production paths pass and asserts they agree.
/// It FAILS on the current code, confirming the index-segment cache-key seam.
TEST(IndexSegmentKeyScale, PreloadAndReadKeysAgreeForGranularityGreaterThanOne)
{
    const size_t segment_size = 4; // hybrid-aligned segment size, in data-mark units
    const size_t data_marks = 32;
    const size_t granularity = 4; // e.g. body.idx GRANULARITY 4

    auto strat = makeStrategy(segment_size);

    // PRELOAD keys: full index range in index-mark scale.
    auto preload_keys = strat->transferRangesToSegmentNumbers(
        MarkRanges{MarkRange(0, indexMarks(data_marks, granularity))}); // {0, 1}

    // READ keys (current behaviour): data-granule scale.
    auto read_keys = strat->transferRangesToSegmentNumbers(
        MarkRanges{MarkRange(0, data_marks)}); // {0,1,2,3,4,5,6,7}

    EXPECT_EQ(read_keys, preload_keys)
        << "skip-index cache keys diverge: preload writes index-mark-scale segment numbers, "
           "the read path looks up data-mark-scale segment numbers; preloaded index segments "
           "are never hit. Fix: scale the read path's data ranges down by GRANULARITY before keying.";
}

/// Control: data columns (GRANULARITY == 1) — both paths use data marks, so keys already match.
/// This passes today and isolates the defect to skip indexes.
TEST(IndexSegmentKeyScale, DataColumnKeysAlreadyAgree)
{
    const size_t segment_size = 4;
    const size_t data_marks = 32;

    auto strat = makeStrategy(segment_size);
    auto preload_keys = strat->transferRangesToSegmentNumbers(MarkRanges{MarkRange(0, data_marks)});
    auto read_keys = strat->transferRangesToSegmentNumbers(MarkRanges{MarkRange(0, data_marks)});
    EXPECT_EQ(read_keys, preload_keys);
}
