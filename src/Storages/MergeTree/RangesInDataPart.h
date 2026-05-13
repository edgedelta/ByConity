#pragma once

#include <memory>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MarkRange.h>
#include <roaring.hh>

namespace DB
{

/// GIN index coverage: when a GIN condition covers a single column with only positive atoms,
/// the PREWHERE reader can skip reading that column and inject a dummy value instead.
struct GinIndexCoverage
{
    String source_column;
    String dummy_value;
};

struct RangesInDataPart
{
    MergeTreeData::DataPartPtr data_part;
    size_t part_index_in_query;
    MarkRanges ranges;
    std::shared_ptr<roaring::Roaring> filter_bitmap;
    std::vector<GinIndexCoverage> gin_coverage;

    RangesInDataPart() = default;

    RangesInDataPart(const MergeTreeData::DataPartPtr & data_part_, const size_t part_index_in_query_,
                     const MarkRanges & ranges_ = MarkRanges{})
        : data_part{data_part_}, part_index_in_query{part_index_in_query_}, ranges{ranges_}
    {
    }

    size_t getMarksCount() const
    {
        size_t total = 0;
        for (const auto & range : ranges)
            total += range.end - range.begin;

        return total;
    }

    size_t getRowsCount() const
    {
        return data_part->index_granularity.getRowsCountInRanges(ranges);
    }
};

using RangesInDataParts = std::vector<RangesInDataPart>;

}
