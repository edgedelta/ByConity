#include <cstddef>
#include <Interpreters/GinFilter.h>
#include <roaring.hh>
#include <Poco/Logger.h>
#include "common/logger_useful.h"
#include <common/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void printValuesInRoaring(GinIndexPostingsList & post_list)
{
    LOG_TRACE(&Poco::Logger::get(__func__), "post_list.cardinality: {}", post_list.cardinality());
    LOG_TRACE(&Poco::Logger::get(__func__), "post_list.toString: {}", post_list.toString());
}

GinFilterParameters::GinFilterParameters(size_t ngrams_, Float64 density_)
    : ngrams(ngrams_)
    , density(density_)
{
    if (ngrams > 8)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The size of inverted index filter cannot be greater than 8");
    if (density <= 0 || density > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The density inverted index gin filter must be between 0 and 1");

}

GinFilter::GinFilter(const GinFilterParameters & params_)
    : params(params_)
{
}

void GinFilter::add(const char * data, size_t len, UInt32 rowID, GinIndexStorePtr & store) const
{
    if (len > FST::MAX_TERM_LENGTH)
        return;

    StringRef term(data, len);
    auto it = store->getPostingsListBuilder().builders.find(term);

    if (it)
    {
        const GinIndexPostingsBuilderPtr& builder = it.getMapped();
        if (builder->last_row_id != rowID)
        {
            builder->add(rowID);
            builder->last_row_id = rowID;
        }
    }
    else
    {
        auto builder = std::make_shared<GinIndexPostingsBuilder>();
        builder->add(rowID);

        store->setPostingsBuilder(term, builder);
    }
}

/// This method assumes segmentIDs are in increasing order, which is true since rows are
/// digested sequentially and segments are created sequentially too.
void GinFilter::addRowRangeToGinFilter(UInt32 segmentID, UInt32 rowIDStart, UInt32 rowIDEnd)
{
    /// check segment ids are monotonic increasing
    assert(rowid_ranges.empty() || rowid_ranges.back().segment_id <= segmentID);

    if (!rowid_ranges.empty())
    {
        /// Try to merge the rowID range with the last one in the container
        GinSegmentWithRowIdRange & last_rowid_range = rowid_ranges.back();

        if (last_rowid_range.segment_id == segmentID &&
            last_rowid_range.range_end+1 == rowIDStart)
        {
            last_rowid_range.range_end = rowIDEnd;
            return;
        }
    }
    rowid_ranges.push_back({segmentID, rowIDStart, rowIDEnd});
}

void GinFilter::clear()
{
    query_string.clear();
    terms.clear();
    rowid_ranges.clear();
}

bool GinFilter::contains(const GinFilter & filter, PostingsCacheForStore & cache_store, roaring::Roaring & filter_result) const
{
    if (filter.getTerms().empty())
        return true;

    GinPostingsCachePtr postings_cache = cache_store.getPostings(filter.getQueryString());
    if (postings_cache == nullptr)
    {
        GinIndexStoreDeserializer reader(cache_store.store);
        postings_cache = reader.createPostingsCacheFromTerms(filter.getTerms());
        cache_store.cache[filter.getQueryString()] = postings_cache;
    }

    return match(*postings_cache, filter_result);
}

void GinFilter::filpWithRange(roaring::Roaring & result) const
{
    for(const auto & range : rowid_ranges )
    {
        result.flipClosed(range.range_start, range.range_end);
    }
}

size_t GinFilter::getAllRangeSize() const
{
    size_t result = 0 ;
    for(const auto & range : rowid_ranges)
    {
        result += range.range_end - range.range_start + 1;
    }
    return result;
}

namespace
{

/// Helper method for checking if postings list cache is empty
bool hasEmptyPostingsList(const GinPostingsCache & postings_cache)
{
    if (postings_cache.empty())
        return true;

    for (const auto & term_postings : postings_cache)
    {
        const GinSegmentedPostingsListContainer & container = term_postings.second;
        if (container.empty())
            return true;
    }
    return false;
}

/// Sentinel cardinality used to sort CONTAINS_ALL bitmaps last in multi-term AND.
/// A real bitmap can never reach UINT64_MAX cardinality (roaring max elements = 2^32).
static constexpr UInt64 CONTAINS_ALL_CARDINALITY = std::numeric_limits<UInt64>::max();

/// Helper method to check if the postings list cache has intersection with given row ID range
bool matchInRange(const GinPostingsCache & postings_cache, UInt32 segment_id, UInt32 range_start, UInt32 range_end, roaring::Roaring & filter_result)
{
    /// Single-term fast path: skips SegEntry allocation and sort entirely.
    if (postings_cache.size() == 1)
    {
        const auto & term_postings = *postings_cache.begin();
        const auto container_it = term_postings.second.find(segment_id);
        if (container_it == term_postings.second.cend())
            return false;

        const auto & bitmap = container_it->second;
        if (bitmap->cardinality() == 1 && bitmap->minimum() == UINT32_MAX)
        {
            filter_result.addRange(range_start, range_end + 1);
            return true;
        }
        if (range_start > bitmap->maximum() || bitmap->minimum() > range_end)
            return false;
        GinIndexPostingsList intersection_result;
        intersection_result.addRange(range_start, range_end + 1);
        intersection_result &= *bitmap;
        if (intersection_result.cardinality() == 0)
            return false;
        filter_result = std::move(intersection_result);
        return true;
    }

    /// Multi-term AND: sort by ascending cardinality so the rarest term intersects first,
    /// collapsing to zero sooner. CONTAINS_ALL sentinels sort last (never prune).
    struct SegEntry
    {
        GinSegmentedPostingsListContainer::const_iterator it;
        UInt64 cardinality;
    };
    /// Stack storage avoids heap allocation for typical 2–4 term queries.
    SegEntry stack_buf[8];
    std::vector<SegEntry> heap_buf;
    SegEntry * entries;
    size_t n = 0;

    if (postings_cache.size() <= 8)
        entries = stack_buf;
    else
    {
        heap_buf.reserve(postings_cache.size());
        entries = nullptr;
    }

    for (const auto & term_postings : postings_cache)
    {
        const auto container_it = term_postings.second.find(segment_id);
        if (container_it == term_postings.second.cend())
            return false;
        UInt64 card = container_it->second->cardinality();
        if (card == 1 && container_it->second->minimum() == UINT32_MAX)
            card = CONTAINS_ALL_CARDINALITY;
        if (entries)
            entries[n++] = {container_it, card};
        else
            heap_buf.push_back({container_it, card});
    }

    if (!entries)
    {
        entries = heap_buf.data();
        n = heap_buf.size();
    }

    std::sort(entries, entries + n, [](const SegEntry & a, const SegEntry & b) {
        return a.cardinality < b.cardinality;
    });

    GinIndexPostingsList intersection_result;
    bool intersection_result_init = false;

    for (size_t i = 0; i < n; ++i)
    {
        const auto & entry = entries[i];
        if (entry.cardinality == CONTAINS_ALL_CARDINALITY)
            continue;

        auto min_in_container = entry.it->second->minimum();
        auto max_in_container = entry.it->second->maximum();

        if (range_start > max_in_container || min_in_container > range_end)
            return false;

        if (!intersection_result_init)
        {
            intersection_result_init = true;
            intersection_result.addRange(range_start, range_end + 1);
        }

        intersection_result &= *entry.it->second;

        if (intersection_result.cardinality() == 0)
            return false;
    }

    // All terms were CONTAINS_ALL — no real intersection was built.
    // Populate the full range so callers get the correct row IDs.
    if (!intersection_result_init)
    {
        filter_result.addRange(range_start, range_end + 1);
        return true;
    }

    filter_result = std::move(intersection_result);
    return true;
}

}

bool GinFilter::match(const GinPostingsCache & postings_cache , roaring::Roaring & filter_result) const
{
    if (hasEmptyPostingsList(postings_cache))
        return false;

    /// Check for each row ID ranges
    for (const auto & rowid_range: rowid_ranges)
        if (matchInRange(postings_cache, rowid_range.segment_id, rowid_range.range_start, rowid_range.range_end, filter_result))
            return true;
    return false;
}

String GinFilter::getTermsInString() const
{
    String result;
    for (const String & term : terms)
    {
        result += " " + term;
    }
    return result;
}

}
