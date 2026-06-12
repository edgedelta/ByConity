#include <gtest/gtest.h>

#include <Interpreters/GinFilter.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Storages/MergeTree/GinIndexDataPartHelper.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

namespace DB
{
namespace
{

// ===== Helpers =====

GinIndexPostingsListPtr makeBitmap(std::vector<UInt32> rows)
{
    auto b = std::make_shared<GinIndexPostingsList>();
    for (UInt32 r : rows)
        b->add(r);
    return b;
}

GinIndexPostingsListPtr containsAllBitmap()
{
    auto b = std::make_shared<GinIndexPostingsList>();
    b->add(UINT32_MAX);
    return b;
}

struct PostingEntry
{
    std::string term;
    UInt32 seg_id;
    GinIndexPostingsListPtr bitmap;
};

GinPostingsCachePtr makePostings(const std::vector<PostingEntry> & entries)
{
    auto cache = std::make_shared<GinPostingsCache>();
    for (const auto & e : entries)
        (*cache)[e.term][e.seg_id] = e.bitmap;
    return cache;
}

// Build the index-side GinFilter (carries row ranges from the skip index).
GinFilter makeIndexFilter(UInt32 seg_id, UInt32 range_start, UInt32 range_end)
{
    GinFilterParameters params(0, 0.01);
    GinFilter f(params);
    f.addRowRangeToGinFilter(seg_id, range_start, range_end);
    return f;
}

// Build the query-side GinFilter (carries query string + tokenised terms).
GinFilter makeQueryFilter(const std::string & query, const std::vector<std::string> & terms)
{
    GinFilterParameters params(0, 0.01);
    GinFilter f(params);
    f.setQueryString(query.data(), query.size());
    for (const auto & t : terms)
        f.addTerm(t.data(), t.size());
    return f;
}

// ===== matchInRange tests (exercised via GinFilter::contains with pre-filled cache) =====

TEST(GinFilter_MatchInRange, SingleTerm_Match)
{
    auto idx = makeIndexFilter(0, 0, 99);
    auto qry = makeQueryFilter("hello", {"hello"});

    PostingsCacheForStore cs;
    cs.cache["hello"] = makePostings({{"hello", 0, makeBitmap({10, 20, 30})}});

    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
    EXPECT_EQ(result.cardinality(), 3u);
    EXPECT_TRUE(result.contains(10));
    EXPECT_TRUE(result.contains(20));
    EXPECT_TRUE(result.contains(30));
}

TEST(GinFilter_MatchInRange, SingleTerm_OutOfRange)
{
    auto idx = makeIndexFilter(0, 0, 9);
    auto qry = makeQueryFilter("hello", {"hello"});

    PostingsCacheForStore cs;
    cs.cache["hello"] = makePostings({{"hello", 0, makeBitmap({50, 60})}});

    roaring::Roaring result;
    EXPECT_FALSE(idx.contains(qry, cs, result));
}

TEST(GinFilter_MatchInRange, SingleTerm_ContainsAll)
{
    auto idx = makeIndexFilter(0, 5, 14);
    auto qry = makeQueryFilter("hello", {"hello"});

    PostingsCacheForStore cs;
    cs.cache["hello"] = makePostings({{"hello", 0, containsAllBitmap()}});

    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
    EXPECT_EQ(result.cardinality(), 10u); // rows 5..14 inclusive
    EXPECT_TRUE(result.contains(5));
    EXPECT_TRUE(result.contains(14));
}

TEST(GinFilter_MatchInRange, MultiTerm_And_NonEmptyIntersection)
{
    auto idx = makeIndexFilter(0, 0, 99);
    auto qry = makeQueryFilter("hello world", {"hello", "world"});

    // hello:{10,20,30} ∩ world:{20,30,40} = {20,30}
    PostingsCacheForStore cs;
    cs.cache["hello world"] = makePostings({
        {"hello", 0, makeBitmap({10, 20, 30})},
        {"world", 0, makeBitmap({20, 30, 40})},
    });

    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
    EXPECT_EQ(result.cardinality(), 2u);
    EXPECT_TRUE(result.contains(20));
    EXPECT_TRUE(result.contains(30));
}

TEST(GinFilter_MatchInRange, MultiTerm_And_EmptyIntersection)
{
    auto idx = makeIndexFilter(0, 0, 99);
    auto qry = makeQueryFilter("hello world", {"hello", "world"});

    PostingsCacheForStore cs;
    cs.cache["hello world"] = makePostings({
        {"hello", 0, makeBitmap({10, 20})},
        {"world", 0, makeBitmap({30, 40})},
    });

    roaring::Roaring result;
    EXPECT_FALSE(idx.contains(qry, cs, result));
}

TEST(GinFilter_MatchInRange, TermAbsentInSegment)
{
    auto idx = makeIndexFilter(1, 0, 99); // segment 1
    auto qry = makeQueryFilter("hello", {"hello"});

    PostingsCacheForStore cs;
    cs.cache["hello"] = makePostings({{"hello", 0, makeBitmap({10, 20})}}); // only seg 0

    roaring::Roaring result;
    EXPECT_FALSE(idx.contains(qry, cs, result));
}

TEST(GinFilter_MatchInRange, AllContainsAll_ReturnsFullRange)
{
    auto idx = makeIndexFilter(0, 100, 199);
    auto qry = makeQueryFilter("hello world", {"hello", "world"});

    PostingsCacheForStore cs;
    cs.cache["hello world"] = makePostings({
        {"hello", 0, containsAllBitmap()},
        {"world", 0, containsAllBitmap()},
    });

    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
    EXPECT_EQ(result.cardinality(), 100u); // 100..199 inclusive
    EXPECT_TRUE(result.contains(100));
    EXPECT_TRUE(result.contains(199));
}

TEST(GinFilter_MatchInRange, RareTermFirst_EarlyPrune)
{
    // Rare term (1 row) should be processed first; intersection collapses immediately
    // when the common term doesn't include that row.
    auto idx = makeIndexFilter(0, 0, 999);
    auto qry = makeQueryFilter("rare common", {"rare", "common"});

    // "common" has 900 rows, "rare" has 1 row (row 5) — but "common" doesn't include row 5
    auto common_bitmap = makeBitmap({});
    for (UInt32 i = 10; i < 910; ++i)
        common_bitmap->add(i);

    PostingsCacheForStore cs;
    cs.cache["rare common"] = makePostings({
        {"rare",   0, makeBitmap({5})},
        {"common", 0, common_bitmap},
    });

    roaring::Roaring result;
    EXPECT_FALSE(idx.contains(qry, cs, result));
}

// ===== Additional matchInRange coverage =====

TEST(GinFilter_MatchInRange, MixedContainsAll_RealBitmapDeterminesResult)
{
    // ContainsAll sorts last and is skipped; real term drives the intersection.
    auto idx = makeIndexFilter(0, 0, 99);
    auto qry = makeQueryFilter("hello world", {"hello", "world"});

    PostingsCacheForStore cs;
    cs.cache["hello world"] = makePostings({
        {"hello", 0, containsAllBitmap()},
        {"world", 0, makeBitmap({20, 30, 40})},
    });

    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
    EXPECT_EQ(result.cardinality(), 3u);
    EXPECT_TRUE(result.contains(20));
    EXPECT_TRUE(result.contains(30));
    EXPECT_TRUE(result.contains(40));
}

TEST(GinFilter_MatchInRange, NineTerms_HeapBufPath)
{
    // 9 terms exceeds stack_buf[8] and forces heap allocation.
    auto idx = makeIndexFilter(0, 0, 99);
    std::vector<std::string> terms;
    for (int i = 0; i < 9; ++i)
        terms.push_back("term" + std::to_string(i));
    auto qry = makeQueryFilter("query", terms);

    std::vector<PostingEntry> entries;
    for (int i = 0; i < 9; ++i)
        entries.push_back({"term" + std::to_string(i), 0, makeBitmap({10, 20})});

    PostingsCacheForStore cs;
    cs.cache["query"] = makePostings(entries);

    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
    EXPECT_EQ(result.cardinality(), 2u);
    EXPECT_TRUE(result.contains(10));
    EXPECT_TRUE(result.contains(20));
}

TEST(GinFilter_MatchInRange, SingleRowRange_Match)
{
    auto idx = makeIndexFilter(0, 5, 5);
    auto qry = makeQueryFilter("hello", {"hello"});

    PostingsCacheForStore cs;
    cs.cache["hello"] = makePostings({{"hello", 0, makeBitmap({5})}});

    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
    EXPECT_EQ(result.cardinality(), 1u);
    EXPECT_TRUE(result.contains(5));
}

TEST(GinFilter_MatchInRange, SingleRowRange_Miss)
{
    auto idx = makeIndexFilter(0, 5, 5);
    auto qry = makeQueryFilter("hello", {"hello"});

    PostingsCacheForStore cs;
    cs.cache["hello"] = makePostings({{"hello", 0, makeBitmap({6})}});

    roaring::Roaring result;
    EXPECT_FALSE(idx.contains(qry, cs, result));
}

// ===== GinFilter::match (multi-range) =====

TEST(GinFilter_Match, MultiSegment_OneMatchOneMiss)
{
    GinFilterParameters params(0, 0.01);
    GinFilter idx(params);
    idx.addRowRangeToGinFilter(0, 0, 99);
    idx.addRowRangeToGinFilter(1, 0, 99);

    auto qry = makeQueryFilter("hello", {"hello"});

    // No seg 1 postings → seg 1 range misses, seg 0 still matches.
    PostingsCacheForStore cs;
    cs.cache["hello"] = makePostings({
        {"hello", 0, makeBitmap({10, 20})},
    });

    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
    EXPECT_EQ(result.cardinality(), 2u);
    EXPECT_TRUE(result.contains(10));
    EXPECT_TRUE(result.contains(20));
}

TEST(GinFilter_Match, AllRangesMiss_ReturnsFalse)
{
    GinFilterParameters params(0, 0.01);
    GinFilter idx(params);
    idx.addRowRangeToGinFilter(0, 0, 9);

    auto qry = makeQueryFilter("hello", {"hello"});

    PostingsCacheForStore cs;
    cs.cache["hello"] = makePostings({{"hello", 0, makeBitmap({50, 60})}});

    roaring::Roaring result;
    EXPECT_FALSE(idx.contains(qry, cs, result));
}

// ===== addRowRangeToGinFilter merging =====

TEST(GinFilter_AddRowRange, ConsecutiveRanges_Merged)
{
    GinFilterParameters params(0, 0.01);
    GinFilter f(params);
    f.addRowRangeToGinFilter(0, 0, 9);
    f.addRowRangeToGinFilter(0, 10, 19);

    EXPECT_EQ(f.getFilter().size(), 1u);
    EXPECT_EQ(f.getFilter()[0].range_end, 19u);
}

TEST(GinFilter_AddRowRange, NonConsecutiveRanges_NotMerged)
{
    GinFilterParameters params(0, 0.01);
    GinFilter f(params);
    f.addRowRangeToGinFilter(0, 0, 9);
    f.addRowRangeToGinFilter(0, 11, 19); // gap at row 10

    EXPECT_EQ(f.getFilter().size(), 2u);
}

TEST(GinFilter_AddRowRange, DifferentSegments_AlwaysSeparate)
{
    GinFilterParameters params(0, 0.01);
    GinFilter f(params);
    f.addRowRangeToGinFilter(0, 0, 9);
    f.addRowRangeToGinFilter(1, 10, 19); // consecutive rows but different segment

    EXPECT_EQ(f.getFilter().size(), 2u);
    EXPECT_EQ(f.getFilter()[0].segment_id, 0u);
    EXPECT_EQ(f.getFilter()[1].segment_id, 1u);
}

// ===== Empty terms =====

TEST(GinFilter_Contains, EmptyTerms_AlwaysTrue)
{
    auto idx = makeIndexFilter(0, 0, 99);
    auto qry = makeQueryFilter("", {});

    PostingsCacheForStore cs;
    roaring::Roaring result;
    EXPECT_TRUE(idx.contains(qry, cs, result));
}

// ===== GinFilter::filpWithRange =====

TEST(GinFilter_FilpWithRange, FlipSetsRange)
{
    GinFilterParameters params(0, 0.01);
    GinFilter f(params);
    f.addRowRangeToGinFilter(0, 0, 9);

    roaring::Roaring result;
    f.filpWithRange(result);

    EXPECT_EQ(result.cardinality(), 10u);
    EXPECT_TRUE(result.contains(0));
    EXPECT_TRUE(result.contains(9));
    EXPECT_FALSE(result.contains(10));
}

TEST(GinFilter_FilpWithRange, FlipTwiceRestoresEmpty)
{
    GinFilterParameters params(0, 0.01);
    GinFilter f(params);
    f.addRowRangeToGinFilter(0, 5, 14);

    roaring::Roaring result;
    f.filpWithRange(result);
    f.filpWithRange(result);

    EXPECT_EQ(result.cardinality(), 0u);
}

} // namespace
} // namespace DB
