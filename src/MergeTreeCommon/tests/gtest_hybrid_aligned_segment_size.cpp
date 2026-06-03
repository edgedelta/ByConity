#include <MergeTreeCommon/assignCnchParts.h>
#include <gtest/gtest.h>

using namespace DB;

/// deriveHybridAlignedSegmentSize must return the same value as the hybrid virtual-part size, so that
/// cache-segment boundaries line up with work-slice boundaries. When they line up, a part sliced across
/// workers has every cache segment owned by exactly one worker -> no cross-worker cache duplication.

TEST(HybridAlignedSegmentSize, DisabledOrInvalidReturnsZero)
{
    // Hybrid off -> 0 means "keep the global default segment size".
    EXPECT_EQ(deriveHybridAlignedSegmentSize(false, 10000000, 8192), 0u);
    // Degenerate inputs are treated as "no override".
    EXPECT_EQ(deriveHybridAlignedSegmentSize(true, 0, 8192), 0u);
    EXPECT_EQ(deriveHybridAlignedSegmentSize(true, 10000000, 0), 0u);
}

TEST(HybridAlignedSegmentSize, MatchesVirtualPartSize)
{
    const size_t ig = 8192;
    // ceil(min_rows / index_granularity)
    EXPECT_EQ(deriveHybridAlignedSegmentSize(true, 5000000, ig), 611u);
    EXPECT_EQ(deriveHybridAlignedSegmentSize(true, 10000000, ig), 1221u);
    EXPECT_EQ(deriveHybridAlignedSegmentSize(true, 20000000, ig), 2442u);
    // exact multiple of index_granularity -> no rounding
    EXPECT_EQ(deriveHybridAlignedSegmentSize(true, 67108864, ig), 8192u);
}

TEST(HybridAlignedSegmentSize, NoDuplicationInvariant)
{
    // The whole point: derived segment_size must equal the virtual-part size, so slices align to
    // segments (virtual_part_size % segment_size == 0). Guards against the two drifting apart again.
    const size_t ig = 8192;
    for (size_t min_rows : std::initializer_list<size_t>{1000000, 5000000, 10000000, 20000000, 50000000})
    {
        const size_t seg = deriveHybridAlignedSegmentSize(true, min_rows, ig);
        const size_t vps = computeVirtualPartSize(min_rows, ig);
        ASSERT_GT(seg, 0u) << "min_rows=" << min_rows;
        EXPECT_EQ(seg, vps) << "min_rows=" << min_rows;
        EXPECT_EQ(vps % seg, 0u) << "min_rows=" << min_rows;
    }
}
