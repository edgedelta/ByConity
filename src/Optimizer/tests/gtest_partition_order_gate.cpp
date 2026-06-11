#include <gtest/gtest.h>

#include <Optimizer/PartitionOrderGate.h>

using namespace DB;

namespace
{
struct Case
{
    const char * name;
    size_t limit;
    double selectivity;
    UInt64 rows_newest;
    bool fulltext_default;
    double safety;
    bool want;
};
}

/// Exhaustive, deterministic coverage of the cost gate's branches. The decision logic lives in a pure
/// function so it can be tested without the optimizer plumbing (FilterEstimator / statistics / plan walk).
/// rows_newest = EXACT rows in the newest selected partition (the caller sums it from pruned parts).
TEST(PartitionOrderGate, TableDriven)
{
    const Case cases[] = {
        // --- selectivity-driven (s * rows_newest vs N) ---
        // Common value: newest partition holds plenty (0.5 * 33.3M = 16.7M >> 200) -> ON.
        {"common_dense", 200, 0.5, 33'333'333ULL, true, 1.0, true},
        // Globally rare value: newest partition thin (1e-7 * 33.3M = 3.3 < 200) -> OFF.
        {"globally_rare", 200, 1e-7, 33'333'333ULL, true, 1.0, false},
        // Common fraction BUT a thin newest partition (0.5 * 200 = 100 < 200) -> OFF.
        // This is why the gate weighs the newest partition's exact row count, not bare selectivity.
        {"common_but_thin_newest", 200, 0.5, 200ULL, true, 1.0, false},

        // --- LIMIT branch ---
        // No LIMIT -> early termination impossible -> OFF even for a very common value.
        {"no_limit_common", 0, 0.9, 33'333'333ULL, true, 1.0, false},

        // --- full-text (unknown selectivity = negative) -> policy default ---
        {"fulltext_default_on", 200, -1.0, 33'333'333ULL, true, 1.0, true},
        {"fulltext_default_off", 200, -1.0, 33'333'333ULL, false, 1.0, false},
        // Full-text but no LIMIT -> OFF regardless of the policy default.
        {"fulltext_no_limit", 0, -1.0, 33'333'333ULL, true, 1.0, false},

        // --- boundary: est exactly equals limit*safety (0.001 * 200000 = 200) -> ON (>=) ---
        {"boundary_equal", 200, 0.001, 200'000ULL, true, 1.0, true},
        // Same inputs but safety=1.5 -> threshold 300 > 200 -> OFF.
        {"safety_rejects_boundary", 200, 0.001, 200'000ULL, true, 1.5, false},
        // Just below boundary (est = 199.99...) -> OFF.
        {"just_below_boundary", 200, 0.0009995, 200'000ULL, true, 1.0, false},

        // --- guards ---
        {"zero_rows_newest", 200, 0.5, 0ULL, true, 1.0, false},
    };

    for (const auto & c : cases)
    {
        bool got = partitionOrderGate(c.limit, c.selectivity, c.rows_newest, c.fulltext_default, c.safety);
        EXPECT_EQ(got, c.want) << "case: " << c.name << " (limit=" << c.limit << " sel=" << c.selectivity
                               << " rows_newest=" << c.rows_newest
                               << " ft_default=" << c.fulltext_default << " safety=" << c.safety << ")";
    }
}
