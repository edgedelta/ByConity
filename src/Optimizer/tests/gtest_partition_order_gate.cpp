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
    UInt64 row_count;
    size_t partitions;
    bool fulltext_default;
    double safety;
    bool want;
};
}

/// Exhaustive, deterministic coverage of the cost gate's branches. The decision logic lives in a pure
/// function so it can be tested without the optimizer plumbing (FilterEstimator / statistics / plan walk).
TEST(PartitionOrderGate, TableDriven)
{
    const Case cases[] = {
        // --- selectivity-driven (s * R / P vs N) ---
        // Common value: newest partition holds plenty (0.5 * 1e9 / 30 = 16.7M >> 200) -> ON.
        {"common_dense", 200, 0.5, 1'000'000'000ULL, 30, true, 1.0, true},
        // Globally rare value: newest partition thin (1e-7 * 1e9 / 30 = 3.3 < 200) -> OFF.
        {"globally_rare", 200, 1e-7, 1'000'000'000ULL, 30, true, 1.0, false},
        // Common fraction BUT small table / many partitions: newest partition thin
        // (0.5 * 6000 / 30 = 100 < 200) -> OFF. This is exactly why the gate uses s*R/P, not bare s.
        {"common_but_thin_newest", 200, 0.5, 6000ULL, 30, true, 1.0, false},

        // --- LIMIT branch ---
        // No LIMIT -> early termination impossible -> OFF even for a very common value.
        {"no_limit_common", 0, 0.9, 1'000'000'000ULL, 30, true, 1.0, false},

        // --- full-text (unknown selectivity = negative) -> policy default ---
        {"fulltext_default_on", 200, -1.0, 1'000'000'000ULL, 30, true, 1.0, true},
        {"fulltext_default_off", 200, -1.0, 1'000'000'000ULL, 30, false, 1.0, false},
        // Full-text but no LIMIT -> OFF regardless of the policy default.
        {"fulltext_no_limit", 0, -1.0, 1'000'000'000ULL, 30, true, 1.0, false},

        // --- boundary: est exactly equals limit*safety (0.001 * 6e6 / 30 = 200) -> ON (>=) ---
        {"boundary_equal", 200, 0.001, 6'000'000ULL, 30, true, 1.0, true},
        // Same inputs but safety=1.5 -> threshold 300 > 200 -> OFF.
        {"safety_rejects_boundary", 200, 0.001, 6'000'000ULL, 30, true, 1.5, false},
        // Just below boundary (est = 199.99...) -> OFF.
        {"just_below_boundary", 200, 0.0009995, 6'000'000ULL, 30, true, 1.0, false},

        // --- guards ---
        {"zero_partitions", 200, 0.5, 1'000'000ULL, 0, true, 1.0, false},
        {"zero_rows", 200, 0.5, 0ULL, 30, true, 1.0, false},
    };

    for (const auto & c : cases)
    {
        bool got = partitionOrderGate(c.limit, c.selectivity, c.row_count, c.partitions, c.fulltext_default, c.safety);
        EXPECT_EQ(got, c.want) << "case: " << c.name << " (limit=" << c.limit << " sel=" << c.selectivity
                               << " rows=" << c.row_count << " parts=" << c.partitions
                               << " ft_default=" << c.fulltext_default << " safety=" << c.safety << ")";
    }
}
