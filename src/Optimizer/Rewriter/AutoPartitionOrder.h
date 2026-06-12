#pragma once

#include <Optimizer/Rewriter/Rewriter.h>

namespace DB
{

/// Auto partition-order optimizer pass.
///
/// For each TableScan that reads in primary-key order and carries a LIMIT, estimate the filter
/// selectivity from statistics and stash it with the LIMIT (AutoPartitionOrderEstimate) on the scan's
/// query_info. ReadFromMergeTree later applies the final cost gate (s * rows_newest >= N) using the exact
/// row count of the newest pruned partition, which the optimizer cannot know.
///
/// Must run AFTER PushIntoTableScan so the filter (pushdown_filter) and the limit are on the
/// TableScanStep. It only records inputs; it does not change the plan shape.
class AutoPartitionOrder : public Rewriter
{
public:
    String name() const override { return "AutoPartitionOrder"; }

private:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_auto_partition_order && context->getSettingsRef().optimize_read_in_order;
    }
};

}
