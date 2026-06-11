#include <Optimizer/Rewriter/AutoPartitionOrder.h>

#include <Optimizer/PartitionOrderGate.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include <QueryPlan/TableScanStep.h>
#include <QueryPlan/SortingStep.h>
#include <QueryPlan/LimitStep.h>

namespace DB
{

namespace
{
    /// Walks the plan carrying the effective LIMIT down from the Sorting/Limit node.
    /// At each eligible TableScan it records the auto partition-order gate inputs (selectivity, limit).
    /// The traversal context (UInt64) is the applicable limit; 0 = no limit in scope.
    class Visitor : public SimplePlanVisitor<UInt64>
    {
    public:
        Visitor(ContextMutablePtr context_, CTEInfo & cte_info) : SimplePlanVisitor<UInt64>(cte_info), context(context_) { }

        Void visitLimitNode(LimitNode & node, UInt64 & limit) override
        {
            const auto * step = dynamic_cast<const LimitStep *>(node.getStep().get());
            UInt64 next = (step && !step->hasPreparedParam()) ? step->getLimitForSorting() : limit;
            for (const auto & child : node.getChildren())
                VisitorUtil::accept(*child, *this, next);
            return Void{};
        }

        Void visitSortingNode(SortingNode & node, UInt64 & limit) override
        {
            const auto * step = dynamic_cast<const SortingStep *>(node.getStep().get());
            UInt64 l = step ? step->getLimitValue() : 0;
            UInt64 next = l ? l : limit;
            for (const auto & child : node.getChildren())
                VisitorUtil::accept(*child, *this, next);
            return Void{};
        }

        Void visitTableScanNode(TableScanNode & node, UInt64 & limit) override
        {
            auto step = node.getStep();

            /// Only relevant when read-in-order was chosen and a LIMIT is in scope.
            if (!step->getQueryInfo().input_order_info || limit == 0)
                return visitPlanNode(node, limit);

            /// Selectivity of the pushed-down filter. No filter => everything passes and the
            /// newest partition trivially satisfies any LIMIT.
            double selectivity = 1.0;
            if (auto filter_step = step->getPushdownFilter())
            {
                /// Statistics are needed to estimate the filter.
                PlanNodeStatisticsPtr stat;
                if (node.getStatistics().has_value())
                    stat = node.getStatistics().value();
                else
                    stat = TableScanEstimator::estimate(context, static_cast<const TableScanStep &>(*step));

                /// No table statistics at all => we cannot judge the filter => do NOT arm the gate.
                if (!stat)
                    return visitPlanNode(node, limit);

                ConstASTPtr predicate = filter_step->getFilter();
                IdentifierNameSet used_columns;
                predicate->collectIdentifierNames(used_columns);
                const auto & columns_desc = step->getStorage()->getInMemoryMetadataPtr()->getColumns();
                NamesAndTypes column_types;
                for (const auto & name : used_columns)
                    if (columns_desc.hasPhysical(name))
                        column_types.emplace_back(columns_desc.getPhysical(name));
                /// Stats exist but the predicate is unestimable => negative sentinel.
                auto s_opt = FilterEstimator::estimateFilterSelectivityOpt(stat, predicate, column_types, context);
                selectivity = s_opt.value_or(PARTITION_ORDER_UNKNOWN_SELECTIVITY);
            }

            step->setAutoPartitionOrderEstimate(AutoPartitionOrderEstimate{selectivity, limit});
            return visitPlanNode(node, limit);
        }

    private:
        ContextMutablePtr context;
    };
}

void AutoPartitionOrder::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    UInt64 limit = 0;
    Visitor visitor{context, plan.getCTEInfo()};
    VisitorUtil::accept(plan.getPlanNode(), visitor, limit);
}

}
