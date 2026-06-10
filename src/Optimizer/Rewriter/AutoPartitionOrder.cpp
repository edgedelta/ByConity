#include <Optimizer/Rewriter/AutoPartitionOrder.h>

#include <Optimizer/PartitionOrderGate.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include <QueryPlan/TableScanStep.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

namespace
{
    /// Walks the plan and records the auto partition-order gate inputs on each eligible TableScan.
    class Visitor : public SimplePlanVisitor<Void>
    {
    public:
        Visitor(ContextMutablePtr context_, CTEInfo & cte_info) : SimplePlanVisitor<Void>(cte_info), context(context_) { }

        Void visitTableScanNode(TableScanNode & node, Void & c) override
        {
            auto step = node.getStep();

            /// Only relevant when read-in-order was chosen for this scan.
            if (!step->getQueryInfo().input_order_info)
                return visitPlanNode(node, c);

            /// LIMIT (pushed onto the step by PushLimitIntoTableScan). 0 => no early termination => gate inactive.
            UInt64 limit = 0;
            if (const auto * select = step->getQueryInfo().query->as<ASTSelectQuery>())
            {
                if (auto limit_length = select->limitLength())
                {
                    try
                    {
                        auto [field, type] = evaluateConstantExpression(limit_length, context);
                        limit = convertFieldToType(field, DataTypeUInt64()).safeGet<UInt64>();
                    }
                    catch (...)
                    {
                        limit = 0;
                    }
                }
            }
            if (limit == 0)
                return visitPlanNode(node, c);

            /// Table row count from statistics (fallback to a fresh estimate, as PushStorageFilter does).
            PlanNodeStatisticsPtr stat;
            if (node.getStatistics().has_value())
                stat = node.getStatistics().value();
            else
                stat = TableScanEstimator::estimate(context, static_cast<const TableScanStep &>(*step));
            if (!stat)
                return visitPlanNode(node, c);

            /// Selectivity of the pushed-down filter. No filter => everything passes (s = 1).
            /// estimateFilterSelectivityOpt returns nullopt when it cannot estimate (full-text) => -1 = unknown,
            /// which ReadFromMergeTree resolves via auto_partition_order_fulltext_default.
            double selectivity = 1.0;
            if (auto filter_step = step->getPushdownFilter())
            {
                ConstASTPtr predicate = filter_step->getFilter();
                IdentifierNameSet used_columns;
                predicate->collectIdentifierNames(used_columns);
                const auto & columns_desc = step->getStorage()->getInMemoryMetadataPtr()->getColumns();
                NamesAndTypes column_types;
                for (const auto & name : used_columns)
                    if (columns_desc.hasPhysical(name))
                        column_types.emplace_back(columns_desc.getPhysical(name));
                auto s_opt = FilterEstimator::estimateFilterSelectivityOpt(stat, predicate, column_types, context);
                selectivity = s_opt.value_or(PARTITION_ORDER_UNKNOWN_SELECTIVITY);
            }

            step->setAutoPartitionOrderEstimate(AutoPartitionOrderEstimate{selectivity, stat->getRowCount(), limit});
            return visitPlanNode(node, c);
        }

    private:
        ContextMutablePtr context;
    };
}

void AutoPartitionOrder::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    Void c;
    Visitor visitor{context, plan.getCTEInfo()};
    VisitorUtil::accept(plan.getPlanNode(), visitor, c);
}

}
