#include <Interpreters/InterpreterAlterDiskCacheQuery.h>

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <CloudServices/CnchPartsHelper.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Parsers/ASTAlterDiskCacheQuery.h>
#include <Storages/StorageCnchMergeTree.h>
#include "Common/tests/gtest_global_context.h"

#include <map>
#include <common/logger_useful.h>

namespace DB
{

namespace
{
    /// PRELOAD debug: "L0:2,L3:8,L4:24" style breakdown of parts by merge level.
    String levelHistogram(const ServerDataPartsVector & parts)
    {
        std::map<UInt32, size_t> by_level;
        for (const auto & part : parts)
            ++by_level[part->get_info().level];
        String out;
        for (const auto & [lvl, cnt] : by_level)
            out += (out.empty() ? "" : ",") + ("L" + std::to_string(lvl) + ":" + std::to_string(cnt));
        return out;
    }
}

InterpreterAlterDiskCacheQuery::InterpreterAlterDiskCacheQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

BlockIO InterpreterAlterDiskCacheQuery::execute()
{
    const auto & query = query_ptr->as<ASTAlterDiskCacheQuery &>();
    /// apply settings
    if (query.settings_ast)
    {
        InterpreterSetQuery(query.settings_ast, getContext()).executeForCurrentContext();
    }

    StoragePtr table = DatabaseCatalog::instance().getTable({query.database, query.table}, getContext());
    auto * storage = dynamic_cast<StorageCnchMergeTree *>(table.get());
    if (!storage)
        throw Exception("Preload only support CnchMergeTree engine", ErrorCodes::LOGICAL_ERROR);

    auto * preload_log = &Poco::Logger::get("InterpreterAlterDiskCacheQuery");

    ServerDataPartsVector parts;
    if (query.partition)
    {
        String partition_id = storage->getPartitionIDFromQuery(query.partition, getContext());
        UInt64 ts = getContext()->getTimestamp();
        parts = getContext()->getCnchCatalog()->getServerDataPartsInPartitions(table, {partition_id}, ts, nullptr);
        LOG_INFO(
            preload_log,
            "PRELOAD-DEBUG: after getServerDataPartsInPartitions: table={}, partition_id={}, ts={}, parts={}, levels=[{}]",
            table->getStorageID().getNameForLogs(),
            partition_id,
            ts,
            parts.size(),
            levelHistogram(parts));
    }
    else
    {
        parts = storage->getAllPartsWithDBM(getContext()).first;
        LOG_INFO(preload_log, "PRELOAD-DEBUG: after getAllPartsWithDBM: parts={}, levels=[{}]", parts.size(), levelHistogram(parts));
    }
    parts = CnchPartsHelper::calcVisibleParts(parts, false);
    LOG_INFO(preload_log, "PRELOAD-DEBUG: after calcVisibleParts: parts={}, levels=[{}]", parts.size(), levelHistogram(parts));

    if (query.type == ASTAlterDiskCacheQuery::Type::PRELOAD)
    {
        storage->sendPreloadTasks(getContext(), std::move(parts), query.sync, getContext()->getSettings().parts_preload_level, time(nullptr));
    }
    else if (query.type == ASTAlterDiskCacheQuery::Type::DROP)
    {
        storage->sendDropDiskCacheTasks(getContext(), std::move(parts), query.sync, getContext()->getSettings().drop_vw_disk_cache);
    }
    else
    {
        throw Exception("Unknown alter disk cache query type", ErrorCodes::NOT_IMPLEMENTED);
    }

    return {};
}
}
