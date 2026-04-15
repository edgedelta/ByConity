#include <Storages/System/StorageSystemDiskTTLCacheTables.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/ColumnMap.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/DiskCache/DiskCacheTTL.h>

namespace DB
{

NamesAndTypesList StorageSystemDiskTTLCacheTables::getNamesAndTypes()
{
    return {
        {"worker_id", std::make_shared<DataTypeString>()},
        {"table_name", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeString>()},
        {"ttl_minutes", std::make_shared<DataTypeUInt64>()},
        {"max_size_bytes", std::make_shared<DataTypeUInt64>()},
        {"last_eviction_run", std::make_shared<DataTypeDateTime>()},
        {"eviction_stats", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>())},
        {"rejection_stats", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>())},
        {"write_stats", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>())},
    };
}

StorageSystemDiskTTLCacheTables::StorageSystemDiskTTLCacheTables(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

static void dumpStatsToMapColumn(const std::unordered_map<String, UInt64> & map, IColumn * column)
{
    auto * column_map = column ? &typeid_cast<ColumnMap &>(*column) : nullptr;
    if (!column_map)
        return;

    auto & offsets = column_map->getOffsets();
    auto & key_column = column_map->getKey();
    auto & value_column = column_map->getValue();

    size_t size = 0;
    for (const auto & entry : map)
    {
        key_column.insertData(entry.first.c_str(), entry.first.size());
        value_column.insert(entry.second);
        size++;
    }

    offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + size);
}

void StorageSystemDiskTTLCacheTables::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    // Get worker_id from context
    String worker_id = context->getHostWithPorts().getReadableID();

    // Iterate through all databases and tables
    const auto databases = DatabaseCatalog::instance().getDatabases(context);
    for (const auto & [db_name, database] : databases)
    {
        for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
        {
            const auto & table = it->table();
            auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
            if (!cnch_table)
                continue;

            // Check if table has per-table TTL cache
            auto disk_cache = cnch_table->getDiskCache();
            if (!disk_cache)
                continue;

            // Try to cast to DiskCacheTTL
            auto * ttl_cache = dynamic_cast<DiskCacheTTL *>(disk_cache.get());
            if (!ttl_cache)
                continue;  // Global LRU cache, not per-table TTL

            // Get stats
            auto stats = ttl_cache->getStats();

            size_t col_idx = 0;

            // worker_id
            res_columns[col_idx++]->insert(worker_id);

            // table_name
            res_columns[col_idx++]->insert(it->name());

            // table_uuid
            res_columns[col_idx++]->insert(stats.table_uuid);

            // ttl_minutes
            res_columns[col_idx++]->insert(ttl_cache->getTTLMinutes());

            // max_size_bytes
            res_columns[col_idx++]->insert(ttl_cache->getMaxSizeBytes());

            // last_eviction_run
            res_columns[col_idx++]->insert(stats.last_eviction_run);

            // eviction_stats Map
            {
                std::unordered_map<String, UInt64> eviction_map;
                eviction_map["expired"] = stats.evicted_expired;
                eviction_map["size_limit"] = stats.evicted_size_limit;
                eviction_map["async_triggered_local"] = stats.async_eviction_triggered;
                eviction_map["async_skipped_rate_limit_local"] = stats.async_eviction_skipped_rate_limit;
                eviction_map["async_triggered_global"] = stats.async_eviction_triggered_global;
                eviction_map["async_skipped_rate_limit_global"] = stats.async_eviction_skipped_rate_limit_global;
                dumpStatsToMapColumn(eviction_map, res_columns[col_idx++].get());
            }

            // rejection_stats Map
            {
                std::unordered_map<String, UInt64> rejection_map;
                rejection_map["non_time_partition"] = stats.rejected_non_time_partition;
                rejection_map["too_old"] = stats.rejected_too_old;
                dumpStatsToMapColumn(rejection_map, res_columns[col_idx++].get());
            }

            // write_stats Map
            {
                std::unordered_map<String, UInt64> write_map;
                write_map["count_preload"] = stats.cached_from_preload;
                write_map["count_query"] = stats.cached_from_query;
                write_map["bytes_preload"] = stats.cached_bytes_preload;
                write_map["bytes_query"] = stats.cached_bytes_query;
                dumpStatsToMapColumn(write_map, res_columns[col_idx++].get());
            }
        }
    }
}

}
