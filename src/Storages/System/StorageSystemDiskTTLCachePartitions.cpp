#include <Storages/System/StorageSystemDiskTTLCachePartitions.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/HostWithPorts.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/DiskCache/DiskCacheTTL.h>

namespace DB
{

NamesAndTypesList StorageSystemDiskTTLCachePartitions::getNamesAndTypes()
{
    return {
        {"worker_id", std::make_shared<DataTypeString>()},
        {"table_name", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeString>()},
        {"partition", std::make_shared<DataTypeString>()},
        {"entry_count", std::make_shared<DataTypeUInt64>()},
        {"bytes", std::make_shared<DataTypeUInt64>()},
        {"hits", std::make_shared<DataTypeUInt64>()},
        {"misses", std::make_shared<DataTypeUInt64>()},
    };
}

StorageSystemDiskTTLCachePartitions::StorageSystemDiskTTLCachePartitions(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemDiskTTLCachePartitions::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    // Get worker_id from context
    String worker_id = getWorkerID(context);

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

            // Get partition stats
            auto partition_stats_list = ttl_cache->getPartitionStats();

            // Get table UUID from stats
            auto stats = ttl_cache->getStats();

            for (const auto & ps : partition_stats_list)
            {
                size_t col_idx = 0;

                // worker_id
                res_columns[col_idx++]->insert(worker_id);

                // table_name
                res_columns[col_idx++]->insert(it->name());

                // table_uuid
                res_columns[col_idx++]->insert(stats.table_uuid);

                // partition
                res_columns[col_idx++]->insert(ps.partition_id);

                // entry_count
                res_columns[col_idx++]->insert(ps.entry_count);

                // bytes
                res_columns[col_idx++]->insert(ps.total_bytes);

                // hits
                res_columns[col_idx++]->insert(ps.hits);

                // misses
                res_columns[col_idx++]->insert(ps.misses);
            }
        }
    }
}

}
