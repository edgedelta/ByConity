#include <Storages/System/StorageSystemDiskTTLCachePartitions.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Common/HostWithPorts.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
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
    String worker_id = getWorkerID(context);

    auto ttl_caches = DiskCacheFactory::instance().getAllTableTTLCaches();
    for (const auto & [uuid, cache_ptr] : ttl_caches)
    {
        auto * ttl_cache = dynamic_cast<DiskCacheTTL *>(cache_ptr.get());
        if (!ttl_cache)
            continue;

        auto stats = ttl_cache->getStats();
        auto partition_stats_list = ttl_cache->getPartitionStats();

        for (const auto & ps : partition_stats_list)
        {
            size_t col_idx = 0;
            res_columns[col_idx++]->insert(worker_id);
            res_columns[col_idx++]->insert(ttl_cache->getName());
            res_columns[col_idx++]->insert(stats.table_uuid);
            res_columns[col_idx++]->insert(ps.partition_id);
            res_columns[col_idx++]->insert(ps.entry_count);
            res_columns[col_idx++]->insert(ps.total_bytes);
            res_columns[col_idx++]->insert(ps.hits);
            res_columns[col_idx++]->insert(ps.misses);
        }
    }
}

}
