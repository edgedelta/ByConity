#include <Storages/System/StorageSystemDiskTTLCachePartitions.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Common/HostWithPorts.h>
#include <CloudServices/CnchWorkerClient.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ResourceManagement/VirtualWarehouseType.h>
#include <Protos/cnch_worker_rpc.pb.h>
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

static void fillPartitionRow(MutableColumns & res_columns, const String & worker_id, const Protos::TTLCachePartitionStats & p)
{
    size_t col_idx = 0;
    res_columns[col_idx++]->insert(worker_id);
    res_columns[col_idx++]->insert(p.table_name());
    res_columns[col_idx++]->insert(p.table_uuid());
    res_columns[col_idx++]->insert(p.partition());
    res_columns[col_idx++]->insert(p.entry_count());
    res_columns[col_idx++]->insert(p.bytes());
    res_columns[col_idx++]->insert(p.hits());
    res_columns[col_idx++]->insert(p.misses());
}

void StorageSystemDiskTTLCachePartitions::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    if (context->getServerType() == ServerType::cnch_server)
    {
        auto * log = &Poco::Logger::get("StorageSystemDiskTTLCachePartitions");
        std::vector<WorkerNodeResourceData> all_workers;
        try
        {
            auto rm_client = context->getResourceManagerClient();
            if (!rm_client)
            {
                LOG_WARNING(log, "ResourceManager client unavailable, returning empty result");
                return;
            }
            rm_client->getAllWorkers(all_workers);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to get workers from ResourceManager");
            return;
        }

        LOG_INFO(log, "Querying TTL partition stats from {} worker(s)", all_workers.size());
        auto & pools = context->getCnchWorkerClientPools();
        for (const auto & wd : all_workers)
        {
            if (wd.vw_name == ResourceManagement::toSystemVWName(ResourceManagement::VirtualWarehouseType::Write))
                continue;
            try
            {
                auto worker = pools.getWorker(wd.host_ports);
                auto partitions = worker->getTTLCachePartitionStats();
                for (const auto & p : partitions)
                    fillPartitionRow(res_columns, wd.id.empty() ? wd.host_ports.getRPCAddress() : wd.id, p);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
        return;
    }

    // On worker: read directly from local DiskCacheFactory registry
    String worker_id = getWorkerID(context);
    auto ttl_caches = DiskCacheFactory::instance().getAllTableTTLCaches();
    for (const auto & [uuid, cache_ptr] : ttl_caches)
    {
        auto * ttl_cache = dynamic_cast<DiskCacheTTL *>(cache_ptr.get());
        if (!ttl_cache)
            continue;

        auto table_stats = ttl_cache->getStats();
        for (const auto & ps : ttl_cache->getPartitionStats())
        {
            Protos::TTLCachePartitionStats p;
            p.set_table_name(ttl_cache->getName());
            p.set_table_uuid(table_stats.table_uuid);
            p.set_partition(ps.partition_id);
            p.set_entry_count(ps.entry_count);
            p.set_bytes(ps.total_bytes);
            p.set_hits(ps.hits);
            p.set_misses(ps.misses);
            fillPartitionRow(res_columns, worker_id, p);
        }
    }
}

}
