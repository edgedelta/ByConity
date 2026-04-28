#include <Storages/System/StorageSystemDiskTTLCacheTables.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/ColumnMap.h>
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

static void fillRowFromProto(MutableColumns & res_columns, const String & worker_id, const Protos::TTLCacheTableStats & t)
{
    size_t col_idx = 0;

    res_columns[col_idx++]->insert(worker_id);
    res_columns[col_idx++]->insert(t.table_name());
    res_columns[col_idx++]->insert(t.table_uuid());
    res_columns[col_idx++]->insert(t.ttl_minutes());
    res_columns[col_idx++]->insert(t.max_size_bytes());
    res_columns[col_idx++]->insert(t.last_eviction_run());

    {
        std::unordered_map<String, UInt64> eviction_map;
        eviction_map["expired"] = t.evicted_expired();
        eviction_map["size_limit"] = t.evicted_size_limit();
        eviction_map["async_triggered_local"] = t.async_triggered_local();
        eviction_map["async_skipped_rate_limit_local"] = t.async_skipped_rate_limit_local();
        eviction_map["async_triggered_global"] = t.async_triggered_global();
        eviction_map["async_skipped_rate_limit_global"] = t.async_skipped_rate_limit_global();
        dumpStatsToMapColumn(eviction_map, res_columns[col_idx++].get());
    }

    {
        std::unordered_map<String, UInt64> rejection_map;
        rejection_map["non_time_partition"] = t.rejected_non_time_partition();
        rejection_map["too_old"] = t.rejected_too_old();
        dumpStatsToMapColumn(rejection_map, res_columns[col_idx++].get());
    }

    {
        std::unordered_map<String, UInt64> write_map;
        write_map["count_preload"] = t.count_preload();
        write_map["count_query"] = t.count_query();
        write_map["bytes_preload"] = t.bytes_preload();
        write_map["bytes_query"] = t.bytes_query();
        write_map["count_restored"] = t.count_restored();
        write_map["bytes_restored"] = t.bytes_restored();
        dumpStatsToMapColumn(write_map, res_columns[col_idx++].get());
    }
}

void StorageSystemDiskTTLCacheTables::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    if (context->getServerType() == ServerType::cnch_server)
    {
        // Fan out to all workers via RPC using RM worker list — same pattern as system.workers.
        // This works in any context without requiring a VW to be set.
        auto * log = &Poco::Logger::get("StorageSystemDiskTTLCacheTables");
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

        LOG_INFO(log, "Querying TTL cache stats from {} worker(s)", all_workers.size());
        auto & pools = context->getCnchWorkerClientPools();
        for (const auto & wd : all_workers)
        {
            if (wd.vw_name == ResourceManagement::toSystemVWName(ResourceManagement::VirtualWarehouseType::Write))
                continue;
            LOG_INFO(log, "Sending getTTLCacheStats RPC to {}", wd.host_ports.getRPCAddress());
            try
            {
                auto worker = pools.getWorker(wd.host_ports);
                auto stats = worker->getTTLCacheStats();
                LOG_INFO(log, "Got {} TTL cache entries from {}", stats.size(), wd.host_ports.getRPCAddress());
                for (const auto & t : stats)
                    fillRowFromProto(res_columns, wd.id.empty() ? wd.host_ports.getRPCAddress() : wd.id, t);
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

        auto stats = ttl_cache->getStats();

        Protos::TTLCacheTableStats t;
        t.set_table_name(ttl_cache->getName());
        t.set_table_uuid(stats.table_uuid);
        t.set_ttl_minutes(ttl_cache->getTTLMinutes());
        t.set_max_size_bytes(ttl_cache->getMaxSizeBytes());
        t.set_last_eviction_run(stats.last_eviction_run);
        t.set_evicted_expired(stats.evicted_expired);
        t.set_evicted_size_limit(stats.evicted_size_limit);
        t.set_async_triggered_local(stats.async_eviction_triggered);
        t.set_async_skipped_rate_limit_local(stats.async_eviction_skipped_rate_limit);
        t.set_async_triggered_global(stats.async_eviction_triggered_global);
        t.set_async_skipped_rate_limit_global(stats.async_eviction_skipped_rate_limit_global);
        t.set_rejected_non_time_partition(stats.rejected_non_time_partition);
        t.set_rejected_too_old(stats.rejected_too_old);
        t.set_count_preload(stats.cached_from_preload);
        t.set_count_query(stats.cached_from_query);
        t.set_bytes_preload(stats.cached_bytes_preload);
        t.set_bytes_query(stats.cached_bytes_query);
        t.set_count_restored(stats.cached_from_restored);
        t.set_bytes_restored(stats.cached_bytes_restored);

        fillRowFromProto(res_columns, worker_id, t);
    }
}

}
