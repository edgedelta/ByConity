#include <Storages/System/StorageSystemDiskTTLCachePreloads.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <Common/HostWithPorts.h>
#include <Common/ThreadPool.h>
#include <CloudServices/CnchWorkerClient.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ResourceManagement/VirtualWarehouseType.h>
#include <Protos/cnch_worker_rpc.pb.h>
#include <Storages/DiskCache/PreloadRegistry.h>

namespace DB
{

NamesAndTypesList StorageSystemDiskTTLCachePreloads::getNamesAndTypes()
{
    return {
        {"worker_id", std::make_shared<DataTypeString>()},
        {"table_name", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"parts_in_flight", std::make_shared<DataTypeUInt64>()},
        {"parts_submitted", std::make_shared<DataTypeUInt64>()},
        {"elapsed_ms", std::make_shared<DataTypeUInt64>()},
        {"preload_level", std::make_shared<DataTypeUInt64>()},
    };
}

StorageSystemDiskTTLCachePreloads::StorageSystemDiskTTLCachePreloads(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

static void fillPreloadRow(MutableColumns & res_columns, const String & worker_id, const Protos::PreloadPartitionStats & p)
{
    size_t col_idx = 0;
    res_columns[col_idx++]->insert(worker_id);
    res_columns[col_idx++]->insert(p.table_name());
    res_columns[col_idx++]->insert(p.table_uuid());
    res_columns[col_idx++]->insert(p.partition_id());
    res_columns[col_idx++]->insert(p.parts_in_flight());
    res_columns[col_idx++]->insert(p.parts_submitted());
    res_columns[col_idx++]->insert(p.elapsed_ms());
    res_columns[col_idx++]->insert(p.preload_level());
}

void StorageSystemDiskTTLCachePreloads::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    if (context->getServerType() == ServerType::cnch_server)
    {
        auto * log = &Poco::Logger::get("StorageSystemDiskTTLCachePreloads");
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

        auto & pools = context->getCnchWorkerClientPools();

        // Parallel fan-out: each RPC is capped at 5s, so a serial loop costs O(num_workers * 5s)
        // when workers are slow/dead. Column filling stays single-threaded afterwards.
        std::vector<std::pair<String, std::vector<Protos::PreloadPartitionStats>>> per_worker(all_workers.size());
        ThreadPool rpc_pool(std::min<size_t>(16, std::max<size_t>(all_workers.size(), 1)));
        try
        {
            for (size_t i = 0; i < all_workers.size(); ++i)
            {
                if (all_workers[i].vw_name == ResourceManagement::toSystemVWName(ResourceManagement::VirtualWarehouseType::Write))
                    continue;
                rpc_pool.scheduleOrThrowOnError([&, i, thread_group = CurrentThread::getGroup()] {
                    DB::ThreadStatus thread_status;
                    if (thread_group)
                        CurrentThread::attachTo(thread_group);
                    const auto & wd = all_workers[i];
                    try
                    {
                        auto worker = pools.getWorker(wd.host_ports);
                        per_worker[i] = {wd.id.empty() ? wd.host_ports.getRPCAddress() : wd.id, worker->getPreloadStats()};
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                });
            }
        }
        catch (...)
        {
            rpc_pool.wait();
            throw;
        }
        rpc_pool.wait();

        for (const auto & [wid, partitions] : per_worker)
            for (const auto & p : partitions)
                fillPreloadRow(res_columns, wid, p);
        return;
    }

    // On worker: read directly from PreloadRegistry
    String worker_id = getWorkerID(context);
    for (const auto & snap : PreloadRegistry::instance().getSnapshot())
    {
        Protos::PreloadPartitionStats p;
        p.set_table_name(snap.table_name);
        p.set_table_uuid(snap.table_uuid);
        p.set_partition_id(snap.partition_id);
        p.set_parts_in_flight(snap.parts_in_flight);
        p.set_parts_submitted(snap.parts_submitted);
        p.set_elapsed_ms(snap.elapsed_ms);
        p.set_preload_level(snap.preload_level);
        fillPreloadRow(res_columns, worker_id, p);
    }
}

}
