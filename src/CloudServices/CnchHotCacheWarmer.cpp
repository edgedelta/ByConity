/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <CloudServices/CnchHotCacheWarmer.h>

#include <Catalog/StringHelper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Common/serverLocality.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Protos/RPCHelpers.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/TxnTimestamp.h>
#include <Poco/Net/SocketAddress.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>

#include <ctime>

namespace DB
{

bool isPreloadTargetWorker(const std::unordered_set<String> & target_workers, const String & worker_id)
{
    return target_workers.empty() || target_workers.contains(worker_id);
}

namespace
{

String serializeBaseline(const std::map<String, UInt32> & baseline)
{
    WriteBufferFromOwnString buf;
    writeBinary(static_cast<UInt64>(baseline.size()), buf);
    for (const auto & [id, register_time] : baseline)
    {
        writeBinary(id, buf);
        writeBinary(register_time, buf);
    }
    return buf.str();
}

std::map<String, UInt32> deserializeBaseline(const String & raw)
{
    std::map<String, UInt32> res;
    ReadBufferFromString buf(raw);
    UInt64 n = 0;
    readBinary(n, buf);
    for (UInt64 i = 0; i < n; ++i)
    {
        String id;
        UInt32 register_time;
        readBinary(id, buf);
        readBinary(register_time, buf);
        res.emplace(std::move(id), register_time);
    }
    return res;
}

}

CnchHotCacheWarmer::CnchHotCacheWarmer(ContextPtr context_, const Poco::Util::AbstractConfiguration & config)
    : WithContext(context_)
    , catalog(getContext()->getCnchCatalog())
    , log(&Poco::Logger::get("CnchHotCacheWarmer"))
{
    interval_ms = config.getUInt64("hot_cache_warmup_interval_ms", 60000);
    warmup_grace_seconds = config.getUInt("hot_cache_warmup_grace_sec", 30);
    preload_database = config.getString("hot_cache_warmup_database", "");

    /// Key the baseline per-server so co-located servers don't clobber each other's state.
    const String name_space = getContext()->getCnchConfigRef().getString("catalog.name_space", "default");
    const String host = getContext()->getHostWithPorts().getRPCAddress();
    state_key = Catalog::escapeString(name_space) + "_HOT_CACHE_WARM_BASELINE_" + Catalog::escapeString(host);

    task = getContext()->getSchedulePool().createTask("CnchHotCacheWarmer", [this] { run(); });
}

void CnchHotCacheWarmer::start()
{
    task->activateAndSchedule();
}

void CnchHotCacheWarmer::shutDown()
{
    task->deactivate();
}

void CnchHotCacheWarmer::loadBaselineFromFDB()
{
    String raw;
    try
    {
        if (catalog->getMetastore()->get(state_key, raw) != 0 && !raw.empty())
            baseline = deserializeBaseline(raw);
    }
    catch (...)
    {
        tryLogCurrentException(log, "CnchHotCacheWarmer: failed to load baseline from FDB, starting empty");
        baseline.clear();
    }
}

void CnchHotCacheWarmer::persistBaselineToFDB()
{
    try
    {
        catalog->getMetastore()->put(state_key, serializeBaseline(baseline));
    }
    catch (...)
    {
        tryLogCurrentException(log, "CnchHotCacheWarmer: failed to persist baseline to FDB");
    }
}

void CnchHotCacheWarmer::run()
{
    try
    {
        /// The server initializes the ResourceManager client at startup; if it isn't ready yet,
        /// just skip this tick and retry on the next one.
        auto rm_client = getContext()->getResourceManagerClient();
        if (!rm_client)
        {
            LOG_WARNING(log, "ResourceManager client not available yet, skipping this tick");
            task->scheduleAfter(interval_ms);
            return;
        }

        std::vector<ResourceManagement::WorkerNodeResourceData> worker_data;
        rm_client->getAllWorkers(worker_data);

        /// FDB-persisted baseline means restarts that happened while this server was down are still
        /// caught (the loaded register_time differs). An empty map on the very first load = fresh
        /// deploy => seed without warming (initial warming is done by hand), so we don't storm.
        bool warm_new_workers = true;
        if (!loaded_from_fdb)
        {
            loadBaselineFromFDB();
            loaded_from_fdb = true;
            if (baseline.empty())
                warm_new_workers = false;
        }

        std::vector<HotCacheWarmerHelpers::WorkerRegistration> workers;
        workers.reserve(worker_data.size());
        for (const auto & w : worker_data)
            workers.push_back({w.id, w.register_time, w.state == ResourceManagement::WorkerState::Running});

        const UInt32 now = static_cast<UInt32>(time(nullptr));
        auto decision = HotCacheWarmerHelpers::decideWarm(workers, baseline, now, warmup_grace_seconds, warm_new_workers);

        if (!decision.restarted_workers.empty())
        {
            /// Until our topology view settles we cannot resolve which tables we host. Defer without
            /// persisting the baseline so the next tick retries instead of consuming the restart.
            auto topology = getContext()->getCnchTopologyMaster();
            if (!topology || !HotCacheWarmerHelpers::isPreloadTopologyReady(topology->getCurrentTopology()))
            {
                LOG_WARNING(log, "Server topology not settled; deferring hot-cache warm to next tick");
                task->scheduleAfter(interval_ms);
                return;
            }

            const std::unordered_set<String> target_workers(decision.restarted_workers.begin(), decision.restarted_workers.end());
            const UInt32 warmed = warmHostedTables(target_workers, now);
            LOG_INFO(log, "CnchHotCacheWarmer: warmed {} TTL tables for {} restarted worker(s)", warmed, decision.restarted_workers.size());
        }

        /// Only touch FDB when the baseline actually changed; steady state is a no-op every tick.
        if (baseline != decision.updated_baseline)
        {
            baseline = std::move(decision.updated_baseline);
            persistBaselineToFDB();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "CnchHotCacheWarmer: tick failed");
    }

    task->scheduleAfter(interval_ms);
}

UInt32 CnchHotCacheWarmer::warmHostedTables(const std::unordered_set<String> & target_workers, UInt32 now)
{
    /// Background read-only query context with a transaction: we only read parts and dispatch
    /// fire-and-forget preload RPCs to the workers (no commit). Mirrors AutoStatisticsManager.
    auto task_context = Context::createCopy(getContext());
    task_context->makeQueryContext();
    auto [interserver_user, interserver_password] = const_cast<const Context &>(*task_context).getCnchInterserverCredentials();
    task_context->setUser(interserver_user, interserver_password, Poco::Net::SocketAddress{});
    auto txn = task_context->getCnchTransactionCoordinator().createTransaction(
        CreateTransactionOption().setContext(task_context).setReadOnly(true));
    task_context->setCurrentTransaction(txn);
    SCOPE_EXIT({
        try { task_context->getCnchTransactionCoordinator().finishTransaction(txn); }
        catch (...) { tryLogCurrentException(log, "CnchHotCacheWarmer: finishTransaction failed"); }
    });

    const UInt64 ts = static_cast<UInt64>(now);
    const time_t now_sec = static_cast<time_t>(now);
    const String rpc_port = std::to_string(getContext()->getRPCPort());
    auto topology = getContext()->getCnchTopologyMaster();

    UInt32 warmed = 0;
    for (const auto & model : catalog->getAllTables(preload_database))
    {
        const String uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(model.uuid()));
        const String server_vw = model.has_server_vw_name() ? model.server_vw_name() : String(DEFAULT_SERVER_VW_NAME);

        /// Ownership check first — cheap, avoids loading storages we don't host.
        auto host = topology->getTargetServer(uuid, server_vw, /*allow_empty_result=*/true);
        if (host.empty() || !isLocalServer(host.getRPCAddress(), rpc_port))
            continue;

        StoragePtr storage = catalog->tryGetTableByUUID(*task_context, uuid, TxnTimestamp::maxTS());
        auto * cnch = dynamic_cast<StorageCnchMergeTree *>(storage.get());
        if (!cnch)
            continue;
        auto settings = cnch->getSettings();
        if (settings->disk_cache_ttl_hours.value == 0)
            continue;

        try
        {
            ServerDataPartsVector parts = cnch->getAllPartsWithDBM(task_context).first;
            parts = CnchPartsHelper::calcVisibleParts(parts, false);
            /// Pre-filter to the TTL window server-side: shipping a table's whole history and letting
            /// each worker reject out-of-window parts one at a time is too slow. The worker's
            /// shouldCache remains the backstop; both key off disk_cache_ttl_hours.
            cnch->filterPartsWithinDiskCacheTTL(parts, now_sec);
            if (parts.empty())
                continue;

            cnch->sendPreloadTasks(
                task_context,
                std::move(parts),
                /*enable_parts_sync_preload=*/false, // background warm: never block on completion
                (settings->enable_preload_parts ? PreloadLevelSettings::AllPreload
                                                 : settings->parts_preload_level.value),
                ts,
                target_workers);
            ++warmed;
        }
        catch (...)
        {
            /// One bad table shouldn't abort warming the rest.
            tryLogCurrentException(log, "CnchHotCacheWarmer: warm failed for table " + uuid);
        }
    }
    return warmed;
}

namespace HotCacheWarmerHelpers
{

WarmDecision decideWarm(
    const std::vector<WorkerRegistration> & workers,
    const std::map<String, UInt32> & baseline,
    UInt32 now,
    UInt32 warmup_grace_seconds,
    bool warm_new_workers)
{
    WarmDecision decision;
    decision.updated_baseline = baseline;

    for (const auto & w : workers)
    {
        if (!w.running)
            continue;

        /// Clock-skew guard: never trust a register_time in the future.
        if (now < w.register_time)
            continue;

        /// Seed tick: establish the baseline for EVERY running worker and warm none, regardless of grace.
        if (!warm_new_workers)
        {
            decision.updated_baseline[w.id] = w.register_time;
            continue;
        }

        /// Steady state: skip until the worker clears the grace window, so we don't warm a
        /// half-initialized worker. We record the baseline only when we actually warm, so a restart
        /// detected mid-grace keeps being seen (register_time > baseline) until grace clears.
        if (now - w.register_time < warmup_grace_seconds)
            continue;

        auto it = decision.updated_baseline.find(w.id);
        if (it == decision.updated_baseline.end())
        {
            /// First time this server sees this worker_id: warm it.
            decision.updated_baseline.emplace(w.id, w.register_time);
            decision.restarted_workers.push_back(w.id);
            continue;
        }

        if (w.register_time > it->second)
        {
            /// Known worker came back with a newer registration => it restarted and wiped its disk
            /// cache => re-warm. Stamp the new register_time so we don't warm it again next tick.
            it->second = w.register_time;
            decision.restarted_workers.push_back(w.id);
        }
    }

    return decision;
}

bool isPreloadTopologyReady(const std::list<CnchServerTopology> & topology)
{
    return !topology.empty() && !topology.back().getServerList().empty();
}

}

}
