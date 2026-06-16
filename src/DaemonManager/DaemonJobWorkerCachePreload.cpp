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

#include <DaemonManager/DaemonJobWorkerCachePreload.h>

#include <DaemonManager/DaemonFactory.h>
#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchServerClientPool.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ResourceManagement/CommonData.h>
#include <Catalog/StringHelper.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <ctime>

namespace DB::DaemonManager
{

namespace
{

String serializeLastSeen(const std::map<String, UInt32> & last_seen)
{
    WriteBufferFromOwnString buf;
    writeBinary(static_cast<UInt64>(last_seen.size()), buf);
    for (const auto & [id, register_time] : last_seen)
    {
        writeBinary(id, buf);
        writeBinary(register_time, buf);
    }
    return buf.str();
}

std::map<String, UInt32> deserializeLastSeen(const String & raw)
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

DaemonJobWorkerCachePreload::DaemonJobWorkerCachePreload(ContextMutablePtr global_context_)
    : DaemonJob{std::move(global_context_), CnchBGThreadType::WorkerCachePreload}
    , catalog(getContext()->getCnchCatalog())
{
    const auto & config = getContext()->getConfigRef();
    warmup_grace_seconds = config.getUInt("worker_cache_preload_grace_sec", 30);
    cooldown_seconds = config.getUInt("worker_cache_preload_cooldown_sec", 300);
    preload_database = config.getString("worker_cache_preload_database", "");

    const String name_space = getContext()->getCnchConfigRef().getString("catalog.name_space", "default");
    state_key = Catalog::escapeString(name_space) + "_WORKER_CACHE_PRELOAD_LAST_SEEN";
}

void DaemonJobWorkerCachePreload::loadStateFromFDB()
{
    String raw;
    try
    {
        if (catalog->getMetastore()->get(state_key, raw) != 0 && !raw.empty())
            last_seen = deserializeLastSeen(raw);
    }
    catch (...)
    {
        tryLogCurrentException(log, "WorkerCachePreload: failed to load last_seen from FDB, starting empty");
        last_seen.clear();
    }
}

void DaemonJobWorkerCachePreload::persistStateToFDB()
{
    try
    {
        catalog->getMetastore()->put(state_key, serializeLastSeen(last_seen));
    }
    catch (...)
    {
        tryLogCurrentException(log, "WorkerCachePreload: failed to persist last_seen to FDB");
    }
}

bool DaemonJobWorkerCachePreload::executeImpl()
{
    /// Lazily init the RM client on first use so DaemonManager startup never blocks on
    /// RM availability; if RM is down the daemon just skips ticks and self-heals.
    auto rm_client = getContext()->getResourceManagerClient();
    if (!rm_client)
    {
        try
        {
            getContext()->initResourceManagerClient();
            rm_client = getContext()->getResourceManagerClient();
        }
        catch (...)
        {
            tryLogCurrentException(log, "WorkerCachePreload: ResourceManager client init failed, will retry next tick");
        }
        if (!rm_client)
        {
            LOG_WARNING(log, "ResourceManager client not available yet, skipping this tick");
            return true;
        }
    }

    std::vector<ResourceManagement::WorkerNodeResourceData> worker_data;
    try
    {
        rm_client->getAllWorkers(worker_data);
    }
    catch (...)
    {
        tryLogCurrentException(log, "WorkerCachePreload: getAllWorkers failed");
        return false;
    }

    /// FDB-persisted state means restarts that happened while the daemon itself was
    /// down are still caught: the loaded register_time differs from the new one.
    /// An empty map on the very first load = fresh deploy => seed without warming, so we
    /// don't preload every worker at once. After that, a new worker_id is a real scale-up.
    bool warm_new_workers = true;
    if (!loaded_from_fdb)
    {
        loadStateFromFDB();
        loaded_from_fdb = true;
        if (last_seen.empty())
            warm_new_workers = false;
    }

    std::vector<WorkerCachePreloadHelpers::WorkerRegistration> workers;
    workers.reserve(worker_data.size());
    for (const auto & w : worker_data)
        workers.push_back({w.id, w.register_time, w.state == ResourceManagement::WorkerState::Running});

    const UInt32 now = static_cast<UInt32>(time(nullptr));
    auto decision = WorkerCachePreloadHelpers::decideCachePreload(
        workers, last_seen, now, warmup_grace_seconds, warm_new_workers);

    if (decision.need_preload)
    {
        /// Cooldown: collapse a burst of (re)registrations into one broadcast. We do not advance last_seen while suppressed,
        /// so the pending preload is re-detected and fires once the window expires.
        if (last_broadcast_time != 0 && now >= last_broadcast_time && now - last_broadcast_time < cooldown_seconds)
        {
            LOG_INFO(log, "Preload needed but within {}s cooldown; deferring to a later tick", cooldown_seconds);
            return true;
        }

        LOG_INFO(log, "Detected worker (re)registration, broadcasting hot-cache preload to all servers");

        std::list<CnchServerTopology> topology = getContext()->getCnchTopologyMaster()->getCurrentTopology();
        if (topology.empty())
        {
            /// Topology not available yet (e.g. not settled after a cluster event). Do not
            /// advance last_seen; otherwise this restart is consumed and the worker is
            /// treated as warm forever. Returning without persisting makes the next tick retry.
            LOG_WARNING(log, "Server topology empty; deferring preload broadcast to next tick");
            return false;
        }

        /// Per-server RPC failures stay best-effort (logged): requiring EVERY server to
        /// succeed could wedge the restart forever on a single faulty server.
        const UInt64 ts = static_cast<UInt64>(now);
        size_t succeeded = 0;
        for (const auto & host_port : topology.back().getServerList())
        {
            auto client = getContext()->getCnchServerClientPool().get(host_port);
            if (!client)
                continue;
            try
            {
                UInt32 n = client->preloadHotCacheTables(ts, preload_database);
                ++succeeded;
                LOG_DEBUG(log, "Server {} triggered preload for {} TTL tables", client->getRPCAddress(), n);
            }
            catch (...)
            {
                tryLogCurrentException(log, "WorkerCachePreload: preload RPC to " + host_port.getRPCAddress() + " failed");
            }
        }

        /// But if no server accepted the broadcast (transient cluster-wide failure / no
        /// servers reachable), treat it like an empty topology; don't advance last_seen so
        /// the next tick retries instead of silently leaving the restarted worker cold.
        if (succeeded == 0)
        {
            LOG_WARNING(log, "Preload broadcast reached no server; deferring to next tick");
            return false;
        }

        last_broadcast_time = now;
    }

    /// Only touch FDB when the map actually changed; in steady state this is a no-op every tick instead of a metastore write every 30s.
    const bool changed = (last_seen != decision.updated_last_seen);
    last_seen = std::move(decision.updated_last_seen);
    if (changed)
        persistStateToFDB();
    return true;
}

namespace WorkerCachePreloadHelpers
{

PreloadDecision decideCachePreload(
    const std::vector<WorkerRegistration> & workers,
    const std::map<String, UInt32> & last_seen,
    UInt32 now,
    UInt32 warmup_grace_seconds,
    bool warm_new_workers)
{
    PreloadDecision decision;
    decision.updated_last_seen = last_seen;

    for (const auto & w : workers)
    {
        if (!w.running)
            continue;

        /// Guard against clock skew and skip workers still inside the warmup grace.
        if (now < w.register_time || now - w.register_time < warmup_grace_seconds)
            continue;

        auto it = decision.updated_last_seen.find(w.id);
        if (it == decision.updated_last_seen.end())
        {
            /// First time we see this worker_id: always record it. Warm it only when
            /// not seeding. On the initial seed tick we must not preload every worker at once.
            decision.updated_last_seen.emplace(w.id, w.register_time);
            if (warm_new_workers)
                decision.need_preload = true;
            continue;
        }

        if (w.register_time > it->second)
        {
            /// Known worker came back with a newer registration => it restarted and
            /// wiped its disk cache => preload
            it->second = w.register_time;
            decision.need_preload = true;
        }
    }

    return decision;
}

}

void registerWorkerCachePreloadDaemon(DaemonFactory & factory)
{
    factory.registerLocalDaemonJob<DaemonJobWorkerCachePreload>("WORKER_CACHE_PRELOAD");
}

}
