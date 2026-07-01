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

#pragma once

#include <Catalog/Catalog.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <MergeTreeCommon/CnchServerTopology.h>

#include <list>
#include <map>
#include <unordered_set>
#include <vector>

namespace Poco { class Logger; namespace Util { class AbstractConfiguration; } }

namespace DB
{

/// Authoritative per-worker filter for a hot-cache preload dispatch. Returns whether the worker
/// identified by worker_id (a HostWithPorts id) should be warmed. An empty target set means a full
/// sweep -- warm every worker; otherwise only the explicitly restarted workers. Applied in the
/// dispatch *after* the real (consistent-hash OR hybrid) part assignment, so it stays consistent
/// with the dispatch mapping for every allocation mode.
bool isPreloadTargetWorker(const std::unordered_set<String> & target_workers, const String & worker_id);

/// Server-side reconciliation loop that keeps the worker instance-disk TTL cache warm.
///
/// A worker restart wipes its disk cache. This loop detects that — each worker's ResourceManager
/// register_time advances on restart — and re-warms the TTL-cached tables THIS server hosts, scoped
/// to the restarted workers only (a stable worker is never re-warmed). It runs on EVERY server (each
/// hosts a different set of tables; ownership is enforced per-table inside the loop), so it is not
/// leader-gated.
///
/// The per-server baseline (worker_id -> register_time as of the last warm) is persisted to FDB, so
/// a server restart resumes from it instead of re-warming everything, and a worker that restarted
/// while this server was down is still caught on the next tick. Initial warming of a brand-new table
/// is out of scope (done by hand); the loop only re-warms after restarts. Worker-side preload skips
/// already-cached segments, so any redundant dispatch is a cheap in-memory cache-hit check.
class CnchHotCacheWarmer : public WithContext
{
public:
    CnchHotCacheWarmer(ContextPtr context_, const Poco::Util::AbstractConfiguration & config);

    void start();
    void shutDown();

private:
    void run();
    /// Warm every TTL-cached table this server hosts, scoped to target_workers. Returns table count.
    UInt32 warmHostedTables(const std::unordered_set<String> & target_workers, UInt32 now);
    void loadBaselineFromFDB();
    void persistBaselineToFDB();

    std::shared_ptr<Catalog::Catalog> catalog;

    /// FDB key holding this server's serialized baseline map (per-server so servers don't clobber).
    String state_key;

    /// worker_id -> register_time as of the last time this server warmed it. Persisted to FDB.
    std::map<String, UInt32> baseline;
    bool loaded_from_fdb = false;

    /// Loop period. Config-overridable.
    UInt64 interval_ms = 60000;
    /// Skip a worker until it has been (re)registered at least this long, so we don't warm a
    /// half-initialized worker. Config-overridable.
    UInt32 warmup_grace_seconds = 30;
    /// Restrict warming to this database; empty = every database this server hosts. Config-overridable.
    String preload_database;

    BackgroundSchedulePool::TaskHolder task;
    Poco::Logger * log;
};

using CnchHotCacheWarmerPtr = std::shared_ptr<CnchHotCacheWarmer>;

namespace HotCacheWarmerHelpers
{

/// Minimal view of a worker needed for restart detection — decoupled from
/// ResourceManagement::WorkerNodeResourceData so the decision logic stays pure and unit-testable.
struct WorkerRegistration
{
    String id;
    UInt32 register_time;
    bool running;
};

struct WarmDecision
{
    /// Workers that restarted since the baseline and must be re-warmed; empty means nothing to do.
    std::vector<String> restarted_workers;
    /// The baseline to persist for the next tick.
    std::map<String, UInt32> updated_baseline;
};

/// Pure restart-detection step. Given the current worker registrations and this server's baseline
/// register_times, decides which workers restarted and returns the baseline to persist.
///
/// Rules:
///   - non-running worker            -> ignored
///   - up for less than grace        -> skipped this tick (avoid warming a half-initialized worker)
///   - worker_id not in baseline     -> recorded; warmed iff warm_new_workers (false on first seed)
///   - register_time increased       -> restart detected, warm
///   - register_time unchanged       -> no-op
WarmDecision decideWarm(
    const std::vector<WorkerRegistration> & workers,
    const std::map<String, UInt32> & baseline,
    UInt32 now,
    UInt32 warmup_grace_seconds,
    bool warm_new_workers);

/// A server can only resolve which TTL-cached tables it hosts once its topology view has settled.
/// Immediately after a (re)start getCurrentTopology() is empty until the topology is fetched, so a
/// warm in that window would silently own nothing. Returns false while the (latest) topology has no
/// servers, so the loop defers and retries instead of consuming the restart and leaving caches cold.
bool isPreloadTopologyReady(const std::list<CnchServerTopology> & topology);

}

}
