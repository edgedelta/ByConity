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

#include <DaemonManager/DaemonJob.h>
#include <Catalog/Catalog.h>

#include <map>
#include <vector>

namespace DB::DaemonManager
{

/// Detects worker restarts which wipe the instance-disk TTL cache and re-warms
/// the cache by broadcasting a parameterless preload signal to every server. Each
/// server then preloads the TTL-cached tables it hosts; idempotent cache writes
/// mean only the freshly-restarted worker actually fetches.
class DaemonJobWorkerCachePreload : public DaemonJob
{
public:
    explicit DaemonJobWorkerCachePreload(ContextMutablePtr global_context_);

protected:
    bool executeImpl() override;

private:
    std::shared_ptr<Catalog::Catalog> catalog;

    /// FDB key holding the serialized last_seen map
    String state_key;

    /// worker_id -> last observed register_time. Persisted to FDB so restarts that
    /// happen while the daemon itself is down are still caught on the next tick.
    std::map<String, UInt32> last_seen;
    bool loaded_from_fdb = false;

    /// A freshly (re)registered worker is skipped until it has been up this long,
    /// so we don't preload into a half-initialized worker. Config-overridable.
    UInt32 warmup_grace_seconds = 30;

    /// Minimum gap between broadcasts. Collapses bursts (RM failover, rolling restart,
    /// mass scale-out) into a single fan-out instead of one-per-tick. Config-overridable.
    UInt32 cooldown_seconds = 300;
    UInt32 last_broadcast_time = 0;

    /// Restrict warming to this database; empty = every database. Config-overridable.
    String preload_database;

    void loadStateFromFDB();
    void persistStateToFDB();
};

namespace WorkerCachePreloadHelpers
{

/// Minimal view of a worker needed for restart detection — decoupled from
/// ResourceManagement::WorkerNodeResourceData so the decision logic stays pure
/// and unit-testable.
struct WorkerRegistration
{
    String id;
    UInt32 register_time;
    bool running;
};

struct PreloadDecision
{
    bool need_preload = false;
    std::map<String, UInt32> updated_last_seen;
};

/// Pure restart-detection step. Given the current worker registrations and the
/// previously-seen register_times, decides whether any known worker restarted
/// and returns the map to persist for the next tick.
///
/// Rules:
///   - non-running worker            -> ignored
///   - up for less than grace        -> skipped this tick
///   - worker_id not seen before     -> recorded; triggers iff warm_new_workers
///   - register_time increased       -> restart detected, trigger preload
///   - register_time unchanged       -> no-op
PreloadDecision decideCachePreload(
    const std::vector<WorkerRegistration> & workers,
    const std::map<String, UInt32> & last_seen,
    UInt32 now,
    UInt32 warmup_grace_seconds,
    bool warm_new_workers);

}
}
