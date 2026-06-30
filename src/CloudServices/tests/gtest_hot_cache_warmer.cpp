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
#include <MergeTreeCommon/CnchServerTopology.h>
#include <Common/HostWithPorts.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <list>
#include <unordered_set>

namespace GtestHotCacheWarmer
{

using namespace DB;
using namespace DB::HotCacheWarmerHelpers;

namespace
{
bool contains(const std::vector<String> & v, const String & id)
{
    return std::find(v.begin(), v.end(), id) != v.end();
}

CnchServerTopology topologyWith(const std::vector<String> & server_hosts)
{
    CnchServerTopology topo;
    for (const auto & h : server_hosts)
        topo.addServer(HostWithPorts(h, /*rpc_port=*/1234));
    return topo;
}
}

/// Fresh deploy: every running worker is recorded but NONE warmed,
TEST(HotCacheWarmer, seed_tick_records_without_warming)
{
    std::vector<WorkerRegistration> workers{{"w0", 100, true}, {"w1", 100, true}};
    auto d = decideWarm(workers, /*baseline=*/{}, /*now=*/1000, /*grace=*/30, /*warm_new=*/false);
    EXPECT_TRUE(d.restarted_workers.empty());
    EXPECT_EQ(d.updated_baseline.size(), 2u);
    EXPECT_EQ(d.updated_baseline.at("w0"), 100u);
}

/// A known worker that comes back with a newer register_time restarted => warm it, advance baseline.
TEST(HotCacheWarmer, increased_register_time_triggers_warm)
{
    std::vector<WorkerRegistration> workers{{"w0", 500, true}};
    std::map<String, UInt32> baseline{{"w0", 100}};
    auto d = decideWarm(workers, baseline, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);
    EXPECT_TRUE(contains(d.restarted_workers, "w0"));
    EXPECT_EQ(d.updated_baseline.at("w0"), 500u);
}

/// Unchanged register_time => steady state, nothing to do.
TEST(HotCacheWarmer, unchanged_register_time_no_warm)
{
    std::vector<WorkerRegistration> workers{{"w0", 100, true}};
    std::map<String, UInt32> baseline{{"w0", 100}};
    auto d = decideWarm(workers, baseline, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);
    EXPECT_TRUE(d.restarted_workers.empty());
}

/// A brand-new worker id seen after seeding (warm_new=true, e.g. scale-out) is warmed.
TEST(HotCacheWarmer, new_worker_warmed_when_not_seeding)
{
    std::vector<WorkerRegistration> workers{{"w0", 100, true}, {"w_new", 100, true}};
    std::map<String, UInt32> baseline{{"w0", 100}};
    auto d = decideWarm(workers, baseline, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);
    EXPECT_TRUE(contains(d.restarted_workers, "w_new"));
    EXPECT_FALSE(contains(d.restarted_workers, "w0"));
}

/// A worker still inside the warmup grace window is skipped (avoid warming a half-initialized worker).
TEST(HotCacheWarmer, within_grace_window_skipped)
{
    std::vector<WorkerRegistration> workers{{"w0", 990, true}};
    std::map<String, UInt32> baseline{{"w0", 100}};
    auto d = decideWarm(workers, baseline, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);
    EXPECT_TRUE(d.restarted_workers.empty());
    /// Baseline not advanced either — it gets picked up once it clears grace.
    EXPECT_EQ(d.updated_baseline.at("w0"), 100u);
}

/// Non-running workers are ignored entirely.
TEST(HotCacheWarmer, non_running_worker_skipped)
{
    std::vector<WorkerRegistration> workers{{"w0", 500, false}};
    std::map<String, UInt32> baseline{{"w0", 100}};
    auto d = decideWarm(workers, baseline, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);
    EXPECT_TRUE(d.restarted_workers.empty());
    EXPECT_EQ(d.updated_baseline.at("w0"), 100u);
}

/// Only the restarted worker among several is warmed; stable ones are left alone.
TEST(HotCacheWarmer, only_restarted_worker_warmed)
{
    std::vector<WorkerRegistration> workers{{"w0", 100, true}, {"w1", 700, true}, {"w2", 100, true}};
    std::map<String, UInt32> baseline{{"w0", 100}, {"w1", 100}, {"w2", 100}};
    auto d = decideWarm(workers, baseline, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);
    EXPECT_EQ(d.restarted_workers.size(), 1u);
    EXPECT_TRUE(contains(d.restarted_workers, "w1"));
}

/// Clock skew: a register_time in the future (now < register_time) must not warm and must not
/// advance the baseline — the `now < register_time` guard protects against an unsigned underflow.
TEST(HotCacheWarmer, future_register_time_skipped)
{
    std::vector<WorkerRegistration> workers{{"w0", 2000, true}};
    std::map<String, UInt32> baseline{{"w0", 100}};
    auto d = decideWarm(workers, baseline, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);
    EXPECT_TRUE(d.restarted_workers.empty());
    EXPECT_EQ(d.updated_baseline.at("w0"), 100u);
}

/// NOTE: these cover the pure predicate only. The load-bearing claim that the filter stays correct
/// when applied *after* the real consistent-hash/bucket/hybrid part assignment lives in
/// StorageCnchMergeTree::sendPreloadTasks and is not unit-testable here (needs a live cluster); it
/// rests on the id-namespace match (RM worker id == HostWithPorts::id == client->getHostWithPortsID())
/// and is verified by manual bucket/hybrid cluster testing.

/// Empty target set => full sweep: every worker warmed.
TEST(HotCacheWarmer, empty_target_warms_every_worker)
{
    std::unordered_set<String> target;
    EXPECT_TRUE(isPreloadTargetWorker(target, "w0"));
    EXPECT_TRUE(isPreloadTargetWorker(target, "w1"));
}

/// Non-empty target set restricts the warm to the listed (restarted) workers.
TEST(HotCacheWarmer, target_set_warms_only_listed_workers)
{
    std::unordered_set<String> target{"w1", "w3"};
    EXPECT_FALSE(isPreloadTargetWorker(target, "w0"));
    EXPECT_TRUE(isPreloadTargetWorker(target, "w1"));
    EXPECT_FALSE(isPreloadTargetWorker(target, "w2"));
    EXPECT_TRUE(isPreloadTargetWorker(target, "w3"));
}

TEST(HotCacheWarmer, empty_topology_not_ready)
{
    EXPECT_FALSE(isPreloadTopologyReady(std::list<CnchServerTopology>{}));
}

TEST(HotCacheWarmer, topology_with_no_servers_not_ready)
{
    std::list<CnchServerTopology> topology{topologyWith({})};
    EXPECT_FALSE(isPreloadTopologyReady(topology));
}

TEST(HotCacheWarmer, settled_topology_ready)
{
    std::list<CnchServerTopology> topology{topologyWith({"10.0.0.1", "10.0.0.2"})};
    EXPECT_TRUE(isPreloadTopologyReady(topology));
}

/// Readiness keys off the most-recent topology (back()): an empty latest entry means a fresh cluster event has not settled yet, even if an older entry still had servers.
TEST(HotCacheWarmer, latest_topology_empty_not_ready)
{
    std::list<CnchServerTopology> topology{topologyWith({"10.0.0.1"}), topologyWith({})};
    EXPECT_FALSE(isPreloadTopologyReady(topology));
}

} // namespace GtestHotCacheWarmer
