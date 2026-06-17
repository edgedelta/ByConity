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

#include <CloudServices/HotCachePreloadHelper.h>
#include <MergeTreeCommon/CnchServerTopology.h>
#include <Common/HostWithPorts.h>
#include <gtest/gtest.h>

#include <list>

namespace GtestHotCachePreload
{

using namespace DB;

namespace
{
CnchServerTopology topologyWith(const std::vector<String> & server_hosts)
{
    CnchServerTopology topo;
    for (const auto & h : server_hosts)
        topo.addServer(HostWithPorts(h, /*rpc_port=*/1234));
    return topo;
}
}

/// Right after a server (re)start getCurrentTopology() is empty until the topology is
/// fetched. The server must report not-ready so the cache-preload daemon retries instead
/// of consuming the worker-restart event -- the simultaneous server+worker restart gap.
TEST(HotCachePreload, empty_topology_not_ready)
{
    EXPECT_FALSE(isPreloadTopologyReady(std::list<CnchServerTopology>{}));
}

/// A topology entry whose server list is empty is likewise not settled.
TEST(HotCachePreload, topology_with_no_servers_not_ready)
{
    std::list<CnchServerTopology> topology{topologyWith({})};
    EXPECT_FALSE(isPreloadTopologyReady(topology));
}

/// A settled topology (has servers) lets the server resolve ownership and warm -- ready.
TEST(HotCachePreload, settled_topology_ready)
{
    std::list<CnchServerTopology> topology{topologyWith({"10.0.0.1", "10.0.0.2"})};
    EXPECT_TRUE(isPreloadTopologyReady(topology));
}

/// Readiness keys off the most-recent topology (back()): an empty latest entry means a
/// fresh cluster event has not settled yet, even if an older entry still had servers.
TEST(HotCachePreload, latest_topology_empty_not_ready)
{
    std::list<CnchServerTopology> topology{topologyWith({"10.0.0.1"}), topologyWith({})};
    EXPECT_FALSE(isPreloadTopologyReady(topology));
}

/// A single-server settled topology is ready.
TEST(HotCachePreload, single_server_ready)
{
    std::list<CnchServerTopology> topology{topologyWith({"10.0.0.1"})};
    EXPECT_TRUE(isPreloadTopologyReady(topology));
}

} // namespace GtestHotCachePreload
