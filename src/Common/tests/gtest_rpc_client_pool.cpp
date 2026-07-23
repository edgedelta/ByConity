#include <Common/HostWithPorts.h>
#include <Common/RpcClientPool.h>

#include <gtest/gtest.h>

#include <atomic>

using namespace DB;

namespace
{
/// Minimal client satisfying the part of the RpcClientPool<T> contract exercised by
/// get(host_ports): constructible from HostWithPorts, ok()/reset()/getActiveTime(), and
/// static getName(). Health and age are test-controlled instead of going through brpc.
struct MockRpcClient
{
    explicit MockRpcClient(HostWithPorts host_ports_) : host_ports(std::move(host_ports_)) { }
    static String getName() { return "MockRpcClient"; }
    bool ok() const { return ok_.load(); }
    void reset() { ok_.store(true); }
    void setOk(bool v) { ok_.store(v); }
    time_t getActiveTime() const { return active_time; }

    HostWithPorts host_ports;
    std::atomic_bool ok_{true};
    time_t active_time{5}; /// default: already past the 1s rebuild cooldown
};

HostWithPorts makeHost()
{
    return HostWithPorts("1.2.3.4", /*rpc*/ 1000, /*tcp*/ 1001, /*http*/ 1002, 0, 0, "worker-0");
}

HostWithPorts makeOtherHost()
{
    return HostWithPorts("1.2.3.5", /*rpc*/ 1000, /*tcp*/ 1001, /*http*/ 1002, 0, 0, "worker-1");
}
}

TEST(RpcClientPool, ReusesHealthyClient)
{
    RpcClientPool<MockRpcClient> pool("test_vw", [] { return HostWithPortsVec{}; });
    auto host = makeHost();

    auto c1 = pool.get(host);
    ASSERT_TRUE(c1);
    auto c2 = pool.get(host);
    /// A healthy cached client must be reused (same instance).
    EXPECT_EQ(c1.get(), c2.get());
}

TEST(RpcClientPool, RecreatesUnhealthyClient)
{
    RpcClientPool<MockRpcClient> pool("test_vw", [] { return HostWithPortsVec{}; });
    auto host = makeHost();

    auto c1 = pool.get(host);
    ASSERT_TRUE(c1);
    c1->setOk(false);

    /// The cached client is now unhealthy and past the rebuild cooldown. Before the fix,
    /// try_emplace was a no-op for the existing key and kept returning the same dead client;
    /// now the entry is dropped and a fresh, healthy client is created.
    auto c2 = pool.get(host);
    EXPECT_NE(c1.get(), c2.get());
    EXPECT_TRUE(c2->ok());
    /// The dead client must have been erased, not kept alongside the new one.
    EXPECT_EQ(pool.getClientsMapSize(), 1);

    /// The rebuilt client is served from cache again.
    auto c3 = pool.get(host);
    EXPECT_EQ(c2.get(), c3.get());
}

TEST(RpcClientPool, KeepsUnhealthyClientWithinCooldown)
{
    RpcClientPool<MockRpcClient> pool("test_vw", [] { return HostWithPortsVec{}; });
    auto host = makeHost();

    auto c1 = pool.get(host);
    ASSERT_TRUE(c1);
    c1->setOk(false);
    c1->active_time = 0; /// broken but younger than the 1s cooldown

    /// Within the cooldown the broken client is returned as-is, capping rebuild churn at
    /// one client per second per endpoint during brownouts.
    auto c2 = pool.get(host);
    EXPECT_EQ(c1.get(), c2.get());
    EXPECT_EQ(pool.getClientsMapSize(), 1);
}

TEST(RpcClientPool, EndpointsAreIndependent)
{
    RpcClientPool<MockRpcClient> pool("test_vw", [] { return HostWithPortsVec{}; });

    auto w0 = pool.get(makeHost());
    auto w1 = pool.get(makeOtherHost());
    EXPECT_NE(w0.get(), w1.get());

    w0->setOk(false);

    auto w0_rebuilt = pool.get(makeHost());
    auto w1_again = pool.get(makeOtherHost());

    EXPECT_NE(w0.get(), w0_rebuilt.get());
    /// A healthy endpoint is untouched by the other endpoint's eviction.
    EXPECT_EQ(w1.get(), w1_again.get());
    EXPECT_EQ(pool.getClientsMapSize(), 2);
}
