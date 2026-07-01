#include <atomic>

#include <Common/HostWithPorts.h>
#include <Common/RpcClientPool.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
/// Minimal client satisfying the part of the RpcClientPool<T> contract exercised by
/// get(host_ports): constructible from HostWithPorts, ok()/reset(), and static getName().
struct MockRpcClient
{
    explicit MockRpcClient(HostWithPorts host_ports_) : host_ports(std::move(host_ports_)) { }
    static String getName() { return "MockRpcClient"; }
    bool ok() const { return ok_.load(); }
    void reset() { ok_.store(true); }
    void setOk(bool v) { ok_.store(v); }

    HostWithPorts host_ports;
    std::atomic_bool ok_{true};
};

HostWithPorts makeHost()
{
    return HostWithPorts("1.2.3.4", /*rpc*/ 1000, /*tcp*/ 1001, /*http*/ 1002, 0, 0, "worker-0");
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

    /// The cached client is now unhealthy. Before the fix, try_emplace was a no-op for the
    /// existing key and kept returning the same dead client; now the entry is dropped and a
    /// fresh, healthy client is created.
    auto c2 = pool.get(host);
    EXPECT_NE(c1.get(), c2.get());
    EXPECT_TRUE(c2->ok());
}
