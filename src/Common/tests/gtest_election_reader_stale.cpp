#include <optional>
#include <string>

#include <Common/StorageElection/ElectionReader.h>
#include <Common/StorageElection/KvStorage.h>
#include <Protos/cnch_common.pb.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
/// In-memory KV store backing an ElectionReader. get() returns whatever was last set(),
/// with a non-zero version so refresh() treats it as a valid read.
class FakeKvStore : public IKvStorage
{
public:
    void put(const String &, const String &, bool) override { }
    std::pair<bool, String> putCAS(const String &, const String &, const String &, bool) override { return {true, ""}; }
    uint64_t get(const String &, String & value) override
    {
        value = stored;
        return stored.empty() ? 0 : 1;
    }

    void set(const String & v) { stored = v; }

private:
    String stored;
};

/// Serialize a LeaderInfo pointing at host:rpc_port with the given lease status.
String makeLeaderInfo(const String & host, uint32_t rpc_port, Protos::LeaderLease::Status status)
{
    Protos::LeaderInfo info;
    auto * addr = info.mutable_address();
    addr->set_hostname(host);
    addr->set_host(host);
    addr->set_rpc_port(rpc_port);
    addr->set_tcp_port(rpc_port + 1);

    auto * lease = info.mutable_lease();
    lease->set_elected_time(1);
    lease->set_last_refresh_time(1);
    lease->set_refresh_interval_ms(1000);
    lease->set_expired_interval_ms(5000);
    lease->set_status(status);

    String out;
    EXPECT_TRUE(info.SerializeToString(&out));
    return out;
}
}

/// Confirms the incident's root-cause defect: when the election lease is not Ready (e.g. during an
/// out-of-sequence RM restart), refresh() does NOT update curr_leader_info — it keeps serving the
/// previous (now-dead) leader address. Combined with callToLeaderWrapper's `new_leader ==
/// leader_host_port` terminal throw, this latches an RM client onto a dead endpoint until restart.
TEST(ElectionReader, KeepsStaleLeaderWhenLeaseNotReady)
{
    auto store = std::make_shared<FakeKvStore>();
    ElectionReader reader(store, "/test/election");

    /// 1. A Ready leader at A is picked up.
    store->set(makeLeaderInfo("10.0.0.1", 100, Protos::LeaderLease::Ready));
    ASSERT_TRUE(reader.refresh());
    auto a = reader.tryGetLeaderInfo();
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->getRPCAddress(), "10.0.0.1:100");

    /// 2. Leader moves to B but the lease is not yet Ready (restart / re-election window).
    ///    refresh() returns false AND the reader keeps returning the stale A — this is the bug.
    store->set(makeLeaderInfo("10.0.0.2", 200, Protos::LeaderLease::Wait));
    EXPECT_FALSE(reader.refresh());
    auto during = reader.tryGetLeaderInfo();
    ASSERT_TRUE(during.has_value());
    EXPECT_EQ(during->getRPCAddress(), "10.0.0.1:100"); /// still the dead A, not B

    /// 3. Once B's lease is Ready, the reader finally updates.
    store->set(makeLeaderInfo("10.0.0.2", 200, Protos::LeaderLease::Ready));
    ASSERT_TRUE(reader.refresh());
    auto b = reader.tryGetLeaderInfo();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->getRPCAddress(), "10.0.0.2:200");
}
