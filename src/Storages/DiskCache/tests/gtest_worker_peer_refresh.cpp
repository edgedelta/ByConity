#include <Storages/DiskCache/DiskCacheFactory.h>

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>

namespace DB
{
namespace
{
class WorkerPeerRefresh : public ::testing::Test
{
protected:
    DiskCacheFactory & factory = DiskCacheFactory::instance();
    void TearDown() override { factory.setWorkerResolverForTest(nullptr); }
};

TEST_F(WorkerPeerRefresh, SlowResolverDoesNotBlockOtherCallers)
{
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<unsigned> calls{0};
    factory.setWorkerResolverForTest([&] {
        ++calls;
        entered.set_value();
        released.wait();
        return std::unordered_map<String, WorkerPeerInfo>{{"worker", {"host:8124", 42}}};
    });
    auto owner = std::async(std::launch::async, [&] { return factory.resolvePeer("worker"); });
    entered.get_future().wait();
    auto follower = std::async(std::launch::async, [&] { return factory.resolvePeer("worker"); });
    const auto status = follower.wait_for(std::chrono::seconds(2));
    // Release before assertions so a regression fails rather than hanging teardown.
    release.set_value();
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_FALSE(follower.get().has_value());
    auto peer = owner.get();
    ASSERT_TRUE(peer.has_value());
    EXPECT_EQ(peer->register_time, 42);
    EXPECT_TRUE(factory.resolvePeer("worker").has_value());
    EXPECT_FALSE(factory.resolvePeer("missing").has_value());
    EXPECT_EQ(calls.load(), 1);
}

TEST_F(WorkerPeerRefresh, ExceptionReleasesRefreshLock)
{
    unsigned calls = 0;
    factory.setWorkerResolverForTest([&] {
        if (++calls == 1)
            throw std::runtime_error("RM unavailable");
        return std::unordered_map<String, WorkerPeerInfo>{{"worker", {"host:8124", 42}}};
    });
    EXPECT_THROW(factory.resolvePeer("worker"), std::runtime_error);
    EXPECT_TRUE(factory.resolvePeer("worker").has_value());
    EXPECT_EQ(calls, 2);
}
}
}
