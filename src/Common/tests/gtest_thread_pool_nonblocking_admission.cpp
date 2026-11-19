#include <Common/ThreadPool.h>
#include <gtest/gtest.h>
#include <chrono>
#include <future>

TEST(ThreadPool, FullQueueRejectsWithoutWaitingForRunningJob)
{
    ThreadPool pool(1, 1, 1);
    std::promise<void> release;
    auto released = release.get_future().share();
    pool.scheduleOrThrow([released] { released.wait(); }, 0, 0);
    auto admission = std::async(std::launch::async, [&] {
        try
        {
            pool.scheduleOrThrow([] {}, 0, 0);
            return false;
        }
        catch (...)
        {
            return true;
        }
    });
    const auto status = admission.wait_for(std::chrono::seconds(2));
    release.set_value();
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(admission.get());
    pool.wait();
    EXPECT_NO_THROW(pool.scheduleOrThrow([] {}, 0, 0));
    pool.wait();
}
