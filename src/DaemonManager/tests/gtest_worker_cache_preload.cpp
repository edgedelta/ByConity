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
#include <gtest/gtest.h>

namespace GtestWorkerCachePreload
{

using namespace DB;
using namespace DB::DaemonManager::WorkerCachePreloadHelpers;

namespace
{
WorkerRegistration running(const String & id, UInt32 register_time)
{
    return WorkerRegistration{id, register_time, /*running=*/true};
}
}

/// Seeding tick (fresh deploy, FDB empty => warm_new_workers=false): record every
/// worker but NEVER trigger a preload, otherwise the first tick storms.
TEST(WorkerCachePreload, seed_tick_records_without_preload)
{
    std::vector<WorkerRegistration> workers{running("w1", 100), running("w2", 500)};
    std::map<String, UInt32> last_seen;

    PreloadDecision d = decideCachePreload(workers, last_seen, /*now=*/1000, /*grace=*/30, /*warm_new=*/false);

    EXPECT_FALSE(d.need_preload);
    EXPECT_EQ(d.updated_last_seen.size(), 2u);
    EXPECT_EQ(d.updated_last_seen.at("w1"), 100u);
    EXPECT_EQ(d.updated_last_seen.at("w2"), 500u);
}

/// A worker we already knew about comes back with a larger register_time => it
/// restarted and wiped its cache => trigger preload, and remember the new time.
TEST(WorkerCachePreload, increased_register_time_triggers_preload)
{
    std::vector<WorkerRegistration> workers{running("w1", 200)};
    std::map<String, UInt32> last_seen{{"w1", 100}};

    PreloadDecision d = decideCachePreload(workers, last_seen, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);

    EXPECT_TRUE(d.need_preload);
    EXPECT_EQ(d.updated_last_seen.at("w1"), 200u);
}

/// Same register_time as before => no restart => no preload.
TEST(WorkerCachePreload, unchanged_register_time_no_preload)
{
    std::vector<WorkerRegistration> workers{running("w1", 100)};
    std::map<String, UInt32> last_seen{{"w1", 100}};

    PreloadDecision d = decideCachePreload(workers, last_seen, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);

    EXPECT_FALSE(d.need_preload);
    EXPECT_EQ(d.updated_last_seen.at("w1"), 100u);
}

/// Once we are past seeding (warm_new=true), a brand-new worker_id is a genuine
/// scale-up => warm it AND record it.
TEST(WorkerCachePreload, new_worker_warmed_when_not_seeding)
{
    std::vector<WorkerRegistration> workers{running("w1", 100), running("w2", 500)};
    std::map<String, UInt32> last_seen{{"w1", 100}};

    PreloadDecision d = decideCachePreload(workers, last_seen, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);

    EXPECT_TRUE(d.need_preload);
    EXPECT_EQ(d.updated_last_seen.at("w2"), 500u);
}

/// During seeding a new worker_id is only recorded, never warmed (covered above for
/// the empty-map case; here a non-empty map can still be a seed if FDB load returned
/// partial state — warm_new=false must suppress the trigger regardless).
TEST(WorkerCachePreload, new_worker_not_warmed_during_seed)
{
    std::vector<WorkerRegistration> workers{running("w1", 100), running("w2", 500)};
    std::map<String, UInt32> last_seen{{"w1", 100}};

    PreloadDecision d = decideCachePreload(workers, last_seen, /*now=*/1000, /*grace=*/30, /*warm_new=*/false);

    EXPECT_FALSE(d.need_preload);
    EXPECT_EQ(d.updated_last_seen.at("w2"), 500u);
}

/// A worker that just (re)registered within the grace window is still warming up;
/// skip it this tick (don't record, don't trigger) so we re-evaluate after grace.
TEST(WorkerCachePreload, within_grace_window_skipped)
{
    std::vector<WorkerRegistration> workers{running("w1", 980)};  // now-20 < grace 30
    std::map<String, UInt32> last_seen{{"w1", 100}};

    PreloadDecision d = decideCachePreload(workers, last_seen, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);

    EXPECT_FALSE(d.need_preload);
    EXPECT_EQ(d.updated_last_seen.at("w1"), 100u);  // unchanged this tick
}

/// Non-Running workers (Registering/Stopped) are ignored entirely.
TEST(WorkerCachePreload, non_running_worker_skipped)
{
    std::vector<WorkerRegistration> workers{WorkerRegistration{"w1", 200, /*running=*/false}};
    std::map<String, UInt32> last_seen{{"w1", 100}};

    PreloadDecision d = decideCachePreload(workers, last_seen, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);

    EXPECT_FALSE(d.need_preload);
    EXPECT_EQ(d.updated_last_seen.at("w1"), 100u);  // unchanged
}

/// One restarted worker among several unchanged ones still triggers preload.
TEST(WorkerCachePreload, multiple_workers_one_restarted)
{
    std::vector<WorkerRegistration> workers{running("w1", 100), running("w2", 300), running("w3", 100)};
    std::map<String, UInt32> last_seen{{"w1", 100}, {"w2", 100}, {"w3", 100}};

    PreloadDecision d = decideCachePreload(workers, last_seen, /*now=*/1000, /*grace=*/30, /*warm_new=*/true);

    EXPECT_TRUE(d.need_preload);
    EXPECT_EQ(d.updated_last_seen.at("w2"), 300u);
}

} // namespace GtestWorkerCachePreload
