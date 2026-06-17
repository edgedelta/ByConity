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

#include <list>
#include <MergeTreeCommon/CnchServerTopology.h>

namespace DB
{

/// A server can only resolve which TTL-cached tables it owns once its topology view has
/// settled. Immediately after a (re)start getCurrentTopology() is empty until the topology
/// is fetched, so a hot-cache preload sweep in that window would silently own nothing.
/// Returns false while the (latest) topology has no servers, so the server reports
/// not-ready and the cache-preload daemon retries the broadcast instead of consuming the
/// worker-restart event and leaving caches cold.
bool isPreloadTopologyReady(const std::list<CnchServerTopology> & topology);

}
