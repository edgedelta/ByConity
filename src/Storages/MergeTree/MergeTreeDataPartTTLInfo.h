/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/TTLDescription.h>

#include <map>

namespace DB
{

/// Minimal and maximal ttl for column or table
struct MergeTreeDataPartTTLInfo
{
    time_t min = 0;
    time_t max = 0;

    /// This TTL was computed on completely expired part. It doesn't make sense
    /// to select such parts for TTL again. But make sense to recalcuate TTL
    /// again for merge with multiple parts.
    bool finished = false;

    void update(time_t time)
    {
        if (time && (!min || time < min))
            min = time;

        max = std::max(time, max);
    }

    void update(const MergeTreeDataPartTTLInfo & other_info)
    {
        if (other_info.min && (!min || other_info.min < min))
            min = other_info.min;

        max = std::max(other_info.max, max);
        finished &= other_info.finished;
    }
};

/// Order is important as it would be serialized and hashed for checksums
using TTLInfoMap = std::map<String, MergeTreeDataPartTTLInfo>;

/// PartTTLInfo for all columns and table with minimal ttl for whole part
struct MergeTreeDataPartTTLInfos
{
    TTLInfoMap columns_ttl;
    MergeTreeDataPartTTLInfo table_ttl;

    /// `part_min_ttl` and `part_max_ttl` are TTLs which are used for selecting parts
    /// to merge in order to remove expired rows.
    time_t part_min_ttl = 0;
    time_t part_max_ttl = 0;
    std::optional<bool> part_finished = std::nullopt;

    TTLInfoMap rows_where_ttl;

    TTLInfoMap moves_ttl;

    TTLInfoMap recompression_ttl;

    TTLInfoMap group_by_ttl;

    /// Return the smallest max recompression TTL value
    time_t getMinimalMaxRecompressionTTL() const;
    /// FIXME: old_meta_format is used to make it compatible with old part metadata. Remove it later.
    void read(ReadBuffer & in, bool old_meta_format = false);
    void write(WriteBuffer & out) const;
    void update(const MergeTreeDataPartTTLInfos & other_infos);

    void evalPartFinished();
    /// Has any TTLs which are not calculated on completely expired parts.
    bool hasAnyNonFinishedTTLs() const;

    void updatePartMinMaxTTL(time_t time_min, time_t time_max)
    {
        if (time_min && (!part_min_ttl || time_min < part_min_ttl))
            part_min_ttl = time_min;

        if (time_max && (!part_max_ttl || time_max > part_max_ttl))
            part_max_ttl = time_max;
    }

    bool empty() const
    {
        /// part_min_ttl in minimum of rows, rows_where and group_by TTLs
        return !part_min_ttl && moves_ttl.empty() && recompression_ttl.empty();
    }
};

/// Selects the most appropriate TTLDescription using TTL info and current time.
std::optional<TTLDescription> selectTTLDescriptionForTTLInfos(const TTLDescriptions & descriptions, const TTLInfoMap & ttl_info_map, time_t current_time, bool use_max);

}
