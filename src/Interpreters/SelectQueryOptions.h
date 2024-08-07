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

#include <Core/QueryProcessingStage.h>
#include <Processors/IntermediateResult/TableScanCacheInfo.h>

namespace DB
{

/**
 * to_stage
 * - the stage to which the query is to be executed. By default - till to the end.
 *   You can perform till the intermediate aggregation state, which are combined from different servers for distributed query processing.
 *
 * subquery_depth
 * - to control the limit on the depth of nesting of subqueries. For subqueries, a value that is incremented by one is passed;
 *   for INSERT SELECT, a value 1 is passed instead of 0.
 *
 * only_analyze
 * - the object was created only for query analysis.
 *
 * is_subquery
 * - there could be some specific for subqueries. Ex. there's no need to pass duplicated columns in results, cause of indirect results.
 *
 * is_internal
 * - the object was created only for internal queries.
 */
struct SelectQueryOptions
{
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;
    bool only_analyze = false;
    bool modify_inplace = false;
    bool remove_duplicates = false;
    bool ignore_quota = false;
    bool ignore_limits = false;
    /// This flag is needed to analyze query ignoring table projections.
    /// It is needed because we build another one InterpreterSelectQuery while analyzing projections.
    /// It helps to avoid infinite recursion.
    bool ignore_projections = false;
    /// This flag is also used for projection analysis.
    /// It is needed because lazy normal projections require special planning in FetchColumns stage, such as adding WHERE transform.
    /// It is also used to avoid adding aggregating step when aggregate projection is chosen.
    bool is_projection_query = false;
    bool ignore_alias = false;
    bool is_internal = false;
    bool is_subquery = false; // non-subquery can also have subquery_depth > 0, e.g. insert select
    bool with_all_cols = false; /// asterisk include materialized and aliased columns
    bool without_extended_objects = false;

    /// cache info, used for matching with cache when enable query cache
    TableScanCacheInfo cache_info;

    /**
     * if true, it means we should generate plans being compatible with distributed plansegments.
     */
    bool distributed_stages = false;

    SelectQueryOptions(
        QueryProcessingStage::Enum stage = QueryProcessingStage::Complete,
        size_t depth = 0,
        bool is_subquery_ = false)
        : to_stage(stage), subquery_depth(depth), is_subquery(is_subquery_)
    {}

    SelectQueryOptions copy() const { return *this; }

    SelectQueryOptions subquery() const
    {
        SelectQueryOptions out = *this;
        out.to_stage = QueryProcessingStage::Complete;
        ++out.subquery_depth;
        out.is_subquery = true;
        return out;
    }

    SelectQueryOptions & analyze(bool dry_run = true)
    {
        only_analyze = dry_run;
        return *this;
    }

    SelectQueryOptions & modify(bool value = true)
    {
        modify_inplace = value;
        return *this;
    }

    SelectQueryOptions & noModify() { return modify(false); }

    SelectQueryOptions & removeDuplicates(bool value = true)
    {
        remove_duplicates = value;
        return *this;
    }

    SelectQueryOptions & noSubquery()
    {
        subquery_depth = 0;
        return *this;
    }

    SelectQueryOptions & ignoreLimits(bool value = true)
    {
        ignore_limits = value;
        return *this;
    }

    SelectQueryOptions & ignoreProjections(bool value = true)
    {
        ignore_projections = value;
        return *this;
    }

    SelectQueryOptions & projectionQuery(bool value = true)
    {
        is_projection_query = value;
        return *this;
    }

    SelectQueryOptions & ignoreAlias(bool value = true)
    {
        ignore_alias = value;
        return *this;
    }

    SelectQueryOptions & setInternal(bool value = false)
    {
        is_internal = value;
        return *this;
    }

    SelectQueryOptions & setWithAllColumns(bool value = true)
    {
        with_all_cols = value;
        return *this;
    }

    SelectQueryOptions & distributedStages(bool value = true)
    {
        distributed_stages = value;
        return *this;
    }

    SelectQueryOptions & setWithoutExtendedObject(bool value = true)
    {
        without_extended_objects = value;
        return *this;
    }
};

}
