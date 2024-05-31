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
#include <Parsers/IParserBase.h>

namespace DB
{

/* ALTER WAREHOUSE name
 * [RENAME TO some_name]
 * [AUTO_SUSPEND = <some number>,]
 * [AUTO_RESUME = 0|1,]
 * [MAX_CLUSTER_COUNT = <num>,]
 * [MIN_CLUSTER_COUNT = <num>,]
 * [WORKER_COUNT = <num>,]
 * [MAX_CONCURRENCY_LEVEL = num]
 * [DELETE RULE where rule_name = <rule> and queue_name = <queue>]
 * [ADD RULE rule_name set database = [<db>], table = [<table>] WHERE queue_name = <queue>]
 * [MODIFY RULE  set max_concurrency = <size> WHERE queue_name = <queue>]
 */
class ParserAlterWarehouseQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER WAREHOUSE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
