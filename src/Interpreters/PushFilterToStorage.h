#pragma once

#include "common/logger_useful.h"
#include "Interpreters/Context_fwd.h"
#include "Parsers/IAST_fwd.h"
#include "Storages/IStorage.h"
#include "Storages/IStorage_fwd.h"
namespace DB
{
class PushFilterToStorage : public WithContext
{
public:
    PushFilterToStorage(ConstStoragePtr storage, ContextPtr local_context);


    std::tuple<ASTs, ASTs> extractPartitionFilter(ASTPtr query_filter);

    std::tuple<ASTs, ASTs> extractPrewhereWithStats(ASTPtr query_filter, PlanNodeStatisticsPtr storage_statistics);


private:
    ConstStoragePtr storage;
    const Poco::Logger * logger = &Poco::Logger::get("PushFilterToStorage");
};
}
