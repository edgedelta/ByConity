#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

/** Implements system table disk_ttl_cache_preloads
  * Shows in-flight async preload tasks grouped by (worker, table, partition)
  */
class StorageSystemDiskTTLCachePreloads final : public shared_ptr_helper<StorageSystemDiskTTLCachePreloads>,
    public IStorageSystemOneBlock<StorageSystemDiskTTLCachePreloads>
{
    friend struct shared_ptr_helper<StorageSystemDiskTTLCachePreloads>;
public:
    std::string getName() const override { return "SystemDiskTTLCachePreloads"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    StorageSystemDiskTTLCachePreloads(const StorageID & table_id_);

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
