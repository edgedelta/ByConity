#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

/** Implements system table disk_ttl_cache_tables
  * Shows per-table TTL disk cache statistics
  */
class StorageSystemDiskTTLCacheTables final : public shared_ptr_helper<StorageSystemDiskTTLCacheTables>,
    public IStorageSystemOneBlock<StorageSystemDiskTTLCacheTables>
{
    friend struct shared_ptr_helper<StorageSystemDiskTTLCacheTables>;
public:
    std::string getName() const override { return "SystemDiskTTLCacheTables"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    StorageSystemDiskTTLCacheTables(const StorageID & table_id_);

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
