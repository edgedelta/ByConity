#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

/** Implements system table disk_ttl_cache_partitions
  * Shows per-partition TTL disk cache statistics (hits, misses, size)
  */
class StorageSystemDiskTTLCachePartitions final : public shared_ptr_helper<StorageSystemDiskTTLCachePartitions>,
    public IStorageSystemOneBlock<StorageSystemDiskTTLCachePartitions>
{
    friend struct shared_ptr_helper<StorageSystemDiskTTLCachePartitions>;
public:
    std::string getName() const override { return "SystemDiskTTLCachePartitions"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    StorageSystemDiskTTLCachePartitions(const StorageID & table_id_);

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
