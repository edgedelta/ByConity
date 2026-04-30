#include "PreloadRegistry.h"

namespace DB
{

PreloadRegistry & PreloadRegistry::instance()
{
    static PreloadRegistry inst;
    return inst;
}

void PreloadRegistry::registerParts(
    const String & table_name,
    const String & table_uuid,
    const String & partition_id,
    size_t parts_count,
    UInt64 preload_level)
{
    if (parts_count == 0)
        return;

    Key key{table_uuid, partition_id};
    std::lock_guard lock(mu);
    auto it = entries.find(key);
    if (it == entries.end())
    {
        auto entry = std::make_shared<PreloadEntry>(table_name, table_uuid, partition_id, parts_count, preload_level);
        entry->parts_in_flight.store(parts_count, std::memory_order_relaxed);
        entries.emplace(key, std::move(entry));
    }
    else
    {
        it->second->parts_submitted += parts_count;
        it->second->parts_in_flight.fetch_add(parts_count, std::memory_order_relaxed);
    }
}

void PreloadRegistry::partFinished(const String & table_uuid, const String & partition_id)
{
    Key key{table_uuid, partition_id};
    std::lock_guard lock(mu);
    auto it = entries.find(key);
    if (it == entries.end())
        return;
    if (it->second->parts_in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1)
        entries.erase(it);
}

std::vector<PreloadPartitionSnapshot> PreloadRegistry::getSnapshot() const
{
    std::lock_guard lock(mu);
    std::vector<PreloadPartitionSnapshot> result;
    result.reserve(entries.size());
    auto now = std::chrono::steady_clock::now();
    for (const auto & [_, e] : entries)
    {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - e->start_time).count();
        result.push_back({
            e->table_name,
            e->table_uuid,
            e->partition_id,
            e->parts_in_flight.load(std::memory_order_relaxed),
            e->parts_submitted,
            static_cast<UInt64>(elapsed),
            e->preload_level,
        });
    }
    return result;
}

}
