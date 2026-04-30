#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>

#include <common/types.h>

namespace DB
{

struct PreloadEntry
{
    String table_name;
    String table_uuid;
    String partition_id;
    std::atomic<size_t> parts_in_flight{0};
    size_t parts_submitted{0};
    std::chrono::steady_clock::time_point start_time;
    UInt64 preload_level{0};

    PreloadEntry(String tn, String uuid, String pid, size_t submitted, UInt64 level)
        : table_name(std::move(tn))
        , table_uuid(std::move(uuid))
        , partition_id(std::move(pid))
        , parts_submitted(submitted)
        , start_time(std::chrono::steady_clock::now())
        , preload_level(level)
    {
    }

    // non-copyable due to atomics
    PreloadEntry(const PreloadEntry &) = delete;
    PreloadEntry & operator=(const PreloadEntry &) = delete;
};

struct PreloadPartitionSnapshot
{
    String table_name;
    String table_uuid;
    String partition_id;
    size_t parts_in_flight;
    size_t parts_submitted;
    UInt64 elapsed_ms;
    UInt64 preload_level;
};

/// Global registry tracking in-flight async preload tasks, grouped by (table_uuid, partition_id).
class PreloadRegistry
{
public:
    static PreloadRegistry & instance();

    /// Register parts_count tasks for a partition. Returns a handle whose destructor
    /// decrements the counter (call once per part from within the task lambda).
    /// The entry is removed automatically when parts_in_flight drops to zero.
    void registerParts(const String & table_name, const String & table_uuid,
                       const String & partition_id, size_t parts_count, UInt64 preload_level);

    /// Decrement in-flight count for a partition. Removes entry when it reaches zero.
    void partFinished(const String & table_uuid, const String & partition_id);

    std::vector<PreloadPartitionSnapshot> getSnapshot() const;

private:
    using Key = std::pair<String, String>; // (table_uuid, partition_id)
    struct PairHash
    {
        size_t operator()(const Key & k) const
        {
            size_t h = std::hash<String>{}(k.first);
            h ^= std::hash<String>{}(k.second) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };

    mutable std::mutex mu;
    std::unordered_map<Key, std::shared_ptr<PreloadEntry>, PairHash> entries;
};

}
