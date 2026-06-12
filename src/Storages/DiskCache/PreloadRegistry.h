#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <vector>

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

class PreloadRegistry;

/// RAII handle for a single in-flight preload part. PreloadRegistry::registerPart increments the
/// partition's in-flight count and returns one of these; the destructor decrements it. 
///  Move-only; wrap in a shared_ptr to capture in a task lambda.
class PreloadHandle
{
public:
    PreloadHandle() = default;
    PreloadHandle(PreloadRegistry * registry_, String table_uuid_, String partition_id_)
        : registry(registry_), table_uuid(std::move(table_uuid_)), partition_id(std::move(partition_id_)) {}
    PreloadHandle(PreloadHandle && other) noexcept;
    PreloadHandle & operator=(PreloadHandle && other) noexcept;
    PreloadHandle(const PreloadHandle &) = delete;
    PreloadHandle & operator=(const PreloadHandle &) = delete;
    ~PreloadHandle();

private:
    void release();
    PreloadRegistry * registry = nullptr;
    String table_uuid;
    String partition_id;
};

/// Global registry tracking in-flight async preload tasks, grouped by (table_uuid, partition_id).
class PreloadRegistry
{
public:
    static PreloadRegistry & instance();

    /// Count one in-flight part and return a RAII handle; the part is decremented when the handle is destroyed. 
    /// Call once per part and tie the handle to the task so the count balances on completion, exception, or failure to schedule.
    PreloadHandle registerPart(const String & table_name, const String & table_uuid,
                               const String & partition_id, UInt64 preload_level);

    /// Decrement in-flight count for a partition.
    /// Called by PreloadHandle's destructor.
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
