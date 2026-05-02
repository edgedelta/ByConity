#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

#include <Catalog/IMetastore.h>
#include <Core/Types.h>
#include <Disks/IVolume.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{

class DiskCacheTTL;
class DiskCacheTTLMeta;

/// FDB-backed index for DiskCacheTTL.
/// On set(): async-writes an entry so the in-memory cache_map can be restored from
/// FDB on the next startup instead of doing a slow disk scan.
/// On evictPart(): issues a single FDB clean() covering all segments of a part.
/// reconcile(): called from load() — scans FDB, verifies files on disk, populates cache_map.
class TTLCacheFDBIndex
{
public:
    TTLCacheFDBIndex(
        std::shared_ptr<Catalog::IMetaStore> metastore_,
        const String & name_space,
        const String & worker_id,
        const String & table_uuid,
        const String & own_endpoint_);

    ~TTLCacheFDBIndex();

    /// Enqueue async FDB write after a segment is successfully cached.
    void onSet(UInt128 key, const String & seg_name, size_t size, time_t part_ts);

    /// Issue FDB clean() for all segments of one part (hash_high).
    /// partition_id: YYYYMMDD string derived from max_timestamp (same as path structure).
    void evictPart(const String & partition_id, UInt64 hash_high);

    /// Look up whether any peer worker has this segment cached.
    /// Returns peer RPC endpoint (host:port) if found, nullopt otherwise.
    std::optional<String> findPeerOwner(UInt128 key, const String & partition_id);

    /// Scan FDB index and restore cache_map.
    /// Calls on_restore for each successfully restored entry so the
    /// caller can update partition_stats without re-scanning cache_map
    /// Returns {entries, bytes} restored, or nullopt if index is empty/unavailable.
    std::optional<std::pair<size_t, size_t>> reconcile(
        std::map<UInt128, std::shared_ptr<DiskCacheTTLMeta>> & cache_map,
        std::mutex & cache_mutex,
        const VolumePtr & volume,
        std::function<std::filesystem::path(UInt128, const String &)> get_rel_path,
        std::function<bool(time_t)> should_cache,
        std::function<void(time_t, size_t)> on_restore = nullptr);

private:
    struct PendingOp
    {
        enum class Type { Set, Evict } type;
        String key;    // full FDB key (Set) or prefix to clean (Evict)
        String value;  // serialized entry (Set only)
    };

    void bgLoop();
    void flush(std::vector<PendingOp> & ops);

    String makeSegKey(UInt128 key, const String & partition_id) const;
    String makePartPrefix(const String & partition_id, UInt64 hash_high) const;

    static String encodeValue(const String & seg_name, size_t size, time_t part_ts);
    static bool decodeValue(const String & raw, String & seg_name, size_t & size, time_t & part_ts);

    String makeRevKey(UInt128 key, const String & partition_id) const;
    String makeRevPartPrefix(const String & partition_id, UInt64 hash_high) const;

    std::shared_ptr<Catalog::IMetaStore> metastore;
    String key_prefix;       // escapeString(ns) + "_DCI_" + escapeString(worker_id) + "_" + table_uuid
    String rev_key_prefix;   // escapeString(ns) + "_DCIREV_" + table_uuid
    String own_worker_id;    // stable worker identity (WORKER_ID env), stored in DCIREV_ values and used to skip self

    std::mutex mu;
    std::deque<PendingOp> queue;
    std::condition_variable cv;
    std::thread bg;
    std::atomic<bool> stopped{false};

    static constexpr size_t BATCH_SIZE = 100;
    static constexpr size_t MAX_WAIT_MS = 5000;

    Poco::Logger * log;
};

}
