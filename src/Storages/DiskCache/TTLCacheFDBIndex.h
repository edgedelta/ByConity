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
#include <tuple>
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

/// FDB-backed reverse index (DCIREV) for DiskCacheTTL peer-steal.
/// On set(): async-writes a key entry so peers can steal it.
/// On evictPart(): issues a single FDB clean() covering the reverse entries of a part.
/// findPeerOwner(): looks up which worker has a segment cached.
/// There is no forward index / reconcile: on instance disk the cache does not survive a
/// restart, so there is nothing to restore (DiskCacheTTL::load starts cold).
class TTLCacheFDBIndex
{
public:
    TTLCacheFDBIndex(
        std::shared_ptr<Catalog::IMetaStore> metastore_,
        const String & name_space,
        const String & table_uuid,
        const String & own_worker_id_);

    ~TTLCacheFDBIndex();

    /// Enqueue async reverse-index write after a segment is successfully cached.
    /// partition_id must be the same value the rest of the cache uses for this segment.
    void onSet(UInt128 key, const String & partition_id);

    /// Issue FDB clean() for the reverse entries of one part.
    /// partition_id: same derivation used at onSet().
    void evictPart(const String & partition_id, UInt64 hash_high);

    /// Issue FDB clean() for the reverse entry of a single segment. The reverse key is fixed-width,
    /// so it is not a prefix of any other key: use this instead of evictPart() when only one
    /// segment is gone and the part's other segments are still cached.
    void evictSegment(UInt128 key, const String & partition_id);

    /// Issue FDB clean() for all reverse entries of this table.
    void evictTable();

    /// Look up whether any peer worker has this segment cached.
    /// Returns peer worker_id if found, nullopt otherwise.
    std::optional<String> findPeerOwner(UInt128 key, const String & partition_id);

private:
    struct PendingOp
    {
        enum class Type { Set, Evict } type;
        String key;    // full FDB key (Set) or prefix to clean (Evict)
        String value;  // serialized entry (Set only)
    };

    void bgLoop();
    void flush(std::vector<PendingOp> & ops);

    String makeRevKey(UInt128 key, const String & partition_id) const;
    String makeRevPartPrefix(const String & partition_id, UInt64 hash_high) const;

    std::shared_ptr<Catalog::IMetaStore> metastore;
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
