#include <Storages/DiskCache/TTLCacheFDBIndex.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheFactory.h>

#include <Catalog/MetastoreCommon.h>
#include <Catalog/MetastoreProxy.h>
#include <Catalog/StringHelper.h>
#include <Common/hex.h>
#include <fmt/core.h>
#include <string>

namespace DB
{


TTLCacheFDBIndex::TTLCacheFDBIndex(
    std::shared_ptr<Catalog::IMetaStore> metastore_,
    const String & name_space,
    const String & table_uuid,
    const String & own_worker_id_)
    : metastore(std::move(metastore_))
    , rev_key_prefix(Catalog::escapeString(name_space) + "_DCIREV_" + table_uuid)
    , own_worker_id(own_worker_id_)
    , log(&Poco::Logger::get("TTLCacheFDBIndex"))
{
    bg = std::thread([this] { bgLoop(); });
}

TTLCacheFDBIndex::~TTLCacheFDBIndex()
{
    {
        std::lock_guard lk(mu);
        stopped = true;
    }
    cv.notify_all();
    if (bg.joinable())
        bg.join();
}

String TTLCacheFDBIndex::makeRevKey(UInt128 key, const String & partition_id) const
{
    return rev_key_prefix + "_" + partition_id + "_" + getHexUIntLowercase(key.items[0]) + "_" + getHexUIntLowercase(key.items[1]);
}

String TTLCacheFDBIndex::makeRevPartPrefix(const String & partition_id, UInt64 hash_high) const
{
    return rev_key_prefix + "_" + partition_id + "_" + getHexUIntLowercase(hash_high) + "_";
}

// Only the reverse (DCIREV) index is written: it's the peer-steal index.
// partition_id is supplied by the caller so the key matches the prefixes evictPart cleans and the lookup in findPeerOwner.
void TTLCacheFDBIndex::onSet(UInt128 key, const String & partition_id)
{
    // Value is "<worker_id>:<register_time>". register_time is our RM registration epoch: after
    // a restart we re-register with a new one, so entries written by the previous incarnation
    // become detectably stale in findPeerOwner.
    UInt32 own_epoch = 0;
    if (auto self = DiskCacheFactory::instance().resolvePeer(own_worker_id))
        own_epoch = self->register_time;

    PendingOp rev;
    rev.type  = PendingOp::Type::Set;
    rev.key   = makeRevKey(key, partition_id);
    rev.value = own_worker_id + ":" + std::to_string(own_epoch);

    {
        std::lock_guard lk(mu);
        queue.push_back(std::move(rev));
    }
    cv.notify_one();
}

void TTLCacheFDBIndex::evictPart(const String & partition_id, UInt64 hash_high)
{
    PendingOp rev;
    rev.type = PendingOp::Type::Evict;
    rev.key  = makeRevPartPrefix(partition_id, hash_high);

    {
        std::lock_guard lk(mu);
        queue.push_back(std::move(rev));
    }
    cv.notify_one();
}

void TTLCacheFDBIndex::evictTable()
{
    PendingOp rev;
    rev.type = PendingOp::Type::Evict;
    rev.key  = rev_key_prefix;

    {
        std::lock_guard lk(mu);
        queue.push_back(std::move(rev));
    }
    cv.notify_one();
}

void TTLCacheFDBIndex::bgLoop()
{
    while (true)
    {
        std::vector<PendingOp> batch;
        {
            std::unique_lock lk(mu);
            cv.wait_for(lk, std::chrono::milliseconds(MAX_WAIT_MS),
                [this] { return stopped || queue.size() >= BATCH_SIZE; });

            if (stopped && queue.empty())
                return;

            size_t n = std::min(queue.size(), BATCH_SIZE);
            batch.reserve(n);
            for (size_t i = 0; i < n; ++i)
            {
                batch.push_back(std::move(queue.front()));
                queue.pop_front();
            }
        }

        if (!batch.empty())
            flush(batch);
    }
}

void TTLCacheFDBIndex::flush(std::vector<PendingOp> & ops)
{
    // Split: sets go through batchWrite, evicts go through clean() individually
    Catalog::BatchCommitRequest batch;
    for (auto & op : ops)
    {
        if (op.type == PendingOp::Type::Set)
            batch.AddPut(Catalog::SinglePutRequest(op.key, op.value));
    }

    if (!batch.puts.empty())
    {
        try
        {
            Catalog::BatchCommitResponse resp;
            metastore->batchWrite(batch, resp);
        }
        catch (...)
        {
            tryLogCurrentException(log, "TTLCacheFDBIndex: batch write failed");
        }
    }

    for (auto & op : ops)
    {
        if (op.type == PendingOp::Type::Evict)
        {
            try { metastore->clean(op.key); }
            catch (...) { tryLogCurrentException(log, "TTLCacheFDBIndex: clean failed for " + op.key); }
        }
    }
}

std::optional<String> TTLCacheFDBIndex::findPeerOwner(UInt128 key, const String & partition_id)
{
    String rev_key = makeRevKey(key, partition_id);
    String raw;
    try
    {
        if (metastore->get(rev_key, raw) == 0)
            return std::nullopt;  // key not found
    }
    catch (...)
    {
        tryLogCurrentException(log, "TTLCacheFDBIndex: findPeerOwner FDB get failed");
        return std::nullopt;
    }

    // Value is "<worker_id>:<register_time>"
    UInt32 epoch = 0;
    String worker = raw;
    if (auto colon = raw.rfind(':'); colon != String::npos)
    {
        worker = raw.substr(0, colon);
        try { epoch = static_cast<UInt32>(std::stoul(raw.substr(colon + 1))); } catch (...) {}
    }

    if (worker.empty() || worker == own_worker_id)
        return std::nullopt;

    auto peer = DiskCacheFactory::instance().resolvePeer(worker);
    if (!peer)
        return std::nullopt;  // can't resolve (RM transient / unknown worker) — skip

    if (peer->register_time != epoch)
    {
        // Definitely stale: `worker` re-registered since this entry was written. 
        // Lazily delete it
        PendingOp del;
        del.type = PendingOp::Type::Evict;
        del.key  = rev_key;
        {
            std::lock_guard lk(mu);
            queue.push_back(std::move(del));
        }
        cv.notify_one();
        return std::nullopt;
    }

    return worker;  // caller resolves worker_id → endpoint via DiskCacheFactory
}

}
