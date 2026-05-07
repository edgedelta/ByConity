#include <Storages/DiskCache/TTLCacheFDBIndex.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheFactory.h>

#include <Catalog/MetastoreCommon.h>
#include <Catalog/MetastoreProxy.h>
#include <Catalog/StringHelper.h>
#include <Common/hex.h>
#include <fmt/core.h>

namespace DB
{


TTLCacheFDBIndex::TTLCacheFDBIndex(
    std::shared_ptr<Catalog::IMetaStore> metastore_,
    const String & name_space,
    const String & worker_id,
    const String & table_uuid,
    const String & own_endpoint_)
    : metastore(std::move(metastore_))
    , key_prefix(Catalog::escapeString(name_space) + "_DCI_" + Catalog::escapeString(worker_id) + "_" + table_uuid)
    , rev_key_prefix(Catalog::escapeString(name_space) + "_DCIREV_" + table_uuid)
    , own_worker_id(own_endpoint_)
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

String TTLCacheFDBIndex::makeSegKey(UInt128 key, const String & partition_id) const
{
    return key_prefix + "_" + partition_id + "_" + getHexUIntLowercase(key.items[0]) + "_" + getHexUIntLowercase(key.items[1]);
}

String TTLCacheFDBIndex::makePartPrefix(const String & partition_id, UInt64 hash_high) const
{
    return key_prefix + "_" + partition_id + "_" + getHexUIntLowercase(hash_high) + "_";
}

String TTLCacheFDBIndex::makeRevKey(UInt128 key, const String & partition_id) const
{
    return rev_key_prefix + "_" + partition_id + "_" + getHexUIntLowercase(key.items[0]) + "_" + getHexUIntLowercase(key.items[1]);
}

String TTLCacheFDBIndex::makeRevPartPrefix(const String & partition_id, UInt64 hash_high) const
{
    return rev_key_prefix + "_" + partition_id + "_" + getHexUIntLowercase(hash_high) + "_";
}

String TTLCacheFDBIndex::encodeValue(const String & seg_name, size_t size, time_t part_ts)
{
    // Format: "part_ts:size:seg_name"
    // seg_name uses '/' as separator internally, no ':' — safe delimiter
    return fmt::format("{}:{}:{}", static_cast<int64_t>(part_ts), size, seg_name);
}

bool TTLCacheFDBIndex::decodeValue(const String & raw, String & seg_name, size_t & size, time_t & part_ts)
{
    auto p1 = raw.find(':');
    if (p1 == String::npos)
        return false;
    auto p2 = raw.find(':', p1 + 1);
    if (p2 == String::npos)
        return false;

    try
    {
        part_ts = static_cast<time_t>(std::stoll(raw.substr(0, p1)));
        size    = static_cast<size_t>(std::stoull(raw.substr(p1 + 1, p2 - p1 - 1)));
        seg_name = raw.substr(p2 + 1);
        return !seg_name.empty();
    }
    catch (...) { return false; }
}

void TTLCacheFDBIndex::onSet(UInt128 key, const String & seg_name, size_t size, time_t part_ts)
{
    // partition_id is the YYYYMMDD component of the file path, derived from part_ts
    struct tm t{};
    gmtime_r(&part_ts, &t);
    String partition_id = fmt::format("{:04d}{:02d}{:02d}", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);

    PendingOp fwd;
    fwd.type  = PendingOp::Type::Set;
    fwd.key   = makeSegKey(key, partition_id);
    fwd.value = encodeValue(seg_name, size, part_ts);

    PendingOp rev;
    rev.type  = PendingOp::Type::Set;
    rev.key   = makeRevKey(key, partition_id);
    rev.value = own_worker_id;

    {
        std::lock_guard lk(mu);
        queue.push_back(std::move(fwd));
        queue.push_back(std::move(rev));
    }
    cv.notify_one();
}

void TTLCacheFDBIndex::evictPart(const String & partition_id, UInt64 hash_high)
{
    PendingOp fwd;
    fwd.type = PendingOp::Type::Evict;
    fwd.key  = makePartPrefix(partition_id, hash_high);

    PendingOp rev;
    rev.type = PendingOp::Type::Evict;
    rev.key  = makeRevPartPrefix(partition_id, hash_high);

    {
        std::lock_guard lk(mu);
        queue.push_back(std::move(fwd));
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
    String endpoint;
    try
    {
        if (metastore->get(rev_key, endpoint) == 0)
            return std::nullopt;  // key not found
    }
    catch (...)
    {
        tryLogCurrentException(log, "TTLCacheFDBIndex: findPeerOwner FDB get failed");
        return std::nullopt;
    }

    // endpoint now holds the peer's worker_id; skip if it's ourselves
    if (endpoint.empty() || endpoint == own_worker_id)
        return std::nullopt;

    return endpoint;  // caller resolves worker_id → host:port via DiskCacheFactory
}

std::optional<std::pair<size_t, size_t>> TTLCacheFDBIndex::reconcile(
    const VolumePtr & volume,
    std::function<std::filesystem::path(UInt128, const String &)> get_rel_path,
    std::function<bool(time_t)> should_cache,
    std::function<void(ReconcileBatch &)> on_reconcile_batch,
    std::function<void(time_t, size_t)> on_stats_update)
{
    // Page through FDB in chunks to avoid hitting the 5-second transaction timeout
    // that occurs when scanning millions of entries in a single transaction.
    static constexpr size_t PAGE_SIZE = 100'000;

    const auto & disks = volume->getDisks();
    if (disks.empty())
        return std::nullopt;

    size_t total_restored = 0;
    size_t total_restored_bytes = 0;
    size_t total_stale = 0;
    String scan_start_key;  // empty = start from key_prefix

    while (true)
    {
        ReconcileBatch page;
        page.reserve(PAGE_SIZE);
        std::vector<String> stale_fwd_keys;
        size_t page_bytes = 0;
        size_t page_count = 0;
        String last_key;

        Catalog::IMetaStore::IteratorPtr it;
        try { it = metastore->getByPrefix(key_prefix, PAGE_SIZE, DEFAULT_SCAN_BATCH_COUNT, scan_start_key); }
        catch (...) { tryLogCurrentException(log, "TTLCacheFDBIndex: getByPrefix failed"); return std::nullopt; }

        while (it->next())
        {
            last_key = it->key();
            page_count++;

            String seg_name;
            size_t size{0};
            time_t part_ts{0};

            if (!decodeValue(it->value(), seg_name, size, part_ts))
            {
                LOG_WARNING(log, "TTLCacheFDBIndex reconcile: decode failed for key={} value={}", it->key(), it->value());
                stale_fwd_keys.push_back(last_key);
                continue;
            }

            if (!should_cache(part_ts))
            {
                LOG_DEBUG(log, "TTLCacheFDBIndex reconcile: TTL expired for seg={} part_ts={}", seg_name, part_ts);
                stale_fwd_keys.push_back(last_key);
                continue;
            }

            auto key = DiskCacheTTL::hash(seg_name);
            auto rel_path = get_rel_path(key, seg_name);

            // TODO: multi-disk JBOD support — store disk name in FDB value so reconcile can
            // assign the correct disk without a per-file exists() scan across all disks.
            // For now assume single-disk volume (one PVC per pod) and trust FDB as authoritative,
            // skipping the per-file exists() syscall (too costly at millions of entries).
            page.emplace_back(key, std::make_shared<DiskCacheTTLMeta>(
                DiskCacheTTLMeta::State::Cached, disks[0], size, time(nullptr), part_ts, rel_path.string()));
            page_bytes += size;
        }

        if (!page.empty())
        {
            on_reconcile_batch(page);
            DiskCacheFactory::instance().addGlobalTTLUsage(page_bytes);
            if (on_stats_update)
            {
                for (const auto & [key, meta] : page)
                    on_stats_update(meta->max_timestamp, meta->size);
            }
        }

        if (!stale_fwd_keys.empty())
        {
            try
            {
                Catalog::BatchCommitRequest batch;
                for (const auto & fwd : stale_fwd_keys)
                {
                    batch.AddDelete(Catalog::SingleDeleteRequest(fwd));
                    String rev = rev_key_prefix + fwd.substr(key_prefix.size());
                    batch.AddDelete(Catalog::SingleDeleteRequest(rev));
                }
                Catalog::BatchCommitResponse resp;
                metastore->batchWrite(batch, resp);
                LOG_DEBUG(log, "TTLCacheFDBIndex reconcile: removed {} stale fwd+rev pairs", stale_fwd_keys.size());
            }
            catch (...) { tryLogCurrentException(log, "TTLCacheFDBIndex: stale cleanup failed"); }
        }

        total_restored += page.size();
        total_restored_bytes += page_bytes;
        total_stale += stale_fwd_keys.size();

        if (page_count < PAGE_SIZE)
            break;

        // Advance past the last key seen ('\x00' suffix = next key in FDB ordering).
        scan_start_key = last_key + '\x00';
    }

    LOG_INFO(log, "TTLCacheFDBIndex reconcile complete: {} entries restored, {} stale removed", total_restored, total_stale);

    if (total_restored == 0)
        return std::nullopt;
    return std::make_pair(total_restored, total_restored_bytes);
}

}
