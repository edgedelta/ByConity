#include <Storages/DiskCache/TTLCacheFDBIndex.h>
#include <Storages/DiskCache/DiskCacheTTL.h>

#include <Catalog/MetastoreProxy.h>
#include <Catalog/StringHelper.h>
#include <Common/hex.h>
#include <fmt/core.h>

namespace DB
{

namespace
{
    String hexU64(UInt64 v)
    {
        String s(16, '\0');
        writeHexUIntLowercase(v, s.data());
        return s;
    }
}

TTLCacheFDBIndex::TTLCacheFDBIndex(
    std::shared_ptr<IMetaStore> metastore_,
    const String & name_space,
    const String & worker_id,
    const String & table_uuid)
    : metastore(std::move(metastore_))
    , key_prefix(Catalog::escapeString(name_space) + "_DCI_" + Catalog::escapeString(worker_id) + "_" + table_uuid)
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

String TTLCacheFDBIndex::makeSegKey(UInt128 key, const String & partition_id, UInt64 hash_high) const
{
    return key_prefix + "_" + partition_id + "_" + hexU64(hash_high) + "_" + hexU64(key.items[1]);
}

String TTLCacheFDBIndex::makePartPrefix(const String & partition_id, UInt64 hash_high) const
{
    return key_prefix + "_" + partition_id + "_" + hexU64(hash_high) + "_";
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

    UInt64 hash_high = key.items[0];
    PendingOp op;
    op.type  = PendingOp::Type::Set;
    op.key   = makeSegKey(key, partition_id, hash_high);
    op.value = encodeValue(seg_name, size, part_ts);

    {
        std::lock_guard lk(mu);
        queue.push_back(std::move(op));
    }
    cv.notify_one();
}

void TTLCacheFDBIndex::evictPart(const String & partition_id, UInt64 hash_high)
{
    PendingOp op;
    op.type = PendingOp::Type::Evict;
    op.key  = makePartPrefix(partition_id, hash_high);

    {
        std::lock_guard lk(mu);
        queue.push_back(std::move(op));
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
                [this] { return stopped.load() || queue.size() >= BATCH_SIZE; });

            if (stopped && queue.empty())
                return;

            size_t n = std::min(queue.size(), BATCH_SIZE);
            batch.assign(queue.begin(), queue.begin() + static_cast<ptrdiff_t>(n));
            queue.erase(queue.begin(), queue.begin() + static_cast<ptrdiff_t>(n));
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

bool TTLCacheFDBIndex::reconcile(
    std::map<UInt128, std::shared_ptr<DiskCacheTTLMeta>> & cache_map,
    std::mutex & cache_mutex,
    const VolumePtr & volume,
    std::function<std::filesystem::path(UInt128, const String &)> get_rel_path,
    std::function<bool(time_t)> should_cache)
{
    size_t restored = 0;
    std::vector<String> stale_keys;

    IMetaStore::IteratorPtr it;
    try { it = metastore->getByPrefix(key_prefix); }
    catch (...) { tryLogCurrentException(log, "TTLCacheFDBIndex: getByPrefix failed"); return false; }

    const auto & disks = volume->getDisks();

    while (it->next())
    {
        String seg_name;
        size_t size{0};
        time_t part_ts{0};

        if (!decodeValue(it->value(), seg_name, size, part_ts))
        {
            stale_keys.push_back(it->key());
            continue;
        }

        // Re-apply TTL check — don't restore already-expired entries
        if (!should_cache(part_ts))
        {
            stale_keys.push_back(it->key());
            continue;
        }

        auto key = DiskCacheTTL::hash(seg_name);
        auto rel_path = get_rel_path(key, seg_name);

        // Find which disk has the file
        DiskPtr found_disk;
        for (const auto & disk : disks)
        {
            if (disk->exists(rel_path))
            {
                found_disk = disk;
                break;
            }
        }

        if (!found_disk)
        {
            stale_keys.push_back(it->key());
            continue;
        }

        {
            std::lock_guard lk(cache_mutex);
            cache_map[key] = std::make_shared<DiskCacheTTLMeta>(
                DiskCacheTTLMeta::State::Cached, found_disk, size, time(nullptr), part_ts);
        }
        DiskCacheFactory::instance().addGlobalTTLUsage(size);
        ++restored;
    }

    // Bulk-delete stale FDB entries
    if (!stale_keys.empty())
    {
        try
        {
            Catalog::BatchCommitRequest batch;
            for (const auto & k : stale_keys)
                batch.AddDelete(Catalog::SingleDeleteRequest(k));
            Catalog::BatchCommitResponse resp;
            metastore->batchWrite(batch, resp);
            LOG_DEBUG(log, "TTLCacheFDBIndex reconcile: removed {} stale entries", stale_keys.size());
        }
        catch (...) { tryLogCurrentException(log, "TTLCacheFDBIndex: stale cleanup failed"); }
    }

    LOG_INFO(log, "TTLCacheFDBIndex reconcile complete: {} entries restored, {} stale removed", restored, stale_keys.size());
    return restored > 0;
}

}
