#include <Storages/DiskCache/TTLCacheFDBIndex.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheFactory.h>

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

String TTLCacheFDBIndex::makeSegKey(UInt128 key, const String & partition_id) const
{
    return key_prefix + "_" + partition_id + "_" + getHexUIntLowercase(key.items[0]) + "_" + getHexUIntLowercase(key.items[1]);
}

String TTLCacheFDBIndex::makePartPrefix(const String & partition_id, UInt64 hash_high) const
{
    return key_prefix + "_" + partition_id + "_" + getHexUIntLowercase(hash_high) + "_";
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

    PendingOp op;
    op.type  = PendingOp::Type::Set;
    op.key   = makeSegKey(key, partition_id);
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

std::optional<std::pair<size_t, size_t>> TTLCacheFDBIndex::reconcile(
    std::map<UInt128, std::shared_ptr<DiskCacheTTLMeta>> & cache_map,
    std::mutex & cache_mutex,
    const VolumePtr & volume,
    std::function<std::filesystem::path(UInt128, const String &)> get_rel_path,
    std::function<bool(time_t)> should_cache)
{
    std::vector<String> stale_keys;
    std::vector<std::pair<UInt128, std::shared_ptr<DiskCacheTTLMeta>>> to_insert;
    size_t restored_bytes = 0;

    Catalog::IMetaStore::IteratorPtr it;
    try { it = metastore->getByPrefix(key_prefix); }
    catch (...) { tryLogCurrentException(log, "TTLCacheFDBIndex: getByPrefix failed"); return std::nullopt; }

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

        to_insert.emplace_back(key, std::make_shared<DiskCacheTTLMeta>(
            DiskCacheTTLMeta::State::Cached, found_disk, size, time(nullptr), part_ts));
        restored_bytes += size;
    }

    // Bulk-insert into cache_map under a single lock
    if (!to_insert.empty())
    {
        std::lock_guard lk(cache_mutex);
        for (auto & [key, meta] : to_insert)
            cache_map[key] = std::move(meta);
        DiskCacheFactory::instance().addGlobalTTLUsage(restored_bytes);
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

    LOG_INFO(log, "TTLCacheFDBIndex reconcile complete: {} entries restored, {} stale removed", to_insert.size(), stale_keys.size());

    if (to_insert.empty())
        return std::nullopt;
    return std::make_pair(to_insert.size(), restored_bytes);
}

}
