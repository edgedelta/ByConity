#include <filesystem>
#include <map>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <Storages/DiskCache/TTLCacheFDBIndex.h>
#include <Catalog/IMetastore.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>

namespace fs = std::filesystem;

namespace DB
{

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static String fdbMakeSegKey(const String & uuid, const String & part, const String & col, const String & ext)
{
    return fmt::format("{}/{}/{}#0{}", uuid, part, col, ext);
}

static String fdbTodayPart()
{
    time_t now = time(nullptr);
    struct tm t;
    gmtime_r(&now, &t);
    return fmt::format("{:04d}{:02d}{:02d}_1_100_2", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
}

// ---------------------------------------------------------------------------
// Mock metastore — respects limit and start_key for pagination testing
// ---------------------------------------------------------------------------

class FDBMockMetaStore : public Catalog::IMetaStore
{
public:
    struct MockIterator : public Iterator
    {
        std::vector<std::pair<String, String>> entries;
        int pos = -1;
        bool next() override { return ++pos < static_cast<int>(entries.size()); }
        String key()   override { return entries[pos].first; }
        String value() override { return entries[pos].second; }
    };

    void put(const String & key, const String & value, bool = false) override { store[key] = value; }
    std::pair<bool, String> putCAS(const String &, const String &, const String &, bool) override { return {false, {}}; }
    uint64_t get(const String & key, String & value) override
    {
        auto it = store.find(key);
        if (it == store.end()) return 0;
        value = it->second;
        return 1;
    }
    std::vector<std::pair<String, UInt64>> multiGet(const std::vector<String> &) override { return {}; }
    bool batchWrite(const Catalog::BatchCommitRequest & req, Catalog::BatchCommitResponse &) override
    {
        for (auto & d : req.deletes)
            store.erase(d.key);
        return true;
    }
    void drop(const String & key, const UInt64 &) override { store.erase(key); }
    void drop(const String & key, const String &)  override { store.erase(key); }
    IteratorPtr getAll() override { return getByPrefix(""); }
    IteratorPtr getByPrefix(const String & prefix, const size_t & limit = 0, uint32_t = 0, const String & start_key = "") override
    {
        auto iter = std::make_shared<MockIterator>();
        for (auto & [k, v] : store)
        {
            if (!k.starts_with(prefix))
                continue;
            if (!start_key.empty() && k < start_key)
                continue;
            iter->entries.emplace_back(k, v);
            if (limit > 0 && iter->entries.size() >= limit)
                break;
        }
        return iter;
    }
    IteratorPtr getByRange(const String &, const String &, bool, bool) override { return std::make_shared<MockIterator>(); }
    void clean(const String & prefix) override
    {
        for (auto it = store.begin(); it != store.end(); )
            it = it->first.starts_with(prefix) ? store.erase(it) : std::next(it);
    }
    void close() override {}
    uint32_t getMaxBatchSize() override { return 1000; }
    uint32_t getMaxKVSize()    override { return 1024 * 1024; }

    std::map<String, String> store;
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class TTLCacheFDBIndexTest : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
        Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);
        Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_channel));
        Poco::Logger::root().setLevel("warning");
        Poco::Logger::root().setChannel(channel);
        ctx = getContext().context;
    }

    static void TearDownTestCase() { ctx->shutdown(); }

    void SetUp() override
    {
        fs::remove_all("tmp_fdb/");
        fs::create_directories("tmp_fdb/ttl_disk/");
        UnitTest::initLogger();
        DB::IDiskCache::init(*getContext().context);
    }

    void TearDown() override
    {
        fs::remove_all("tmp_fdb/");
        DB::IDiskCache::close();
    }

    VolumePtr createVolume()
    {
        auto disk = std::make_shared<DiskLocal>("fdb_ttl_disk", "tmp_fdb/ttl_disk/", DiskStats{});
        return std::make_shared<SingleDiskVolume>("fdb_ttl_volume", std::move(disk), 0);
    }

    DiskCacheSettings makeSettings(size_t max_bytes = 64 * 1024 * 1024) {
        DiskCacheSettings s;
        s.ttl_cache_max_size = max_bytes;
        return s;
    }

    static std::shared_ptr<Context> ctx;
};

std::shared_ptr<Context> TTLCacheFDBIndexTest::ctx = nullptr;

// ---------------------------------------------------------------------------
// Helpers to seed the mock store with valid encoded FDB entries
// ---------------------------------------------------------------------------

static void seedFDBEntry(FDBMockMetaStore & store, const String & key_prefix,
    const String & fdb_key_suffix, const String & seg, size_t size, time_t ts)
{
    store.store[key_prefix + fdb_key_suffix] = fmt::format("{}:{}:{}", static_cast<int64_t>(ts), size, seg);
}

// ---------------------------------------------------------------------------
// Test: all entries restored, on_reconcile_batch called
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, RestoresAllEntries)
{
    auto volume = createVolume();
    auto settings = makeSettings();
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    const String uuid = "restore-uuid";
    const String ns = "ns", worker = "w1";
    const String kp = fmt::format("{}_DCI_{}_{}", ns, worker, uuid);
    const time_t now = time(nullptr);

    auto mock = std::make_shared<FDBMockMetaStore>();
    const int N = 5;
    std::vector<String> segs;
    for (int i = 0; i < N; ++i)
    {
        String seg = fdbMakeSegKey(uuid, fdbTodayPart(), fmt::format("col{}", i), ".bin");
        segs.push_back(seg);
        seedFDBEntry(*mock, kp, fmt::format("_k{:04d}", i), seg, 64, now);
    }

    TTLCacheFDBIndex idx(mock, ns, worker, uuid, worker);
    DiskCacheTTL cache("rc", uuid, volume, nullptr, settings, strategy, 60 * 24, 0);

    size_t batch_calls = 0;
    std::map<UInt128, std::shared_ptr<DiskCacheTTLMeta>> restored;
    auto result = idx.reconcile(
        volume,
        [&](UInt128 key, const String & seg) { return cache.getRelativePath(key, seg); },
        [&](time_t ts) { return ts > now - 3600; },
        [&](TTLCacheFDBIndex::ReconcileBatch & batch) {
            batch_calls++;
            for (auto & [k, m] : batch) restored[k] = m;
        }
    );

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->first, static_cast<size_t>(N));
    EXPECT_EQ(restored.size(), static_cast<size_t>(N));
    EXPECT_GE(batch_calls, 1u);

    for (auto & seg : segs)
        EXPECT_NE(restored.find(DiskCacheTTL::hash(seg)), restored.end()) << "missing: " << seg;
}

// ---------------------------------------------------------------------------
// Test: expired entries skipped and cleaned from FDB per page
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, StaleEntriesCleanedFromFDB)
{
    auto volume = createVolume();
    auto settings = makeSettings();
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    const String uuid = "stale-uuid";
    const String ns = "ns", worker = "w1";
    const String kp = fmt::format("{}_DCI_{}_{}", ns, worker, uuid);
    const time_t now = time(nullptr);
    const time_t old_ts = now - 7 * 24 * 3600; // 7 days ago

    auto mock = std::make_shared<FDBMockMetaStore>();

    // 3 fresh entries
    for (int i = 0; i < 3; ++i)
        seedFDBEntry(*mock, kp, fmt::format("_fresh_{:04d}", i),
            fdbMakeSegKey(uuid, fdbTodayPart(), fmt::format("c{}", i), ".bin"), 64, now);

    // 2 expired entries
    for (int i = 0; i < 2; ++i)
        seedFDBEntry(*mock, kp, fmt::format("_stale_{:04d}", i),
            fdbMakeSegKey(uuid, fdbTodayPart(), fmt::format("s{}", i), ".bin"), 64, old_ts);

    ASSERT_EQ(mock->store.size(), 5u);

    TTLCacheFDBIndex idx(mock, ns, worker, uuid, worker);
    DiskCacheTTL cache("stale", uuid, volume, nullptr, settings, strategy, 60 * 24, 0);

    std::map<UInt128, std::shared_ptr<DiskCacheTTLMeta>> restored;
    auto result = idx.reconcile(
        volume,
        [&](UInt128 key, const String & seg) { return cache.getRelativePath(key, seg); },
        [&](time_t ts) { return ts > now - 3600; }, // only very recent
        [&](TTLCacheFDBIndex::ReconcileBatch & batch) {
            for (auto & [k, m] : batch) restored[k] = m;
        }
    );

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->first, 3u);
    EXPECT_EQ(restored.size(), 3u);

    // Stale entries must have been deleted from the mock store
    for (auto & [k, v] : mock->store)
        EXPECT_EQ(k.find("_stale_"), String::npos) << "stale key not cleaned: " << k;
}

// ---------------------------------------------------------------------------
// Test: pagination — entries spanning multiple pages all restored, no duplicates
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, PaginationRestoresAllEntries)
{
    auto volume = createVolume();
    auto settings = makeSettings(256 * 1024 * 1024);
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    const String uuid = "page-uuid";
    const String ns = "ns", worker = "w1";
    const String kp = fmt::format("{}_DCI_{}_{}", ns, worker, uuid);
    const time_t now = time(nullptr);

    auto mock = std::make_shared<FDBMockMetaStore>();

    // Seed PAGE_SIZE + 3 entries to force at least 2 pages (PAGE_SIZE = 100000).
    // MockMetaStore respects limit + start_key, so pagination is exercised end-to-end.
    const size_t PAGE_SIZE = 100'000;
    const size_t TOTAL = PAGE_SIZE + 3;
    for (size_t i = 0; i < TOTAL; ++i)
    {
        // Use zero-padded keys so std::map ordering matches FDB lexicographic ordering.
        String seg = fdbMakeSegKey(uuid, fdbTodayPart(), fmt::format("col{:07d}", i), ".bin");
        seedFDBEntry(*mock, kp, fmt::format("_{:07d}", i), seg, 32, now);
    }
    ASSERT_EQ(mock->store.size(), TOTAL);

    TTLCacheFDBIndex idx(mock, ns, worker, uuid, worker);
    DiskCacheTTL cache("page", uuid, volume, nullptr, settings, strategy, 60 * 24, 0);

    size_t batch_calls = 0;
    std::map<UInt128, std::shared_ptr<DiskCacheTTLMeta>> restored;
    auto result = idx.reconcile(
        volume,
        [&](UInt128 key, const String & seg) { return cache.getRelativePath(key, seg); },
        [&](time_t ts) { return ts > now - 3600; },
        [&](TTLCacheFDBIndex::ReconcileBatch & batch) {
            batch_calls++;
            for (auto & [k, m] : batch) restored[k] = m;
        }
    );

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->first, TOTAL) << "got: " << result->first << ", want: " << TOTAL;
    // No duplicates
    EXPECT_EQ(restored.size(), TOTAL) << "duplicates detected: map size " << restored.size() << " vs total " << TOTAL;
    // At least 2 batch calls (one per page)
    EXPECT_GE(batch_calls, 2u) << "expected pagination but only got " << batch_calls << " batch call(s)";
}

// ---------------------------------------------------------------------------
// Test: empty FDB returns nullopt
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, EmptyFDBReturnsNullopt)
{
    auto volume = createVolume();
    auto settings = makeSettings();
    auto strategy = std::make_shared<DiskCacheSimpleStrategy>(settings);

    const String uuid = "empty-uuid";
    auto mock = std::make_shared<FDBMockMetaStore>();

    TTLCacheFDBIndex idx(mock, "ns", "w1", uuid, "w1");
    DiskCacheTTL cache("empty", uuid, volume, nullptr, settings, strategy, 60, 0);

    bool batch_called = false;
    auto result = idx.reconcile(
        volume,
        [&](UInt128 key, const String & seg) { return cache.getRelativePath(key, seg); },
        [](time_t) { return true; },
        [&](TTLCacheFDBIndex::ReconcileBatch &) { batch_called = true; }
    );

    EXPECT_FALSE(result.has_value());
    EXPECT_FALSE(batch_called);
}

// ---------------------------------------------------------------------------
// Test: evictTable clears all forward + reverse index entries for the table
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, EvictTableClearsAllEntries)
{
    const String uuid       = "evict-table-uuid";
    const String other_uuid = "other-uuid";
    const String ns = "ns", worker = "w1";

    // Forward-index prefix for target table and another table
    const String kp       = fmt::format("{}_DCI_{}_{}", ns, worker, uuid);
    const String other_kp = fmt::format("{}_DCI_{}_{}", ns, worker, other_uuid);
    // Reverse-index prefix for target table
    const String rev_kp   = fmt::format("{}_DCIREV_{}", ns, uuid);

    auto mock = std::make_shared<FDBMockMetaStore>();
    const time_t now = time(nullptr);

    // Seed 5 forward-index entries for our table
    for (int i = 0; i < 5; ++i)
        seedFDBEntry(*mock, kp, fmt::format("_k{:04d}", i),
            fdbMakeSegKey(uuid, fdbTodayPart(), fmt::format("col{}", i), ".bin"), 64, now);

    // Seed 3 reverse-index entries for our table
    for (int i = 0; i < 3; ++i)
        mock->store[rev_kp + fmt::format("_rev{:04d}", i)] = "peer:1234";

    // Seed 2 forward-index entries for a different table (must survive)
    for (int i = 0; i < 2; ++i)
        seedFDBEntry(*mock, other_kp, fmt::format("_k{:04d}", i),
            fdbMakeSegKey(other_uuid, fdbTodayPart(), fmt::format("col{}", i), ".bin"), 64, now);

    ASSERT_EQ(mock->store.size(), 10u);

    {
        TTLCacheFDBIndex idx(mock, ns, worker, uuid, worker);
        idx.evictTable();
        // Destructor joins bg thread, guaranteeing flush
    }

    // All entries for our table gone
    for (auto & [k, v] : mock->store)
    {
        EXPECT_FALSE(k.starts_with(kp))     << "forward-index entry not cleaned: " << k;
        EXPECT_FALSE(k.starts_with(rev_kp)) << "reverse-index entry not cleaned: " << k;
    }

    // Other table's entries intact
    size_t other_count = 0;
    for (auto & [k, v] : mock->store)
        if (k.starts_with(other_kp)) ++other_count;
    EXPECT_EQ(other_count, 2u);
}

} // namespace DB
