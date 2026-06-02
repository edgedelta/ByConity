#include <filesystem>
#include <map>
#include <unordered_map>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/DiskCache/DiskCacheTTL.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
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

// Build the DCIREV key for a segment exactly as TTLCacheFDBIndex::makeRevKey does:
// rev_key_prefix + "_" + partition_id + "_" + hex(key.items[0]) + "_" + hex(key.items[1]).
// hexKey() lays items[0] in the upper 16 hex chars and items[1] in the lower 16 (see getPath).
static String revKeyFor(const String & rev_key_prefix, const String & pid, const String & seg)
{
    auto key = DiskCacheTTL::hash(seg);
    String hex = DiskCacheTTL::hexKey(key);
    return fmt::format("{}_{}_{}_{}", rev_key_prefix, pid, hex.substr(16, 16), hex.substr(0, 16));
}

// ---------------------------------------------------------------------------
// Mock metastore — batchWrite applies puts+deletes; clean() is a prefix delete.
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
        for (auto & p : req.puts)
            store[p.key] = p.value;
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
        // Reset the injected resolver so it doesn't leak into other tests via the singleton.
        DiskCacheFactory::instance().setWorkerResolverForTest(nullptr);
        fs::remove_all("tmp_fdb/");
        DB::IDiskCache::close();
    }

    // Inject a fixed worker_id -> {endpoint, register_time} map into the factory singleton.
    static void setPeers(std::unordered_map<String, WorkerPeerInfo> peers)
    {
        DiskCacheFactory::instance().setWorkerResolverForTest(
            [peers = std::move(peers)]() { return peers; });
    }

    static std::shared_ptr<Context> ctx;
};

std::shared_ptr<Context> TTLCacheFDBIndexTest::ctx = nullptr;

// ---------------------------------------------------------------------------
// onSet writes a reverse entry stamped "<worker_id>:<register_time>".
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, OnSetStampsWorkerEpoch)
{
    const String ns = "ns", worker = "w1", uuid = "onset-uuid";
    const String rev_kp = fmt::format("{}_DCIREV_{}", ns, uuid);
    setPeers({{worker, WorkerPeerInfo{"w1host:9000", 12345}}});

    auto mock = std::make_shared<FDBMockMetaStore>();
    const String pid = "202403";                              // monthly partition id (non-daily)
    String seg = fdbMakeSegKey(uuid, pid + "_1_100_2", "col", ".bin");
    auto key = DiskCacheTTL::hash(seg);

    {
        TTLCacheFDBIndex idx(mock, ns, uuid, worker);
        idx.onSet(key, pid);
    }  // destructor drains the queue → batchWrite stores the Set op

    String rk = revKeyFor(rev_kp, pid, seg);
    ASSERT_EQ(mock->store.count(rk), 1u) << "reverse key not written under partition_id: " << rk;
    EXPECT_EQ(mock->store[rk], worker + ":12345") << "value must be <worker_id>:<register_time>";
}

// ---------------------------------------------------------------------------
// findPeerOwner returns the peer when the stamped epoch matches its current register_time.
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, FindPeerOwnerReturnsPeerOnEpochMatch)
{
    const String ns = "ns", own = "w1", peer = "w2", uuid = "fpo-match";
    const String rev_kp = fmt::format("{}_DCIREV_{}", ns, uuid);
    setPeers({{own, {"w1:9000", 100}}, {peer, {"w2:9000", 200}}});

    auto mock = std::make_shared<FDBMockMetaStore>();
    const String pid = "20240315";
    String seg = fdbMakeSegKey(uuid, pid + "_1_100_2", "col", ".bin");
    auto key = DiskCacheTTL::hash(seg);
    String rk = revKeyFor(rev_kp, pid, seg);
    mock->store[rk] = peer + ":200";  // matches peer's current register_time

    TTLCacheFDBIndex idx(mock, ns, uuid, own);
    auto r = idx.findPeerOwner(key, pid);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, peer);
    EXPECT_EQ(mock->store.count(rk), 1u) << "valid entry must not be deleted";
}

// ---------------------------------------------------------------------------
// A stale entry (epoch != peer's current register_time → previous incarnation) is skipped
// and lazily deleted.
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, FindPeerOwnerStaleEpochLazyDeletes)
{
    const String ns = "ns", own = "w1", peer = "w2", uuid = "fpo-stale";
    const String rev_kp = fmt::format("{}_DCIREV_{}", ns, uuid);
    setPeers({{own, {"w1:9000", 100}}, {peer, {"w2:9000", 200}}});

    auto mock = std::make_shared<FDBMockMetaStore>();
    const String pid = "20240315";
    String seg = fdbMakeSegKey(uuid, pid + "_1_100_2", "col", ".bin");
    auto key = DiskCacheTTL::hash(seg);
    String rk = revKeyFor(rev_kp, pid, seg);
    mock->store[rk] = peer + ":150";  // peer re-registered since (current is 200) → stale

    {
        TTLCacheFDBIndex idx(mock, ns, uuid, own);
        auto r = idx.findPeerOwner(key, pid);
        EXPECT_FALSE(r.has_value()) << "stale-epoch entry must not be returned";
    }  // destructor flushes the enqueued lazy delete

    EXPECT_EQ(mock->store.count(rk), 0u) << "stale entry must be lazily deleted";
}

// ---------------------------------------------------------------------------
// An entry owned by ourselves is skipped (no self-steal) and not deleted.
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, FindPeerOwnerSkipsSelf)
{
    const String ns = "ns", own = "w1", uuid = "fpo-self";
    const String rev_kp = fmt::format("{}_DCIREV_{}", ns, uuid);
    setPeers({{own, {"w1:9000", 100}}});

    auto mock = std::make_shared<FDBMockMetaStore>();
    const String pid = "20240315";
    String seg = fdbMakeSegKey(uuid, pid + "_1_100_2", "col", ".bin");
    auto key = DiskCacheTTL::hash(seg);
    String rk = revKeyFor(rev_kp, pid, seg);
    mock->store[rk] = own + ":100";

    {
        TTLCacheFDBIndex idx(mock, ns, uuid, own);
        EXPECT_FALSE(idx.findPeerOwner(key, pid).has_value());
    }
    EXPECT_EQ(mock->store.count(rk), 1u) << "our own entry must not be deleted";
}

// ---------------------------------------------------------------------------
// A worker that can't be resolved (RM transient / unknown) is skipped WITHOUT deletion,
// so a transient RM blip doesn't purge valid entries.
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, FindPeerOwnerUnresolvableSkipsWithoutDelete)
{
    const String ns = "ns", own = "w1", peer = "w2", uuid = "fpo-unres";
    const String rev_kp = fmt::format("{}_DCIREV_{}", ns, uuid);
    setPeers({{own, {"w1:9000", 100}}});  // peer NOT in the map

    auto mock = std::make_shared<FDBMockMetaStore>();
    const String pid = "20240315";
    String seg = fdbMakeSegKey(uuid, pid + "_1_100_2", "col", ".bin");
    auto key = DiskCacheTTL::hash(seg);
    String rk = revKeyFor(rev_kp, pid, seg);
    mock->store[rk] = peer + ":200";

    {
        TTLCacheFDBIndex idx(mock, ns, uuid, own);
        EXPECT_FALSE(idx.findPeerOwner(key, pid).has_value());
    }
    EXPECT_EQ(mock->store.count(rk), 1u) << "unresolvable peer must not trigger deletion";
}

// ---------------------------------------------------------------------------
// evictPart cleans the reverse entries of one part (same hash_high) and nothing else.
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, EvictPartCleansReverseForPart)
{
    const String ns = "ns", worker = "w1", uuid = "evp-uuid";
    const String rev_kp = fmt::format("{}_DCIREV_{}", ns, uuid);

    auto mock = std::make_shared<FDBMockMetaStore>();
    const String pid = "20240315";
    const String part = pid + "_1_100_2";
    String seg1 = fdbMakeSegKey(uuid, part, "col1", ".bin");
    String seg2 = fdbMakeSegKey(uuid, part, "col2", ".bin");   // same part_name → same hash_high
    String other_seg = fdbMakeSegKey(uuid, pid + "_2_200_2", "col", ".bin"); // different part

    mock->store[revKeyFor(rev_kp, pid, seg1)] = worker + ":1";
    mock->store[revKeyFor(rev_kp, pid, seg2)] = worker + ":1";
    String other_rk = revKeyFor(rev_kp, pid, other_seg);
    mock->store[other_rk] = worker + ":1";

    UInt64 hash_high = DiskCacheTTL::hash(seg1).items[0];
    {
        TTLCacheFDBIndex idx(mock, ns, uuid, worker);
        idx.evictPart(pid, hash_high);
    }  // destructor flushes the clean()

    EXPECT_EQ(mock->store.count(revKeyFor(rev_kp, pid, seg1)), 0u);
    EXPECT_EQ(mock->store.count(revKeyFor(rev_kp, pid, seg2)), 0u);
    EXPECT_EQ(mock->store.count(other_rk), 1u) << "a different part's entry must survive";
}

// ---------------------------------------------------------------------------
// evictTable clears all reverse entries for this table, leaving other tables intact.
// ---------------------------------------------------------------------------

TEST_F(TTLCacheFDBIndexTest, EvictTableClearsReverseEntries)
{
    const String ns = "ns", worker = "w1", uuid = "evict-table-uuid", other_uuid = "other-uuid";
    const String rev_kp     = fmt::format("{}_DCIREV_{}", ns, uuid);
    const String other_revkp = fmt::format("{}_DCIREV_{}", ns, other_uuid);

    auto mock = std::make_shared<FDBMockMetaStore>();
    for (int i = 0; i < 3; ++i)
        mock->store[rev_kp + fmt::format("_20240315_aa{:02d}_bb{:02d}", i, i)] = worker + ":1";
    for (int i = 0; i < 2; ++i)
        mock->store[other_revkp + fmt::format("_20240315_aa{:02d}_bb{:02d}", i, i)] = worker + ":1";
    ASSERT_EQ(mock->store.size(), 5u);

    {
        TTLCacheFDBIndex idx(mock, ns, uuid, worker);
        idx.evictTable();
    }

    for (auto & [k, v] : mock->store)
        EXPECT_FALSE(k.starts_with(rev_kp)) << "reverse-index entry not cleaned: " << k;
    size_t other_count = 0;
    for (auto & [k, v] : mock->store)
        if (k.starts_with(other_revkp)) ++other_count;
    EXPECT_EQ(other_count, 2u);
}

} // namespace DB
