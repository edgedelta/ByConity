#include "ParallelDecodingBlockInputFormat.h"
#include "Common/setThreadName.h"
#include "Formats/FormatSettings.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

ParallelDecodingBlockInputFormat::ParallelDecodingBlockInputFormat(
    ReadBuffer & buf,
    const Block & header_,
    const FormatSettings &  format_settings_,
    size_t max_decoding_threads_,
    bool preserve_order_,
    std::unordered_set<int> skip_row_groups_)
    : IInputFormat(header_, buf)
    , format_settings(format_settings_)
    , skip_row_groups(skip_row_groups_)
    , max_decoding_threads(max_decoding_threads_)
    , preserve_order(preserve_order_)
    , pending_chunks(PendingChunk::Compare { .row_group_first = preserve_order })
{
    if (max_decoding_threads > 1)
    {
        LOG_TRACE(&Poco::Logger::get("ParallelDecodingBlockInputFormat"), "Use parallel decoding with {} threads", max_decoding_threads);
        pool = std::make_unique<ThreadPool>(max_decoding_threads);
    }
}

ParallelDecodingBlockInputFormat::~ParallelDecodingBlockInputFormat()
{
    // is_stopped = true;
    // if (pool)
    //     pool->wait();
}

void ParallelDecodingBlockInputFormat::close()
{
    is_stopped = true;
    if (pool)
        pool->wait();
}

void ParallelDecodingBlockInputFormat::initializeIfNeeded()
{
    if (std::exchange(is_initialized, true))
        return;

    if (is_stopped)
        return;

    initializeFileReader();
    row_groups.resize(getNumberOfRowGroups());
}

void ParallelDecodingBlockInputFormat::scheduleRowGroup(size_t row_group_idx)
{
    chassert(!mutex.try_lock());

    auto & status = row_groups[row_group_idx].status;
    chassert(status == RowGroupState::Status::NotStarted || status == RowGroupState::Status::Paused);

    status = RowGroupState::Status::Running;

    pool->scheduleOrThrowOnError(
        [this, row_group_idx, thread_group = CurrentThread::getGroup()]()
        {
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
            SCOPE_EXIT({if (thread_group) CurrentThread::detachQueryIfNotDetached();});

            try
            {
                setThreadName("ParalDecoder");
                threadFunction(row_group_idx);
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                background_exception = std::current_exception();
                condvar.notify_all();
            }
        });
}


void ParallelDecodingBlockInputFormat::threadFunction(size_t row_group_idx)
{
    std::unique_lock lock(mutex);

    auto & row_group = row_groups[row_group_idx];
    chassert(row_group.status == RowGroupState::Status::Running);

    while (true)
    {
        if (is_stopped || row_group.num_pending_chunks >= max_pending_chunks_per_row_group)
        {
            row_group.status = RowGroupState::Status::Paused;
            return;
        }

        decodeOneChunk(row_group_idx, lock);

        if (row_group.status == RowGroupState::Status::Done)
            return;
    }
}

void ParallelDecodingBlockInputFormat::decodeOneChunk(size_t row_group_idx, std::unique_lock<std::mutex> & lock)
{
    auto & row_group = row_groups[row_group_idx];
    chassert(row_group.status != RowGroupState::Status::Done);
    chassert(lock.owns_lock());
    SCOPE_EXIT({ chassert(lock.owns_lock() || std::uncaught_exceptions()); });

    lock.unlock();

    auto end_of_row_group = [&] {
        resetRowGroupReader(row_group_idx);
        lock.lock();
        row_group.status = RowGroupState::Status::Done;

        // We may be able to schedule more work now, but can't call scheduleMoreWorkIfNeeded() right
        // here because we're running on the same thread pool, so it'll deadlock if thread limit is
        // reached. Wake up generate() instead.
        condvar.notify_all();
    };

    if (skip_row_groups.contains(static_cast<int>(row_group_idx)))
    {
        // Pretend that the row group is empty.
        // (We could avoid scheduling the row group on a thread in the first place. But the
        // skip_row_groups feature is mostly unused, so it's better to be a little inefficient
        // than to add a bunch of extra mostly-dead code for this.)
        end_of_row_group();
        return;
    }

    initializeRowGroupReaderIfNeeded(row_group_idx);

    auto res = readBatch(row_group_idx);

    if (!res)
    {
        end_of_row_group();
        return;
    }

    lock.lock();

    ++row_group.next_chunk_idx;
    ++row_group.num_pending_chunks;
    pending_chunks.push(std::move(*res));
    condvar.notify_all();
}

void ParallelDecodingBlockInputFormat::scheduleMoreWorkIfNeeded(std::optional<size_t> row_group_touched)
{
    while (row_groups_completed < row_groups.size())
    {
        auto & row_group = row_groups[row_groups_completed];
        if (row_group.status != RowGroupState::Status::Done || row_group.num_pending_chunks != 0)
            break;
        ++row_groups_completed;
    }

    if (pool)
    {
        while (row_groups_started - row_groups_completed < max_decoding_threads &&
               row_groups_started < row_groups.size())
            scheduleRowGroup(row_groups_started++);

        if (row_group_touched)
        {
            auto & row_group = row_groups[*row_group_touched];
            if (row_group.status == RowGroupState::Status::Paused &&
                row_group.num_pending_chunks < max_pending_chunks_per_row_group)
                scheduleRowGroup(*row_group_touched);
        }
    }
}


Chunk ParallelDecodingBlockInputFormat::generate()
{
    initializeIfNeeded();

    std::unique_lock lock(mutex);

    while (true)
    {
        if (background_exception)
        {
            is_stopped = true;
            std::rethrow_exception(background_exception);
        }
        if (is_stopped)
            return {};

        scheduleMoreWorkIfNeeded();

        if (!pending_chunks.empty() &&
            (!preserve_order ||
             pending_chunks.top().row_group_idx == row_groups_completed))
        {
            PendingChunk chunk = std::move(const_cast<PendingChunk&>(pending_chunks.top()));
            pending_chunks.pop();

            auto & row_group = row_groups[chunk.row_group_idx];
            chassert(row_group.num_pending_chunks != 0);
            chassert(chunk.chunk_idx == row_group.next_chunk_idx - row_group.num_pending_chunks);
            --row_group.num_pending_chunks;

            scheduleMoreWorkIfNeeded(chunk.row_group_idx);

            previous_block_missing_values = std::move(chunk.block_missing_values);
            previous_approx_bytes_read_for_chunk = chunk.approx_original_chunk_size;
            return std::move(chunk.chunk);
        }

        if (row_groups_completed == row_groups.size())
            return {};

        if (pool)
            condvar.wait(lock);
        else
            decodeOneChunk(row_groups_completed, lock);
    }
}

void ParallelDecodingBlockInputFormat::resetParser()
{
    is_stopped = true;
    if (pool)
        pool->wait();

    row_groups.clear();
    while (!pending_chunks.empty())
        pending_chunks.pop();
    row_groups_completed = 0;
    previous_block_missing_values.clear();
    row_groups_started = 0;
    background_exception = nullptr;

    is_stopped = false;
    is_initialized = false;

    IInputFormat::resetParser();
}

const BlockMissingValues & ParallelDecodingBlockInputFormat::getMissingValues() const
{
    return previous_block_missing_values;
}

}
