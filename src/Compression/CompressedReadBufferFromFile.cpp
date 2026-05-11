/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <cassert>

#include "CompressedReadBufferFromFile.h"

#include <Compression/LZ4_decompress_faster.h>
#include <IO/WriteHelpers.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event DiskCacheDiskReadMicroseconds;
    extern const Event DiskCacheDecompressMicroseconds;
}

static Poco::Logger * getLog() { return &Poco::Logger::get("CompressedReadBufferFromFile"); }

namespace DB
{
namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


bool CompressedReadBufferFromFile::nextImpl()
{
    /// TODO: handle hdfs case
    if (/*(storage_type == StorageType::Hdfs ||*/ is_limit /*)*/ && file_in.getPosition() >= limit_offset_in_file)
    {
        size_compressed = 0;

        return false;
    }

    size_t size_decompressed = 0;
    size_t size_compressed_without_checksum;
    Stopwatch io_sw;
    size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, false);
    const auto io_us = io_sw.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::DiskCacheDiskReadMicroseconds, io_us);

    if (!size_compressed)
        return false;

    auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

    /// This is for clang static analyzer.
    assert(size_decompressed + additional_size_at_the_end_of_buffer > 0);

    memory.resize(size_decompressed + additional_size_at_the_end_of_buffer);
    working_buffer = Buffer(memory.data(), &memory[size_decompressed]);

    Stopwatch decomp_sw;
    decompress(working_buffer, size_decompressed, size_compressed_without_checksum);
    const auto decomp_us = decomp_sw.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::DiskCacheDecompressMicroseconds, decomp_us);

    LOG_DEBUG(getLog(), "[cache-perf] path={} compressed={}B decompressed={}B disk_read={}us decompress={}us",
        file_in.getFileName(), size_compressed, size_decompressed, io_us, decomp_us);

    /// nextimpl_working_buffer_offset is set in the seek function (lazy seek). So we have to
    /// check that we are not seeking beyond working buffer.
    if (nextimpl_working_buffer_offset > working_buffer.size())
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Required to move position beyond the decompressed block (pos: "
        "{}, block size: {})", nextimpl_working_buffer_offset, toString(working_buffer.size()));

    return true;
}

CompressedReadBufferFromFile::CompressedReadBufferFromFile(
    std::unique_ptr<ReadBufferFromFileBase> buf,
    bool allow_different_codecs_,
    off_t file_offset_,
    size_t file_size_,
    bool is_limit_)
    : BufferWithOwnMemory<ReadBuffer>(0)
    , p_file_in(std::move(buf))
    , file_in(*p_file_in)
    , limit_offset_in_file(file_offset_ + file_size_)
    , is_limit(is_limit_)
{
    compressed_in = &file_in;
    allow_different_codecs = allow_different_codecs_;
}


CompressedReadBufferFromFile::CompressedReadBufferFromFile(
    const std::string & path,
    size_t estimated_size,
    size_t aio_threshold,
    size_t mmap_threshold,
    MMappedFileCache * mmap_cache,
    size_t buf_size,
    bool allow_different_codecs_,
    off_t file_offset_,
    size_t file_size_,
    bool is_limit_)
    : BufferWithOwnMemory<ReadBuffer>(0)
    , p_file_in(createReadBufferFromFileBase(path, {.local_fs_buffer_size = buf_size, .aio_threshold = aio_threshold, .mmap_threshold = mmap_threshold, .mmap_cache = mmap_cache, .estimated_size = estimated_size}))
    , file_in(*p_file_in)
    , limit_offset_in_file(file_offset_ + file_size_)
    , is_limit(is_limit_)
{
    compressed_in = &file_in;
    allow_different_codecs = allow_different_codecs_;
}

void CompressedReadBufferFromFile::prefetch(Priority priority)
{
    file_in.prefetch(priority);
}

void CompressedReadBufferFromFile::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    /// Nothing to do if we already at required position
    if (!size_compressed && static_cast<size_t>(file_in.getPosition()) == offset_in_compressed_file && /// correct position in compressed file
        ((!buffer().empty() && offset() == offset_in_decompressed_block)     /// correct position in buffer or
         || nextimpl_working_buffer_offset == offset_in_decompressed_block)) /// we will move our position to correct one
        return;

    /// Our seek is within working_buffer, so just move the position
    if (size_compressed &&
        offset_in_compressed_file == file_in.getPosition() - size_compressed &&
        offset_in_decompressed_block <= working_buffer.size())
    {
        pos = working_buffer.begin() + offset_in_decompressed_block;
    }
    else /// Our seek outside working buffer, so perform "lazy seek"
    {
        /// Actually seek compressed file
        file_in.seek(offset_in_compressed_file, SEEK_SET);
        /// We will discard our working_buffer, but have to account rest bytes
        bytes += offset();
        /// No data, everything discarded
        resetWorkingBuffer();
        size_compressed = 0;
        /// Remember required offset in decompressed block which will be set in
        /// the next ReadBuffer::next() call
        nextimpl_working_buffer_offset = offset_in_decompressed_block;
    }
}


size_t CompressedReadBufferFromFile::readBig(char * to, size_t n)
{
    size_t bytes_read = 0;

    /// If there are unread bytes in the buffer, then we copy needed to `to`.
    if (pos < working_buffer.end())
        bytes_read += read(to, std::min(static_cast<size_t>(working_buffer.end() - pos), n));

    /// If you need to read more - we will, if possible, decompress at once to `to`.
    while (bytes_read < n)
    {
        /// TODO: handle hdfs case
        if (/*(storage_type == StorageType::Hdfs ||*/ is_limit /*)*/ && file_in.getPosition() >= limit_offset_in_file)
            return bytes_read;

        size_t size_decompressed = 0;
        size_t size_compressed_without_checksum = 0;

        Stopwatch io_sw2;
        size_t new_size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, false);
        const auto io_us2 = io_sw2.elapsedMicroseconds();
        ProfileEvents::increment(ProfileEvents::DiskCacheDiskReadMicroseconds, io_us2);

        size_compressed = 0; /// file_in no longer points to the end of the block in working_buffer.
        if (!new_size_compressed)
            return bytes_read;

        auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

        /// If the decompressed block fits entirely where it needs to be copied and we don't
        /// need to skip some bytes in decompressed data (seek happened before readBig call).
        if (nextimpl_working_buffer_offset == 0 && size_decompressed + additional_size_at_the_end_of_buffer <= n - bytes_read)
        {
            Stopwatch decomp_sw2;
            decompressTo(to + bytes_read, size_decompressed, size_compressed_without_checksum);
            const auto decomp_us2 = decomp_sw2.elapsedMicroseconds();
            ProfileEvents::increment(ProfileEvents::DiskCacheDecompressMicroseconds, decomp_us2);

            LOG_DEBUG(getLog(), "[cache-perf] path={} compressed={}B decompressed={}B disk_read={}us decompress={}us",
                file_in.getFileName(), new_size_compressed, size_decompressed, io_us2, decomp_us2);

            bytes_read += size_decompressed;
            bytes += size_decompressed;
        }
        else
        {
            size_compressed = new_size_compressed;
            bytes += offset();

            /// This is for clang static analyzer.
            assert(size_decompressed + additional_size_at_the_end_of_buffer > 0);

            memory.resize(size_decompressed + additional_size_at_the_end_of_buffer);
            working_buffer = Buffer(memory.data(), &memory[size_decompressed]);

            decompress(working_buffer, size_decompressed, size_compressed_without_checksum);

            /// Manually take nextimpl_working_buffer_offset into account, because we don't use
            /// nextImpl in this method.
            pos = working_buffer.begin() + nextimpl_working_buffer_offset;
            nextimpl_working_buffer_offset = 0;

            bytes_read += read(to + bytes_read, n - bytes_read);
            break;
        }
    }

    return bytes_read;
}

std::pair<size_t, size_t> CompressedReadBufferFromFile::position() const
{
    return {file_in.getPosition() - size_compressed, size_compressed == 0 ? 0 : offset()};
}

}
