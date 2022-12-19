// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/context_with_dependency.h"
#include "storage/chunk_helper.h"

namespace starrocks {

class Status;
template <typename T>
class StatusOr;

namespace pipeline {

class CollectStatsContext;
class CollectStatsState;
using CollectStatsStatePtr = std::shared_ptr<CollectStatsState>;
using CollectStatsStateRawPtr = CollectStatsState*;

enum class CollectStatsStateEnum { BUFFER = 0, PASSTHROUGH, ROUND_ROBIN_PER_CHUNK, ROUND_ROBIN_PER_SEQ };

class CollectStatsState {
public:
    CollectStatsState(CollectStatsContext* const ctx) : _ctx(ctx) {}
    virtual ~CollectStatsState() = default;

    virtual bool need_input(int32_t driver_seq) const = 0;
    virtual bool has_output(int32_t driver_seq) const = 0;
    virtual bool is_finished(int32_t driver_seq) const = 0;

    virtual Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) = 0;
    virtual StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) = 0;
    virtual Status set_finishing(int32_t driver_seq) = 0;

    virtual void set_adjusted_dop(size_t adjusted_dop) {}

protected:
    CollectStatsContext* const _ctx;
};

class BufferState final : public CollectStatsState {
public:
    BufferState(CollectStatsContext* const ctx, int max_buffer_rows)
            : CollectStatsState(ctx), _max_buffer_rows(max_buffer_rows) {}
    ~BufferState() override = default;

    bool need_input(int32_t driver_seq) const override;
    bool has_output(int32_t driver_seq) const override;
    bool is_finished(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) override;
    Status set_finishing(int32_t driver_seq) override;

private:
    std::atomic<int> _num_finished_seqs = 0;
    std::atomic<size_t> _num_rows = 0;
    const int _max_buffer_rows;
};

class PassthroughState final : public CollectStatsState {
public:
    PassthroughState(CollectStatsContext* const ctx);
    ~PassthroughState() override = default;

    bool need_input(int32_t driver_seq) const override;
    bool has_output(int32_t driver_seq) const override;
    bool is_finished(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) override;
    Status set_finishing(int32_t driver_seq) override;

private:
    struct DriverInfo {
    public:
        int idx_in_buffer = 0;
        vectorized::ChunkPtr in_chunk = nullptr;
    };

    std::vector<DriverInfo> _info_per_driver_seq;
};

class RoundRobinPerChunkState final : public CollectStatsState {
public:
    RoundRobinPerChunkState(CollectStatsContext* const ctx);
    ~RoundRobinPerChunkState() override = default;

    void set_adjusted_dop(size_t adjusted_dop) override;

    bool need_input(int32_t driver_seq) const override;
    bool has_output(int32_t driver_seq) const override;
    bool is_finished(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) override;
    Status set_finishing(int32_t driver_seq) override;

private:
    struct DriverInfo {
    public:
        DriverInfo(int32_t driver_seq, size_t chunk_size) : buffer_idx(driver_seq), accumulator(chunk_size) {}

        int num_read_buffers = 0;
        int buffer_idx;
        ChunkAccumulator accumulator;
    };

    size_t _adjusted_dop = 0;
    std::vector<std::atomic<int>> _idx_in_buffers;
    std::vector<DriverInfo> _info_per_driver_seq;
};

class RoundRobinPerSeqState final : public CollectStatsState {
public:
    RoundRobinPerSeqState(CollectStatsContext* const ctx) : CollectStatsState(ctx) {}
    ~RoundRobinPerSeqState() override = default;

    void set_adjusted_dop(size_t adjusted_dop) override;

    bool need_input(int32_t driver_seq) const override;
    bool has_output(int32_t driver_seq) const override;
    bool is_finished(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) override;
    Status set_finishing(int32_t driver_seq) override;

private:
    struct DriverInfo {
    public:
        DriverInfo(int32_t driver_seq, size_t chunk_size) : buffer_idx(driver_seq), accumulator(chunk_size) {}

        int buffer_idx;
        int idx_in_buffer = 0;
        ChunkAccumulator accumulator;
    };

    size_t _adjusted_dop = 0;
    std::vector<DriverInfo> _info_per_driver_seq;
};

class CollectStatsContext final : public ContextWithDependency {
public:
    enum class AssignChunkStrategy { ROUND_ROBIN_CHUNK, ROUND_ROBIN_DRIVER_SEQ };

    CollectStatsContext(RuntimeState* const runtime_state, size_t dop, AssignChunkStrategy assign_chunk_strategy);
    ~CollectStatsContext() override = default;

    void close(RuntimeState* state) override;

    bool need_input(int32_t driver_seq) const;
    bool has_output(int32_t driver_seq) const;
    bool is_finished(int32_t driver_seq) const;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk);
    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq);
    Status set_finishing(int32_t driver_seq);

    bool is_source_ready() const;

private:
    CollectStatsStateRawPtr _get_state(CollectStatsStateEnum state) const;
    CollectStatsStateRawPtr _state_ref() const;
    void _set_state(CollectStatsStateRawPtr state);
    std::vector<vectorized::ChunkPtr>& _buffer_chunks(int32_t driver_seq);

private:
    friend class BufferState;
    friend class RoundRobinPerChunkState;
    friend class RoundRobinPerSeqState;
    friend class PassthroughState;

    std::atomic<CollectStatsStateRawPtr> _state = nullptr;
    std::unordered_map<CollectStatsStateEnum, CollectStatsStatePtr> _state_payloads;

    size_t _dop;
    AssignChunkStrategy _assign_chunk_strategy;

    std::vector<std::vector<vectorized::ChunkPtr>> _buffer_chunks_per_driver_seq;
    std::vector<bool> _is_finishing_per_driver_seq;

    RuntimeState* const _runtime_state;
};

} // namespace pipeline
} // namespace starrocks
