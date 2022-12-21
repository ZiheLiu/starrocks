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
#include "exec/pipeline/adaptive/adaptive_fwd.h"
#include "exec/pipeline/context_with_dependency.h"
#include "storage/chunk_helper.h"
#include "util/moodycamel/concurrentqueue.h"

namespace starrocks {

class Status;
template <typename T>
class StatusOr;

namespace pipeline {

enum class CollectStatsStateEnum { BUFFER = 0, PASSTHROUGH, ROUND_ROBIN_PER_SEQ };

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
    const size_t MAX_PASSTHROUGH_CHUNKS_PER_DRIVER_SEQ = config::pipeline_cs_passthrough_buffer;

    using ChunkQueue = moodycamel::ConcurrentQueue<vectorized::ChunkPtr>;
    std::vector<ChunkQueue> _in_chunk_queue_per_driver_seq;
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
        ChunkAccumulator accumulator;
    };

    size_t _adjusted_dop = 0;
    std::vector<DriverInfo> _info_per_driver_seq;
};

class CollectStatsContext final : public ContextWithDependency {
public:
    CollectStatsContext(RuntimeState* const runtime_state, size_t dop);
    ~CollectStatsContext() override = default;

    void close(RuntimeState* state) override;

    bool need_input(int32_t driver_seq) const;
    bool has_output(int32_t driver_seq) const;
    bool is_finished(int32_t driver_seq) const;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk);
    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq);
    Status set_finishing(int32_t driver_seq);

    bool is_source_ready() const;
    size_t source_dop() const { return _source_dop; }

private:
    using BufferChunkQueue = std::queue<vectorized::ChunkPtr>;

    CollectStatsStateRawPtr _get_state(CollectStatsStateEnum state) const;
    CollectStatsStateRawPtr _state_ref() const;
    void _set_state(CollectStatsStateEnum state_enum);
    void _transform_state(CollectStatsStateEnum state_enum, size_t source_dop);
    BufferChunkQueue& _buffer_chunk_queue(int32_t driver_seq);

private:
    friend class BufferState;
    friend class RoundRobinPerSeqState;
    friend class PassthroughState;

    std::atomic<CollectStatsStateRawPtr> _state = nullptr;
    std::unordered_map<CollectStatsStateEnum, CollectStatsStatePtr> _state_payloads;

    const size_t _sink_dop;
    size_t _source_dop;

    std::vector<BufferChunkQueue> _buffer_chunk_queue_per_driver_seq;
    std::vector<uint8_t> _is_finishing_per_driver_seq;

    RuntimeState* const _runtime_state;
};

} // namespace pipeline
} // namespace starrocks
