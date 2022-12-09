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

#include "exec/pipeline/adaptive/collect_stats_context.h"

#include <utility>

#include "column/chunk.h"
#include "common/statusor.h"

namespace starrocks::pipeline {

/// CollectStatsState.
class CollectStatsState {
public:
    CollectStatsState(CollectStatsContext* const ctx) : _ctx(ctx) {}
    virtual ~CollectStatsState() = default;

    virtual bool need_input(int32_t driver_seq) const = 0;
    virtual Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) = 0;

    virtual bool has_output(int32_t driver_seq) const = 0;
    virtual StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) = 0;

    virtual Status set_finishing(int32_t driver_seq) = 0;
    virtual bool is_finished(int32_t driver_seq) const = 0;

protected:
    CollectStatsContext* const _ctx;
};

class BufferState final : public CollectStatsState {
public:
    BufferState(CollectStatsContext* const ctx, int max_buffer_rows)
            : CollectStatsState(ctx), _max_buffer_rows(max_buffer_rows) {}

    ~BufferState() override = default;

    bool need_input(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) override;

    bool has_output(int32_t driver_seq) const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) override;

    bool is_finished(int32_t driver_seq) const override;

    Status set_finishing(int32_t driver_seq) override;

private:
    std::atomic<int> _num_finished_seqs = 0;
    std::atomic<size_t> _num_rows = 0;
    const int _max_buffer_rows;
};

class PassthroughState final : public CollectStatsState {
public:
    PassthroughState(CollectStatsContext* const ctx) : CollectStatsState(ctx), _info_per_driver_seq(ctx->_dop) {}

    ~PassthroughState() override = default;

    bool need_input(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) override;

    bool has_output(int32_t driver_seq) const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) override;

    bool is_finished(int32_t driver_seq) const override;

    Status set_finishing(int32_t driver_seq) override;

private:
    struct InfoPerDriverSeq {
    public:
        int buffer_chunk_idx = 0;
        vectorized::ChunkPtr in_chunk = nullptr;
    };

    std::vector<InfoPerDriverSeq> _info_per_driver_seq;
};

class RoundRobinPerChunkState final : public CollectStatsState {
public:
    RoundRobinPerChunkState(CollectStatsContext* const ctx, size_t adjusted_dop)
            : CollectStatsState(ctx), _adjusted_dop(adjusted_dop), _buffer_chunk_idx_per_in_driver_seq(ctx->_dop) {
        _info_per_driver_seq.reserve(_adjusted_dop);
        for (int i = 0; i < _adjusted_dop; i++) {
            _info_per_driver_seq.emplace_back(i, ctx->_runtime_state->chunk_size());
        }
    }

    ~RoundRobinPerChunkState() override = default;

    bool need_input(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) override;

    bool has_output(int32_t driver_seq) const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) override;

    bool is_finished(int32_t driver_seq) const override;

    Status set_finishing(int32_t driver_seq) override;

private:
    struct InfoPerDriverSeq {
    public:
        InfoPerDriverSeq(int32_t driver_seq, size_t chunk_size)
                : buffer_chunk_seq(driver_seq), accumulator(chunk_size) {}

        int num_read_buffers = 0;
        int buffer_chunk_seq;
        ChunkAccumulator accumulator;
    };

    const size_t _adjusted_dop;
    std::vector<std::atomic<int>> _buffer_chunk_idx_per_in_driver_seq;
    std::vector<InfoPerDriverSeq> _info_per_driver_seq;
};

class RoundRobinPerSeqState final : public CollectStatsState {
public:
    RoundRobinPerSeqState(CollectStatsContext* const ctx, size_t adjusted_dop)
            : CollectStatsState(ctx), _adjusted_dop(adjusted_dop) {
        _info_per_driver_seq.reserve(_adjusted_dop);
        for (int i = 0; i < _adjusted_dop; i++) {
            _info_per_driver_seq.emplace_back(i, ctx->_runtime_state->chunk_size());
        }
    }

    ~RoundRobinPerSeqState() override = default;

    bool need_input(int32_t driver_seq) const override;

    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) override;

    bool has_output(int32_t driver_seq) const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq) override;

    bool is_finished(int32_t driver_seq) const override;

    Status set_finishing(int32_t driver_seq) override;

private:
    struct InfoPerDriverSeq {
    public:
        InfoPerDriverSeq(int32_t driver_seq, size_t chunk_size)
                : buffer_chunk_seq(driver_seq), accumulator(chunk_size) {}

        int buffer_chunk_idx = 0;
        int buffer_chunk_seq;
        ChunkAccumulator accumulator;
    };

    const size_t _adjusted_dop;
    std::vector<InfoPerDriverSeq> _info_per_driver_seq;
};

/// BufferState.
bool BufferState::need_input(int32_t driver_seq) const {
    return true;
}

Status BufferState::push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) {
    size_t num_chunk_rows = chunk->num_rows();
    _ctx->_chunks(driver_seq).emplace_back(std::move(chunk));
    size_t prev_num_rows = _num_rows.fetch_add(num_chunk_rows);

    //    LOG(WARNING) << "[ADAPTIVE] BufferState::push_chunk "
    //                 << "[driver_seq=" << driver_seq << "] "
    //                 << "[prev_num_rows=" << prev_num_rows << "] "
    //                 << "[_max_buffer_rows=" << _max_buffer_rows << "] "
    //                 << "[num_chunk_rows=" << num_chunk_rows << "] "
    //                 << "[change_to_passthrough="
    //                 << (prev_num_rows < _max_buffer_rows && prev_num_rows + num_chunk_rows >= _max_buffer_rows) << "] ";

    if (prev_num_rows < _max_buffer_rows && prev_num_rows + num_chunk_rows >= _max_buffer_rows) {
        _ctx->_set_state(std::make_shared<PassthroughState>(_ctx));
    }
    return Status::OK();
}

bool BufferState::has_output(int32_t driver_seq) const {
    return false;
}

StatusOr<vectorized::ChunkPtr> BufferState::pull_chunk(int32_t driver_seq) {
    return Status::InternalError("Shouldn't call BufferState::pull_chunk");
}

size_t compute_max_le_power2(size_t num) {
    num |= (num >> 1);
    num |= (num >> 2);
    num |= (num >> 4);
    num |= (num >> 8);
    num |= (num >> 16);
    return num - (num >> 1);
}

Status BufferState::set_finishing(int32_t driver_seq) {
    int num_finished_seqs = ++_num_finished_seqs;
    if (num_finished_seqs == _ctx->_dop) {
        int num_partial_rows = _ctx->_dop * _ctx->_runtime_state->chunk_size();
        size_t adjusted_dop = _num_rows / num_partial_rows;
        adjusted_dop = compute_max_le_power2(adjusted_dop);
        adjusted_dop = std::max<size_t>(adjusted_dop, 1);
        adjusted_dop = std::min<size_t>(adjusted_dop, _ctx->_dop);

        //        LOG(WARNING) << "[ADAPTIVE] BufferState::set_finishing "
        //                     << "[driver_seq=" << driver_seq << "] "
        //                     << "[num_finished_seqs=" << num_finished_seqs << "] "
        //                     << "[change_to_round_robin=1] "
        //                     << "[adjusted_dop=" << adjusted_dop << "] ";

        switch (_ctx->_assign_chunk_strategy) {
        case CollectStatsContext::AssignChunkStrategy::ROUND_ROBIN_CHUNK:
            _ctx->_set_state(std::make_shared<RoundRobinPerChunkState>(_ctx, adjusted_dop));
            break;
        case CollectStatsContext::AssignChunkStrategy::ROUND_ROBIN_DRIVER_SEQ:
            _ctx->_set_state(std::make_shared<RoundRobinPerSeqState>(_ctx, adjusted_dop));
            break;
        }
    } else {
        //        LOG(WARNING) << "[ADAPTIVE] BufferState::set_finishing "
        //                     << "[driver_seq=" << driver_seq << "] "
        //                     << "[num_finished_seqs=" << num_finished_seqs << "] "
        //                     << "[change_to_round_robin=0] ";
    }

    return Status::OK();
}

bool BufferState::is_finished(int32_t driver_seq) const {
    return false;
}

/// PassthroughState.
bool PassthroughState::need_input(int32_t driver_seq) const {
    return _info_per_driver_seq[driver_seq].in_chunk == nullptr;
}

Status PassthroughState::push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) {
    _info_per_driver_seq[driver_seq].in_chunk = std::move(chunk);
    return Status::OK();
}

bool PassthroughState::has_output(int32_t driver_seq) const {
    const auto& info = _info_per_driver_seq[driver_seq];
    const auto& buffer_chunks = _ctx->_chunks(driver_seq);

    //    LOG(WARNING) << "[ADAPTIVE] PassthroughState::has_output "
    //                 << "[driver_seq=" << driver_seq << "] "
    //                 << "[buffer_chunk_idx=" << info.buffer_chunk_idx << "] "
    //                 << "[buffer_chunks_size " << buffer_chunks.size() << "] "
    //                 << "[in_chunk=" << info.in_chunk.get() << "] ";

    if (info.buffer_chunk_idx < buffer_chunks.size()) {
        return true;
    }

    return info.in_chunk != nullptr;
}

StatusOr<vectorized::ChunkPtr> PassthroughState::pull_chunk(int32_t driver_seq) {
    auto& info = _info_per_driver_seq[driver_seq];
    auto& buffer_chunks = _ctx->_chunks(driver_seq);

    if (info.buffer_chunk_idx < buffer_chunks.size()) {
        return std::move(buffer_chunks[info.buffer_chunk_idx++]);
    }
    return std::move(info.in_chunk);
}

Status PassthroughState::set_finishing(int32_t driver_seq) {
    //    LOG(WARNING) << "[ADAPTIVE] PassthroughState::set_finishing "
    //                 << "[driver_seq=" << driver_seq << "] ";
    return Status::OK();
}

bool PassthroughState::is_finished(int32_t driver_seq) const {
    bool res = !has_output(driver_seq);
    //    LOG(WARNING) << "[ADAPTIVE] PassthroughState::is_finished "
    //                 << "[driver_seq=" << driver_seq << "] "
    //                 << "[res=" << res << "] ";
    return res;
}

/// RoundRobinPerChunkState.
bool RoundRobinPerChunkState::need_input(int32_t driver_seq) const {
    return false;
}

Status RoundRobinPerChunkState::push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) {
    return Status::InternalError("Shouldn't call RoundRobinPerChunkState::push_chunk");
}

bool RoundRobinPerChunkState::has_output(int32_t driver_seq) const {
    if (driver_seq >= _adjusted_dop) {
        return false;
    }

    const auto& info = _info_per_driver_seq[driver_seq];
    if (!info.accumulator.empty()) {
        return true;
    }

    int seq = info.buffer_chunk_seq;
    for (int i = info.num_read_buffers; i < _ctx->_dop; ++i) {
        const auto& buffer_chunks = _ctx->_chunks(seq);
        if (_buffer_chunk_idx_per_in_driver_seq[seq] < buffer_chunks.size()) {
            return true;
        }
        seq = (seq + 1) % _ctx->_dop;
    }
    return false;
}

StatusOr<vectorized::ChunkPtr> RoundRobinPerChunkState::pull_chunk(int32_t driver_seq) {
    DCHECK_LT(driver_seq, _adjusted_dop);

    auto& info = _info_per_driver_seq[driver_seq];
    if (!info.accumulator.empty()) {
        return info.accumulator.pull();
    }

    while (info.num_read_buffers < _ctx->_dop) {
        auto& buffer_chunks = _ctx->_chunks(info.buffer_chunk_seq);
        auto& buffer_chunk_idx = _buffer_chunk_idx_per_in_driver_seq[info.buffer_chunk_seq];
        for (int idx = buffer_chunk_idx.fetch_add(1); idx < buffer_chunks.size(); idx = buffer_chunk_idx.fetch_add(1)) {
            info.accumulator.push(std::move(buffer_chunks[idx]));
        }
        ++info.num_read_buffers;
        info.buffer_chunk_seq = (info.buffer_chunk_seq + 1) % _ctx->_dop;
    }

    info.accumulator.finalize();
    return info.accumulator.pull();
}

Status RoundRobinPerChunkState::set_finishing(int32_t driver_seq) {
    return Status::InternalError("Should already call RoundRobinPerChunkState::set_finishing before");
}

bool RoundRobinPerChunkState::is_finished(int32_t driver_seq) const {
    if (driver_seq >= _adjusted_dop) {
        return true;
    }

    const auto& info = _info_per_driver_seq[driver_seq];
    return info.num_read_buffers >= _ctx->_dop && info.accumulator.empty();
}

/// RoundRobinPerSeqState.
bool RoundRobinPerSeqState::need_input(int32_t driver_seq) const {
    return false;
}

Status RoundRobinPerSeqState::push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) {
    return Status::InternalError("Shouldn't call RoundRobinPerSeqState::push_chunk");
}

bool RoundRobinPerSeqState::has_output(int32_t driver_seq) const {
    if (driver_seq >= _adjusted_dop) {
        return false;
    }

    const auto& info = _info_per_driver_seq[driver_seq];
    if (!info.accumulator.empty()) {
        return true;
    }

    for (int seq = info.buffer_chunk_seq; seq < _ctx->_dop; seq += _adjusted_dop) {
        const auto& buffer_chunks = _ctx->_chunks(seq);
        int buffer_chunk_idx = seq == info.buffer_chunk_seq ? info.buffer_chunk_idx : 0;
        if (buffer_chunk_idx < buffer_chunks.size()) {
            return true;
        }
    }
    return false;
}

StatusOr<vectorized::ChunkPtr> RoundRobinPerSeqState::pull_chunk(int32_t driver_seq) {
    auto& info = _info_per_driver_seq[driver_seq];
    auto& accumulator = info.accumulator;
    if (!accumulator.empty()) {
        return accumulator.pull();
    }

    int& seq = info.buffer_chunk_seq;
    int& idx = info.buffer_chunk_idx;
    while (seq < _ctx->_dop) {
        auto& buffer_chunks = _ctx->_chunks(seq);
        while (idx < buffer_chunks.size()) {
            accumulator.push(std::move(buffer_chunks[idx]));
            ++idx;
            if (!accumulator.empty()) {
                return accumulator.pull();
            }
        }

        seq += _adjusted_dop;
        idx = 0;
    }

    accumulator.finalize();
    return accumulator.pull();
}

Status RoundRobinPerSeqState::set_finishing(int32_t driver_seq) {
    return Status::InternalError("Should already call RoundRobinPerSeqState::set_finishing before");
}

bool RoundRobinPerSeqState::is_finished(int32_t driver_seq) const {
    if (driver_seq >= _adjusted_dop) {
        return true;
    }

    const auto& info = _info_per_driver_seq[driver_seq];
    return info.buffer_chunk_seq >= _ctx->_dop && info.accumulator.empty();
}

/// CollectStatsContext.
CollectStatsContext::CollectStatsContext(RuntimeState* const runtime_state, size_t dop,
                                         AssignChunkStrategy assign_chunk_strategy)
        : _state(std::make_shared<BufferState>(this, runtime_state->chunk_size() * dop)),
          _dop(dop),
          _assign_chunk_strategy(assign_chunk_strategy),
          _chunks_per_driver_seq(dop),
          _runtime_state(runtime_state) {}

void CollectStatsContext::close(RuntimeState* state) {}

bool CollectStatsContext::need_input(int32_t driver_seq) const {
    return _state_ref()->need_input(driver_seq);
}

Status CollectStatsContext::push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) {
    return _state_ref()->push_chunk(driver_seq, std::move(chunk));
}

bool CollectStatsContext::has_output(int32_t driver_seq) const {
    return _state_ref()->has_output(driver_seq);
}

StatusOr<vectorized::ChunkPtr> CollectStatsContext::pull_chunk(int32_t driver_seq) {
    return _state_ref()->pull_chunk(driver_seq);
}

Status CollectStatsContext::set_finishing(int32_t driver_seq) {
    return _state_ref()->set_finishing(driver_seq);
}

bool CollectStatsContext::is_finished(int32_t driver_seq) const {
    return _state_ref()->is_finished(driver_seq);
}

CollectStatsStatePtr CollectStatsContext::_state_ref() const {
    return std::atomic_load(&_state);
}

void CollectStatsContext::_set_state(CollectStatsStatePtr state) {
    std::atomic_store(&_state, std::move(state));
}

std::vector<vectorized::ChunkPtr>& CollectStatsContext::_chunks(int32_t driver_seq) {
    return _chunks_per_driver_seq[driver_seq];
}

} // namespace starrocks::pipeline