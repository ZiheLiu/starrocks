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

/// BufferState.
bool BufferState::need_input(int32_t driver_seq) const {
    return true;
}

Status BufferState::push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk) {
    size_t num_chunk_rows = chunk->num_rows();
    _ctx->_chunks(driver_seq).emplace_back(std::move(chunk));
    size_t prev_num_rows = _num_rows.fetch_add(num_chunk_rows);

    if (prev_num_rows < _max_buffer_rows && prev_num_rows + num_chunk_rows >= _max_buffer_rows) {
        auto* next_state = _ctx->_get_state(CollectStatsStateEnum::PASSTHROUGH);
        next_state->set_adjusted_dop(_ctx->_dop);
        _ctx->_set_state(next_state);
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
        int num_partial_rows = _ctx->_runtime_state->chunk_size();
        size_t adjusted_dop = _num_rows / num_partial_rows;
        adjusted_dop = compute_max_le_power2(adjusted_dop);
        adjusted_dop = std::max<size_t>(adjusted_dop, 1);
        adjusted_dop = std::min<size_t>(adjusted_dop, _ctx->_dop);

        CollectStatsState* next_state = nullptr;
        switch (_ctx->_assign_chunk_strategy) {
        case CollectStatsContext::AssignChunkStrategy::ROUND_ROBIN_CHUNK:
            next_state = _ctx->_get_state(CollectStatsStateEnum::ROUND_ROBIN_PER_CHUNK);
            break;
        case CollectStatsContext::AssignChunkStrategy::ROUND_ROBIN_DRIVER_SEQ:
            next_state = _ctx->_get_state(CollectStatsStateEnum::ROUND_ROBIN_PER_SEQ);
            break;
        }
        if (next_state != nullptr) {
            next_state->set_adjusted_dop(adjusted_dop);
            _ctx->_set_state(next_state);
        }
    }

    return Status::OK();
}

bool BufferState::is_finished(int32_t driver_seq) const {
    return false;
}

/// PassthroughState.
PassthroughState::PassthroughState(CollectStatsContext* const ctx)
        : CollectStatsState(ctx), _info_per_driver_seq(ctx->_dop) {}

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

    if (info.idx_in_buffer < buffer_chunks.size()) {
        return true;
    }

    return info.in_chunk != nullptr;
}

StatusOr<vectorized::ChunkPtr> PassthroughState::pull_chunk(int32_t driver_seq) {
    auto& info = _info_per_driver_seq[driver_seq];
    auto& buffer_chunks = _ctx->_chunks(driver_seq);

    if (info.idx_in_buffer < buffer_chunks.size()) {
        return std::move(buffer_chunks[info.idx_in_buffer++]);
    }
    return std::move(info.in_chunk);
}

Status PassthroughState::set_finishing(int32_t driver_seq) {
    return Status::OK();
}

bool PassthroughState::is_finished(int32_t driver_seq) const {
    return !has_output(driver_seq);
}

/// RoundRobinPerChunkState.
RoundRobinPerChunkState::RoundRobinPerChunkState(CollectStatsContext* const ctx)
        : CollectStatsState(ctx), _idx_in_buffers(ctx->_dop) {}

void RoundRobinPerChunkState::set_adjusted_dop(size_t adjusted_dop) {
    _adjusted_dop = adjusted_dop;

    _info_per_driver_seq.reserve(_adjusted_dop);
    for (int i = 0; i < _adjusted_dop; i++) {
        _info_per_driver_seq.emplace_back(i, _ctx->_runtime_state->chunk_size());
    }
}

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
    return info.num_read_buffers < _ctx->_dop || !info.accumulator.empty();
}

StatusOr<vectorized::ChunkPtr> RoundRobinPerChunkState::pull_chunk(int32_t driver_seq) {
    DCHECK_LT(driver_seq, _adjusted_dop);

    auto& info = _info_per_driver_seq[driver_seq];
    if (!info.accumulator.empty()) {
        return info.accumulator.pull();
    }

    while (info.num_read_buffers < _ctx->_dop) {
        auto& buffer_chunks = _ctx->_chunks(info.buffer_idx);
        auto& idx_in_buffer = _idx_in_buffers[info.buffer_idx];
        for (int idx = idx_in_buffer.fetch_add(1); idx < buffer_chunks.size(); idx = idx_in_buffer.fetch_add(1)) {
            info.accumulator.push(std::move(buffer_chunks[idx]));
        }
        ++info.num_read_buffers;
        info.buffer_idx = (info.buffer_idx + 1) % _ctx->_dop;
    }

    info.accumulator.finalize();
    return info.accumulator.pull();
}

Status RoundRobinPerChunkState::set_finishing(int32_t driver_seq) {
    return Status::InternalError("Should already call RoundRobinPerChunkState::set_finishing before");
}

bool RoundRobinPerChunkState::is_finished(int32_t driver_seq) const {
    return !has_output(driver_seq);
}

/// RoundRobinPerSeqState.
void RoundRobinPerSeqState::set_adjusted_dop(size_t adjusted_dop) {
    _adjusted_dop = adjusted_dop;

    _info_per_driver_seq.reserve(_adjusted_dop);
    for (int i = 0; i < _adjusted_dop; i++) {
        _info_per_driver_seq.emplace_back(i, _ctx->_runtime_state->chunk_size());
    }
}

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

    const auto& [buffer_idx, _, accumulator] = _info_per_driver_seq[driver_seq];
    return buffer_idx < _ctx->_dop || !accumulator.empty();
}

StatusOr<vectorized::ChunkPtr> RoundRobinPerSeqState::pull_chunk(int32_t driver_seq) {
    auto& [buffer_idx, idx_in_buffer, accumulator] = _info_per_driver_seq[driver_seq];
    if (!accumulator.empty()) {
        return accumulator.pull();
    }

    while (buffer_idx < _ctx->_dop) {
        auto& buffer_chunks = _ctx->_chunks(buffer_idx);
        while (idx_in_buffer < buffer_chunks.size()) {
            accumulator.push(std::move(buffer_chunks[idx_in_buffer]));
            ++idx_in_buffer;
            if (!accumulator.empty()) {
                return accumulator.pull();
            }
        }

        buffer_idx += _adjusted_dop;
        idx_in_buffer = 0;
    }

    accumulator.finalize();
    return accumulator.pull();
}

Status RoundRobinPerSeqState::set_finishing(int32_t driver_seq) {
    return Status::InternalError("Should already call RoundRobinPerSeqState::set_finishing before");
}

bool RoundRobinPerSeqState::is_finished(int32_t driver_seq) const {
    return !has_output(driver_seq);
}

/// CollectStatsContext.
CollectStatsContext::CollectStatsContext(RuntimeState* const runtime_state, size_t dop,
                                         AssignChunkStrategy assign_chunk_strategy)
        : _dop(dop),
          _assign_chunk_strategy(assign_chunk_strategy),
          _chunks_per_driver_seq(dop),
          _runtime_state(runtime_state) {
    int max_buffer_rows = runtime_state->chunk_size() * dop;
    _state_payloads[CollectStatsStateEnum::BUFFER] = std::make_unique<BufferState>(this, max_buffer_rows);
    _state_payloads[CollectStatsStateEnum::PASSTHROUGH] = std::make_unique<PassthroughState>(this);
    _state_payloads[CollectStatsStateEnum::ROUND_ROBIN_PER_CHUNK] = std::make_unique<RoundRobinPerChunkState>(this);
    _state_payloads[CollectStatsStateEnum::ROUND_ROBIN_PER_SEQ] = std::make_unique<RoundRobinPerSeqState>(this);
    _set_state(_get_state(CollectStatsStateEnum::BUFFER));
}

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

CollectStatsStateRawPtr CollectStatsContext::_get_state(CollectStatsStateEnum state) const {
    return _state_payloads.at(state).get();
}
CollectStatsStateRawPtr CollectStatsContext::_state_ref() const {
    return _state.load();
}
void CollectStatsContext::_set_state(CollectStatsStateRawPtr state) {
    _state = state;
}

std::vector<vectorized::ChunkPtr>& CollectStatsContext::_chunks(int32_t driver_seq) {
    return _chunks_per_driver_seq[driver_seq];
}

} // namespace starrocks::pipeline