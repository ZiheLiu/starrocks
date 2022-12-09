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
using CollectStatsStatePtr = std::unique_ptr<CollectStatsState>;
using CollectStatsStateRawPtr = CollectStatsState*;
enum class CollectStatsStateEnum;

class CollectStatsContext final : public ContextWithDependency {
public:
    enum class AssignChunkStrategy { ROUND_ROBIN_CHUNK, ROUND_ROBIN_DRIVER_SEQ };

    CollectStatsContext(RuntimeState* const runtime_state, size_t dop, AssignChunkStrategy assign_chunk_strategy);
    ~CollectStatsContext() override = default;

    void close(RuntimeState* state) override;

    bool need_input(int32_t driver_seq) const;
    Status push_chunk(int32_t driver_seq, vectorized::ChunkPtr chunk);
    bool has_output(int32_t driver_seq) const;
    StatusOr<vectorized::ChunkPtr> pull_chunk(int32_t driver_seq);
    Status set_finishing(int32_t driver_seq);
    bool is_finished(int32_t driver_seq) const;

private:
    CollectStatsStateRawPtr _get_state(CollectStatsStateEnum state) const;
    CollectStatsStateRawPtr _state_ref() const;
    void _set_state(CollectStatsStateRawPtr state);
    std::vector<vectorized::ChunkPtr>& _chunks(int32_t driver_seq);

private:
    friend class BufferState;
    friend class RoundRobinPerChunkState;
    friend class RoundRobinPerSeqState;
    friend class PassthroughState;

    std::atomic<CollectStatsStateRawPtr> _state = nullptr;
    std::unordered_map<CollectStatsStateEnum, CollectStatsStatePtr> _state_payloads;

    size_t _dop;
    AssignChunkStrategy _assign_chunk_strategy;

    std::vector<std::vector<vectorized::ChunkPtr>> _chunks_per_driver_seq;

    RuntimeState* const _runtime_state;
};

} // namespace pipeline
} // namespace starrocks
