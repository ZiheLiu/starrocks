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

#include "exec/pipeline/adaptive/collect_stats_operator.h"

#include "exec/pipeline/adaptive/collect_stats_context.h"

namespace starrocks::pipeline {

/// CollectStatsOperator.
CollectStatsOperator::CollectStatsOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                           const int32_t driver_sequence, CollectStatsContextRawPtr ctx)
        : Operator(factory, id, "collect_stats", plan_node_id, driver_sequence), _ctx(ctx) {
    _ctx->ref();
}

void CollectStatsOperator::close(RuntimeState* state) {
    _ctx->unref(state);
    Operator::close(state);
}

bool CollectStatsOperator::has_output() const {
    return _ctx->has_output(_driver_sequence);
}
bool CollectStatsOperator::need_input() const {
    return _ctx->need_input(_driver_sequence);
}
bool CollectStatsOperator::is_finished() const {
    return _is_finishing && _ctx->is_finished(_driver_sequence);
}
Status CollectStatsOperator::set_finishing(RuntimeState* state) {
    _is_finishing = true;
    return _ctx->set_finishing(_driver_sequence);
}

StatusOr<vectorized::ChunkPtr> CollectStatsOperator::pull_chunk(RuntimeState* state) {
    return _ctx->pull_chunk(_driver_sequence);
}

Status CollectStatsOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _ctx->push_chunk(_driver_sequence, chunk);
}

/// CollectStatsOperatorFactory.
CollectStatsOperatorFactory::CollectStatsOperatorFactory(int32_t id, int32_t plan_node_id, CollectStatsContextPtr ctx)
        : OperatorFactory(id, "collect_stats", plan_node_id), _ctx(std::move(ctx)) {}

} // namespace starrocks::pipeline
