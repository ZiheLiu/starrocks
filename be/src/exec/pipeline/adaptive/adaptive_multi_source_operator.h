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

#include "exec/pipeline/adaptive/adaptive_fwd.h"
#include "exec/pipeline/source_operator.h"
#include "util/phmap/phmap.h"

namespace starrocks::pipeline {

class AdaptiveMultiSourceOperator final : public SourceOperator {
public:
    struct InnerPipelineDriver {
        OperatorPtr source;
        OperatorPtr sink;
    };

    AdaptiveMultiSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                std::vector<InnerPipelineDriver>&& inner_drivers);

    [[nodiscard]] Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    [[nodiscard]] Status set_finishing(RuntimeState* state) override;
    [[nodiscard]] Status set_finished(RuntimeState* state) override;
    [[nodiscard]] Status set_cancelled(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    [[nodiscard]] StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    Status _mark_operator_finishing(OperatorPtr& op, OperatorStage& op_stage, RuntimeState* state);
    Status _mark_operator_finished(OperatorPtr& op, OperatorStage& op_stage, RuntimeState* state);

private:
    struct InnerPipelineDriverContext {
        OperatorStage source_stage = OperatorStage::INIT;
        OperatorStage sink_stage = OperatorStage::INIT;
    };

    const int32_t _inner_dop;
    std::vector<InnerPipelineDriver> _inner_drivers;
    std::vector<InnerPipelineDriverContext> _driver_contexts;
    mutable std::atomic<int> _num_log_times;
};

class AdaptiveMultiSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AdaptiveMultiSourceOperatorFactory(int32_t id, int32_t plan_node_id, SourceOperatorFactoryPtr source_factory,
                                       OperatorFactoryPtr sink_factory);
    ~AdaptiveMultiSourceOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool could_local_shuffle() const override;
    TPartitionType::type partition_type() const override;

    AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }

private:
    SourceOperatorFactoryPtr _source_factory;
    OperatorFactoryPtr _sink_factory;
};

} // namespace starrocks::pipeline
