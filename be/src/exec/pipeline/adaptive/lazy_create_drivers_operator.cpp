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

#include "exec/pipeline/adaptive/lazy_create_drivers_operator.h"

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_executor.h"

namespace starrocks::pipeline {

/// LazyCreateDriversOperator.
LazyCreateDriversOperator::LazyCreateDriversOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                     const int32_t driver_sequence,
                                                     const std::vector<Pipelines>& unready_pipeline_groups)
        : SourceOperator(factory, id, "lazy_create_drivers", plan_node_id, driver_sequence) {
    LOG(WARNING) << "[ADAPTIVE] [unready_pipeline_groups=" << unready_pipeline_groups.size() << "] ";
    for (const auto& pipe_group : unready_pipeline_groups) {
        PipelineItems pipe_item_group;
        pipe_item_group.reserve(pipe_group.size());
        for (const auto& pipe : pipe_group) {
            pipe_item_group.emplace_back(pipe, pipe->source_operator_factory()->degree_of_parallelism());
        }
        _unready_pipeline_groups.emplace_back(std::move(pipe_item_group));
    }
}

bool LazyCreateDriversOperator::has_output() const {
    return std::any_of(_unready_pipeline_groups.begin(), _unready_pipeline_groups.end(),
                       [](const auto& pipeline_group) {
                           auto* source_op_of_first_pipeline = pipeline_group[0].pipeline->source_operator_factory();
                           return source_op_of_first_pipeline->state() == SourceOperatorFactory::State::READY;
                       });
}
bool LazyCreateDriversOperator::need_input() const {
    return false;
}
bool LazyCreateDriversOperator::is_finished() const {
    return _unready_pipeline_groups.empty();
}

Status LazyCreateDriversOperator::set_finishing(RuntimeState* state) {
    return Status::OK();
}
Status LazyCreateDriversOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return Status::InternalError("Not support");
}

StatusOr<vectorized::ChunkPtr> LazyCreateDriversOperator::pull_chunk(RuntimeState* state) {
    auto* query_ctx = state->query_ctx();
    auto* fragment_ctx = state->fragment_ctx();

    Drivers drivers;
    auto pipe_group_it = _unready_pipeline_groups.begin();
    while (pipe_group_it != _unready_pipeline_groups.end()) {
        auto& pipe_item_group = *pipe_group_it;
        auto* source_op_of_first_pipeline = pipe_item_group[0].pipeline->source_operator_factory();
        if (source_op_of_first_pipeline->state() != SourceOperatorFactory::State::READY) {
            pipe_group_it++;
            continue;
        }

        // TODO: what about local shuffle? This will limit dop of LocalShuffleSource to too small.
        size_t max_dop = source_op_of_first_pipeline->degree_of_parallelism();

        LOG(WARNING) << "[ADAPTIVE] pipe_item_group is ready "
                     << "[max_odp=" << max_dop << "] ";

        for (const auto& pipe_item : pipe_item_group) {
            auto& [pipe, original_dop] = pipe_item;
            pipe->source_operator_factory()->set_max_dop(max_dop);
            size_t cur_dop = pipe->source_operator_factory()->degree_of_parallelism();
            for (size_t i = 0; i < cur_dop; ++i) {
                auto&& operators = pipe->create_operators(cur_dop, i);
                DriverPtr driver = std::make_shared<PipelineDriver>(std::move(operators), query_ctx, fragment_ctx,
                                                                    fragment_ctx->next_driver_id());
                pipe->setup_profile_hierarchy(driver);
                drivers.emplace_back(std::move(driver));
            }

            bool res = fragment_ctx->count_down_drivers(original_dop - cur_dop);
            LOG(WARNING) << "[ADAPTIVE] count_down_drivers "
                         << "[cur_do=p" << cur_dop << "] "
                         << "[original_dop=" << original_dop << "] "
                         << "[res=" << res << "]";
        }

        _unready_pipeline_groups.erase(pipe_group_it++);
    }

    fragment_ctx->add_lazy_drivers(drivers);
    query_ctx->query_trace()->register_drivers(fragment_ctx->fragment_instance_id(), drivers);
    if (fragment_ctx->workgroup() != nullptr) {
        for (const auto& driver : drivers) {
            driver->set_workgroup(fragment_ctx->workgroup());
        }
    }

    for (const auto& driver : drivers) {
        RETURN_IF_ERROR(driver->prepare(state));
    }

    auto* executor = fragment_ctx->enable_resource_group() ? state->exec_env()->wg_driver_executor()
                                                           : state->exec_env()->driver_executor();
    for (const auto& driver : drivers) {
        DCHECK(!fragment_ctx->enable_resource_group() || driver->workgroup() != nullptr);
        executor->submit(driver.get());
    }

    return nullptr;
}

/// LazyCreateDriversOperatorFactory.
LazyCreateDriversOperatorFactory::LazyCreateDriversOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                   std::vector<Pipelines>&& unready_pipeline_groups)
        : SourceOperatorFactory(id, "lazy_create_drivers", plan_node_id),
          _unready_pipeline_groups(std::move(unready_pipeline_groups)) {}

} // namespace starrocks::pipeline
