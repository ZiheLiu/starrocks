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

#include "exec/pipeline/adaptive/adaptive_multi_source_operator.h"

#include "exec/pipeline/adaptive/collect_stats_context.h"

namespace starrocks::pipeline {

AdaptiveMultiSourceOperator::AdaptiveMultiSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                         int32_t driver_sequence,
                                                         std::vector<InnerPipelineDriver>&& inner_drivers)
        : SourceOperator(factory, id, "adap_multi_" + inner_drivers[0].source->get_raw_name(), plan_node_id, false,
                         driver_sequence),
          _inner_dop(inner_drivers.size()),
          _inner_drivers(std::move(inner_drivers)),
          _driver_contexts(_inner_dop) {}

Status AdaptiveMultiSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    auto* dop_counter = ADD_COUNTER(_unique_metrics, "InnerDegreeOfParallelism", TUnit::UNIT);
    COUNTER_SET(dop_counter, static_cast<int64_t>(_inner_dop));

    for (auto& [source, sink] : _inner_drivers) {
        RETURN_IF_ERROR(source->prepare(state));
        RETURN_IF_ERROR(sink->prepare(state));

        _unique_metrics->add_child(source->runtime_profile(), true, nullptr);
        _unique_metrics->add_child(sink->runtime_profile(), true, nullptr);

        auto& [source_stage, sink_stage] = _driver_contexts[source->get_driver_sequence()];
        source_stage = OperatorStage::PREPARED;
        sink_stage = OperatorStage::PREPARED;
    }
    return Status::OK();
}

void AdaptiveMultiSourceOperator::close(RuntimeState* state) {
    for (auto& [source, sink] : _inner_drivers) {
        source->close(state);
        sink->close(state);
    }
    Operator::close(state);
}

Status AdaptiveMultiSourceOperator::set_finishing(RuntimeState* state) {
    for (auto& [source, sink] : _inner_drivers) {
        auto& [source_stage, sink_stage] = _driver_contexts[source->get_driver_sequence()];
        RETURN_IF_ERROR(_mark_operator_finishing(source, source_stage, state));
        RETURN_IF_ERROR(_mark_operator_finishing(sink, sink_stage, state));
    }
    return Status::OK();
}

Status AdaptiveMultiSourceOperator::set_finished(RuntimeState* state) {
    for (auto& [source, sink] : _inner_drivers) {
        auto& [source_stage, sink_stage] = _driver_contexts[source->get_driver_sequence()];
        RETURN_IF_ERROR(_mark_operator_finished(source, source_stage, state));
        RETURN_IF_ERROR(_mark_operator_finished(sink, sink_stage, state));
    }
    return Status::OK();
}

Status AdaptiveMultiSourceOperator::set_cancelled(RuntimeState* state) {
    for (auto& [source, sink] : _inner_drivers) {
        RETURN_IF_ERROR(source->set_cancelled(state));
        RETURN_IF_ERROR(sink->set_cancelled(state));
    }
    return Status::OK();
}

bool AdaptiveMultiSourceOperator::has_output() const {
    return std::ranges::any_of(_inner_drivers, [this](const auto& driver) {
        const auto& [source, sink] = driver;
        if (sink->is_finished() && _driver_contexts[sink->get_driver_sequence()].sink_stage < OperatorStage::FINISHED) {
            return true;
        }
        if (source->has_output() && sink->need_input()) {
            return true;
        }
        return false;
    });
}

bool AdaptiveMultiSourceOperator::is_finished() const {
    return std::ranges::all_of(_inner_drivers, [this](const auto& driver) {
        auto& [source, sink] = driver;
        bool source_res = source->is_finished();
        bool sink_res = sink->is_finished();
        bool res = source_res || sink_res;
        return res;
    });
}

StatusOr<ChunkPtr> AdaptiveMultiSourceOperator::pull_chunk(RuntimeState* state) {
    bool has_any_chunk = true;
    while (has_any_chunk) {
        has_any_chunk = false;
        for (int i = 0; i < _inner_dop; i++) {
            auto& [source, sink] = _inner_drivers[i];

            if (sink->is_finished()) {
                auto& [source_stage, sink_stage] = _driver_contexts[source->get_driver_sequence()];
                RETURN_IF_ERROR(_mark_operator_finished(source, source_stage, state));
                RETURN_IF_ERROR(_mark_operator_finished(sink, sink_stage, state));
                continue;
            }

            while (source->has_output() && sink->need_input()) {
                // TODO(lzh): deal with EosState.
                ASSIGN_OR_RETURN(auto chunk, source->pull_chunk(state));
                if (chunk != nullptr) {
                    has_any_chunk = true;
                    RETURN_IF_ERROR(sink->push_chunk(state, chunk));
                }
            }
        }
    }

    return nullptr;
}

Status AdaptiveMultiSourceOperator::_mark_operator_finishing(OperatorPtr& op, OperatorStage& op_stage,
                                                             RuntimeState* state) {
    if (op_stage >= OperatorStage::FINISHING) {
        return Status::OK();
    }
    op_stage = OperatorStage::FINISHING;

    VLOG_ROW << strings::Substitute("[AdaptiveMulti] set_finishing inner [op=$0]", op->get_name());

    return op->set_finishing(state);
}

Status AdaptiveMultiSourceOperator::_mark_operator_finished(OperatorPtr& op, OperatorStage& op_stage,
                                                            RuntimeState* state) {
    RETURN_IF_ERROR(_mark_operator_finishing(op, op_stage, state));
    if (op_stage >= OperatorStage::FINISHED) {
        return Status::OK();
    }
    op_stage = OperatorStage::FINISHED;

    VLOG_ROW << strings::Substitute("[AdaptiveMulti] set_finished inner [op=$0]", op->get_name());

    return op->set_finished(state);
}

AdaptiveMultiSourceOperatorFactory::AdaptiveMultiSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                       SourceOperatorFactoryPtr source_factory,
                                                                       OperatorFactoryPtr sink_factory)
        : SourceOperatorFactory(id, "adap_multi_" + source_factory->get_raw_name(), plan_node_id),
          _source_factory(std::move(source_factory)),
          _sink_factory(std::move(sink_factory)) {}

Status AdaptiveMultiSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));
    RETURN_IF_ERROR(_source_factory->prepare(state));
    RETURN_IF_ERROR(_sink_factory->prepare(state));
    return Status::OK();
}
void AdaptiveMultiSourceOperatorFactory::close(RuntimeState* state) {
    _source_factory->close(state);
    _sink_factory->close(state);
    SourceOperatorFactory::close(state);
}

OperatorPtr AdaptiveMultiSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    DCHECK_EQ(degree_of_parallelism, 1);

    const int32_t inner_dop = _source_factory->degree_of_parallelism();
    std::vector<AdaptiveMultiSourceOperator::InnerPipelineDriver> inner_drivers;
    inner_drivers.reserve(inner_dop);
    for (int i = 0; i < inner_dop; i++) {
        auto source = _source_factory->create(inner_dop, i);
        auto sink = _sink_factory->create(inner_dop, i);
        inner_drivers.emplace_back(
                AdaptiveMultiSourceOperator::InnerPipelineDriver{std::move(source), std::move(sink)});
    }
    return std::make_shared<AdaptiveMultiSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                         std::move(inner_drivers));
}

bool AdaptiveMultiSourceOperatorFactory::could_local_shuffle() const {
    return _source_factory->could_local_shuffle();
}

TPartitionType::type AdaptiveMultiSourceOperatorFactory::partition_type() const {
    return _source_factory->partition_type();
}

} // namespace starrocks::pipeline
