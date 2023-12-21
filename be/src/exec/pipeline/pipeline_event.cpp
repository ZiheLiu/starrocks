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

#include "exec/pipeline/pipeline_event.h"

#include <utility>

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

Status PipelineEvent::finish(RuntimeState* state) {
    if (_finished) {
        return Status::OK();
    }
    _finished = true;

    for (auto& dependee_entry : _dependees) {
        auto dependee = dependee_entry.lock();
        if (dependee == nullptr) {
            continue;
        }
        RETURN_IF_ERROR(dependee->finish_dependency(state));
    }

    return Status::OK();
}

Status PipelineEvent::finish_dependency(RuntimeState* state) {
    if (_num_finished_dependencies.fetch_add(1) + 1 == _num_dependencies) {
        RETURN_IF_ERROR(schedule(state));
    }

    return Status::OK();
}

void PipelineEvent::add_dependency(const std::shared_ptr<PipelineEvent>& event) {
    _num_dependencies++;
    event->_dependees.emplace_back(shared_from_this());
}

CollectStatsSourceInitializeEvent::CollectStatsSourceInitializeEvent(DriverExecutor* const executor,
                                                                     SourceOperatorFactory* const leader_source_op,
                                                                     std::vector<Pipeline*> pipelines)
        : _executor(executor),
          _original_leader_dop(leader_source_op->degree_of_parallelism()),
          _leader_source_op(leader_source_op),
          _pipelines(std::move(pipelines)) {}

Status CollectStatsSourceInitializeEvent::schedule(RuntimeState* state) {
    _leader_source_op->adjust_dop();
    size_t leader_dop = _leader_source_op->degree_of_parallelism();
    bool adjust_dop = leader_dop != _original_leader_dop;

    for (auto& pipeline : _pipelines) {
        if (adjust_dop) {
            pipeline->source_operator_factory()->adjust_max_dop(leader_dop);
        }
        pipeline->instantiate_drivers(state);
    }

    for (const auto& pipeline : _pipelines) {
        for (const auto& driver : pipeline->drivers()) {
            RETURN_IF_ERROR(driver->prepare(state));
        }
    }

    for (const auto& pipeline : _pipelines) {
        for (const auto& driver : pipeline->drivers()) {
            _executor->submit(driver.get());
        }
    }

    return Status::OK();
}

} // namespace starrocks::pipeline