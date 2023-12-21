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

#include <atomic>
#include <memory>
#include <vector>

#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

class DriverExecutor;
class SourceOperatorFactory;

class PipelineEvent : public std::enable_shared_from_this<PipelineEvent> {
public:
    PipelineEvent() = default;
    virtual ~PipelineEvent() = default;

    virtual Status schedule(RuntimeState* state) { return Status::OK(); }

    Status finish(RuntimeState* state);

    Status finish_dependency(RuntimeState* state);

    void add_dependency(const std::shared_ptr<PipelineEvent>& event);

protected:
    size_t _num_dependencies{0};
    std::vector<std::weak_ptr<PipelineEvent>> _dependees;

    std::atomic<bool> _finished{false};
    std::atomic<size_t> _num_finished_dependencies{0};
};

class CollectStatsSourceInitializeEvent final : public PipelineEvent {
public:
    CollectStatsSourceInitializeEvent(DriverExecutor* const executor, SourceOperatorFactory* const leader_source_op,
                                      std::vector<Pipeline*> pipelines);

    ~CollectStatsSourceInitializeEvent() override = default;

    Status schedule(RuntimeState* state) override;

private:
    DriverExecutor* const _executor;

    const size_t _original_leader_dop;
    SourceOperatorFactory* const _leader_source_op;
    std::vector<Pipeline*> _pipelines;
};

} // namespace starrocks::pipeline