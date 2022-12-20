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

#include "exec/pipeline/pipeline.h"

#include "exec/pipeline/pipeline_driver.h"

namespace starrocks::pipeline {

void Pipeline::setup_profile_hierarchy(const DriverPtr& driver) {
    runtime_profile()->add_child(driver->runtime_profile(), true, nullptr);
    auto* dop_counter = ADD_COUNTER_SKIP_MERGE(runtime_profile(), "DegreeOfParallelism", TUnit::UNIT);
    COUNTER_SET(dop_counter, static_cast<int64_t>(source_operator_factory()->degree_of_parallelism()));
    auto* total_dop_counter = ADD_COUNTER(runtime_profile(), "TotalDegreeOfParallelism", TUnit::UNIT);
    COUNTER_SET(total_dop_counter, dop_counter->value());
    auto& operators = driver->operators();
    for (int32_t i = operators.size() - 1; i >= 0; --i) {
        auto& curr_op = operators[i];
        driver->runtime_profile()->add_child(curr_op->get_runtime_profile(), true, nullptr);
    }
}

} // namespace starrocks::pipeline