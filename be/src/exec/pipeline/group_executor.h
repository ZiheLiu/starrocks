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

#include <unordered_map>

#include "common/statusor.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::pipeline {

struct GroupContext {
    const int64_t cpu_weight;
    const int64_t max_cpu_cores;
    std::unique_ptr<DriverExecutor> driver_executor = nullptr;
    std::unique_ptr<workgroup::ScanExecutor> scan_executor = nullptr;
    std::unique_ptr<workgroup::ScanExecutor> connector_scan_executor = nullptr;
};

class GroupExecutor {
public:
    GroupExecutor(workgroup::CGroupOps* cgroup_ops, int64_t max_driver_threads, int64_t max_scan_threads,
                  int64_t max_connector_scan_threads);
    ~GroupExecutor();

    StatusOr<DriverExecutor*> get_or_create_driver_executor(const workgroup::WorkGroup& wg);
    StatusOr<workgroup::ScanExecutor*> get_or_create_scan_executor(const workgroup::WorkGroup& wg);
    StatusOr<workgroup::ScanExecutor*> get_or_create_connector_scan_executor(const workgroup::WorkGroup& wg);

    void close();

private:
    GroupContext& _get_or_create_context_inlock(const workgroup::WorkGroup& wg);

    workgroup::CGroupOps* const _cgroup_ops;

    std::mutex _lock;
    std::unordered_map<int64_t, GroupContext> _wg_id_to_context;

    const int64_t _max_driver_threads;
    const int64_t _max_scan_threads;
    const int64_t _max_connector_scan_threads;
};

} // namespace starrocks::pipeline
