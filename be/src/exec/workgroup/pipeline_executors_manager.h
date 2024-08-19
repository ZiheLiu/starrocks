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

#include <cstdint>

#include "exec/workgroup/pipeline_executors.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/cpu_util.h"

namespace starrocks::workgroup {

/// All the methods need to be protected by the `_parent->_mutex` outside by callers.
class ExecutorsManager {
public:
    ExecutorsManager(WorkGroupManager* parent, PipelineExecutorsConfig conf);

    void close() const;

    Status start_common_executors();
    void update_common_executors() const;
    PipelineExecutors* common_executors() const { return _common_executors.get(); }

    void assign_cpuids_to_workgroup(WorkGroup* wg);
    void reclaim_cpuids_from_worgroup(WorkGroup* wg);
    CpuUtil::CpuIds get_cpuids_of_workgroup(WorkGroup* wg) const;

    PipelineExecutors* create_and_assign_executors(WorkGroup* wg) const;

    void change_num_connector_scan_threads(uint32_t num_connector_scan_threads);
    void change_enable_resource_group_cpu_borrowing(bool val);

    using ExecutorsConsumer = std::function<void(PipelineExecutors&)>;
    void for_each_executors(const ExecutorsConsumer& consumer) const;

private:
    WorkGroupManager* const _parent;
    PipelineExecutorsConfig _conf;
    std::unordered_map<CpuUtil::CpuId, WorkGroup*> _cpuid_to_wg;
    std::unique_ptr<PipelineExecutors> _common_executors;
};

} // namespace starrocks::workgroup
