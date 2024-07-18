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

#include <memory>

#include "common/status.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {

class CGroupOps;
using CGroupOpsPtr = std::unique_ptr<CGroupOps>;

class CGroupOps {
public:
    CGroupOps() = default;
    virtual ~CGroupOps() = default;

    virtual Status init() = 0;

    virtual Status create_group(WorkGroupId wgid) = 0;

    virtual Status set_cpu_weight(WorkGroupId wgid, int64_t cpu_weight) = 0;
    virtual Status set_max_cpu_cores(WorkGroupId wgid, int64_t max_cpu_cores) = 0;

    virtual Status attach(WorkGroupId wgid, int64_t tid) = 0;

    static CGroupOpsPtr create(int num_cpu_cores);
};

} // namespace starrocks::workgroup
