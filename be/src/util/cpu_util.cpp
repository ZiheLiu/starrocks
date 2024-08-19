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

#include "util/cpu_util.h"

#include <fmt/format.h>

#include "util/thread.h"

namespace starrocks {

Status CpuUtil::bind_cpus(int64_t tid, pthread_t thread, const std::vector<size_t>& cpuids) {
    if (cpuids.empty()) {
        return Status::OK();
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (const auto cpu_id : cpuids) {
        CPU_SET(cpu_id, &cpuset);
    }

    const int res = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (res == 0) {
        LOG(INFO) << "bind cpus [tid=" << tid << "] [thread=" << thread << "] [cpuids=" << to_string(cpuids) << "]";
        return Status::OK();
    }

    const std::string error_msg = fmt::format("failed to bind cpus [tid={}] [cpuids={}] [error_code={}] [error={}]",
                                              pthread_self(), to_string(cpuids), res, std::strerror(res));
    LOG(WARNING) << error_msg;
    return Status::InternalError(error_msg);
}

Status CpuUtil::bind_cpus(const std::vector<size_t>& cpuids) {
    return bind_cpus(Thread::current_thread_id(), pthread_self(), cpuids);
}

std::string CpuUtil::to_string(const CpuIds& cpuids) {
    std::string result = "(";
    for (size_t i = 0; i < cpuids.size(); i++) {
        if (i != 0) {
            result += ",";
        }
        result += std::to_string(cpuids[i]);
    }
    result += ")";
    return result;
}

} // namespace starrocks
