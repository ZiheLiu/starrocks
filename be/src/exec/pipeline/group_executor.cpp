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

#include "exec/pipeline/group_executor.h"

#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/workgroup/cgroup_ops.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"

namespace starrocks::pipeline {

GroupExecutor::GroupExecutor(workgroup::CGroupOps* cgroup_ops, int64_t max_driver_threads, int64_t max_scan_threads,
                             int64_t max_connector_scan_threads)
        : _cgroup_ops(cgroup_ops),
          _max_driver_threads(max_driver_threads),
          _max_scan_threads(max_scan_threads),
          _max_connector_scan_threads(max_connector_scan_threads) {}

GroupExecutor::~GroupExecutor() = default;

DriverExecutor* GroupExecutor::get_or_create_driver_executor(const workgroup::WorkGroup& wg) {
    std::lock_guard guard(_lock);

    auto& ctx = _get_or_create_context_inlock(wg);
    if (ctx.driver_executor != nullptr) {
        return ctx.driver_executor.get();
    }

    std::unique_ptr<ThreadPool> wg_driver_executor_thread_pool;
    // TODO(lzh): handle error
    (void)ThreadPoolBuilder("pip_exe" + std::to_string(wg.id()))
            .set_min_threads(0)
            .set_max_threads(_max_driver_threads)
            .set_max_queue_size(1000)
            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
            .set_wgid(wg.id())
            .build(&wg_driver_executor_thread_pool);
    ctx.driver_executor =
            std::make_unique<GlobalDriverExecutor>("pip_exe", std::move(wg_driver_executor_thread_pool), false);
    ctx.driver_executor->initialize(_max_driver_threads);

    return ctx.driver_executor.get();
}

workgroup::ScanExecutor* GroupExecutor::get_or_create_scan_executor(const workgroup::WorkGroup& wg) {
    std::lock_guard guard(_lock);

    auto& ctx = _get_or_create_context_inlock(wg);
    if (ctx.scan_executor != nullptr) {
        return ctx.scan_executor.get();
    }

    std::unique_ptr<ThreadPool> scan_worker_thread_pool_with_workgroup;
    // TODO(lzh): handle error
    (void)ThreadPoolBuilder("scan_io" + std::to_string(wg.id()))
            .set_min_threads(0)
            .set_max_threads(_max_scan_threads)
            .set_max_queue_size(1000)
            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
            .set_wgid(wg.id())
            .build(&scan_worker_thread_pool_with_workgroup);
    ctx.scan_executor = std::make_unique<workgroup::ScanExecutor>(
            std::move(scan_worker_thread_pool_with_workgroup),
            std::make_unique<workgroup::WorkGroupScanTaskQueue>(
                    workgroup::WorkGroupScanTaskQueue::SchedEntityType::OLAP));
    ctx.scan_executor->initialize(_max_scan_threads);

    return ctx.scan_executor.get();
}

workgroup::ScanExecutor* GroupExecutor::get_or_create_connector_scan_executor(const workgroup::WorkGroup& wg) {
    std::lock_guard guard(_lock);

    auto& ctx = _get_or_create_context_inlock(wg);
    if (ctx.connector_scan_executor != nullptr) {
        return ctx.connector_scan_executor.get();
    }

    std::unique_ptr<ThreadPool> connector_scan_worker_thread_pool_with_workgroup;
    // TODO(lzh): handle error
    (void)ThreadPoolBuilder("con_scan_io" + std::to_string(wg.id()))
            .set_min_threads(0)
            .set_max_threads(_max_connector_scan_threads)
            .set_max_queue_size(1000)
            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
            .set_wgid(wg.id())
            .build(&connector_scan_worker_thread_pool_with_workgroup);
    ctx.connector_scan_executor = std::make_unique<workgroup::ScanExecutor>(
            std::move(connector_scan_worker_thread_pool_with_workgroup),
            std::make_unique<workgroup::WorkGroupScanTaskQueue>(
                    workgroup::WorkGroupScanTaskQueue::SchedEntityType::CONNECTOR));
    ctx.connector_scan_executor->initialize(_max_connector_scan_threads);

    return ctx.connector_scan_executor.get();
}

void GroupExecutor::close() {
    std::lock_guard guard(_lock);
    for (auto& [_, ctx] : _wg_id_to_context) {
        ctx.driver_executor->close();
    }
    _wg_id_to_context.clear();
}

GroupContext& GroupExecutor::_get_or_create_context_inlock(const workgroup::WorkGroup& wg) {
    auto iter = _wg_id_to_context.find(wg.id());
    if (iter == _wg_id_to_context.end()) {
        iter = _wg_id_to_context
                       .emplace(wg.id(), GroupContext{static_cast<int64_t>(wg.cpu_limit()),
                                                      static_cast<int64_t>(wg.max_cpu_cores())})
                       .first;

        // TODO(lzh): handle error
        _cgroup_ops->create_group(wg.id());
        _cgroup_ops->set_cpu_weight(wg.id(), wg.cpu_limit());
        if (wg.max_cpu_cores() > 0) {
            _cgroup_ops->set_max_cpu_cores(wg.id(), wg.max_cpu_cores());
        }
    }

    return iter->second;
}

} // namespace starrocks::pipeline
