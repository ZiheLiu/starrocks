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

#include "exec/workgroup/pipeline_executors_manager.h"

#include "work_group.h"

namespace starrocks::workgroup {

ExecutorsManager::ExecutorsManager(WorkGroupManager* parent, PipelineExecutorsConfig conf)
        : _parent(parent), _conf(std::move(conf)) {
    for (const auto& cpuid : _conf.total_cpuids) {
        _cpuid_to_wg[cpuid] = nullptr;
    }
}

void ExecutorsManager::close() const {
    for_each_executors([](auto& executors) { executors.close(); });
}

Status ExecutorsManager::start_common_executors() {
    _common_executors = std::make_unique<PipelineExecutors>(_conf, "com", CpuUtil::CpuIds{});
    return _common_executors->start();
}

void ExecutorsManager::update_common_executors() const {
    _common_executors->change_cpus(get_cpuids_of_workgroup(nullptr));
}

void ExecutorsManager::assign_cpuids_to_workgroup(WorkGroup* wg) {
    const size_t num_target_cpuids = wg->dedicated_cpu_cores();
    size_t num_assigned_cpuids = 0;
    CpuUtil::CpuIds cpuids;
    for (auto& [cpuid, cur_wg] : _cpuid_to_wg) {
        if (num_assigned_cpuids >= num_target_cpuids) {
            break;
        }

        if (cur_wg == nullptr) {
            cur_wg = wg;
            num_assigned_cpuids++;
            cpuids.emplace_back(cpuid);
        }
    }

    LOG(INFO) << "[WORKGROUP] assign cpuids to workgroup "
              << "[workgroup=" << wg->to_string() << "] "
              << "[cpuids=" << CpuUtil::to_string(cpuids) << "] ";
}

void ExecutorsManager::reclaim_cpuids_from_worgroup(WorkGroup* wg) {
    CpuUtil::CpuIds cpuids;
    for (auto& [cpuid, cur_wg] : _cpuid_to_wg) {
        if (cur_wg == wg) {
            cur_wg = nullptr;
            cpuids.emplace_back(cpuid);
        }
    }

    LOG(INFO) << "[WORKGROUP] reclaim cpuids from workgroup "
              << "[workgroup=" << wg->to_string() << "] "
              << "[cpuids=" << CpuUtil::to_string(cpuids) << "] ";
}

CpuUtil::CpuIds ExecutorsManager::get_cpuids_of_workgroup(WorkGroup* wg) const {
    CpuUtil::CpuIds cpuids;
    for (const auto& [cpuid, cur_wg] : _cpuid_to_wg) {
        if (cur_wg == wg) {
            cpuids.emplace_back(cpuid);
        }
    }
    return cpuids;
}

PipelineExecutors* ExecutorsManager::create_and_assign_executors(WorkGroup* wg) const {
    if (wg->dedicated_cpu_cores() == 0) {
        LOG(INFO) << "[WORKGROUP] assign common executors to workgroup "
                  << "[workgroup=" << wg->to_string() << "] ";
        wg->set_executors(_common_executors.get());
        return _common_executors.get();
    }

    auto executors = std::make_unique<PipelineExecutors>(_conf, std::to_string(wg->id()), get_cpuids_of_workgroup(wg));
    if (const Status status = executors->start(); !status.ok()) {
        LOG(WARNING) << "[WORKGROUP] failed to start executors for workgroup "
                     << "[workgroup=" << wg->to_string() << "] "
                     << "[conf=" << _conf.to_string() << "] "
                     << "[cpuids=" << CpuUtil::to_string(get_cpuids_of_workgroup(wg)) << "] "
                     << "[status=" << status << "]";
        LOG(INFO) << "[WORKGROUP] assign common executors to workgroup "
                  << "[workgroup=" << wg->to_string() << "] ";
        executors->close();
        wg->set_executors(_common_executors.get());
        return _common_executors.get();
    }

    LOG(INFO) << "[WORKGROUP] assign dedicated executors to workgroup "
              << "[workgroup=" << wg->to_string() << "] ";
    wg->set_dedicated_executors(std::move(executors));
    return wg->executors();
}

void ExecutorsManager::change_num_connector_scan_threads(uint32_t num_connector_scan_threads) {
    const auto prev_val = _conf.num_total_connector_scan_threads;
    _conf.num_total_connector_scan_threads = num_connector_scan_threads;
    if (_conf.num_total_connector_scan_threads == prev_val) {
        return;
    }

    for_each_executors([](const auto& executors) { executors.notify_num_total_connector_scan_threads_changed(); });
}

void ExecutorsManager::change_enable_resource_group_cpu_borrowing(bool val) {
    const auto prev_val = _conf.enable_bind_cpus;
    _conf.enable_bind_cpus = !val && !CpuInfo::is_cgroup_without_cpuset();
    if (_conf.enable_bind_cpus == prev_val) {
        return;
    }

    for_each_executors([](const auto& executors) { executors.notify_config_changed(); });
}

void ExecutorsManager::for_each_executors(const ExecutorsConsumer& consumer) const {
    for (const auto& [_, wg] : _parent->_workgroups) {
        if (wg != nullptr && wg->dedicated_executors() != nullptr) {
            consumer(*wg->dedicated_executors());
        }
    }
    if (_common_executors) {
        consumer(*_common_executors);
    }
}

} // namespace starrocks::workgroup
