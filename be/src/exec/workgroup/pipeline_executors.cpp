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
#include "exec/workgroup/pipeline_executors.h"

#include <utility>

#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "util/threadpool.h"

namespace starrocks::workgroup {

// ------------------------------------------------------------------------------------
// PipelineExecutorsConfig
// ------------------------------------------------------------------------------------

PipelineExecutorsConfig::PipelineExecutorsConfig(uint32_t num_total_cores, uint32_t num_total_driver_threads,
                                                 uint32_t num_total_scan_threads,
                                                 uint32_t num_total_connector_scan_threads,
                                                 CpuUtil::CpuIds total_cpuids, bool enable_bind_cpus)
        : num_total_cores(num_total_cores),
          num_total_driver_threads(num_total_driver_threads),
          num_total_scan_threads(num_total_scan_threads),
          num_total_connector_scan_threads(num_total_connector_scan_threads),
          total_cpuids(std::move(total_cpuids)),
          enable_bind_cpus(enable_bind_cpus) {}

std::string PipelineExecutorsConfig::to_string() const {
    return fmt::format(
            "([num_total_cores={}] [num_total_driver_threads={}] [num_total_scan_threads={}] "
            "[num_total_connector_scan_threads={}] [enable_bind_cpus={}])",
            num_total_cores, num_total_driver_threads, num_total_scan_threads, num_total_connector_scan_threads,
            enable_bind_cpus);
}

// ------------------------------------------------------------------------------------
// PipelineExecutors
// ------------------------------------------------------------------------------------

PipelineExecutors::PipelineExecutors(const PipelineExecutorsConfig& conf, std::string name, CpuUtil::CpuIds cpuids)
        : _conf(conf), _name(std::move(name)), _cpuids(std::move(cpuids)) {}

PipelineExecutors::~PipelineExecutors() {
    close();
}

std::string PipelineExecutors::to_string() const {
    return fmt::format(
            "([name={}] [num_driver_threads={}] [num_scan_threads={}] [num_connector_scan_threads={}] [cpuids={}] "
            "[conf={}])",
            _name, num_driver_threads(), num_scan_threads(), num_connector_scan_threads(), CpuUtil::to_string(_cpuids),
            _conf.to_string());
}

Status PipelineExecutors::start() {
    if (_stage >= Stage::STARTED) {
        return Status::OK();
    }
    _stage = Stage::STARTED;

    const CpuUtil::CpuIds empty_cpuids;
    const auto& cpuids = _conf.enable_bind_cpus ? _cpuids : empty_cpuids;

    std::unique_ptr<ThreadPool> driver_executor_thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("pip_exec_" + _name) // pipeline executor for workgroup
                            .set_min_threads(0)
                            .set_max_threads(num_driver_threads())
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .set_cpuids(cpuids)
                            .build(&driver_executor_thread_pool));
    _driver_executor = std::make_unique<pipeline::GlobalDriverExecutor>(_name, std::move(driver_executor_thread_pool),
                                                                        true, cpuids);
    _driver_executor->initialize(num_driver_threads());

    std::unique_ptr<ThreadPool> scan_thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("pip_scan_" + _name)
                            .set_min_threads(0)
                            .set_max_threads(num_scan_threads())
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .set_cpuids(cpuids)
                            .build(&scan_thread_pool));
    _scan_executor = std::make_unique<ScanExecutor>(
            std::move(scan_thread_pool),
            std::make_unique<WorkGroupScanTaskQueue>(ScanSchedEntityType::OLAP));
    _scan_executor->initialize(num_scan_threads());

    std::unique_ptr<ThreadPool> connector_scan_thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("pip_con_scan_" + _name)
                            .set_min_threads(0)
                            .set_max_threads(num_connector_scan_threads())
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .set_cpuids(cpuids)
                            .build(&connector_scan_thread_pool));
    _connector_scan_executor = std::make_unique<ScanExecutor>(
            std::move(connector_scan_thread_pool),
            std::make_unique<WorkGroupScanTaskQueue>(ScanSchedEntityType::CONNECTOR));
    _connector_scan_executor->initialize(num_connector_scan_threads());

    LOG(INFO) << "[WORKGROUP] start executors " << to_string();

    return Status::OK();
}

void PipelineExecutors::close() {
    if (_stage >= Stage::CLOSED) {
        return;
    }
    _stage = Stage::CLOSED;

    if (_driver_executor) {
        _driver_executor->close();
    }

    if (_scan_executor) {
        _scan_executor->close();
    }

    if (_connector_scan_executor) {
        _connector_scan_executor->close();
    }

    LOG(INFO) << "[WORKGROUP] close executors " << to_string();
}

void PipelineExecutors::change_cpus(CpuUtil::CpuIds cpuids) {
    const size_t num_pre_cpus = _cpuids.size();
    _cpuids = std::move(cpuids);
    if (num_pre_cpus == _cpuids.size()) {
        return;
    }

    notify_config_changed();
}

void PipelineExecutors::notify_num_total_connector_scan_threads_changed() const {
    _connector_scan_executor->change_num_threads(num_connector_scan_threads());
    LOG(INFO) << "[WORKGROUP] change num_total_connector_scan_threads of executors " << to_string();
}

void PipelineExecutors::notify_config_changed() const {
    const auto& cpuids = _conf.enable_bind_cpus ? _cpuids : _conf.total_cpuids;

    _driver_executor->bind_cpus(cpuids);
    _driver_executor->change_num_threads(num_driver_threads());

    _scan_executor->bind_cpus(cpuids);
    _scan_executor->change_num_threads(num_scan_threads());

    _connector_scan_executor->bind_cpus(cpuids);
    _connector_scan_executor->change_num_threads(num_connector_scan_threads());

    LOG(INFO) << "[WORKGROUP] change cpus and threads of executors " << to_string();
}

} // namespace starrocks::workgroup
