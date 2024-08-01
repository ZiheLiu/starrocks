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

#include "exec/workgroup/bandwidth_manager.h"

#include "exec/workgroup/work_group.h"

namespace starrocks::workgroup {

void BandwidthManager::start() {
    DCHECK(this->_polling_thread.get() == nullptr);
    auto status = Thread::create(
            "pipeline", "bw", [this]() { _run_internal(); }, &this->_polling_thread);
    if (!status.ok()) {
        LOG(FATAL) << "Fail to create BandwidthManager: error=" << status.to_string();
    }
    while (!this->_is_polling_thread_initialized.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void BandwidthManager::shutdown() {
    if (!this->_is_shutdown.load() && _polling_thread.get() != nullptr) {
        this->_is_shutdown.store(true, std::memory_order_release);
        _cv.notify_one();
        _polling_thread->join();
    }
}

bool BandwidthManager::is_throlled(WorkGroup* wg) const {
    if (wg->is_sq_wg()) {
        return false;
    }
    return _usage_ns >= _quota_ns();
}

bool BandwidthManager::is_throlled(WorkGroup* wg, int64_t delta_ns) {
    if (wg->is_sq_wg()) {
        return false;
    }

    int64_t old_usage_ns;
    int64_t new_usage_ns;
    do {
        old_usage_ns = _usage_ns.load(std::memory_order::relaxed);
        new_usage_ns = old_usage_ns + delta_ns;
    } while (!_usage_ns.compare_exchange_strong(old_usage_ns, new_usage_ns));

    // LOG(WARNING) << "[TEST] BandwidthManager::is_throlled() "
    //              << "[old_usage_ns=" << old_usage_ns << "] "
    //              << "[new_usage_ns=" << new_usage_ns << "] "
    //              << "[delta_ns=" << delta_ns << "] "
    //              << "[quota_ns=" << _quota_ns() << "] "
    //              << "[is_throlled=" << (new_usage_ns >= _quota_ns()) << "] ";

    return new_usage_ns >= _quota_ns();
}

void BandwidthManager::update_statistics(WorkGroup* wg, int64_t delta_ns) {
    if (!is_throlled(wg, delta_ns)) {
        return;
    }
    std::lock_guard lock(_mutex);
    if (is_throlled(wg)) {
        wg->set_throlled(true);
        _wgs.emplace(wg);
    }
}

bool BandwidthManager::try_add_task(ScanTaskQueue* ready_queue, ScanTask& task) {
    auto* wg = task.workgroup;
    if (!is_throlled(wg)) {
        return false;
    }

    std::lock_guard lock(_mutex);
    if (!is_throlled(wg)) {
        return false;
    }

    wg->set_throlled(true);
    _wgs.emplace(wg);
    _scan_tasks[ready_queue].emplace_back(std::move(task));
    return true;
}

std::vector<ScanTask> BandwidthManager::try_add_task(ScanTaskQueue* ready_queue, std::vector<ScanTask>& tasks) {
    std::vector<ScanTask> unthrolled_tasks;
    std::vector<ScanTask> throlled_tasks;
    for (auto& task : tasks) {
        if (is_throlled(task.workgroup())) {
            throlled_tasks.emplace_back(std::move(task));
        } else {
            unthrolled_tasks.emplace_back(std::move(task));
        }
    }

    if (!throlled_tasks.empty()) {
        std::lock_guard lock(_mutex);

        for (auto& task : throlled_tasks) {
            if (is_throlled(task.workgroup())) {
                auto* wg = task.workgroup();
                wg->set_throlled(true);
                _wgs.emplace(wg);
                _scan_tasks[ready_queue].emplace_back(std::move(task));
            } else {
                unthrolled_tasks.emplace_back(std::move(task));
            }
        }
    }

    return unthrolled_tasks;
}

bool BandwidthManager::try_add_task(pipeline::DriverQueue* ready_queue, pipeline::DriverRawPtr task) {
    auto* wg = task->workgroup();
    if (!is_throlled(wg)) {
        return false;
    }

    std::lock_guard lock(_mutex);
    if (!is_throlled(wg)) {
        return false;
    }

    wg->set_throlled(true);
    _wgs.emplace(wg);
    _driver_tasks[ready_queue].emplace_back(std::move(task));
    return true;
}

std::vector<pipeline::DriverRawPtr> BandwidthManager::try_add_task(pipeline::DriverQueue* ready_queue,
                                                                   const std::vector<pipeline::DriverRawPtr>& tasks) {
    std::vector<pipeline::DriverRawPtr> unthrolled_tasks;
    std::vector<pipeline::DriverRawPtr> throlled_tasks;
    for (auto* task : tasks) {
        if (is_throlled(task->workgroup())) {
            throlled_tasks.emplace_back(task);
        } else {
            unthrolled_tasks.emplace_back(task);
        }
    }

    if (!throlled_tasks.empty()) {
        std::lock_guard lock(_mutex);

        for (auto* task : throlled_tasks) {
            if (is_throlled(task->workgroup())) {
                auto* wg = task->workgroup();
                wg->set_throlled(true);
                _wgs.emplace(wg);
                _driver_tasks[ready_queue].emplace_back(std::move(task));
            } else {
                unthrolled_tasks.emplace_back(task);
            }
        }
    }

    return unthrolled_tasks;
}

void BandwidthManager::_run_internal() {
    _is_polling_thread_initialized.store(true, std::memory_order_release);

    while (!_is_shutdown.load(std::memory_order_acquire)) {
        const int64_t cur_ns = MonotonicNanos();

        const int64_t quota_ns = _quota_ns();
        int64_t old_usage_ns;
        int64_t new_usage_ns;
        do {
            old_usage_ns = _usage_ns.load(std::memory_order::relaxed);
            if (old_usage_ns <= quota_ns) {
                new_usage_ns = 0;
            } else if (old_usage_ns < 2 * quota_ns) {
                new_usage_ns = old_usage_ns - quota_ns;
            } else {
                new_usage_ns = quota_ns;
            }
        } while (!_usage_ns.compare_exchange_strong(old_usage_ns, new_usage_ns));

        LOG(WARNING) << "[TEST] BandwidthManager::_run_internal() "
                     << "[old_usage_ns=" << old_usage_ns << "] "
                     << "[new_usage_ns=" << new_usage_ns << "] "
                     << "[quota_ns=" << quota_ns << "]";

        std::unique_lock lock(_mutex);

        if (_usage_ns < quota_ns) {
            for (auto* wg : _wgs) {
                wg->set_throlled(false);
            }

            _wgs.clear();

            std::unordered_map<ScanTaskQueue*, std::vector<ScanTask>> scan_tasks;
            std::unordered_map<pipeline::DriverQueue*, std::vector<pipeline::DriverRawPtr>> driver_tasks;
            _driver_tasks.swap(driver_tasks);
            _scan_tasks.swap(scan_tasks);

            {
                lock.unlock();
                DeferOp defer_lock([&lock] { lock.lock(); });

                for (auto& [ready_queue, tasks] : driver_tasks) {
                    ready_queue->put_back(tasks);
                }
                for (auto& [ready_queue, tasks] : scan_tasks) {
                    ready_queue->force_put(std::move(tasks));
                }
            }
        }

        const auto end_ns = cur_ns + PERIOD_NS;
        _end_ns = end_ns;
        const int64_t sleep_ns = std::clamp(end_ns - MonotonicNanos(), 1L, PERIOD_NS);
        _cv.wait_for(lock, std::chrono::nanoseconds(sleep_ns));
    }
}

int64_t BandwidthManager::_quota_ns() {
    return PERIOD_NS * WorkGroupManager::instance()->normal_workgroup_cpu_hard_limit();
}

} // namespace starrocks::workgroup
