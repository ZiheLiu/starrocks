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

#include "pipeline_driver_poller.h"

#include <chrono>

namespace starrocks::pipeline {

void PipelineDriverPoller::start() {
    for (int i = 0; i < POLLER_NUM; ++i) {
        auto status = Thread::create(
                "pipeline", "pipeline_poller", [this, i]() { run_internal(i); },
                &this->_info_per_thread[i].polling_thread);
        if (!status.ok()) {
            LOG(FATAL) << "Fail to create PipelineDriverPoller: error=" << status.to_string();
        }
    }

    while (this->_is_polling_thread_initialized.load(std::memory_order_acquire) < POLLER_NUM) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void PipelineDriverPoller::shutdown() {
    if (!this->_is_shutdown.load()) {
        this->_is_shutdown.store(true, std::memory_order_release);
        for (int i = 0; i < POLLER_NUM; ++i) {
            _info_per_thread[i].cond.notify_one();
            _info_per_thread[i].polling_thread->join();
        }
    }
}

void PipelineDriverPoller::run_internal(int32_t poller_id) {
    this->_is_polling_thread_initialized.fetch_add(1, std::memory_order_release);

    auto& [global_mutex, cond, blocked_drivers, local_mutex, local_blocked_drivers, _] = _info_per_thread[poller_id];

    DriverList tmp_blocked_drivers;
    int spin_count = 0;
    std::vector<DriverRawPtr> ready_drivers;
    while (!_is_shutdown.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lock(global_mutex);
            tmp_blocked_drivers.splice(tmp_blocked_drivers.end(), blocked_drivers);
            if (local_blocked_drivers.empty() && tmp_blocked_drivers.empty() && blocked_drivers.empty()) {
                std::cv_status cv_status = std::cv_status::no_timeout;
                while (!_is_shutdown.load(std::memory_order_acquire) && blocked_drivers.empty()) {
                    cv_status = cond.wait_for(lock, std::chrono::milliseconds(10));
                }
                if (cv_status == std::cv_status::timeout) {
                    continue;
                }
                if (_is_shutdown.load(std::memory_order_acquire)) {
                    break;
                }
                tmp_blocked_drivers.splice(tmp_blocked_drivers.end(), blocked_drivers);
            }
        }

        int64_t iterate_time_ns = 0;
        bool has_ready_drivers = false;
        {
            std::unique_lock write_lock(local_mutex);

            if (!tmp_blocked_drivers.empty()) {
                local_blocked_drivers.splice(local_blocked_drivers.end(), tmp_blocked_drivers);
            }

            auto driver_it = local_blocked_drivers.begin();
            while (driver_it != local_blocked_drivers.end()) {
                SCOPED_RAW_TIMER(&iterate_time_ns);

                auto* driver = *driver_it;

                SCOPED_TIMER(driver->poller_check_timer());
                COUNTER_UPDATE(driver->poller_check_counter(), 1);
                driver->poller_check_drivers_num()->set(local_blocked_drivers.size());

                if (ready_drivers.size() >= config::pipeline_poller_put_back_num) {
                    has_ready_drivers = true;

                    for (auto* d : ready_drivers) {
                        d->update_poller_iterate_timer(iterate_time_ns);
                        d->poller_ready_drivers_num()->set(ready_drivers.size());
                    }

                    _driver_queue->put_back(ready_drivers);
                    ready_drivers.clear();
                }

                if (driver->query_ctx()->is_query_expired()) {
                    // there are not any drivers belonging to a query context can make progress for an expiration period
                    // indicates that some fragments are missing because of failed exec_plan_fragment invocation. in
                    // this situation, query is failed finally, so drivers are marked PENDING_FINISH/FINISH.
                    //
                    // If the fragment is expired when the source operator is already pending i/o task,
                    // The state of driver shouldn't be changed.
                    LOG(WARNING) << "[Driver] Timeout, query_id=" << print_id(driver->query_ctx()->query_id())
                                 << ", instance_id=" << print_id(driver->fragment_ctx()->fragment_instance_id());
                    driver->fragment_ctx()->cancel(
                            Status::TimedOut(fmt::format("Query exceeded time limit of {} seconds",
                                                         driver->query_ctx()->get_query_expire_seconds())));
                    driver->cancel_operators(driver->fragment_ctx()->runtime_state());
                    if (driver->is_still_pending_finish()) {
                        driver->set_driver_state(DriverState::PENDING_FINISH);
                        ++driver_it;
                    } else {
                        driver->set_driver_state(DriverState::FINISH);
                        remove_blocked_driver(local_blocked_drivers, driver_it);
                        ready_drivers.emplace_back(driver);
                    }
                } else if (driver->fragment_ctx()->is_canceled()) {
                    // If the fragment is cancelled when the source operator is already pending i/o task,
                    // The state of driver shouldn't be changed.
                    driver->cancel_operators(driver->fragment_ctx()->runtime_state());
                    if (driver->is_still_pending_finish()) {
                        driver->set_driver_state(DriverState::PENDING_FINISH);
                        ++driver_it;
                    } else {
                        driver->set_driver_state(DriverState::CANCELED);
                        remove_blocked_driver(local_blocked_drivers, driver_it);
                        ready_drivers.emplace_back(driver);
                    }
                } else if (driver->pending_finish()) {
                    if (driver->is_still_pending_finish()) {
                        ++driver_it;
                    } else {
                        // driver->pending_finish() return true means that when a driver's sink operator is finished,
                        // but its source operator still has pending io task that executed in io threads and has
                        // reference to object outside(such as desc_tbl) owned by FragmentContext. So a driver in
                        // PENDING_FINISH state should wait for pending io task's completion, then turn into FINISH state,
                        // otherwise, pending tasks shall reference to destructed objects in FragmentContext since
                        // FragmentContext is unregistered prematurely.
                        driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                                       : DriverState::FINISH);
                        remove_blocked_driver(local_blocked_drivers, driver_it);
                        ready_drivers.emplace_back(driver);
                    }
                } else if (driver->is_finished()) {
                    remove_blocked_driver(local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                } else if (driver->is_not_blocked()) {
                    driver->set_driver_state(DriverState::READY);
                    remove_blocked_driver(local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                } else {
                    ++driver_it;
                }
            }
        }

        if (!ready_drivers.empty()) {
            has_ready_drivers = true;

            for (auto* driver : ready_drivers) {
                driver->update_poller_iterate_timer(iterate_time_ns);
                driver->poller_ready_drivers_num()->set(ready_drivers.size());
            }

            _driver_queue->put_back(ready_drivers);
            ready_drivers.clear();
        }

        if (!has_ready_drivers) {
            spin_count += 1;
        } else {
            spin_count = 0;
        }

        if (spin_count != 0 && spin_count % 64 == 0) {
#ifdef __x86_64__
            _mm_pause();
#else
            // TODO: Maybe there's a better intrinsic like _mm_pause on non-x86_64 architecture.
            sched_yield();
#endif
        }
        if (spin_count == 640) {
            spin_count = 0;
            sched_yield();
        }
    }
}

void PipelineDriverPoller::add_blocked_driver(const DriverRawPtr driver) {
    int64_t cnt = _add_cnt.fetch_add(1);
    int64_t poller_id = cnt % POLLER_NUM;
    auto& info = _info_per_thread[poller_id];

    std::unique_lock<std::mutex> lock(info.global_mutex);
    info.blocked_drivers.push_back(driver);
    driver->_pending_timer_sw->reset();
    info.cond.notify_one();
}

void PipelineDriverPoller::remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it) {
    auto& driver = *driver_it;
    driver->_pending_timer->update(driver->_pending_timer_sw->elapsed_time());
    local_blocked_drivers.erase(driver_it++);
}

void PipelineDriverPoller::iterate_immutable_driver(const IterateImmutableDriverFunc& call) const {
    for (const auto& info : _info_per_thread) {
        std::shared_lock guard(info.local_mutex);
        for (auto* driver : info.local_blocked_drivers) {
            call(driver);
        }
    }
}

} // namespace starrocks::pipeline
