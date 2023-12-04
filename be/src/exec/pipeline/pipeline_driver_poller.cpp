// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "pipeline_driver_poller.h"

#include <chrono>
namespace starrocks::pipeline {

PipelineDriverPoller::PipelineDriverPoller(DriverQueue* driver_queue)
        : _driver_queue(driver_queue), _thread_items(num_threads) {}

void PipelineDriverPoller::start() {
    for (auto& item : _thread_items) {
        auto status = Thread::create(
                "pipeline", "pipeline_poller", [this, pitem = &item]() { run_internal(pitem); }, &item.polling_thread);
        if (!status.ok()) {
            LOG(FATAL) << "Fail to create PipelineDriverPoller: error=" << status.to_string();
        }
        while (!item.is_polling_thread_initialized.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

void PipelineDriverPoller::shutdown() {
    for (auto& item : _thread_items) {
        if (!item.is_shutdown.load() && item.polling_thread.get() != nullptr) {
            item.is_shutdown.store(true, std::memory_order_release);
            item.blocked_drivers.enqueue(nullptr);
            item.polling_thread->join();
        }
    }
}

void PipelineDriverPoller::run_internal(ThreadItem* item) {
    item->is_polling_thread_initialized.store(true, std::memory_order_release);

    int spin_count = 0;
    const int num_buffer_drivers = 1024 * 10;
    std::vector<DriverRawPtr> driver_buffer(num_buffer_drivers);
    DriverList local_blocked_drivers;
    std::vector<DriverRawPtr> ready_drivers;

    while (!item->is_shutdown.load(std::memory_order_acquire)) {
        bool has_ready_drivers = false;

        DriverRawPtr driver = nullptr;
        size_t num_drivers = num_buffer_drivers;
        while (num_drivers * 2 >= num_buffer_drivers) {
            num_drivers = item->blocked_drivers.try_dequeue_bulk(driver_buffer.begin(), num_buffer_drivers);
            for (int i = 0; i < num_drivers; i++) {
                driver = driver_buffer[i];
                if (driver == nullptr) {
                    if (item->is_shutdown.load(std::memory_order_acquire)) {
                        return;
                    }
                } else {
                    local_blocked_drivers.emplace_back(driver);
                }
            }
        }

        if (local_blocked_drivers.empty()) {
            item->blocked_drivers.wait_dequeue(driver);
            if (driver == nullptr) {
                if (item->is_shutdown.load(std::memory_order_acquire)) {
                    return;
                }
            } else {
                local_blocked_drivers.emplace_back(driver);
            }
            continue;
        }

        auto driver_it = local_blocked_drivers.begin();
        while (driver_it != local_blocked_drivers.end()) {
            auto* driver = *driver_it;

            if (driver->query_ctx()->is_query_expired()) {
                // there are not any drivers belonging to a query context can make progress for an expiration period
                // indicates that some fragments are missing because of failed exec_plan_fragment invocation. in
                // this situation, query is failed finally, so drivers are marked PENDING_FINISH/FINISH.
                //
                // If the fragment is expired when the source operator is already pending i/o task,
                // The state of driver shouldn't be changed.
                LOG(WARNING) << "[Driver] Timeout, query_id=" << print_id(driver->query_ctx()->query_id())
                             << ", instance_id=" << print_id(driver->fragment_ctx()->fragment_instance_id());
                driver->fragment_ctx()->cancel(Status::TimedOut(fmt::format(
                        "Query exceeded time limit of {} seconds", driver->query_ctx()->get_query_expire_seconds())));
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

            if (ready_drivers.size() >= config::pipeline_poller_batch_back_num) {
                has_ready_drivers = true;

                _driver_queue->put_back(ready_drivers);
                ready_drivers.clear();
            }
        }

        if (ready_drivers.empty()) {
            if (has_ready_drivers) {
                spin_count = 0;
            } else {
                spin_count += 1;
            }
        } else {
            spin_count = 0;

            _driver_queue->put_back(ready_drivers);
            ready_drivers.clear();
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
    _num_blocked_drivers++;
    auto& item = _thread_items[(_next_thread_index++) % num_threads];
    item.blocked_drivers.enqueue(driver);
    driver->_pending_timer_sw->reset();
}

void PipelineDriverPoller::add_blocked_driver(const std::vector<DriverRawPtr>& drivers) {
    _num_blocked_drivers += drivers.size();

    std::vector<std::vector<DriverRawPtr>> drivers_per_thread(num_threads);
    for (auto* driver : drivers) {
        driver->_pending_timer_sw->reset();
        drivers_per_thread[(_next_thread_index++) % num_threads].emplace_back(driver);
    }

    for (int i = 0; i < num_threads; i++) {
        if (!drivers_per_thread[i].empty()) {
            _thread_items[i].blocked_drivers.enqueue_bulk(drivers_per_thread[i].begin(), drivers_per_thread[i].size());
        }
    }
}

void PipelineDriverPoller::remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it) {
    _num_blocked_drivers--;
    auto& driver = *driver_it;
    COUNTER_UPDATE(driver->_pending_timer, driver->_pending_timer_sw->elapsed_time());
    local_blocked_drivers.erase(driver_it++);
}

void PipelineDriverPoller::iterate_immutable_driver(const IterateImmutableDriverFunc& call) const {}

} // namespace starrocks::pipeline
