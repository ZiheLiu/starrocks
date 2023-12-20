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

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

#include "pipeline_driver.h"
#include "pipeline_driver_queue.h"
#include "pipeline_driver_queue_manager.h"
#include "util/thread.h"

namespace starrocks::pipeline {

class PipelineDriverPoller;
using PipelineDriverPollerPtr = std::unique_ptr<PipelineDriverPoller>;

class PipelineDriverPoller {
public:
    explicit PipelineDriverPoller(DriverQueueManager* driver_queue_manager);

    using DriverList = std::list<DriverRawPtr>;

    ~PipelineDriverPoller() { shutdown(); };
    void start();
    void shutdown();
    // add blocked driver to poller
    void add_blocked_driver(const DriverRawPtr driver);
    // remove blocked driver from poller
    void remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it);
    void on_cancel(DriverRawPtr driver, std::vector<DriverRawPtr>& ready_drivers, DriverList& local_blocked_drivers,
                   DriverList::iterator& driver_it);

    // add driver into the parked driver list
    void park_driver(const DriverRawPtr driver);
    // activate parked driver from poller
    size_t activate_parked_driver(const ImmutableDriverPredicateFunc& predicate_func);
    size_t calculate_parked_driver(const ImmutableDriverPredicateFunc& predicate_func) const;

    // only used for collect metrics
    size_t blocked_driver_queue_len() const { return _blocked_driver_queue_len; }

    void iterate_immutable_driver(const IterateImmutableDriverFunc& call) const;

private:
    struct ThreadItem {
        scoped_refptr<Thread> polling_thread{nullptr};
        std::atomic<bool> is_polling_thread_initialized{false};
        std::atomic<bool> is_shutdown{false};

        mutable std::mutex _global_mutex;
        std::condition_variable _cond;
        DriverList _blocked_drivers;

        mutable std::shared_mutex _local_mutex;
        DriverList _local_blocked_drivers;
    };

    void run_internal(ThreadItem* item);
    PipelineDriverPoller(const PipelineDriverPoller&) = delete;
    PipelineDriverPoller& operator=(const PipelineDriverPoller&) = delete;

    const int num_threads = config::pipeline_poller_thread_num;

    DriverQueueManager* _driver_queue_manager;

    std::vector<ThreadItem> _thread_items;

    // NOTE: The `driver` can be stored in the parked drivers when it will never not be called to run.
    // The parked driver needs to be actived when it needs to be triggered again.
    mutable std::mutex _global_parked_mutex;
    DriverList _parked_drivers;

    std::atomic<size_t> _blocked_driver_queue_len;
    std::atomic<size_t> _next_thread_index{0};
};
} // namespace starrocks::pipeline
