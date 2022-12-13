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
#include "util/thread.h"

namespace starrocks::pipeline {

class PipelineDriverPoller;
using PipelineDriverPollerPtr = std::unique_ptr<PipelineDriverPoller>;

class PipelineDriverPoller {
public:
    explicit PipelineDriverPoller(DriverQueue* driver_queue)
            : _info_per_thread(POLLER_NUM), _driver_queue(driver_queue) {}

    using DriverList = std::list<DriverRawPtr>;

    ~PipelineDriverPoller() { shutdown(); };
    // start poller thread
    void start();
    // shutdown poller thread
    void shutdown();
    // add blocked driver to poller
    void add_blocked_driver(const DriverRawPtr driver);
    // remove blocked driver from poller
    void remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it);
    // only used for collect metrics
    size_t blocked_driver_queue_len() const {
        size_t len = 0;
        for (const auto& info : _info_per_thread) {
            std::shared_lock guard(info.local_mutex);
            len += info.local_blocked_drivers.size();
        }
        return len;
    }

    void iterate_immutable_driver(const IterateImmutableDriverFunc& call) const;

private:
    void run_internal(int32_t poller_id);
    PipelineDriverPoller(const PipelineDriverPoller&) = delete;
    PipelineDriverPoller& operator=(const PipelineDriverPoller&) = delete;

private:
    struct Info {
        mutable std::mutex global_mutex;
        std::condition_variable cond;
        DriverList blocked_drivers;

        mutable std::shared_mutex local_mutex;
        DriverList local_blocked_drivers;

        scoped_refptr<Thread> polling_thread;
    };

    const int POLLER_NUM{config::pipeline_poller_num};

    std::vector<Info> _info_per_thread;

    DriverQueue* _driver_queue;

    std::atomic<int32_t> _is_polling_thread_initialized{0};
    std::atomic<bool> _is_shutdown{false};

    std::atomic<int> _add_cnt{0};
};
} // namespace starrocks::pipeline
