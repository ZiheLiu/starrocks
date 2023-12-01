// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

#include "pipeline_driver.h"
#include "pipeline_driver_queue.h"
#include "util/moodycamel/blockingconcurrentqueue.h"
#include "util/thread.h"

namespace starrocks {
namespace pipeline {

class PipelineDriverPoller;
using PipelineDriverPollerPtr = std::unique_ptr<PipelineDriverPoller>;

class PipelineDriverPoller {
public:
    explicit PipelineDriverPoller(DriverQueue* driver_queue);

    using DriverList = std::list<DriverRawPtr>;

    ~PipelineDriverPoller() { shutdown(); };
    // start poller thread
    void start();
    // shutdown poller thread
    void shutdown();
    // add blocked driver to poller
    __attribute__((noinline)) void add_blocked_driver(const DriverRawPtr driver);
    void add_blocked_driver(const std::vector<DriverRawPtr>& drivers);
    // remove blocked driver from poller
    void remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it);
    // only used for collect metrics
    size_t blocked_driver_queue_len() const { return _num_blocked_drivers; }

    void iterate_immutable_driver(const IterateImmutableDriverFunc& call) const;

private:
    using Queue = moodycamel::BlockingConcurrentQueue<DriverRawPtr>;
    struct ThreadItem {
        Queue blocked_drivers;
        scoped_refptr<Thread> polling_thread{nullptr};
        std::atomic<bool> is_polling_thread_initialized{false};
        std::atomic<bool> is_shutdown{false};
    };

    void run_internal(ThreadItem* item);
    PipelineDriverPoller(const PipelineDriverPoller&) = delete;
    PipelineDriverPoller& operator=(const PipelineDriverPoller&) = delete;

private:
    const int num_threads = config::pipeline_poller_thread_num;

    DriverQueue* _driver_queue;

    std::vector<ThreadItem> _thread_items;

    std::atomic<size_t> _num_blocked_drivers{0};
    std::atomic<size_t> _next_thread_index{0};
};
} // namespace pipeline
} // namespace starrocks
