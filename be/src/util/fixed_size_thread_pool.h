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

#include <atomic>
#include <mutex>
#include <vector>

#include "common/status.h"
#include "util/moodycamel/blockingconcurrentqueue.h"
#include "util/thread.h"

namespace starrocks {

class FixedSizeThreadPool {
public:
    using Task = std::function<void()>;

    FixedSizeThreadPool(const std::string& name, size_t num_threads) : _name(name), _num_threads(num_threads) {
        _threads.reserve(_num_threads);
        for (int i = 0; i < _num_threads; i++) {
            create_thread();
        }

        while (_num_initialized_threads < _num_threads) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    ~FixedSizeThreadPool() { close(); }

    void close() {
        if (_is_closed) {
            return;
        }

        _is_closed = true;
        try_offer([] {});

        {
            std::lock_guard<std::mutex> l(_lock);
            for (auto* thread : _threads) {
                thread->join();
            }
        }
    }

    bool try_offer(const Task& task) { return _task_queue.enqueue(task); }
    bool try_offer(Task&& task) { return _task_queue.enqueue(std::move(task)); }

private:
    Status create_thread() {
        return Thread::create("thread pool", _name, &FixedSizeThreadPool::worker_thread, this, nullptr);
    }

    void worker_thread() {
        {
            std::lock_guard<std::mutex> l(_lock);
            _threads.emplace_back(Thread::current_thread());
        }
        _num_initialized_threads++;

        while (!_is_closed) {
            Task task;
            _task_queue.wait_dequeue(task);
            if (_is_closed) {
                try_offer([] {});
                return;
            }
            task();
        }
    }

private:
    using TaskQueue = moodycamel::BlockingConcurrentQueue<Task>;

    const std::string _name;
    const size_t _num_threads;

    std::mutex _lock;
    std::vector<Thread*> _threads;
    std::atomic<size_t> _num_initialized_threads{0};

    std::atomic<bool> _is_closed{false};
    TaskQueue _task_queue;
};

} // namespace starrocks