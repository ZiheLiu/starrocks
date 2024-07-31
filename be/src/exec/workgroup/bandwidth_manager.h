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

#include <mutex>
#include <unordered_set>
#include <vector>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/workgroup/work_group_fwd.h"
#include "gutil/ref_counted.h"
#include "util/thread.h"

namespace starrocks::workgroup {

class BandwidthManager {
public:
    void start();
    void shutdown();

    bool is_throlled(WorkGroup* wg, int64_t delta_ns);
    bool is_throlled(WorkGroup* wg) const;

    void update_statistics(WorkGroup* wg, int64_t delta_ns);

    bool try_add_task(ScanTaskQueue* ready_queue, ScanTask& task);
    bool try_add_task(pipeline::DriverQueue* ready_queue, pipeline::DriverRawPtr task);
    std::vector<pipeline::DriverRawPtr> try_add_task(pipeline::DriverQueue* ready_queue,
                                                     const std::vector<pipeline::DriverRawPtr>& tasks);

private:
    void _run_internal();
    static int64_t _quota_ns();

    static constexpr int64_t PERIOD_NS = 100'000'000;

    scoped_refptr<Thread> _polling_thread{nullptr};
    std::atomic<bool> _is_polling_thread_initialized;

    std::atomic<bool> _is_shutdown{false};

    std::atomic<int64_t> _usage_ns{0};

    std::mutex _mutex;
    std::condition_variable _cv;

    std::unordered_set<WorkGroup*> _wgs;
    std::unordered_map<ScanTaskQueue*, std::vector<ScanTask>> _scan_tasks;
    std::unordered_map<pipeline::DriverQueue*, std::vector<pipeline::DriverRawPtr>> _driver_tasks;
};

} // namespace starrocks::workgroup
