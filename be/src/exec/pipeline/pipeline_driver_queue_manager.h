// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/pipeline_driver_queue.h"
#include "util/sys_futex.h"

namespace starrocks::pipeline {

class ParkingLot {
public:
    class State {
    public:
        explicit State(int32_t val) : _val(val) {}

        int32_t val() const { return _val; }

        bool closed() const { return _val & 1; }

    private:
        int32_t _val;
    };

    State get_state() { return State(_state.load(std::memory_order_acquire)); }

    void close() {
        _state.fetch_or(1, std::memory_order_release);
        notify_all();
    }

    void wait(State expected) { futex_wait_private(reinterpret_cast<int32_t*>(&_state), expected.val(), nullptr); }

    int notify_one() { return notify(1); }

    int notify_all() { return notify(std::numeric_limits<int32_t>::max()); }

    int notify(int nwake) {
        _state.fetch_add(1 << 1, std::memory_order_release);
        return futex_wake_private(reinterpret_cast<int32_t*>(&_state), nwake);
    }

private:
    // The lowest bit is denoted whether it is closed.
    std::atomic<int32_t> _state = 0;
};

class DriverQueueManager {
public:
    using DriverQueuePtr = std::unique_ptr<DriverQueue>;
    using CreateDriverQueueFunc = std::function<DriverQueuePtr(void)>;

    DriverQueueManager(CreateDriverQueueFunc create_driver_queue_func);
    ~DriverQueueManager() = default;

    void initialize(int num_dispatchers);

    void close();

    StatusOr<DriverRawPtr> take(bool blocked, int dispatcher_id);

    void put_back(const DriverRawPtr driver);
    void put_back(const std::vector<DriverRawPtr>& drivers);
    void put_back_from_executor(const DriverRawPtr driver);

    void notify(int dispatcher_id, int num_drivers);

    // Generate dispatcher id when the dispatcher thread starts.
    size_t gen_dispatcher_id() { return _next_dispatcher_id.fetch_add(1, std::memory_order_relaxed); }

    bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const { return false; }
    void update_statistics(const DriverRawPtr driver);
    // TODO: what will hapen, if cancel when stealing?
    void cancel(DriverRawPtr driver);
    size_t size() const;

private:
    size_t _random_dispatcher_id();

    const int _num_pls = config::pipeline_num_pls;

    // Used to block and notify dispatcher threads.
    // Multiple dispatcher waits in one pl.
    std::vector<ParkingLot> _pls;

    CreateDriverQueueFunc _create_driver_queue_func;

    size_t _num_dispatchers = 0;
    std::vector<DriverQueuePtr> _queue_per_dispatcher;

    // Generate dispatcher id when the dispatcher thread starts.
    std::atomic<size_t> _next_dispatcher_id = 0;
    std::atomic<size_t> _next_random_id = 0;

    // Each start pos is corresponding to a different step size coprime with _num_dispatchers.
    // In this way, make the steal order of the different start pos different.
    std::vector<int> _rand_step_sizes;
};

} // namespace starrocks::pipeline
