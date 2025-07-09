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

namespace starrocks {

enum class JoinMatchFlag { NORMAL, ALL_NOT_MATCH, ALL_MATCH_ONE, MOST_MATCH_ONE };

struct HashTableProbeState {
    //TODO: memory release
    Buffer<uint8_t> is_nulls;
    Buffer<uint32_t> buckets;
    Buffer<uint32_t> next;
    Buffer<Slice> probe_slice;
    const Buffer<uint8_t>* null_array = nullptr;
    ColumnPtr probe_key_column;
    const Columns* key_columns = nullptr;
    ColumnPtr build_index_column;
    ColumnPtr probe_index_column;
    Buffer<uint32_t>& build_index;
    Buffer<uint32_t>& probe_index;

    // when exec right join
    // record the build items is matched or not
    // 0: not matched, 1: matched
    Buffer<uint8_t> build_match_index;
    Buffer<uint32_t> probe_match_index;
    Buffer<uint8_t> probe_match_filter;
    uint32_t count = 0; // current return values count
    // the rows of src probe chunk
    size_t probe_row_count = 0;

    // 0: normal
    // 1: all match one
    JoinMatchFlag match_flag = JoinMatchFlag::NORMAL; // all match one

    bool has_remain = false;
    // When one-to-many, one probe may not be able to probe all the data,
    // cur_probe_index records the position of the last probe
    uint32_t cur_probe_index = 0;
    uint32_t cur_build_index = 0;
    uint32_t cur_row_match_count = 0;

    // For nullaware left anti join there are other conjuncts, if the left or right table is null.
    // We need to find all rows (null does not match all rows). This variable helps us keep track of which rows are currently being processed.
    size_t cur_nullaware_build_index = 1;

    std::unique_ptr<MemPool> probe_pool = nullptr;

    RuntimeProfile::Counter* search_ht_timer = nullptr;
    RuntimeProfile::Counter* output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* output_build_column_timer = nullptr;
    RuntimeProfile::Counter* probe_counter = nullptr;

    HashTableProbeState()
            : build_index_column(UInt32Column::create()),
              probe_index_column(UInt32Column::create()),
              build_index(down_cast<UInt32Column*>(build_index_column.get())->get_data()),
              probe_index(down_cast<UInt32Column*>(probe_index_column.get())->get_data()) {}

    struct ProbeCoroutine {
        struct ProbePromise {
            ProbeCoroutine get_return_object() { return std::coroutine_handle<ProbePromise>::from_promise(*this); }
            std::suspend_always initial_suspend() { return {}; }
            // as final_suspend() suspends coroutines, so should destroy manually in final.
            std::suspend_always final_suspend() noexcept { return {}; }
            void unhandled_exception() { exception = std::current_exception(); }
            void return_void() {}
            std::exception_ptr exception = nullptr;
        };

        using promise_type = ProbePromise;
        ProbeCoroutine(std::coroutine_handle<ProbePromise> h) : handle(h) {}
        ~ProbeCoroutine() = default;
        std::coroutine_handle<ProbePromise> handle;
        operator std::coroutine_handle<promise_type>() const { return std::move(handle); }
    };
    uint32_t match_count = 0;
    int active_coroutines = 0;
    // used to adaptively detect time locality
    size_t probe_chunks = 0;
    uint32_t detect_step = 1;
    bool last_enable_interleaving = true;

    std::set<std::coroutine_handle<ProbeCoroutine::ProbePromise>> handles;

    HashTableProbeState(const HashTableProbeState& rhs)
            : is_nulls(rhs.is_nulls),
              buckets(rhs.buckets),
              next(rhs.next),
              probe_slice(rhs.probe_slice),
              null_array(rhs.null_array),
              probe_key_column(rhs.probe_key_column == nullptr ? nullptr : rhs.probe_key_column->clone()),
              key_columns(rhs.key_columns),
              build_index_column(rhs.build_index_column == nullptr
                                         ? UInt32Column::create()->as_mutable_ptr() // to MutableColumnPtr
                                         : rhs.build_index_column->clone()),
              probe_index_column(rhs.probe_index_column == nullptr
                                         ? UInt32Column::create()->as_mutable_ptr() // to MutableColumnPtr
                                         : rhs.probe_index_column->clone()),
              build_index(down_cast<UInt32Column*>(build_index_column.get())->get_data()),
              probe_index(down_cast<UInt32Column*>(probe_index_column.get())->get_data()),
              build_match_index(rhs.build_match_index),
              probe_match_index(rhs.probe_match_index),
              probe_match_filter(rhs.probe_match_filter),
              count(rhs.count),
              probe_row_count(rhs.probe_row_count),
              match_flag(rhs.match_flag),
              has_remain(rhs.has_remain),
              cur_probe_index(rhs.cur_probe_index),
              cur_build_index(rhs.cur_build_index),
              cur_row_match_count(rhs.cur_row_match_count),
              probe_pool(rhs.probe_pool == nullptr ? nullptr : std::make_unique<MemPool>()),
              search_ht_timer(rhs.search_ht_timer),
              output_probe_column_timer(rhs.output_probe_column_timer),
              probe_counter(rhs.probe_counter) {}

    // Disable copy assignment.
    HashTableProbeState& operator=(const HashTableProbeState& rhs) = delete;
    // Disable move ctor and assignment.
    HashTableProbeState(HashTableProbeState&&) = delete;
    HashTableProbeState& operator=(HashTableProbeState&&) = delete;

    void consider_probe_time_locality();

    ~HashTableProbeState() {
        for (auto it = handles.begin(); it != handles.end(); it++) {
            it->destroy();
        }
        handles.clear();
    }
};

} // namespace starrocks
