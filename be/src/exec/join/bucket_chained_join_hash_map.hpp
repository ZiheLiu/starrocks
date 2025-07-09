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

#include "bucket_chained_join_hash_map.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// JoinBucketChainedHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::build_prepare(RuntimeState* state) {
    BuildFunc().prepare(state, _table_items);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::probe_prepare(RuntimeState* state) {
    ProbeFunc().prepare(state, _probe_state);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::build(RuntimeState* state) {
    BuildFunc().construct_hash_table(state, _table_items, _probe_state);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <typename MatchFunctor, typename FinishProbeFunctor>
HashTableProbeState::ProbeCoroutine JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::probe_chunk_coroutine(
        const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data, MatchFunctor match_func,
        FinishProbeFunctor finish_probe_func) {
    return probe_chunk_coroutine(
            build_data, probe_data, match_func, [](const uint32_t, const uint32_t) { return false; },
            finish_probe_func);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <typename MatchFunctor, typename FinishProbeRowFunctor, typename FinishProbeFunctor>
HashTableProbeState::ProbeCoroutine JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::probe_chunk_coroutine(
        const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data, MatchFunctor match_func,
        FinishProbeRowFunctor finish_probe_row_func, FinishProbeFunctor finish_probe_func) {
    const auto probe_row_count = _probe_state->probe_row_count;
    for (size_t i = _probe_state->cur_probe_index++; i < probe_row_count; i = _probe_state->cur_probe_index++) {
        uint32_t build_index = _probe_state->next[i];
        uint32_t match_count = 0;

        if (build_index == 0) {
            if (finish_probe_row_func(i, 0)) {
                _probe_state->has_remain = true;
                co_await std::suspend_always{};
            }
            continue;
        }

        do {
            XXH_PREFETCH(build_data.data() + build_index);
            XXH_PREFETCH(_table_items->next.data() + build_index);
            co_await std::suspend_always{};

            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                match_count++;
                if (match_func(i, build_index)) {
                    _probe_state->has_remain = true;
                    co_await std::suspend_always{};
                }
            }

            build_index = _table_items->next[build_index];
        } while (build_index != 0);

        if (finish_probe_row_func(i, match_count)) {
            _probe_state->has_remain = true;
            co_await std::suspend_always{};
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }

    finish_probe_func();
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <typename NeedRowFunctor, typename ContainsRowFunctor, typename FinishProbeFunctor>
HashTableProbeState::ProbeCoroutine JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::contains_coroutine(
        const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data, NeedRowFunctor need_row_func,
        ContainsRowFunctor contains_row_func, FinishProbeFunctor finish_probe_func) {
    const auto probe_row_count = _probe_state->probe_row_count;
    for (uint32_t i = _probe_state->cur_probe_index++; i < probe_row_count; i = _probe_state->cur_probe_index++) {
        if (!need_row_func(i)) {
            continue;
        }

        uint32_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            contains_row_func(i, false);
            continue;
        }

        bool contains = false;
        do {
            XXH_PREFETCH(build_data.data() + build_index);
            XXH_PREFETCH(_table_items->next.data() + build_index);
            co_await std::suspend_always{};

            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                contains_row_func(i, true);
                contains = true;
                break;
            }

            build_index = _table_items->next[build_index];
        } while (build_index != 0);

        if (!contains) {
            contains_row_func(i, false);
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }

    finish_probe_func();
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe, typename MatchFunctor>
bool JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::probe_chunk(const Buffer<CppType>& build_data,
                                                                     const Buffer<CppType>& probe_data,
                                                                     MatchFunctor match_func) {
    return probe_chunk<first_probe>(build_data, probe_data, match_func,
                                    [](const uint32_t, const uint32_t) { return false; });
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe, typename MatchFunctor, typename FinishProbeRowFunctor>
bool JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::probe_chunk(const Buffer<CppType>& build_data,
                                                                     const Buffer<CppType>& probe_data,
                                                                     MatchFunctor match_func,
                                                                     FinishProbeRowFunctor finish_probe_row_func) {
    uint32_t match_count = _probe_state->cur_row_match_count;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        if (const uint32_t build_index = _probe_state->cur_build_index; build_index != 0) {
            _probe_state->next[i] = _table_items->next[build_index];
        } else {
            i++;
            match_count = 0;
        }
    }

    auto pause_probe = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->has_remain = true;
        _probe_state->cur_row_match_count = match_count;
        _probe_state->cur_probe_index = probe_index;
        _probe_state->cur_build_index = build_index;
    };

    const size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        uint32_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            if (finish_probe_row_func(i, 0)) {
                pause_probe(i, 0);
                return true;
            }
            continue;
        }

        do {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) { // Match.
                match_count++;
                if (match_func(i, build_index)) {
                    pause_probe(i, build_index);
                    return true;
                }
            }

            build_index = _table_items->next[build_index];
        } while (build_index != 0);

        if (finish_probe_row_func(i, match_count)) {
            pause_probe(i, 0);
            return true;
        }

        match_count = 0;
    }

    return false;
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
bool JoinBucketChainedHashMap<LT, BuildFunc, ProbeFunc>::contains(const uint32_t probe_index,
                                                                  const Buffer<CppType>& build_data,
                                                                  const Buffer<CppType>& probe_data) {
    uint32_t build_index = _probe_state->next[probe_index];
    if (build_index == 0) {
        return false;
    }

    do {
        if (ProbeFunc().equal(build_data[build_index], probe_data[probe_index])) {
            return true;
        }
        build_index = _table_items->next[build_index];
    } while (build_index != 0);

    return false;
}

} // namespace starrocks