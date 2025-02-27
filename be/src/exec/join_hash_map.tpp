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

#include <simd/gather.h>

#include "simd/simd.h"
#include "util/runtime_profile.h"

#define JOIN_HASH_MAP_TPP

#ifndef JOIN_HASH_MAP_H
#include "join_hash_map.h"
#endif

namespace starrocks {

static constexpr uint32_t BLOOM_FILTER_MASK = 0x00FF'FFFFul;

static constexpr uint8_t BLOOM_FILTERS[256] = {
        1,  3,   5,  9,   17, 33,  65,  129, 3,   3,   7,   11,  19,  35,  67,  131, 5,   7,   5,   13,  21,  37,
        69, 133, 9,  11,  13, 9,   25,  41,  73,  137, 17,  19,  21,  25,  17,  49,  81,  145, 33,  35,  37,  41,
        49, 33,  97, 161, 65, 67,  69,  73,  81,  97,  65,  193, 129, 131, 133, 137, 145, 161, 193, 129, 3,   3,
        7,  11,  19, 35,  67, 131, 3,   2,   6,   10,  18,  34,  66,  130, 7,   6,   6,   14,  22,  38,  70,  134,
        11, 10,  14, 10,  26, 42,  74,  138, 19,  18,  22,  26,  18,  50,  82,  146, 35,  34,  38,  42,  50,  34,
        98, 162, 67, 66,  70, 74,  82,  98,  66,  194, 131, 130, 134, 138, 146, 162, 194, 130, 5,   7,   5,   13,
        21, 37,  69, 133, 7,  6,   6,   14,  22,  38,  70,  134, 5,   6,   4,   12,  20,  36,  68,  132, 13,  14,
        12, 12,  28, 44,  76, 140, 21,  22,  20,  28,  20,  52,  84,  148, 37,  38,  36,  44,  52,  36,  100, 164,
        69, 70,  68, 76,  84, 100, 68,  196, 133, 134, 132, 140, 148, 164, 196, 132, 9,   11,  13,  9,   25,  41,
        73, 137, 11, 10,  14, 10,  26,  42,  74,  138, 13,  14,  12,  12,  28,  44,  76,  140, 9,   10,  12,  8,
        24, 40,  72, 136, 25, 26,  28,  24,  24,  56,  88,  152, 41,  42,  44,  40,  56,  40,  104, 168, 73,  74,
        76, 72,  88, 104, 72, 200, 137, 138, 140, 136, 152, 168, 200, 136,
};

template <LogicalType LT>
void JoinBuildFunc<LT>::prepare(RuntimeState* runtime, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
    table_items->set_has_value.resize(table_items->bucket_size / 8, 0);
}

template <LogicalType LT>
const Buffer<typename JoinBuildFunc<LT>::CppType>& JoinBuildFunc<LT>::get_key_data(
        const JoinHashTableItems& table_items) {
    ColumnPtr data_column;
    if (table_items.key_columns[0]->is_nullable()) {
        auto* null_column = ColumnHelper::as_raw_column<NullableColumn>(table_items.key_columns[0]);
        data_column = null_column->data_column();
    } else {
        data_column = table_items.key_columns[0];
    }

    if constexpr (lt_is_string<LT>) {
        if (UNLIKELY(data_column->is_large_binary())) {
            return ColumnHelper::as_raw_column<LargeBinaryColumn>(data_column)->get_data();
        } else {
            return ColumnHelper::as_raw_column<BinaryColumn>(data_column)->get_data();
        }
    } else {
        return ColumnHelper::as_raw_column<ColumnType>(data_column)->get_data();
    }
}

template <LogicalType LT>
void JoinBuildFunc<LT>::construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                             HashTableProbeState* probe_state) {
    if (abs(config::enable_simd_hash_join) == 1 && table_items->bucket_size <= BLOOM_FILTER_MASK &&
        (table_items->join_type == TJoinOp::INNER_JOIN || table_items->join_type == TJoinOp::LEFT_OUTER_JOIN ||
         table_items->join_type == TJoinOp::LEFT_ANTI_JOIN || table_items->join_type == TJoinOp::LEFT_SEMI_JOIN)) {
        table_items->mode = 1;
        do_construct_hash_table<1>(state, table_items, probe_state);
        return;
    }

    if (abs(config::enable_simd_hash_join) == 2 && std::is_integral_v<CppType> && sizeof(CppType) == 4 &&
        (table_items->join_type == TJoinOp::LEFT_ANTI_JOIN || table_items->join_type == TJoinOp::LEFT_SEMI_JOIN)) {
        table_items->mode = 2;
        do_construct_hash_table<2>(state, table_items, probe_state);
        return;
    }

    if (abs(config::enable_simd_hash_join) == 3 &&
        (table_items->join_type == TJoinOp::LEFT_ANTI_JOIN || table_items->join_type == TJoinOp::LEFT_SEMI_JOIN) &&
        table_items->row_count > 0) {
        if constexpr (std::is_integral_v<CppType> && sizeof(CppType) == 4) {
            const size_t num_rows = table_items->row_count + 1;
            const auto* keys = reinterpret_cast<const uint32_t*>(get_key_data(*table_items).data());
            const uint32_t min_key = *std::min_element(keys + 1, keys + num_rows);
            const uint32_t max_key = *std::max_element(keys + 1, keys + num_rows);
            if (max_key - min_key + 1 <= table_items->bucket_size) {
                table_items->mode = 3;
                table_items->min_value = min_key;
                table_items->max_value = max_key;
                do_construct_hash_table<3>(state, table_items, probe_state);
                return;
            }
        }
    }

    table_items->mode = 0;
    do_construct_hash_table<0>(state, table_items, probe_state);
}

static size_t multiplicative_hash(auto key) {
    static constexpr uint64_t a = 11400714819323198485ull;
    const uint64_t k = *reinterpret_cast<uint32_t*>(&key);
    return k * a;
}

template <LogicalType LT>
template <uint8_t SIMD>
void JoinBuildFunc<LT>::do_construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                                HashTableProbeState* probe_state) {
    auto& data = get_key_data(*table_items);
    const size_t num_rows = table_items->row_count + 1;

    [[maybe_unused]] auto* firsts = table_items->first.data();
    [[maybe_unused]] auto* nexts = table_items->next.data();
    [[maybe_unused]] const uint32_t bucket_size_mask = table_items->bucket_size - 1;
    [[maybe_unused]] const auto* __restrict pdata = data.data();

    if constexpr (SIMD == 1) {
        auto* __restrict next = table_items->next.data();
        for (size_t i = 1; i < num_rows; i++) {
            // use next to cache bucket_num
            next[i] = JoinHashMapHelper::calc_bucket_num<CppType>(pdata[i], table_items->bucket_size << 8,
                                                                  table_items->log_bucket_size + 8);
        }
    } else if constexpr (SIMD == 2) {
        auto* __restrict next = table_items->next.data();
        for (size_t i = 1; i < num_rows; i++) {
            // use next to cache bucket_num
            next[i] = JoinHashMapHelper::calc_bucket_num<CppType>(pdata[i], table_items->bucket_size,
                                                                  table_items->log_bucket_size);
        }
    }

    if (table_items->key_columns[0]->is_nullable()) {
        const auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items->key_columns[0]);
        const auto& null_array = nullable_column->null_column()->get_data();

        if (nullable_column->has_null()) {
            if (SIMD == 3 && std::is_integral_v<CppType> && sizeof(CppType) == 4) {
                const uint32_t min_value = table_items->min_value;
                const auto* keys = reinterpret_cast<const uint32_t*>(data.data());
                auto* __restrict buckets = table_items->set_has_value.data();
                for (size_t i = 1; i < num_rows; i++) {
                    const uint32_t bucket = keys[i] - min_value;
                    const uint32_t group = bucket / 8;
                    const uint32_t offset = bucket % 8;
                    buckets[group] |= (null_array[i] == 0) << offset;
                }
            } else {
                for (size_t i = 1; i < num_rows; i++) {
                    if (null_array[i] == 0) {
                        if constexpr (SIMD == 1) {
                            const uint32_t hash = table_items->next[i];
                            const uint32_t bucket_num = hash >> 8;
                            const uint32_t fp = BLOOM_FILTERS[hash & 0xFF];

                            const uint32_t prev_first = table_items->first[bucket_num];
                            table_items->next[i] = prev_first & BLOOM_FILTER_MASK;
                            table_items->first[bucket_num] = i | (prev_first & 0xFF00'0000ul) | (fp << 24);
                        } else if (SIMD == 2) {
                            uint32_t bucket = nexts[i];
                            uint32_t probe_times = 1;
                            while (firsts[bucket] != 0) {
                                bucket = (bucket + probe_times) & bucket_size_mask;
                                probe_times++;
                            }
                            firsts[bucket] = (*reinterpret_cast<const uint32_t*>(pdata + i)) | (1 << 31);
                        } else {
                            uint32_t bucket_num = JoinHashMapHelper::calc_bucket_num<CppType>(
                                    data[i], table_items->bucket_size, table_items->log_bucket_size);
                            table_items->next[i] = table_items->first[bucket_num];
                            table_items->first[bucket_num] = i;
                        }
                    }
                }
            }
        } else {
            if (SIMD == 3 && std::is_integral_v<CppType> && sizeof(CppType) == 4) {
                const uint32_t min_value = table_items->min_value;
                const auto* keys = reinterpret_cast<const uint32_t*>(data.data());
                auto* __restrict buckets = table_items->set_has_value.data();
                for (size_t i = 1; i < num_rows; i++) {
                    const uint32_t bucket = keys[i] - min_value;
                    const uint32_t group = bucket / 8;
                    const uint32_t offset = bucket % 8;
                    buckets[group] |= 1 << offset;
                }
            } else {
                for (size_t i = 1; i < num_rows; i++) {
                    if constexpr (SIMD == 1) {
                        const uint32_t hash = table_items->next[i];
                        const uint32_t bucket_num = hash >> 8;
                        const uint32_t fp = BLOOM_FILTERS[hash & 0xFF];

                        const uint32_t prev_first = table_items->first[bucket_num];
                        table_items->next[i] = prev_first & BLOOM_FILTER_MASK;
                        table_items->first[bucket_num] = i | (prev_first & 0xFF00'0000ul) | (fp << 24);
                    } else if (SIMD == 2) {
                        uint32_t bucket = nexts[i];
                        uint32_t probe_times = 1;
                        while (firsts[bucket] != 0) {
                            bucket = (bucket + probe_times) & bucket_size_mask;
                            probe_times++;
                        }
                        firsts[bucket] = (*reinterpret_cast<const uint32_t*>(pdata + i)) | (1 << 31);
                    } else {
                        uint32_t bucket_num = JoinHashMapHelper::calc_bucket_num<CppType>(
                                data[i], table_items->bucket_size, table_items->log_bucket_size);
                        table_items->next[i] = table_items->first[bucket_num];
                        table_items->first[bucket_num] = i;
                    }
                }
            }
        }
    } else {
        if (SIMD == 3 && std::is_integral_v<CppType> && sizeof(CppType) == 4) {
            const uint32_t min_value = table_items->min_value;
            const auto* keys = reinterpret_cast<const uint32_t*>(data.data());
            auto* __restrict buckets = table_items->set_has_value.data();
            for (size_t i = 1; i < num_rows; i++) {
                const uint32_t bucket = keys[i] - min_value;
                const uint32_t group = bucket / 8;
                const uint32_t offset = bucket % 8;
                buckets[group] |= 1 << offset;
            }
        } else {
            for (size_t i = 1; i < num_rows; i++) {
                if constexpr (SIMD == 1) {
                    const uint32_t hash = table_items->next[i];
                    const uint32_t bucket_num = hash >> 8;
                    const uint32_t fp = BLOOM_FILTERS[hash & 0xFF];

                    const uint32_t prev_first = table_items->first[bucket_num];
                    table_items->next[i] = prev_first & BLOOM_FILTER_MASK;
                    table_items->first[bucket_num] = i | (prev_first & 0xFF00'0000ul) | (fp << 24);
                } else if (SIMD == 2) {
                    uint32_t bucket = nexts[i];
                    uint32_t probe_times = 1;
                    while (firsts[bucket] != 0) {
                        bucket = (bucket + probe_times) & bucket_size_mask;
                        probe_times++;
                    }
                    firsts[bucket] = (*reinterpret_cast<const uint32_t*>(pdata + i)) | (1 << 31);
                } else {
                    uint32_t bucket_num = JoinHashMapHelper::calc_bucket_num<CppType>(data[i], table_items->bucket_size,
                                                                                      table_items->log_bucket_size);
                    table_items->next[i] = table_items->first[bucket_num];
                    table_items->first[bucket_num] = i;
                }
            }
        }
    }

    table_items->calculate_ht_info(table_items->key_columns[0]->byte_size());
}

template <LogicalType LT>
void DirectMappingJoinBuildFunc<LT>::prepare(RuntimeState* runtime, JoinHashTableItems* table_items) {
    static constexpr size_t BUCKET_SIZE =
            (int64_t)(RunTimeTypeLimits<LT>::max_value()) - (int64_t)(RunTimeTypeLimits<LT>::min_value()) + 1L;
    table_items->bucket_size = BUCKET_SIZE;
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
}

template <LogicalType LT>
const Buffer<typename DirectMappingJoinBuildFunc<LT>::CppType>& DirectMappingJoinBuildFunc<LT>::get_key_data(
        const JoinHashTableItems& table_items) {
    if (table_items.key_columns[0]->is_nullable()) {
        auto* null_column = ColumnHelper::as_raw_column<NullableColumn>(table_items.key_columns[0]);
        return ColumnHelper::as_raw_column<ColumnType>(null_column->data_column())->get_data();
    }

    return ColumnHelper::as_raw_column<ColumnType>(table_items.key_columns[0])->get_data();
}

template <LogicalType LT>
void DirectMappingJoinBuildFunc<LT>::construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                                          HashTableProbeState* probe_state) {
    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();

    auto& data = get_key_data(*table_items);
    if (table_items->key_columns[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items->key_columns[0]);
        auto& null_array = nullable_column->null_column()->get_data();
        for (size_t i = 1; i < table_items->row_count + 1; i++) {
            if (null_array[i] == 0) {
                size_t buckets = data[i] - MIN_VALUE;
                table_items->next[i] = table_items->first[buckets];
                table_items->first[buckets] = i;
            }
        }
    } else {
        for (size_t i = 1; i < table_items->row_count + 1; i++) {
            size_t buckets = data[i] - MIN_VALUE;
            table_items->next[i] = table_items->first[buckets];
            table_items->first[buckets] = i;
        }
    }
    table_items->calculate_ht_info(table_items->key_columns[0]->byte_size());
}

template <LogicalType LT>
void FixedSizeJoinBuildFunc<LT>::prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
    table_items->build_key_column = ColumnType::create(table_items->row_count + 1);
}

template <LogicalType LT>
void FixedSizeJoinBuildFunc<LT>::construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                                      HashTableProbeState* probe_state) {
    uint32_t row_count = table_items->row_count;

    // prepare columns
    Columns data_columns;
    NullColumns null_columns;
    for (size_t i = 0; i < table_items->key_columns.size(); i++) {
        if (table_items->join_keys[i].is_null_safe_equal) {
            data_columns.emplace_back(table_items->key_columns[i]);
        } else if (table_items->key_columns[i]->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items->key_columns[i]);
            data_columns.emplace_back(nullable_column->data_column());
            if (table_items->key_columns[i]->has_null()) {
                null_columns.emplace_back(nullable_column->null_column());
            }
        } else {
            data_columns.emplace_back(table_items->key_columns[i]);
        }
    }

    // serialize and build hash table
    uint32_t quo = row_count / state->chunk_size();
    uint32_t rem = row_count % state->chunk_size();

    if (!null_columns.empty()) {
        for (size_t i = 0; i < quo; i++) {
            _build_nullable_columns(table_items, probe_state, data_columns, null_columns, 1 + state->chunk_size() * i,
                                    state->chunk_size());
        }
        _build_nullable_columns(table_items, probe_state, data_columns, null_columns, 1 + state->chunk_size() * quo,
                                rem);
    } else {
        for (size_t i = 0; i < quo; i++) {
            _build_columns(table_items, probe_state, data_columns, 1 + state->chunk_size() * i, state->chunk_size());
        }
        _build_columns(table_items, probe_state, data_columns, 1 + state->chunk_size() * quo, rem);
    }
    table_items->calculate_ht_info(table_items->build_key_column->byte_size());
}

template <LogicalType LT>
void FixedSizeJoinBuildFunc<LT>::_build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                                const Columns& data_columns, uint32_t start, uint32_t count) {
    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, table_items->build_key_column.get(), start,
                                                           count);

    const auto& data = get_key_data(*table_items);
    JoinHashMapHelper::calc_bucket_nums<CppType>(&probe_state->buckets, table_items->bucket_size,
                                                 table_items->log_bucket_size, data, start, count);

    for (uint32_t i = 0; i < count; i++) {
        table_items->next[start + i] = table_items->first[probe_state->buckets[i]];
        table_items->first[probe_state->buckets[i]] = start + i;
    }
}

template <LogicalType LT>
void FixedSizeJoinBuildFunc<LT>::_build_nullable_columns(JoinHashTableItems* table_items,
                                                         HashTableProbeState* probe_state, const Columns& data_columns,
                                                         const NullColumns& null_columns, uint32_t start,
                                                         uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[start + i];
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->get_data()[start + j];
        }
    }

    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, table_items->build_key_column.get(), start,
                                                           count);
    const auto& data = get_key_data(*table_items);
    JoinHashMapHelper::calc_bucket_nums<CppType>(&probe_state->buckets, table_items->bucket_size,
                                                 table_items->log_bucket_size, data, start, count);

    for (size_t i = 0; i < count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            table_items->next[start + i] = table_items->first[probe_state->buckets[i]];
            table_items->first[probe_state->buckets[i]] = start + i;
        }
    }
}

template <LogicalType LT>
void DirectMappingJoinProbeFunc<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                 HashTableProbeState* probe_state) {
    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();
    size_t probe_row_count = probe_state->probe_row_count;
    auto& data = get_key_data(*probe_state);
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    if ((*probe_state->key_columns)[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[0]);

        if (nullable_column->has_null()) {
            auto& null_array = nullable_column->null_column()->get_data();
            for (size_t i = 0; i < probe_row_count; i++) {
                if (null_array[i] == 0) {
                    probe_state->next[i] = table_items.first[data[i] - MIN_VALUE];
                } else {
                    probe_state->next[i] = 0;
                }
            }
            probe_state->null_array = &null_array;
        } else {
            for (size_t i = 0; i < probe_row_count; i++) {
                probe_state->next[i] = table_items.first[data[i] - MIN_VALUE];
            }
            probe_state->null_array = nullptr;
        }
        return;
    }

    for (size_t i = 0; i < probe_row_count; i++) {
        probe_state->next[i] = table_items.first[data[i] - MIN_VALUE];
    }
    probe_state->null_array = nullptr;
}

template <LogicalType LT>
const Buffer<typename DirectMappingJoinProbeFunc<LT>::CppType>& DirectMappingJoinProbeFunc<LT>::get_key_data(
        const HashTableProbeState& probe_state) {
    if ((*probe_state.key_columns)[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state.key_columns)[0]);
        return ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data();
    }

    return ColumnHelper::as_raw_column<ColumnType>((*probe_state.key_columns)[0])->get_data();
}

template <LogicalType LT>
void JoinProbeFunc<LT>::lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state) {
    if (table_items.mode == 1) {
        do_lookup_init<1>(table_items, probe_state);
    } else if (table_items.mode == 2) {
        do_lookup_init<2>(table_items, probe_state);
    } else if (table_items.mode == 3) {
        do_lookup_init<3>(table_items, probe_state);
    } else {
        do_lookup_init<0>(table_items, probe_state);
    }
}

template <LogicalType LT>
template <uint8_t SIMD>
void JoinProbeFunc<LT>::do_lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state) {
    if constexpr (SIMD == 3) {
        if ((*probe_state->key_columns)[0]->is_nullable()) {
            const auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[0]);
            if (nullable_column->has_null()) {
                probe_state->null_array = &nullable_column->null_column()->get_data();
                return;
            }
        }

        probe_state->null_array = nullptr;
        return;
    }

    const size_t probe_row_count = probe_state->probe_row_count;

    const auto& data = get_key_data(*probe_state);
    if constexpr (SIMD == 1) {
        JoinHashMapHelper::calc_bucket_nums<CppType>(&probe_state->buckets, table_items.bucket_size << 8,
                                                     table_items.log_bucket_size + 8, data, 0, data.size());
    } else {
        JoinHashMapHelper::calc_bucket_nums<CppType>(&probe_state->buckets, table_items.bucket_size,
                                                     table_items.log_bucket_size, data, 0, data.size());
    }

    const auto* first = table_items.first.data();
    const auto* buckets = probe_state->buckets.data();
    auto* next = probe_state->next.data();

    if ((*probe_state->key_columns)[0]->is_nullable()) {
        const auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[0]);

        if (nullable_column->has_null()) {
            const auto& null_array = nullable_column->null_column()->get_data();

            if constexpr (SIMD != 2) {
                static constexpr uint32_t W = 8;
                uint32_t buffer[W];
                uint32_t bf[W];
                size_t i = 0;
                for (; i + W <= probe_row_count; i += W) {
                    for (uint32_t j = 0; j < W; j++) {
                        if (null_array[i + j] == 0) {
                            if constexpr (SIMD == 1) {
                                buffer[j] = first[buckets[i + j] >> 8];
                            } else {
                                buffer[j] = first[buckets[i + j]];
                            }
                        } else {
                            buffer[j] = 0;
                        }
                    }

                    if constexpr (SIMD == 1) {
                        for (uint32_t j = 0; j < W; j++) {
                            bf[j] = BLOOM_FILTERS[buckets[i + j] & 0xFF];
                        }

                        for (uint32_t j = 0; j < W; j++) {
                            const uint32_t matched = (~(buffer[j] >> 24) & bf[j]) == 0;
                            const uint32_t mask = ~matched + 1;
                            buffer[j] &= mask & BLOOM_FILTER_MASK;
                        }
                    }

                    for (uint32_t j = 0; j < W; j++) {
                        next[i + j] = buffer[j];
                    }
                }
                for (; i < probe_row_count; i++) {
                    if constexpr (SIMD == 1) {
                        uint32_t index = first[buckets[i] >> 8];

                        const uint32_t cur_bf = BLOOM_FILTERS[buckets[i] & 0xFF];
                        const uint32_t matched = (~(index >> 24) & cur_bf) == 0;
                        const uint32_t mask = ~matched + 1;
                        index &= mask & BLOOM_FILTER_MASK;

                        next[i] = index;
                    } else {
                        next[i] = first[buckets[i]];
                    }
                }
            }

            probe_state->null_array = &nullable_column->null_column()->get_data();
        } else {
            if constexpr (SIMD != 2) {
                static constexpr uint32_t W = 8;
                uint32_t buffer[W];
                uint32_t bf[W];
                size_t i = 0;
                for (; i + W <= probe_row_count; i += W) {
                    for (uint32_t j = 0; j < W; j++) {
                        if constexpr (SIMD == 1) {
                            buffer[j] = first[buckets[i + j] >> 8];
                        } else {
                            buffer[j] = first[buckets[i + j]];
                        }
                    }

                    if constexpr (SIMD == 1) {
                        for (uint32_t j = 0; j < W; j++) {
                            bf[j] = BLOOM_FILTERS[buckets[i + j] & 0xFF];
                        }

                        for (uint32_t j = 0; j < W; j++) {
                            const uint32_t matched = (~(buffer[j] >> 24) & bf[j]) == 0;
                            const uint32_t mask = ~matched + 1;
                            buffer[j] &= mask & BLOOM_FILTER_MASK;
                        }
                    }

                    for (uint32_t j = 0; j < W; j++) {
                        next[i + j] = buffer[j];
                    }
                }
                for (; i < probe_row_count; i++) {
                    if constexpr (SIMD == 1) {
                        uint32_t index = first[buckets[i] >> 8];

                        const uint32_t cur_bf = BLOOM_FILTERS[buckets[i] & 0xFF];
                        const uint32_t matched = (~(index >> 24) & cur_bf) == 0;
                        const uint32_t mask = ~matched + 1;
                        index &= mask & BLOOM_FILTER_MASK;

                        next[i] = index;
                    } else {
                        next[i] = first[buckets[i]];
                    }
                }
            }
            probe_state->null_array = nullptr;
        }

        probe_state->consider_probe_time_locality();
        return;
    }

    if constexpr (SIMD != 2) {
        static constexpr uint32_t W = 8;
        uint32_t buffer[W];
        uint32_t bf[W];
        size_t i = 0;
        for (; i + W <= probe_row_count; i += W) {
            for (uint32_t j = 0; j < W; j++) {
                if constexpr (SIMD) {
                    buffer[j] = first[buckets[i + j] >> 8];
                } else {
                    buffer[j] = first[buckets[i + j]];
                }
            }

            if constexpr (SIMD) {
                for (uint32_t j = 0; j < W; j++) {
                    bf[j] = BLOOM_FILTERS[buckets[i + j] & 0xFF];
                }

                for (uint32_t j = 0; j < W; j++) {
                    const uint32_t matched = (~(buffer[j] >> 24) & bf[j]) == 0;
                    const uint32_t mask = ~matched + 1;
                    buffer[j] &= mask & BLOOM_FILTER_MASK;
                }
            }

            for (uint32_t j = 0; j < W; j++) {
                next[i + j] = buffer[j];
            }
        }
        for (; i < probe_row_count; i++) {
            if constexpr (SIMD) {
                uint32_t index = first[buckets[i] >> 8];

                const uint32_t cur_bf = BLOOM_FILTERS[buckets[i] & 0xFF];
                const uint32_t matched = (~(index >> 24) & cur_bf) == 0;
                const uint32_t mask = ~matched + 1;
                index &= mask & BLOOM_FILTER_MASK;

                next[i] = index;
            } else {
                next[i] = first[buckets[i]];
            }
        }
    }

    probe_state->consider_probe_time_locality();
    probe_state->null_array = nullptr;
}

template <LogicalType LT>
const Buffer<typename JoinProbeFunc<LT>::CppType>& JoinProbeFunc<LT>::get_key_data(
        const HashTableProbeState& probe_state) {
    if ((*probe_state.key_columns)[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state.key_columns)[0]);
        return ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data();
    }

    return ColumnHelper::as_raw_column<ColumnType>((*probe_state.key_columns)[0])->get_data();
}

template <LogicalType LT>
void FixedSizeJoinProbeFunc<LT>::lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state) {
    // prepare columns
    Columns data_columns;
    NullColumns null_columns;

    for (size_t i = 0; i < probe_state->key_columns->size(); i++) {
        if (table_items.join_keys[i].is_null_safe_equal) {
            if ((*probe_state->key_columns)[i]->is_nullable()) {
                data_columns.emplace_back((*probe_state->key_columns)[i]);
            } else {
                auto tmp_column = NullableColumn::create((*probe_state->key_columns)[i],
                                                         NullColumn::create(probe_state->probe_row_count, 0));
                data_columns.emplace_back(tmp_column);
            }
        } else if ((*probe_state->key_columns)[i]->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[i]);
            data_columns.emplace_back(nullable_column->data_column());
            if ((*probe_state->key_columns)[i]->has_null()) {
                null_columns.emplace_back(nullable_column->null_column());
            }
        } else {
            data_columns.emplace_back((*probe_state->key_columns)[i]);
        }
    }

    // serialize and init search
    if (!null_columns.empty()) {
        _probe_nullable_column(table_items, probe_state, data_columns, null_columns);
    } else {
        _probe_column(table_items, probe_state, data_columns);
    }
    probe_state->consider_probe_time_locality();
}

template <LogicalType LT>
void FixedSizeJoinProbeFunc<LT>::_probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                               const Columns& data_columns) {
    uint32_t row_count = probe_state->probe_row_count;

    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, probe_state->probe_key_column.get(), 0,
                                                           row_count);
    const auto& data = get_key_data(*probe_state);
    JoinHashMapHelper::calc_bucket_nums<CppType>(&probe_state->buckets, table_items.bucket_size,
                                                 table_items.log_bucket_size, data, 0, row_count);
    probe_state->null_array = nullptr;
    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->next[i] = table_items.first[probe_state->buckets[i]];
    }
}

template <LogicalType LT>
void FixedSizeJoinProbeFunc<LT>::_probe_nullable_column(const JoinHashTableItems& table_items,
                                                        HashTableProbeState* probe_state, const Columns& data_columns,
                                                        const NullColumns& null_columns) {
    uint32_t row_count = probe_state->probe_row_count;

    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[i];
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < row_count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->get_data()[j];
        }
    }
    probe_state->null_array = &null_columns[0]->get_data();

    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, probe_state->probe_key_column.get(), 0,
                                                           row_count);
    const auto& data = get_key_data(*probe_state);
    JoinHashMapHelper::calc_bucket_nums<CppType>(&probe_state->buckets, table_items.bucket_size,
                                                 table_items.log_bucket_size, data, 0, row_count);

    for (uint32_t i = 0; i < row_count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            probe_state->next[i] = table_items.first[probe_state->buckets[i]];
        } else {
            probe_state->next[i] = 0;
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::build_prepare(RuntimeState* state) {
    BuildFunc().prepare(state, _table_items);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::probe_prepare(RuntimeState* state) {
    size_t chunk_size = state->chunk_size();
    _probe_state->build_index.resize(chunk_size + 8);
    _probe_state->probe_index.resize(chunk_size + 8);
    _probe_state->next.resize(chunk_size);
    _probe_state->probe_match_index.resize(chunk_size);
    _probe_state->probe_match_filter.resize(chunk_size);
    _probe_state->buckets.resize(chunk_size);

    if (_table_items->join_type == TJoinOp::RIGHT_OUTER_JOIN || _table_items->join_type == TJoinOp::FULL_OUTER_JOIN ||
        _table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN || _table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN) {
        _probe_state->build_match_index.resize(_table_items->row_count + 1, 0);
        _probe_state->build_match_index[0] = 1;
    }

    ProbeFunc().prepare(state, _probe_state);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::build(RuntimeState* state) {
    BuildFunc().construct_hash_table(state, _table_items, _probe_state);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::probe(RuntimeState* state, const Columns& key_columns,
                                                  ChunkPtr* probe_chunk, ChunkPtr* chunk, bool* has_remain) {
    _probe_state->key_columns = &key_columns;
    {
        SCOPED_TIMER(_probe_state->search_ht_timer);
        _search_ht(state, probe_chunk);
        if (_probe_state->count <= 0) {
            *has_remain = false;
            return;
        }

        *has_remain = _probe_state->has_remain;

        if (UNLIKELY(!_probe_state->has_remain && !_probe_state->handles.empty())) {
            std::string msg =
                    "HashJoin probe haven't remain tuples but have coroutines, likely leaking coroutines, please set "
                    "global interleaving_group_size = 0 to disable coroutines, rerun this query and report to SR";
            LOG(ERROR) << "fragment = " << print_id(state->fragment_instance_id()) << " " << msg;
            throw std::runtime_error(msg);
        }
    }

    if (_table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN || _table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN) {
        // right semi join without other join conjunct
        // right anti join without other join conjunct
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            if (_table_items->with_other_conjunct) {
                _probe_output<false>(probe_chunk, chunk);
            }
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<false>(chunk);
        }
    } else if (_table_items->join_type == TJoinOp::LEFT_SEMI_JOIN ||
               _table_items->join_type == TJoinOp::LEFT_ANTI_JOIN ||
               _table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        // left semi join without other join conjunct
        // left anti join without other join conjunct
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output<false>(probe_chunk, chunk);
        }
        {
            // output default values for build-columns as placeholder.
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            if (_table_items->mor_reader_mode) {
                return;
            }

            if (!_table_items->with_other_conjunct) {
                // When the project doesn't require any cols from join, FE will select the first col in the build table
                // of join as the output col for simple, wo we also need output build column here
                _build_default_output(chunk, _probe_state->count);
            } else {
                _build_output<false>(chunk);
            }
        }
    } else {
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output<false>(probe_chunk, chunk);
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<false>(chunk);
        }
    }

    if (_table_items->enable_late_materialization) {
        _probe_index_output(chunk);
        _build_index_output(chunk);
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) {
    _search_ht_remain(state);
    if (_probe_state->count <= 0) {
        *has_remain = false;
        return;
    }
    *has_remain = _probe_state->has_remain;

    if (_table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN || _table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN) {
        // right anti/semi join without other conjunct output default value of probe-columns as placeholder.
        if (_table_items->with_other_conjunct) {
            _probe_null_output<false>(chunk, _probe_state->count);
        }
        _build_output<false>(chunk);
    } else {
        // RIGHT_OUTER_JOIN || FULL_OUTER_JOIN
        _probe_null_output<false>(chunk, _probe_state->count);
        _build_output<false>(chunk);
    }

    if (_table_items->enable_late_materialization) {
        _build_index_output(chunk);
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool is_lazy>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk) {
    bool to_nullable = _table_items->left_to_nullable;

    for (size_t i = 0; i < _table_items->probe_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->probe_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;
        bool need_output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
        if (need_output) {
            auto& column = (*probe_chunk)->get_column_by_slot_id(slot->id());
            if (!column->is_nullable()) {
                _copy_probe_column(&column, chunk, slot, to_nullable);
            } else {
                _copy_probe_nullable_column(&column, chunk, slot);
            }
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool is_remain>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::lazy_output(RuntimeState* state, ChunkPtr* probe_chunk,
                                                        ChunkPtr* result_chunk) {
    if ((*result_chunk)->num_rows() < _probe_state->count) {
        _probe_state->match_flag = JoinMatchFlag::NORMAL;
        _probe_state->count = (*result_chunk)->num_rows();
    }

    (*result_chunk)->remove_column_by_slot_id(Chunk::HASH_JOIN_BUILD_INDEX_SLOT_ID);
    (*result_chunk)->remove_column_by_slot_id(Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);

    if (_table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN || _table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN) {
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<true>(result_chunk);
        }
    } else if (_table_items->join_type == TJoinOp::LEFT_SEMI_JOIN ||
               _table_items->join_type == TJoinOp::LEFT_ANTI_JOIN ||
               _table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output<true>(probe_chunk, result_chunk);
        }
    } else if (_table_items->join_type == TJoinOp::RIGHT_OUTER_JOIN ||
               _table_items->join_type == TJoinOp::FULL_OUTER_JOIN) {
        if constexpr (is_remain) {
            {
                SCOPED_TIMER(_probe_state->output_probe_column_timer);
                _probe_null_output<true>(result_chunk, _probe_state->count);
            }
        } else {
            {
                SCOPED_TIMER(_probe_state->output_probe_column_timer);
                _probe_output<true>(probe_chunk, result_chunk);
            }
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<true>(result_chunk);
        }
    } else {
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output<true>(probe_chunk, result_chunk);
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<true>(result_chunk);
        }
    }

    _probe_state->count = 0;
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool is_lazy>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_null_output(ChunkPtr* chunk, size_t count) {
    for (size_t i = 0; i < _table_items->probe_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->probe_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;
        bool need_output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
        if (need_output) {
            ColumnPtr column = ColumnHelper::create_column(slot->type(), true);
            column->append_nulls(count);
            (*chunk)->append_column(std::move(column), slot->id());
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool is_lazy>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_build_output(ChunkPtr* chunk) {
    bool to_nullable = _table_items->right_to_nullable;
    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->build_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;

        bool need_output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
        if (need_output) {
            ColumnPtr& column = _table_items->build_chunk->columns()[i];
            if (!column->is_nullable()) {
                _copy_build_column(column, chunk, slot, to_nullable);
            } else {
                _copy_build_nullable_column(column, chunk, slot);
            }
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_build_default_output(ChunkPtr* chunk, size_t count) {
    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        auto hash_tablet_slot = _table_items->build_slots[i];
        SlotDescriptor* slot = hash_tablet_slot.slot;
        if (hash_tablet_slot.need_output) {
            ColumnPtr column = ColumnHelper::create_column(slot->type(), true);
            column->append_nulls(count);
            (*chunk)->append_column(std::move(column), slot->id());
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk,
                                                               const SlotDescriptor* slot, bool to_nullable) {
    if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
        if (to_nullable) {
            ColumnPtr dest_column = NullableColumn::create(*src_column, NullColumn::create((*src_column)->size()));
            (*chunk)->append_column(std::move(dest_column), slot->id());
        } else {
            (*chunk)->append_column(*src_column, slot->id());
        }
    } else if (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE) {
        if (to_nullable) {
            (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
            ColumnPtr dest_column = NullableColumn::create(*src_column, NullColumn::create((*src_column)->size()));
            (*chunk)->append_column(std::move(dest_column), slot->id());
        } else {
            (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
            (*chunk)->append_column(*src_column, slot->id());
        }
    } else {
        ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), to_nullable);
        dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk,
                                                                        const SlotDescriptor* slot) {
    if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
        (*chunk)->append_column(*src_column, slot->id());
    } else if (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE) {
        (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
        (*chunk)->append_column(*src_column, slot->id());
    } else {
        ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
        dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk,
                                                               const SlotDescriptor* slot, bool to_nullable) {
    if (to_nullable) {
        auto data_column = src_column->clone_empty();
        data_column->append_selective(*src_column, _probe_state->build_index.data(), 0, _probe_state->count);

        // When left outer join is executed,
        // build_index[i] Equal to 0 means it is not found in the hash table,
        // but append_selective() has set item of NullColumn to not null
        // so NullColumn needs to be set back to null
        auto null_column = NullColumn::create(_probe_state->count, 0);
        size_t end = _probe_state->count;
        for (size_t i = 0; i < end; i++) {
            if (_probe_state->build_index[i] == 0) {
                null_column->get_data()[i] = 1;
            }
        }
        auto dest_column = NullableColumn::create(std::move(data_column), null_column);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    } else {
        auto dest_column = src_column->clone_empty();
        dest_column->append_selective(*src_column, _probe_state->build_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk,
                                                                        const SlotDescriptor* slot) {
    const uint32_t num_rows = _probe_state->count;
    const auto* build_index = _probe_state->build_index.data();

    const auto num_new_nulls = SIMD::count_zero(build_index, num_rows);
    ColumnPtr dest_column = src_column->clone_empty();
    if (num_new_nulls == num_rows) {
        dest_column->append_nulls(num_rows);
    } else {
        dest_column->append_selective(*src_column, build_index, 0, num_rows);
        // When left outer join is executed,
        // build_index[i] Equal to 0 means it is not found in the hash table,
        // but append_selective() has set item of NullColumn to not null
        // so NullColumn needs to be set back to null
        if (num_new_nulls > 0) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dest_column);
            auto* is_nulls = nullable_column->null_column_data().data();
            for (uint32_t i = 0; i < num_rows; i++) {
                is_nulls[i] |= build_index[i] == 0;
            }
            nullable_column->set_has_null(true);
        }
    }

    (*chunk)->append_column(std::move(dest_column), slot->id());
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_search_ht(RuntimeState* state, ChunkPtr* probe_chunk) {
    if (_table_items->enable_late_materialization) {
        _probe_state->probe_index.resize(state->chunk_size() + 8);
        _probe_state->build_index.resize(state->chunk_size() + 8);
    }
    if (!_probe_state->has_remain) {
        _probe_state->probe_row_count = (*probe_chunk)->num_rows();
        _probe_state->active_coroutines = state->query_options().interleaving_group_size;
        // disable adaptively interleaving if the ht may encounter seriously cache misses.
        if (state->query_options().interleaving_group_size > 0 && !_table_items->ht_cache_miss_serious()) {
            _probe_state->active_coroutines = 0;
        }
        ProbeFunc().lookup_init(*_table_items, _probe_state);

        auto& build_data = BuildFunc().get_key_data(*_table_items);
        auto& probe_data = ProbeFunc().get_key_data(*_probe_state);
        _search_ht_impl<true>(state, build_data, probe_data);
    } else {
        auto& build_data = BuildFunc().get_key_data(*_table_items);
        auto& probe_data = ProbeFunc().get_key_data(*_probe_state);
        _search_ht_impl<false>(state, build_data, probe_data);
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_search_ht_remain(RuntimeState* state) {
    if (!_probe_state->has_remain) {
        size_t zero_count = SIMD::count_zero(_probe_state->build_match_index);
        if (zero_count <= 0) {
            _probe_state->count = 0;
            _probe_state->has_remain = false;
            return;
        }
        _probe_state->cur_build_index = 0;
    }

    if (_table_items->enable_late_materialization) {
        _probe_state->build_index.resize(state->chunk_size() + 8);
    }

    size_t match_count = 0;
    size_t i = _probe_state->cur_build_index;
    for (; i < _probe_state->build_match_index.size(); i++) {
        if (_probe_state->build_match_index[i] == 0) {
            _probe_state->build_index[match_count] = i;
            _probe_state->probe_index[match_count] = 0;
            match_count++;

            if (match_count >= state->chunk_size()) {
                i++;
                _probe_state->cur_build_index = i;
                _probe_state->has_remain = i < _probe_state->build_match_index.size();
                _probe_state->count = match_count;
                return;
            }
        }
    }

    _probe_state->cur_build_index = i;
    _probe_state->has_remain = false;
    _probe_state->count = match_count;
}

#define DO_PROBE(X)                                                                                                  \
    if (_probe_state->active_coroutines != 0) {                                                                      \
        if constexpr (first_probe) {                                                                                 \
            auto group_size = std::abs(state->query_options().interleaving_group_size);                              \
            _probe_state->cur_probe_index = 0;                                                                       \
            if (!_probe_state->handles.empty()) {                                                                    \
                for (auto& h : _probe_state->handles) {                                                              \
                    h.destroy();                                                                                     \
                }                                                                                                    \
                _probe_state->handles.clear();                                                                       \
                std::string msg =                                                                                    \
                        "HashJoin probe leaks coroutines, please set global interleaving_group_size = 0 to disable " \
                        "coroutines, rerun this query and report to SR";                                             \
                LOG(ERROR) << "fragment = " + print_id(state->fragment_instance_id()) << " " << msg;                 \
                throw std::runtime_error(msg);                                                                       \
            }                                                                                                        \
            for (int i = 0; i < group_size; ++i) {                                                                   \
                _probe_state->handles.insert(X(state, build_data, data));                                            \
            }                                                                                                        \
            _probe_state->active_coroutines = group_size;                                                            \
        }                                                                                                            \
        _probe_coroutine<first_probe>(state, build_data, data);                                                      \
    } else {                                                                                                         \
        X<first_probe>(state, build_data, data);                                                                     \
    }

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_search_ht_impl(RuntimeState* state, const Buffer<CppType>& build_data,
                                                            const Buffer<CppType>& data) {
    if (!_table_items->with_other_conjunct) {
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_OUTER_JOIN:
            DO_PROBE(_probe_from_ht_for_left_outer_join);
            break;
        case TJoinOp::LEFT_SEMI_JOIN:
            DO_PROBE(_probe_from_ht_for_left_semi_join);
            break;
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
            DO_PROBE(_probe_from_ht_for_left_anti_join);
            break;
        case TJoinOp::RIGHT_OUTER_JOIN:
            DO_PROBE(_probe_from_ht_for_right_outer_join);
            break;
        case TJoinOp::RIGHT_SEMI_JOIN:
            DO_PROBE(_probe_from_ht_for_right_semi_join);
            break;
        case TJoinOp::RIGHT_ANTI_JOIN:
            DO_PROBE(_probe_from_ht_for_right_anti_join);
            break;
        case TJoinOp::FULL_OUTER_JOIN:
            DO_PROBE(_probe_from_ht_for_full_outer_join);
            break;
        default:
            DO_PROBE(_probe_from_ht);
            break;
        }
    } else {
        // as probing results of join keys are not clustered in one chunk, `probe_match_index` and `build_match_index`
        // are not completely right, resulting in wrong results when filtering other conjunct.
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_SEMI_JOIN:
            _probe_from_ht_for_left_semi_join_with_other_conjunct<first_probe>(state, build_data, data);
            break;
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
            _probe_from_ht_for_null_aware_anti_join_with_other_conjunct<first_probe>(state, build_data, data);
            break;
        case TJoinOp::RIGHT_OUTER_JOIN:
        case TJoinOp::RIGHT_SEMI_JOIN:
        case TJoinOp::RIGHT_ANTI_JOIN:
            _probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct<first_probe>(
                    state, build_data, data);
            break;
        case TJoinOp::LEFT_OUTER_JOIN:
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::FULL_OUTER_JOIN:
            _probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct<first_probe>(state, build_data,
                                                                                                     data);
            break;
        default:
            // can't reach here
            _probe_from_ht<first_probe>(state, build_data, data);
            break;
        }
    }
}

#define CHECK_MATCH()                                                                                          \
    if (match_count > 0 && !one_to_many) {                                                                     \
        size_t zero_count = SIMD::count_zero(_probe_state->probe_match_filter, _probe_state->probe_row_count); \
        if (zero_count == 0) {                                                                                 \
            _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;                                           \
        } else if (zero_count < _probe_state->probe_row_count - zero_count) {                                  \
            _probe_state->match_flag = JoinMatchFlag::MOST_MATCH_ONE;                                          \
        }                                                                                                      \
    }

#define CHECK_ALL_MATCH()                                        \
    if (match_count > 0 && !one_to_many) {                       \
        _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE; \
    }

#define RETURN_IF_CHUNK_FULL()                                   \
    if (UNLIKELY(match_count > state->chunk_size())) {           \
        _probe_state->next[i] = _table_items->next[build_index]; \
        _probe_state->cur_probe_index = i;                       \
        _probe_state->cur_build_index = build_index;             \
        _probe_state->has_remain = true;                         \
        _probe_state->count = state->chunk_size();               \
        return;                                                  \
    }

#define RETURN_IF_CHUNK_FULL2()                                      \
    if (UNLIKELY(match_count > state->chunk_size())) {               \
        if constexpr (SIMD == 1) {                                   \
            _probe_state->next[i] = _table_items->next[build_index]; \
        } else {                                                     \
            _probe_state->next[i] = _table_items->next[build_index]; \
        }                                                            \
        _probe_state->cur_probe_index = i;                           \
        _probe_state->cur_build_index = build_index;                 \
        _probe_state->has_remain = true;                             \
        _probe_state->count = state->chunk_size();                   \
        _probe_state->cur_row_match_count = cur_row_match_count;     \
        return;                                                      \
    }

#define COWAIT_IF_CHUNK_FULL()                              \
    if (_probe_state->match_count == state->chunk_size()) { \
        _probe_state->has_remain = true;                    \
        _probe_state->count = state->chunk_size();          \
        co_await std::suspend_always{};                     \
    }

#define REORDER_PROBE_INDEX()                                                                                         \
    if (_probe_state->match_flag != JoinMatchFlag::NORMAL) {                                                          \
        Buffer<uint32_t> permutation(_probe_state->probe_index.size(), -1);                                           \
        for (auto i = 0; i < _probe_state->match_count; ++i) {                                                        \
            permutation[_probe_state->probe_index[i]] = i;                                                            \
        }                                                                                                             \
        Buffer<uint32_t> new_order(_probe_state->build_index.size(), 0);                                              \
        uint32_t count = 0;                                                                                           \
        for (auto i = 0; i < _probe_state->probe_row_count; ++i) {                                                    \
            if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE ||                                           \
                (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE && _probe_state->probe_match_filter[i])) { \
                DCHECK(permutation[i] != -1);                                                                         \
                new_order[count++] = _probe_state->build_index[permutation[i]];                                       \
            }                                                                                                         \
        }                                                                                                             \
        if (UNLIKELY(count != _probe_state->match_count)) {                                                           \
            auto msg = fmt::format("Coroutine join match count {} != expected {}", count, _probe_state->match_count); \
            LOG(WARNING) << msg;                                                                                      \
            throw std::runtime_error(msg);                                                                            \
        }                                                                                                             \
        _probe_state->build_index.swap(new_order);                                                                    \
    }

#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86)) /* _mm_prefetch() not defined outside of x86/x64 */
#include <mmintrin.h> /* https://msdn.microsoft.com/fr-fr/library/84szxsww(v=vs.90).aspx */
#define XXH_PREFETCH(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#elif defined(__GNUC__) && ((__GNUC__ >= 4) || ((__GNUC__ == 3) && (__GNUC_MINOR__ >= 1)))
#define XXH_PREFETCH(ptr) __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)
#endif

#define PREFETCH_AND_COWAIT(x, y) \
    XXH_PREFETCH(x);              \
    XXH_PREFETCH(y);              \
    co_await std::suspend_always{};

// When a probe row corresponds to multiple Build rows,
// a Probe Chunk may generate multiple ResultChunks,
// so each probe will have search one more row to determine whether it has reached the boundary,
// so the next probe will start from the last recorded position
#define PROCESS_PROBE_STAGE_FOR_RIGHT_JOIN_WITH_OTHER_CONJUNCT()      \
    if constexpr (!first_probe) {                                     \
        _probe_state->probe_index[0] = _probe_state->cur_probe_index; \
        _probe_state->build_index[0] = _probe_state->cur_build_index; \
        match_count = 1;                                              \
        if (_probe_state->next[i] == 0) {                             \
            i++;                                                      \
        }                                                             \
    }

#define PROBE_OVER()                   \
    _probe_state->has_remain = false;  \
    _probe_state->cur_probe_index = 0; \
    _probe_state->cur_build_index = 0; \
    _probe_state->count = match_count; \
    _probe_state->cur_row_match_count = 0;

#define MATCH_RIGHT_TABLE_ROWS()                \
    _probe_state->probe_index[match_count] = i; \
    _probe_state->build_index[match_count] = j; \
    _probe_state->probe_match_index[i]++;       \
    match_count++;                              \
    _probe_state->cur_row_match_count++;

/// TODO (fzh): calculate hash distribution, skew or not.
// NOTE: coroutine only SIMD code of SSE but not AVX
template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_coroutine(RuntimeState* state, const Buffer<CppType>& build_data,
                                                             const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;
    _probe_state->match_count = 0;
    _probe_state->cur_row_match_count = 0;
    _probe_state->count = 0;
    // disorder probe id as matching steps are different for each probe
    while (!_probe_state->handles.empty()) {
        for (auto it = _probe_state->handles.begin(); it != _probe_state->handles.end();) {
            if (it->promise().exception != nullptr) {
                LOG(WARNING) << print_id(state->fragment_instance_id()) << " coroutine rethrow exceptions";
                std::rethrow_exception(it->promise().exception);
            }
            if (it->done()) {
                it->destroy();
                it = _probe_state->handles.erase(it);
            } else {
                it->resume();
                it++;
            }
            if (_probe_state->count == state->chunk_size() && _probe_state->has_remain) {
                return;
            }
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data,
                                                           const Buffer<CppType>& probe_data) {
    if (std::is_same_v<BuildFunc, JoinBuildFunc<LT>> && abs(config::enable_simd_hash_join) == 1 &&
        _table_items->bucket_size <= BLOOM_FILTER_MASK) {
        if (_table_items->no_conflicts) {
            if (_table_items->no_duplicated_build_keys) {
                _do_probe_from_ht<first_probe, true, true, 1>(state, build_data, probe_data);
            } else {
                _do_probe_from_ht<first_probe, true, false, 1>(state, build_data, probe_data);
            }
        } else {
            if (_table_items->no_duplicated_build_keys) {
                _do_probe_from_ht<first_probe, false, true, 1>(state, build_data, probe_data);
            } else {
                _do_probe_from_ht<first_probe, false, false, 1>(state, build_data, probe_data);
            }
        }
    } else {
        if (_table_items->no_conflicts) {
            _do_probe_from_ht<first_probe, true, false, 0>(state, build_data, probe_data);
        } else {
            _do_probe_from_ht<first_probe, false, false, 0>(state, build_data, probe_data);
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe, bool no_conflicts, bool no_duplicated_build_keys, uint8_t SIMD>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_do_probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data,
                                                              const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;
    size_t match_count = 0;
    bool one_to_many = false;
    uint32_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) { // chunk_size + 1 probe
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;

        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    }

    [[maybe_unused]] size_t probe_cont = 0;

    if constexpr (first_probe) {
        memset(_probe_state->probe_match_filter.data(), 0, _probe_state->probe_row_count * sizeof(uint8_t));
    }

    const size_t probe_row_count = _probe_state->probe_row_count;
    const auto* nexts = _probe_state->next.data();
    uint32_t cur_row_match_count = _probe_state->cur_row_match_count;
    for (; i < probe_row_count; i++) {
        uint32_t build_index = nexts[i];

        if (build_index == 0) {
            continue;
        }

        do {
            probe_cont++;
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                match_count++;

                if constexpr (first_probe) {
                    cur_row_match_count++;
                    _probe_state->probe_match_filter[i] = 1;
                }

                if constexpr (!no_conflicts && !no_duplicated_build_keys) {
                    RETURN_IF_CHUNK_FULL2()
                }

                if (no_duplicated_build_keys) {
                    break;
                }
            }

            if constexpr (no_conflicts) {
                break;
            }

            build_index = _table_items->next[build_index];
        } while (build_index != 0);

        if constexpr (first_probe && (!no_conflicts || !no_duplicated_build_keys)) {
            if (cur_row_match_count > 1) {
                one_to_many = true;
            }
            cur_row_match_count = 0;
        }
    }

    COUNTER_UPDATE(_probe_state->probe_counter, probe_cont);

    if constexpr (first_probe) {
        CHECK_MATCH()
    }
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        _probe_state->probe_match_filter[i] = 0;
        uint32_t cur_row_match_count = 0;
        size_t build_index = _probe_state->next[i];
        if (build_index != 0) {
            do {
                PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    COWAIT_IF_CHUNK_FULL()
                    _probe_state->probe_index[_probe_state->match_count] = i;
                    _probe_state->build_index[_probe_state->match_count] = build_index;
                    _probe_state->match_count++;
                    cur_row_match_count++;
                    _probe_state->probe_match_filter[i] = 1;
                }
                build_index = _table_items->next[build_index];
            } while (build_index != 0);

            if (cur_row_match_count > 1) {
                _probe_state->cur_row_match_count = cur_row_match_count; // means one_to_many match
            }
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    bool one_to_many = _probe_state->cur_row_match_count > 1;
    if (!_probe_state->has_remain) {
        CHECK_MATCH()
        REORDER_PROBE_INDEX()
    }
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        int cur_row_match_count = 0;
        size_t build_index = _probe_state->next[i];
        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                COWAIT_IF_CHUNK_FULL()
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->build_index[_probe_state->match_count] = build_index;
                _probe_state->match_count++;
                cur_row_match_count++;
            }
            build_index = _table_items->next[build_index];
        }
        if (cur_row_match_count <= 0) {
            COWAIT_IF_CHUNK_FULL()
            // one key of left table match none key of right table
            _probe_state->probe_index[_probe_state->match_count] = i;
            _probe_state->build_index[_probe_state->match_count] = 0;
            _probe_state->match_count++;
        } else if (cur_row_match_count > 1) {
            // one key of left table match multi key of right table
            _probe_state->cur_row_match_count = cur_row_match_count;
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    bool one_to_many = _probe_state->cur_row_match_count > 1;
    if (!_probe_state->has_remain) {
        CHECK_ALL_MATCH()
        REORDER_PROBE_INDEX()
    }
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_outer_join(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    if (std::is_same_v<BuildFunc, JoinBuildFunc<LT>> && abs(config::enable_simd_hash_join) == 1 &&
        _table_items->bucket_size <= BLOOM_FILTER_MASK) {
        if (_table_items->no_conflicts) {
            _do_probe_from_ht_for_left_outer_join<first_probe, true, 1>(state, build_data, probe_data);
        } else {
            _do_probe_from_ht_for_left_outer_join<first_probe, false, 1>(state, build_data, probe_data);
        }
    } else {
        if (_table_items->no_conflicts) {
            _do_probe_from_ht_for_left_outer_join<first_probe, true, 0>(state, build_data, probe_data);
        } else {
            _do_probe_from_ht_for_left_outer_join<first_probe, false, 0>(state, build_data, probe_data);
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe, bool no_conflicts, uint8_t SIMD>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_do_probe_from_ht_for_left_outer_join(RuntimeState* state,
                                                                                  const Buffer<CppType>& build_data,
                                                                                  const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;
    size_t match_count = 0;
    bool one_to_many = false;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    }

    const auto* nexts = _probe_state->next.data();
    const size_t probe_row_count = _probe_state->probe_row_count;
    uint32_t cur_row_match_count = _probe_state->cur_row_match_count;
    for (; i < probe_row_count; i++) {
        const uint32_t raw_build_index = nexts[i];
        uint32_t build_index = raw_build_index;

        if (build_index == 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            if constexpr (!no_conflicts) {
                RETURN_IF_CHUNK_FULL2()
            }
            cur_row_match_count = 0;
            continue;
        }

        if constexpr (no_conflicts) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                match_count++;
            } else {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = 0;
                match_count++;
            }
            cur_row_match_count = 0;
        } else {
            do {
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    _probe_state->probe_index[match_count] = i;
                    _probe_state->build_index[match_count] = build_index;
                    match_count++;
                    cur_row_match_count++;

                    RETURN_IF_CHUNK_FULL2()
                }

                build_index = _table_items->next[build_index];
            } while (build_index != 0);

            if (cur_row_match_count <= 0) {
                // one key of left table match none key of right table
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = 0;
                match_count++;

                RETURN_IF_CHUNK_FULL2()
            } else if (cur_row_match_count > 1) {
                // one key of left table match multi key of right table
                if constexpr (first_probe) {
                    one_to_many = true;
                }
            }

            cur_row_match_count = 0;
        }
    }

    if constexpr (first_probe) {
        CHECK_ALL_MATCH()
    }
    PROBE_OVER()
}
template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_semi_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
                break;
            }
            build_index = _table_items->next[build_index];
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_semi_join(RuntimeState* state,
                                                                              const Buffer<CppType>& build_data,
                                                                              const Buffer<CppType>& probe_data) {
    if (_table_items->mode == 2) {
        _do_probe_from_ht_for_left_semi_join<first_probe, false, 2>(state, build_data, probe_data);
    } else if (_table_items->mode == 3) {
        _do_probe_from_ht_for_left_semi_join<first_probe, false, 3>(state, build_data, probe_data);
    } else if (_table_items->mode == 1) {
        if (_table_items->no_conflicts) {
            _do_probe_from_ht_for_left_semi_join<first_probe, true, 1>(state, build_data, probe_data);
        } else {
            _do_probe_from_ht_for_left_semi_join<first_probe, false, 1>(state, build_data, probe_data);
        }
    } else {
        if (_table_items->no_conflicts) {
            _do_probe_from_ht_for_left_semi_join<first_probe, true, 0>(state, build_data, probe_data);
        } else {
            _do_probe_from_ht_for_left_semi_join<first_probe, false, 0>(state, build_data, probe_data);
        }
    }
}

// 8*256 = 2048 byte
static constexpr uint8_t move_left_mask_perm[256][8] = {
        {0, 1, 2, 3, 4, 5, 6, 7}, {0, 1, 2, 3, 4, 5, 6, 7}, {1, 0, 2, 3, 4, 5, 6, 7}, {0, 1, 2, 3, 4, 5, 6, 7},
        {2, 0, 1, 3, 4, 5, 6, 7}, {0, 2, 1, 3, 4, 5, 6, 7}, {1, 2, 0, 3, 4, 5, 6, 7}, {0, 1, 2, 3, 4, 5, 6, 7},
        {3, 0, 1, 2, 4, 5, 6, 7}, {0, 3, 1, 2, 4, 5, 6, 7}, {1, 3, 0, 2, 4, 5, 6, 7}, {0, 1, 3, 2, 4, 5, 6, 7},
        {2, 3, 0, 1, 4, 5, 6, 7}, {0, 2, 3, 1, 4, 5, 6, 7}, {1, 2, 3, 0, 4, 5, 6, 7}, {0, 1, 2, 3, 4, 5, 6, 7},
        {4, 0, 1, 2, 3, 5, 6, 7}, {0, 4, 1, 2, 3, 5, 6, 7}, {1, 4, 0, 2, 3, 5, 6, 7}, {0, 1, 4, 2, 3, 5, 6, 7},
        {2, 4, 0, 1, 3, 5, 6, 7}, {0, 2, 4, 1, 3, 5, 6, 7}, {1, 2, 4, 0, 3, 5, 6, 7}, {0, 1, 2, 4, 3, 5, 6, 7},
        {3, 4, 0, 1, 2, 5, 6, 7}, {0, 3, 4, 1, 2, 5, 6, 7}, {1, 3, 4, 0, 2, 5, 6, 7}, {0, 1, 3, 4, 2, 5, 6, 7},
        {2, 3, 4, 0, 1, 5, 6, 7}, {0, 2, 3, 4, 1, 5, 6, 7}, {1, 2, 3, 4, 0, 5, 6, 7}, {0, 1, 2, 3, 4, 5, 6, 7},
        {5, 0, 1, 2, 3, 4, 6, 7}, {0, 5, 1, 2, 3, 4, 6, 7}, {1, 5, 0, 2, 3, 4, 6, 7}, {0, 1, 5, 2, 3, 4, 6, 7},
        {2, 5, 0, 1, 3, 4, 6, 7}, {0, 2, 5, 1, 3, 4, 6, 7}, {1, 2, 5, 0, 3, 4, 6, 7}, {0, 1, 2, 5, 3, 4, 6, 7},
        {3, 5, 0, 1, 2, 4, 6, 7}, {0, 3, 5, 1, 2, 4, 6, 7}, {1, 3, 5, 0, 2, 4, 6, 7}, {0, 1, 3, 5, 2, 4, 6, 7},
        {2, 3, 5, 0, 1, 4, 6, 7}, {0, 2, 3, 5, 1, 4, 6, 7}, {1, 2, 3, 5, 0, 4, 6, 7}, {0, 1, 2, 3, 5, 4, 6, 7},
        {4, 5, 0, 1, 2, 3, 6, 7}, {0, 4, 5, 1, 2, 3, 6, 7}, {1, 4, 5, 0, 2, 3, 6, 7}, {0, 1, 4, 5, 2, 3, 6, 7},
        {2, 4, 5, 0, 1, 3, 6, 7}, {0, 2, 4, 5, 1, 3, 6, 7}, {1, 2, 4, 5, 0, 3, 6, 7}, {0, 1, 2, 4, 5, 3, 6, 7},
        {3, 4, 5, 0, 1, 2, 6, 7}, {0, 3, 4, 5, 1, 2, 6, 7}, {1, 3, 4, 5, 0, 2, 6, 7}, {0, 1, 3, 4, 5, 2, 6, 7},
        {2, 3, 4, 5, 0, 1, 6, 7}, {0, 2, 3, 4, 5, 1, 6, 7}, {1, 2, 3, 4, 5, 0, 6, 7}, {0, 1, 2, 3, 4, 5, 6, 7},
        {6, 0, 1, 2, 3, 4, 5, 7}, {0, 6, 1, 2, 3, 4, 5, 7}, {1, 6, 0, 2, 3, 4, 5, 7}, {0, 1, 6, 2, 3, 4, 5, 7},
        {2, 6, 0, 1, 3, 4, 5, 7}, {0, 2, 6, 1, 3, 4, 5, 7}, {1, 2, 6, 0, 3, 4, 5, 7}, {0, 1, 2, 6, 3, 4, 5, 7},
        {3, 6, 0, 1, 2, 4, 5, 7}, {0, 3, 6, 1, 2, 4, 5, 7}, {1, 3, 6, 0, 2, 4, 5, 7}, {0, 1, 3, 6, 2, 4, 5, 7},
        {2, 3, 6, 0, 1, 4, 5, 7}, {0, 2, 3, 6, 1, 4, 5, 7}, {1, 2, 3, 6, 0, 4, 5, 7}, {0, 1, 2, 3, 6, 4, 5, 7},
        {4, 6, 0, 1, 2, 3, 5, 7}, {0, 4, 6, 1, 2, 3, 5, 7}, {1, 4, 6, 0, 2, 3, 5, 7}, {0, 1, 4, 6, 2, 3, 5, 7},
        {2, 4, 6, 0, 1, 3, 5, 7}, {0, 2, 4, 6, 1, 3, 5, 7}, {1, 2, 4, 6, 0, 3, 5, 7}, {0, 1, 2, 4, 6, 3, 5, 7},
        {3, 4, 6, 0, 1, 2, 5, 7}, {0, 3, 4, 6, 1, 2, 5, 7}, {1, 3, 4, 6, 0, 2, 5, 7}, {0, 1, 3, 4, 6, 2, 5, 7},
        {2, 3, 4, 6, 0, 1, 5, 7}, {0, 2, 3, 4, 6, 1, 5, 7}, {1, 2, 3, 4, 6, 0, 5, 7}, {0, 1, 2, 3, 4, 6, 5, 7},
        {5, 6, 0, 1, 2, 3, 4, 7}, {0, 5, 6, 1, 2, 3, 4, 7}, {1, 5, 6, 0, 2, 3, 4, 7}, {0, 1, 5, 6, 2, 3, 4, 7},
        {2, 5, 6, 0, 1, 3, 4, 7}, {0, 2, 5, 6, 1, 3, 4, 7}, {1, 2, 5, 6, 0, 3, 4, 7}, {0, 1, 2, 5, 6, 3, 4, 7},
        {3, 5, 6, 0, 1, 2, 4, 7}, {0, 3, 5, 6, 1, 2, 4, 7}, {1, 3, 5, 6, 0, 2, 4, 7}, {0, 1, 3, 5, 6, 2, 4, 7},
        {2, 3, 5, 6, 0, 1, 4, 7}, {0, 2, 3, 5, 6, 1, 4, 7}, {1, 2, 3, 5, 6, 0, 4, 7}, {0, 1, 2, 3, 5, 6, 4, 7},
        {4, 5, 6, 0, 1, 2, 3, 7}, {0, 4, 5, 6, 1, 2, 3, 7}, {1, 4, 5, 6, 0, 2, 3, 7}, {0, 1, 4, 5, 6, 2, 3, 7},
        {2, 4, 5, 6, 0, 1, 3, 7}, {0, 2, 4, 5, 6, 1, 3, 7}, {1, 2, 4, 5, 6, 0, 3, 7}, {0, 1, 2, 4, 5, 6, 3, 7},
        {3, 4, 5, 6, 0, 1, 2, 7}, {0, 3, 4, 5, 6, 1, 2, 7}, {1, 3, 4, 5, 6, 0, 2, 7}, {0, 1, 3, 4, 5, 6, 2, 7},
        {2, 3, 4, 5, 6, 0, 1, 7}, {0, 2, 3, 4, 5, 6, 1, 7}, {1, 2, 3, 4, 5, 6, 0, 7}, {0, 1, 2, 3, 4, 5, 6, 7},
        {7, 0, 1, 2, 3, 4, 5, 6}, {0, 7, 1, 2, 3, 4, 5, 6}, {1, 7, 0, 2, 3, 4, 5, 6}, {0, 1, 7, 2, 3, 4, 5, 6},
        {2, 7, 0, 1, 3, 4, 5, 6}, {0, 2, 7, 1, 3, 4, 5, 6}, {1, 2, 7, 0, 3, 4, 5, 6}, {0, 1, 2, 7, 3, 4, 5, 6},
        {3, 7, 0, 1, 2, 4, 5, 6}, {0, 3, 7, 1, 2, 4, 5, 6}, {1, 3, 7, 0, 2, 4, 5, 6}, {0, 1, 3, 7, 2, 4, 5, 6},
        {2, 3, 7, 0, 1, 4, 5, 6}, {0, 2, 3, 7, 1, 4, 5, 6}, {1, 2, 3, 7, 0, 4, 5, 6}, {0, 1, 2, 3, 7, 4, 5, 6},
        {4, 7, 0, 1, 2, 3, 5, 6}, {0, 4, 7, 1, 2, 3, 5, 6}, {1, 4, 7, 0, 2, 3, 5, 6}, {0, 1, 4, 7, 2, 3, 5, 6},
        {2, 4, 7, 0, 1, 3, 5, 6}, {0, 2, 4, 7, 1, 3, 5, 6}, {1, 2, 4, 7, 0, 3, 5, 6}, {0, 1, 2, 4, 7, 3, 5, 6},
        {3, 4, 7, 0, 1, 2, 5, 6}, {0, 3, 4, 7, 1, 2, 5, 6}, {1, 3, 4, 7, 0, 2, 5, 6}, {0, 1, 3, 4, 7, 2, 5, 6},
        {2, 3, 4, 7, 0, 1, 5, 6}, {0, 2, 3, 4, 7, 1, 5, 6}, {1, 2, 3, 4, 7, 0, 5, 6}, {0, 1, 2, 3, 4, 7, 5, 6},
        {5, 7, 0, 1, 2, 3, 4, 6}, {0, 5, 7, 1, 2, 3, 4, 6}, {1, 5, 7, 0, 2, 3, 4, 6}, {0, 1, 5, 7, 2, 3, 4, 6},
        {2, 5, 7, 0, 1, 3, 4, 6}, {0, 2, 5, 7, 1, 3, 4, 6}, {1, 2, 5, 7, 0, 3, 4, 6}, {0, 1, 2, 5, 7, 3, 4, 6},
        {3, 5, 7, 0, 1, 2, 4, 6}, {0, 3, 5, 7, 1, 2, 4, 6}, {1, 3, 5, 7, 0, 2, 4, 6}, {0, 1, 3, 5, 7, 2, 4, 6},
        {2, 3, 5, 7, 0, 1, 4, 6}, {0, 2, 3, 5, 7, 1, 4, 6}, {1, 2, 3, 5, 7, 0, 4, 6}, {0, 1, 2, 3, 5, 7, 4, 6},
        {4, 5, 7, 0, 1, 2, 3, 6}, {0, 4, 5, 7, 1, 2, 3, 6}, {1, 4, 5, 7, 0, 2, 3, 6}, {0, 1, 4, 5, 7, 2, 3, 6},
        {2, 4, 5, 7, 0, 1, 3, 6}, {0, 2, 4, 5, 7, 1, 3, 6}, {1, 2, 4, 5, 7, 0, 3, 6}, {0, 1, 2, 4, 5, 7, 3, 6},
        {3, 4, 5, 7, 0, 1, 2, 6}, {0, 3, 4, 5, 7, 1, 2, 6}, {1, 3, 4, 5, 7, 0, 2, 6}, {0, 1, 3, 4, 5, 7, 2, 6},
        {2, 3, 4, 5, 7, 0, 1, 6}, {0, 2, 3, 4, 5, 7, 1, 6}, {1, 2, 3, 4, 5, 7, 0, 6}, {0, 1, 2, 3, 4, 5, 7, 6},
        {6, 7, 0, 1, 2, 3, 4, 5}, {0, 6, 7, 1, 2, 3, 4, 5}, {1, 6, 7, 0, 2, 3, 4, 5}, {0, 1, 6, 7, 2, 3, 4, 5},
        {2, 6, 7, 0, 1, 3, 4, 5}, {0, 2, 6, 7, 1, 3, 4, 5}, {1, 2, 6, 7, 0, 3, 4, 5}, {0, 1, 2, 6, 7, 3, 4, 5},
        {3, 6, 7, 0, 1, 2, 4, 5}, {0, 3, 6, 7, 1, 2, 4, 5}, {1, 3, 6, 7, 0, 2, 4, 5}, {0, 1, 3, 6, 7, 2, 4, 5},
        {2, 3, 6, 7, 0, 1, 4, 5}, {0, 2, 3, 6, 7, 1, 4, 5}, {1, 2, 3, 6, 7, 0, 4, 5}, {0, 1, 2, 3, 6, 7, 4, 5},
        {4, 6, 7, 0, 1, 2, 3, 5}, {0, 4, 6, 7, 1, 2, 3, 5}, {1, 4, 6, 7, 0, 2, 3, 5}, {0, 1, 4, 6, 7, 2, 3, 5},
        {2, 4, 6, 7, 0, 1, 3, 5}, {0, 2, 4, 6, 7, 1, 3, 5}, {1, 2, 4, 6, 7, 0, 3, 5}, {0, 1, 2, 4, 6, 7, 3, 5},
        {3, 4, 6, 7, 0, 1, 2, 5}, {0, 3, 4, 6, 7, 1, 2, 5}, {1, 3, 4, 6, 7, 0, 2, 5}, {0, 1, 3, 4, 6, 7, 2, 5},
        {2, 3, 4, 6, 7, 0, 1, 5}, {0, 2, 3, 4, 6, 7, 1, 5}, {1, 2, 3, 4, 6, 7, 0, 5}, {0, 1, 2, 3, 4, 6, 7, 5},
        {5, 6, 7, 0, 1, 2, 3, 4}, {0, 5, 6, 7, 1, 2, 3, 4}, {1, 5, 6, 7, 0, 2, 3, 4}, {0, 1, 5, 6, 7, 2, 3, 4},
        {2, 5, 6, 7, 0, 1, 3, 4}, {0, 2, 5, 6, 7, 1, 3, 4}, {1, 2, 5, 6, 7, 0, 3, 4}, {0, 1, 2, 5, 6, 7, 3, 4},
        {3, 5, 6, 7, 0, 1, 2, 4}, {0, 3, 5, 6, 7, 1, 2, 4}, {1, 3, 5, 6, 7, 0, 2, 4}, {0, 1, 3, 5, 6, 7, 2, 4},
        {2, 3, 5, 6, 7, 0, 1, 4}, {0, 2, 3, 5, 6, 7, 1, 4}, {1, 2, 3, 5, 6, 7, 0, 4}, {0, 1, 2, 3, 5, 6, 7, 4},
        {4, 5, 6, 7, 0, 1, 2, 3}, {0, 4, 5, 6, 7, 1, 2, 3}, {1, 4, 5, 6, 7, 0, 2, 3}, {0, 1, 4, 5, 6, 7, 2, 3},
        {2, 4, 5, 6, 7, 0, 1, 3}, {0, 2, 4, 5, 6, 7, 1, 3}, {1, 2, 4, 5, 6, 7, 0, 3}, {0, 1, 2, 4, 5, 6, 7, 3},
        {3, 4, 5, 6, 7, 0, 1, 2}, {0, 3, 4, 5, 6, 7, 1, 2}, {1, 3, 4, 5, 6, 7, 0, 2}, {0, 1, 3, 4, 5, 6, 7, 2},
        {2, 3, 4, 5, 6, 7, 0, 1}, {0, 2, 3, 4, 5, 6, 7, 1}, {1, 2, 3, 4, 5, 6, 7, 0}, {0, 1, 2, 3, 4, 5, 6, 7},
};

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe, bool no_conflicts, uint8_t MODE>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_do_probe_from_ht_for_left_semi_join(RuntimeState* state,
                                                                                 const Buffer<CppType>& build_data,
                                                                                 const Buffer<CppType>& probe_data) {
    const size_t probe_row_count = _probe_state->probe_row_count;
    auto* dst_probe_indexes = _probe_state->probe_index.data();

    uint32_t match_count = 0;

    if constexpr (MODE == 3 && std::is_integral_v<CppType> && sizeof(CppType) == 4) {
        const uint32_t min_value = _table_items->min_value;
        const uint32_t max_value = _table_items->max_value;
        const uint32_t group_mask = _table_items->bucket_size / 8 - 1;

        const auto* probe_values = reinterpret_cast<const uint32_t*>(probe_data.data());
        const auto* build_buckets = _table_items->set_has_value.data();

        uint8_t* dst_matches = _probe_state->probe_match_filter.data();
        memset(dst_matches, 0, sizeof(uint8_t) * probe_row_count);

        uint32_t i = 0;

#if defined(__AVX2__)
        static constexpr uint32_t W = 8;

        const __m256i vmin_value = _mm256_set1_epi32(min_value);
        const __m256i vmax_value = _mm256_set1_epi32(max_value);

        for (; i + W <= probe_row_count; i += W) {
            __m256i vprobe_values = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(probe_values + i));

            // vprobe_values >= vmin_value  not -> vmin_value > vprobe_values
            // vmax_value >= vprobe_values  not -> vprobe_values > vmax_value
            __m256i vnot_in_range = _mm256_or_epi32(_mm256_cmpgt_epi32(vmin_value, vprobe_values),
                                                    _mm256_cmpgt_epi32(vprobe_values, vmax_value));
            uint8_t not_in_range_mask = _mm256_movemask_ps(_mm256_castsi256_ps(vnot_in_range));
            if (not_in_range_mask == 0xFF) {
                continue;
            }

            __m256i vbuckets = _mm256_sub_epi32(vprobe_values, vmin_value);
            vbuckets = _mm256_blend_epi32(vbuckets, _mm256_setzero_si256(), not_in_range_mask);
            __m256i vgroups = _mm256_srli_epi32(vbuckets, 3);
            __m256i voffsets = _mm256_and_si256(vbuckets, _mm256_set1_epi32(7));
            voffsets = _mm256_sllv_epi32(_mm256_set1_epi32(1), voffsets);

            __m256i vbuild_buckets = _mm256_set_epi32(
                    build_buckets[_mm256_extract_epi32(vgroups, 7)], build_buckets[_mm256_extract_epi32(vgroups, 6)],
                    build_buckets[_mm256_extract_epi32(vgroups, 5)], build_buckets[_mm256_extract_epi32(vgroups, 4)],
                    build_buckets[_mm256_extract_epi32(vgroups, 3)], build_buckets[_mm256_extract_epi32(vgroups, 2)],
                    build_buckets[_mm256_extract_epi32(vgroups, 1)], build_buckets[_mm256_extract_epi32(vgroups, 0)]);

            __m256i vnot_match = _mm256_cmpeq_epi32(_mm256_and_si256(vbuild_buckets, voffsets), _mm256_setzero_si256());
            uint8_t not_match_mask = _mm256_movemask_ps(_mm256_castsi256_ps(vnot_match));
            uint8_t match_mask = ~(not_match_mask | not_in_range_mask);

            uint64_t result = (static_cast<uint64_t>(match_mask) * 0xFF) & 0x0101'0101'0101'0101ull;
            reinterpret_cast<uint64_t*>(dst_matches)[i] = result;
            match_count += __builtin_popcount(match_mask);
        }
#endif

        for (; i < probe_row_count; i++) {
            const uint32_t value = probe_values[i];

            const uint32_t bucket = value - min_value;
            const uint32_t group = (bucket / 8) & group_mask;
            const uint32_t offset = bucket % 8;
            bool matched = (min_value <= value) & (value <= max_value) & ((build_buckets[group] & (1 << offset)) != 0);

            dst_matches[i] = matched;
            match_count += matched;
        }

        if (match_count == probe_row_count) {
            _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;
        } else {
            _probe_state->match_flag = JoinMatchFlag::MOST_MATCH_ONE;
        }

        PROBE_OVER()
        return;
    } else if constexpr (MODE == 2) {
        const uint32_t bucket_size_mask = _table_items->bucket_size - 1;
        const auto* build_buckets = _table_items->first.data();
        const auto* probe_buckets = _probe_state->buckets.data();
        const auto* probe_keys = reinterpret_cast<const uint32_t*>(probe_data.data());

        uint32_t i = 0;

#if defined(__AVX2__) && defined(__POPCNT__)

        static constexpr uint32_t W = 8;

        const __m256i vones = _mm256_set1_epi32(1);
        const __m256i vbucket_size_mask = _mm256_set1_epi32(bucket_size_mask);
        const __m256i vprobe_key_mask = _mm256_set1_epi32(0x8000'0000ul);

        uint8_t match_mask = 0xFF;
        __m256i vmatch = _mm256_set1_epi32(0xFFFF'FFFF);
        __m256i vprobe_times = _mm256_setzero_si256();
        __m256i vprobe_buckets = _mm256_setzero_si256();
        __m256i vprobe_keys = _mm256_setzero_si256();
        __m256i vprobe_indexes = _mm256_setzero_si256();
        while (i + W <= probe_row_count) {
            if (match_mask == 0xFF) {
                vprobe_times = _mm256_setzero_si256();
                vprobe_buckets = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(probe_buckets + i));
                vprobe_keys = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(probe_keys + i));
                vprobe_keys = _mm256_or_si256(vprobe_keys, vprobe_key_mask);
                vprobe_indexes = _mm256_set_epi32(i + 7, i + 6, i + 5, i + 4, i + 3, i + 2, i + 1, i);
                i += W;
            } else if (match_mask == 0) {
                vprobe_times = _mm256_add_epi32(vprobe_times, vones);
                vprobe_buckets = _mm256_add_epi32(vprobe_buckets, vprobe_times);
                vprobe_buckets = _mm256_and_si256(vprobe_buckets, vbucket_size_mask);
            } else {
                const __m256i vmove_left_mask = _mm256_cvtepu8_epi32(
                        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(move_left_mask_perm[match_mask])));
                vmatch = _mm256_permutevar8x32_epi32(vmatch, vmove_left_mask); // move matched items to left

                // selectively load vprobe_times
                vprobe_times = _mm256_permutevar8x32_epi32(vprobe_times, vmove_left_mask);
                vprobe_times = _mm256_add_epi32(vprobe_times, vones);
                vprobe_times = _mm256_blendv_epi8(vprobe_times, _mm256_setzero_si256(), vmatch);

                // selectively load vprobe_buckets
                vprobe_buckets = _mm256_permutevar8x32_epi32(vprobe_buckets, vmove_left_mask);
                vprobe_buckets = _mm256_add_epi32(vprobe_buckets, vprobe_times);
                vprobe_buckets = _mm256_and_si256(vprobe_buckets, vbucket_size_mask);
                __m256i vnew_probe_buckets = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(probe_buckets + i));
                vprobe_buckets = _mm256_blendv_epi8(vprobe_buckets, vnew_probe_buckets, vmatch);

                // selectively load probe_keys
                vprobe_keys = _mm256_permutevar8x32_epi32(vprobe_keys, vmove_left_mask);
                __m256i vnew_probe_keys = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(probe_keys + i));
                vnew_probe_keys = _mm256_or_si256(vnew_probe_keys, vprobe_key_mask);
                vprobe_keys = _mm256_blendv_epi8(vprobe_keys, vnew_probe_keys, vmatch);

                // selectively load probe_indexes
                vprobe_indexes = _mm256_permutevar8x32_epi32(vprobe_indexes, vmove_left_mask);
                __m256i vnew_is = _mm256_set_epi32(i + 7, i + 6, i + 5, i + 4, i + 3, i + 2, i + 1, i);
                vprobe_indexes = _mm256_blendv_epi8(vprobe_indexes, vnew_is, vmatch);

                i += __builtin_popcount(match_mask);
            }

            __m256i vbuild_keys =
                    _mm256_i32gather_epi32(reinterpret_cast<const int*>(build_buckets), vprobe_buckets, 4);
            vmatch = _mm256_cmpeq_epi32(vprobe_keys, vbuild_keys);
            match_mask = _mm256_movemask_ps(_mm256_castsi256_ps(vmatch));

            if (match_mask == 0xFF) {
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst_probe_indexes + match_count), vprobe_indexes);
                match_count += W;
            } else if (match_mask != 0) {
                // selectively store
                __m256i vmove_left_mask = _mm256_cvtepu8_epi32(
                        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(move_left_mask_perm[match_mask])));
                __m256i vleft_probe_indexes = _mm256_permutevar8x32_epi32(vprobe_indexes, vmove_left_mask);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst_probe_indexes + match_count), vleft_probe_indexes);
                match_count += __builtin_popcount(match_mask);
            }

            vmatch = _mm256_or_si256(vmatch, _mm256_cmpeq_epi32(vbuild_keys, _mm256_setzero_si256()));
            match_mask = _mm256_movemask_ps(_mm256_castsi256_ps(vmatch));
        }

        if (match_mask != 0xFF) {
            uint32_t cur_probe_times[W];
            _mm256_store_si256(reinterpret_cast<__m256i*>(cur_probe_times), vprobe_times);
            uint32_t cur_probe_buckets[W];
            _mm256_store_si256(reinterpret_cast<__m256i*>(cur_probe_buckets), vprobe_buckets);
            uint32_t cur_probe_keys[W];
            _mm256_store_si256(reinterpret_cast<__m256i*>(cur_probe_keys), vprobe_keys);
            uint32_t cur_probe_indexes[W];
            _mm256_store_si256(reinterpret_cast<__m256i*>(cur_probe_indexes), vprobe_indexes);

            match_mask = ~match_mask; // Get each position i for `match_mask[i] == 0`.
            for (; match_mask != 0; match_mask &= match_mask - 1) {
                const uint32_t j = __builtin_ctz(match_mask);

                const uint32_t probe_key = cur_probe_keys[j];
                uint32_t probe_bucket = cur_probe_buckets[j];
                uint32_t probe_times = cur_probe_times[j];

                while (true) {
                    probe_times++;
                    probe_bucket = (probe_bucket + probe_times) & bucket_size_mask;
                    const uint32_t build_key = build_buckets[probe_bucket];

                    if (build_key == 0) {
                        break;
                    }
                    if (build_key == probe_key) {
                        dst_probe_indexes[match_count] = cur_probe_indexes[j];
                        match_count++;
                        break;
                    }
                }
            }
        }
#endif

        for (; i < probe_row_count; i++) {
            const uint32_t probe_key = probe_keys[i] | 0x8000'0000ul;

            uint32_t probe_bucket = probe_buckets[i];
            uint32_t probe_times = 1;
            while (true) {
                const uint32_t build_key = build_buckets[probe_bucket];
                if (build_key == 0) {
                    break;
                }
                if (build_key == probe_key) {
                    dst_probe_indexes[match_count] = i;
                    match_count++;
                    break;
                }
                probe_bucket = (probe_bucket + probe_times) & bucket_size_mask;
                probe_times++;
            }
        }
    } else {
        const auto* nexts = _probe_state->next.data();
        const auto* build_nexts = _table_items->next.data();
        for (uint32_t i = 0; i < probe_row_count; i++) {
            uint32_t index = nexts[i];
            while (index != 0) {
                if (ProbeFunc().equal(build_data[index], probe_data[i])) {
                    dst_probe_indexes[match_count] = i;
                    match_count++;
                    break;
                }
                if constexpr (no_conflicts) {
                    break;
                }
                index = build_nexts[index];
            }
        }
    }

    if (match_count == probe_row_count) {
        _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;
    } else if (match_count * 2 >= probe_row_count) {
        _probe_state->match_flag = JoinMatchFlag::MOST_MATCH_ONE;
        uint8_t* match_filter_data = _probe_state->probe_match_filter.data();
        memset(match_filter_data, 0, sizeof(uint8_t) * _probe_state->probe_row_count);
        for (uint32_t i = 0; i < match_count; i++) {
            match_filter_data[_probe_state->probe_index[i]] = 1;
        }
    } else {
        _probe_state->match_flag = JoinMatchFlag::NORMAL;
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_anti_join(RuntimeState* state,
                                                                              const Buffer<CppType>& build_data,
                                                                              const Buffer<CppType>& probe_data) {
    if (_table_items->mode == 3) {
        _do_probe_from_ht_for_left_anti_join<first_probe, false, 3>(state, build_data, probe_data);
    } else if (_table_items->mode == 2) {
        _do_probe_from_ht_for_left_anti_join<first_probe, false, 2>(state, build_data, probe_data);
    } else if (_table_items->mode == 1) {
        if (_table_items->no_conflicts) {
            _do_probe_from_ht_for_left_anti_join<first_probe, true, 1>(state, build_data, probe_data);
        } else {
            _do_probe_from_ht_for_left_anti_join<first_probe, false, 1>(state, build_data, probe_data);
        }
    } else {
        if (_table_items->no_conflicts) {
            _do_probe_from_ht_for_left_anti_join<first_probe, true, 0>(state, build_data, probe_data);
        } else {
            _do_probe_from_ht_for_left_anti_join<first_probe, false, 0>(state, build_data, probe_data);
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe, bool no_conflicts, uint8_t SIMD>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_do_probe_from_ht_for_left_anti_join(RuntimeState* state,
                                                                                 const Buffer<CppType>& build_data,
                                                                                 const Buffer<CppType>& probe_data) {
    const size_t probe_row_count = _probe_state->probe_row_count;
    uint32_t match_count = 0;
    auto* probe_indexes = _probe_state->probe_index.data();

    [[maybe_unused]] size_t probe_cont = 0;
    [[maybe_unused]] size_t probe_cont2 = 0;

    DCHECK_LT(0, _table_items->row_count);
    if (_table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _probe_state->null_array != nullptr) {
        // process left anti join from not in
        for (size_t i = 0; i < probe_row_count; i++) {
            size_t index = _probe_state->next[i];
            if ((*_probe_state->null_array)[i] == 1) {
                continue;
            }

            if (index == 0) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
                continue;
            }

            bool found = false;
            while (index != 0) {
                if (ProbeFunc().equal(build_data[index], probe_data[i])) {
                    found = true;
                    break;
                }
                index = _table_items->next[index];
            }
            if (!found) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
            }
        }
    } else {
        if constexpr (SIMD == 3) {
            const uint32_t min_value = _table_items->min_value;
            const uint32_t max_value = _table_items->max_value;

            const auto* probe_values = reinterpret_cast<const uint32_t*>(probe_data.data());
            const auto* build_buckets = _table_items->set_has_value.data();
            uint8_t* dst_matches = _probe_state->probe_match_filter.data();

            for (uint32_t i = 0; i < probe_row_count; i++) {
                const uint32_t value = probe_values[i];
                bool matched = (min_value <= value) & (value <= max_value);
                if (matched) {
                    const uint32_t bucket = value - min_value;
                    const uint32_t group = bucket / 8;
                    const uint32_t offset = bucket % 8;
                    matched &= (build_buckets[group] & (1 << offset)) != 0;
                }

                const bool not_matched = !matched;
                dst_matches[i] = not_matched;
                match_count += not_matched;
            }

            if (match_count == probe_row_count) {
                _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;
            } else {
                _probe_state->match_flag = JoinMatchFlag::MOST_MATCH_ONE;
            }

            PROBE_OVER()
            return;
        } else if constexpr (SIMD == 2) {
            const uint32_t bucket_size_mask = _table_items->bucket_size - 1;
            const auto* buckets = _table_items->first.data();
            const auto* probe_buckets = _probe_state->buckets.data();
            const auto* raw_probe_data = reinterpret_cast<const uint32_t*>(probe_data.data());

            for (uint32_t i = 0; i < probe_row_count; i++) {
                const auto probe_key = raw_probe_data[i];

                uint32_t bucket = probe_buckets[i];
                uint32_t probe_times = 1;
                while (true) {
                    const auto build_key = buckets[bucket];
                    if (build_key == 0) {
                        probe_indexes[match_count] = i;
                        match_count++;
                        break;
                    }
                    if ((build_key & 0x7FFF'FFFFul) == probe_key) {
                        break;
                    }
                    bucket = (bucket + probe_times) & bucket_size_mask;
                    probe_times++;
                }
            }
        } else {
            const auto* nexts = _probe_state->next.data();
            for (uint32_t i = 0; i < probe_row_count; i++) {
                uint32_t index = nexts[i];

                if (index == 0) {
                    _probe_state->probe_index[match_count] = i;
                    match_count++;
                    continue;
                }

                if constexpr (no_conflicts) {
                    if (!ProbeFunc().equal(build_data[index], probe_data[i])) {
                        probe_indexes[match_count] = i;
                        match_count++;
                    }
                } else {
                    bool found = false;
                    do {
                        probe_cont++;
                        if (ProbeFunc().equal(build_data[index], probe_data[i])) {
                            found = true;
                            break;
                        }

                        if constexpr (no_conflicts) {
                            break;
                        }

                        index = _table_items->next[index];
                    } while (index != 0);

                    if (!found) {
                        probe_indexes[match_count] = i;
                        match_count++;
                    }
                }
            }
        }
    }

    COUNTER_UPDATE(_probe_state->probe_counter, probe_cont);
    COUNTER_UPDATE(_probe_state->probe2_counter, probe_cont2);

    if (match_count == probe_row_count) {
        _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;
    } else if (match_count * 2 >= probe_row_count) {
        _probe_state->match_flag = JoinMatchFlag::MOST_MATCH_ONE;
        uint8_t* match_filter_data = _probe_state->probe_match_filter.data();
        memset(match_filter_data, 0, sizeof(uint8_t) * _probe_state->probe_row_count);
        for (uint32_t i = 0; i < match_count; i++) {
            match_filter_data[_probe_state->probe_index[i]] = 1;
        }
    } else {
        _probe_state->match_flag = JoinMatchFlag::NORMAL;
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_anti_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    DCHECK_LT(0, _table_items->row_count);
    if (_table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _probe_state->null_array != nullptr) {
        // process left anti join from not in
        for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
             i = _probe_state->cur_probe_index++) {
            size_t build_index = _probe_state->next[i];
            if ((*_probe_state->null_array)[i] == 1) {
                continue;
            }

            if (build_index == 0) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
                continue;
            }

            bool found = false;
            while (build_index != 0) {
                PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    found = true;
                    break;
                }
                build_index = _table_items->next[build_index];
            }
            if (!found) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
            }
        }
    } else {
        for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
             i = _probe_state->cur_probe_index++) {
            size_t build_index = _probe_state->next[i];
            if (build_index == 0) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
                continue;
            }
            bool found = false;
            while (build_index != 0) {
                PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    found = true;
                    break;
                }
                build_index = _table_items->next[build_index];
            }
            if (!found) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
            }
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_outer_join(RuntimeState* state,
                                                                                const Buffer<CppType>& build_data,
                                                                                const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                _probe_state->build_match_index[build_index] = 1;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
            build_index = _table_items->next[build_index];
        }
    }

    // TODO: all match optimized
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                COWAIT_IF_CHUNK_FULL()
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->build_index[_probe_state->match_count] = build_index;
                _probe_state->build_match_index[build_index] = 1;
                _probe_state->match_count++;
            }
            build_index = _table_items->next[build_index];
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    // TODO: all match optimized
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_semi_join(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                if (_probe_state->build_match_index[build_index] == 0) {
                    _probe_state->build_index[match_count] = build_index;
                    _probe_state->build_match_index[build_index] = 1;
                    match_count++;

                    RETURN_IF_CHUNK_FULL()
                }
            }
            build_index = _table_items->next[build_index];
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_semi_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                if (_probe_state->build_match_index[build_index] == 0) {
                    COWAIT_IF_CHUNK_FULL()
                    _probe_state->build_index[_probe_state->match_count] = build_index;
                    _probe_state->build_match_index[build_index] = 1;
                    _probe_state->match_count++;
                }
            }
            build_index = _table_items->next[build_index];
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_anti_join(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    size_t probe_row_count = _probe_state->probe_row_count;
    for (size_t i = 0; i < probe_row_count; i++) {
        size_t index = _probe_state->next[i];
        if (index == 0) {
            continue;
        }

        while (index != 0) {
            if (ProbeFunc().equal(build_data[index], probe_data[i])) {
                _probe_state->build_match_index[index] = 1;
            }
            index = _table_items->next[index];
        }
    }
    _probe_state->count = 0;
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_anti_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->build_match_index[build_index] = 1;
            }
            build_index = _table_items->next[build_index];
        }
    }
    _probe_state->count = 0;
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_full_outer_join(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        _probe_state->cur_row_match_count = 0;
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            RETURN_IF_CHUNK_FULL()
        } else {
            while (build_index != 0) {
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    _probe_state->probe_index[match_count] = i;
                    _probe_state->build_index[match_count] = build_index;
                    _probe_state->build_match_index[build_index] = 1;
                    _probe_state->cur_row_match_count++;
                    match_count++;

                    RETURN_IF_CHUNK_FULL()
                }
                build_index = _table_items->next[build_index];
            }
            if (_probe_state->cur_row_match_count <= 0) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = 0;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
        }
        _probe_state->cur_row_match_count = 0;
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_full_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        int cur_row_match_count = 0;
        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                COWAIT_IF_CHUNK_FULL()
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->build_index[_probe_state->match_count] = build_index;
                _probe_state->build_match_index[build_index] = 1;
                _probe_state->match_count++;
                cur_row_match_count++;
            }
            build_index = _table_items->next[build_index];
        }
        if (cur_row_match_count <= 0) {
            COWAIT_IF_CHUNK_FULL()
            _probe_state->probe_index[_probe_state->match_count] = i;
            _probe_state->build_index[_probe_state->match_count] = 0;
            _probe_state->match_count++;
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_semi_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    size_t match_count = 0;

    size_t i = _probe_state->cur_probe_index;
    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        for (size_t j = 0; j < state->chunk_size(); j++) {
            _probe_state->probe_match_index[j] = 0;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
            build_index = _table_items->next[build_index];
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine
JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_semi_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                COWAIT_IF_CHUNK_FULL()
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->build_index[_probe_state->match_count] = build_index;
                _probe_state->match_count++;
            }
            build_index = _table_items->next[build_index];
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_null_aware_anti_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    size_t match_count = 0;

    size_t i = _probe_state->cur_probe_index;
    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        _probe_state->cur_row_match_count = 0;
        for (size_t j = 0; j < state->chunk_size(); j++) {
            _probe_state->probe_match_index[j] = 0;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (_probe_state->null_array != nullptr && (*_probe_state->null_array)[i] == 1) {
            // when left table col value is null needs match all rows in right table
            for (size_t j = 1; j < _table_items->row_count + 1; j++) {
                MATCH_RIGHT_TABLE_ROWS()
                RETURN_IF_CHUNK_FULL()
            }
        } else if (_table_items->key_columns[0]->is_nullable()) {
            // when left table col value not hits in hash table needs match all null value rows in right table
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(_table_items->key_columns[0]);
            auto& null_array = nullable_column->null_column()->get_data();
            // TODO: optimize me
            for (size_t j = 1; j < _table_items->row_count + 1; j++) {
                if (null_array[j] == 1) {
                    MATCH_RIGHT_TABLE_ROWS()
                    RETURN_IF_CHUNK_FULL()
                }
            }
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                _probe_state->probe_match_index[i]++;
                match_count++;
                _probe_state->cur_row_match_count++;

                RETURN_IF_CHUNK_FULL()
            }
            build_index = _table_items->next[build_index];
        }

        if (_probe_state->cur_row_match_count <= 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            RETURN_IF_CHUNK_FULL()
        }
        _probe_state->cur_row_match_count = 0;
    }
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::
        _probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct(
                RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t i = _probe_state->cur_probe_index;

    PROCESS_PROBE_STAGE_FOR_RIGHT_JOIN_WITH_OTHER_CONJUNCT()

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
            build_index = _table_items->next[build_index];
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    size_t match_count = 0;

    size_t i = _probe_state->cur_probe_index;
    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        _probe_state->cur_row_match_count = 0;
        for (size_t j = 0; j < state->chunk_size(); j++) {
            _probe_state->probe_match_index[j] = 0;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            RETURN_IF_CHUNK_FULL()
        } else {
            while (build_index != 0) {
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    _probe_state->probe_index[match_count] = i;
                    _probe_state->build_index[match_count] = build_index;
                    _probe_state->probe_match_index[i]++;
                    _probe_state->cur_row_match_count++;
                    match_count++;

                    RETURN_IF_CHUNK_FULL()
                }
                build_index = _table_items->next[build_index];
            }
            if (_probe_state->cur_row_match_count <= 0) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = 0;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
        }
        _probe_state->cur_row_match_count = 0;
    }

    PROBE_OVER()
}

template <bool is_remain>
Status JoinHashTable::lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) {
    switch (_hash_map_type) {
#define M(NAME)                                                            \
    case JoinHashMapType::NAME:                                            \
        _##NAME->lazy_output<is_remain>(state, probe_chunk, result_chunk); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    default:
        assert(false);
    }
    if (_table_items->has_large_column) {
        RETURN_IF_ERROR((*result_chunk)->downgrade());
    }
    return Status::OK();
}

#undef JOIN_HASH_MAP_TPP
} // namespace starrocks
