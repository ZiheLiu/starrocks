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

#include <boost/asio/detail/thread_info_base.hpp>

#include "serde/column_array_serde.h"
#include "simd/gather.h"
#include "simd/simd.h"
#include "util/runtime_profile.h"

#define JOIN_HASH_MAP_TPP

#ifndef JOIN_HASH_MAP_H
#include "join_hash_map.h"
#endif

namespace starrocks {

// ------------------------------------------------------------------------------------
// JoinHashMapWithOneKey
// ------------------------------------------------------------------------------------

template <LogicalType LT>
template <typename HashMap>
void OneColumnKeyBuilder<LT>::build_key_and_append_to_map(HashMap& hash_map, RuntimeState* state,
                                                          JoinHashTableItems* table_items,
                                                          HashTableProbeState* probe_state) {
    const auto& keys = get_build_key_data(*table_items);

    if (table_items->key_columns[0]->is_nullable() && table_items->key_columns[0]->has_null()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items->key_columns[0]);
        const auto& is_nulls = nullable_column->null_column()->get_data();
        hash_map.construct_hash_table(keys, is_nulls, 1, table_items->row_count + 1);
    } else {
        hash_map.construct_hash_table(keys, 1, table_items->row_count + 1);
    }
}

template <LogicalType LT>
auto OneColumnKeyBuilder<LT>::get_build_key_data(const JoinHashTableItems& table_items) -> const Buffer<CppType>& {
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

// ------------------------------------------------------------------------------------
// JoinHashMapWithSerializedKey
// ------------------------------------------------------------------------------------

template <LogicalType LT, typename Map>
void JoinHashMapWithSerializedKey<LT, Map>::construct_hash_map(RuntimeState* state) {

    const uint32_t row_count = _table_items->row_count;

    // prepare columns
    Columns data_columns;
    NullColumns null_columns;

    for (size_t i = 0; i < _table_items->key_columns.size(); i++) {
        if (_table_items->join_keys[i].is_null_safe_equal) {
            data_columns.emplace_back(_table_items->key_columns[i]);
        } else if (_table_items->key_columns[i]->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(_table_items->key_columns[i]);
            data_columns.emplace_back(nullable_column->data_column());
            if (_table_items->key_columns[i]->has_null()) {
                null_columns.emplace_back(nullable_column->null_column());
            }
        } else {
            data_columns.emplace_back(_table_items->key_columns[i]);
        }
    }

    // calc serialize size
    size_t serialize_size = 0;
    for (const auto& data_column : data_columns) {
        serialize_size += serde::ColumnArraySerde::max_serialized_size(*data_column);
    }
    uint8_t* ptr = _table_items->build_pool->allocate(serialize_size);

    // serialize and build hash table
    uint32_t quo = row_count / state->chunk_size();
    uint32_t rem = row_count % state->chunk_size();

    if (!null_columns.empty()) {
        for (size_t i = 0; i < quo; i++) {
            _build_nullable_columns(data_columns, null_columns, 1 + state->chunk_size() * i, state->chunk_size(), &ptr);
        }
        _build_nullable_columns(data_columns, null_columns, 1 + state->chunk_size() * quo, rem, &ptr);
    } else {
        for (size_t i = 0; i < quo; i++) {
            _build_columns(data_columns, 1 + state->chunk_size() * i, state->chunk_size(), &ptr);
        }
        _build_columns(data_columns, 1 + state->chunk_size() * quo, rem, &ptr);
    }
}

template <LogicalType LT, typename Map>
void JoinHashMapWithSerializedKey<LT, Map>::_build_columns(const Columns& data_columns, uint32_t start, uint32_t count,
                                                           uint8_t** ptr) {
    auto* __restrict buckets = _table_items->next.data();
    for (size_t i = 0; i < count; i++) {
        _table_items->build_slice[start + i] = JoinHashMapHelper::get_hash_key(data_columns, start + i, *ptr);
        buckets[start + i] = BucketHasher()(table_items->build_slice[start + i], _table_items->bucket_size,
                                            table_items->log_bucket_size);
        *ptr += _table_items->build_slice[start + i].size;
    }
}
template <LogicalType LT, typename Map>

void JoinHashMapWithSerializedKey<LT, Map>::_build_nullable_columns(const Columns& data_columns,
                                                                    const NullColumns& null_columns, uint32_t start,
                                                                    uint32_t count, uint8_t** ptr) {
    for (uint32_t i = 0; i < count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[start + i];
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->get_data()[start + j];
        }
    }

    auto* __restrict buckets = table_items->next.data();
    for (size_t i = 0; i < count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            table_items->build_slice[start + i] = JoinHashMapHelper::get_hash_key(data_columns, start + i, *ptr);
            buckets[i] = BucketHasher()(table_items->build_slice[start + i], table_items->bucket_size,
                                        table_items->log_bucket_size);
            *ptr += table_items->build_slice[start + i].size;
        }
    }

    for (size_t i = 0; i < count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            table_items->next[start + i] = table_items->first[probe_state->buckets[i]];
            table_items->first[probe_state->buckets[i]] = start + i;
        }
    }
}

// ------------------------------------------------------------------------------------
// JoinHashMapWithSerializedKeyFixedSize
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void FixedSizeSerializedKeyBuilder<LT>::construct_build_key(RuntimeState* state, JoinHashTableItems* table_items,
                                                            HashTableProbeState* probe_state) {}

template <LogicalType LT>
const auto FixedSizeSerializedKeyBuilder<LT>::get_key_data(const JoinHashTableItems& table_items)
        -> const Buffer<CppType>& {}

// ------------------------------------------------------------------------------------
// JoinHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::build_prepare(RuntimeState* state) {
    _map.build_prepare(state);
}

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::probe_prepare(RuntimeState* state) {
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

    _map.probe_prepare(state);
}

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::build(RuntimeState* state) {
    _map.build(state);
}

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk,
                                 ChunkPtr* chunk, bool* has_remain) {
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

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) {
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

template <LogicalType LT, typename Map>
template <bool is_lazy>
void JoinHashMap<LT, Map>::_probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk) {
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

template <LogicalType LT, typename Map>
template <bool is_remain>
void JoinHashMap<LT, Map>::lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) {
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

template <LogicalType LT, typename Map>
template <bool is_lazy>
void JoinHashMap<LT, Map>::_probe_null_output(ChunkPtr* chunk, size_t count) {
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

template <LogicalType LT, typename Map>
template <bool is_lazy>
void JoinHashMap<LT, Map>::_build_output(ChunkPtr* chunk) {
    bool to_nullable = _table_items->right_to_nullable;
    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->build_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;

        bool need_output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
        if (need_output) {
            ColumnPtr& column = _table_items->build_chunk->columns()[i];
            if (!column->is_nullable() && !column->is_nullable_view()) {
                _copy_build_column(column, chunk, slot, to_nullable);
            } else {
                _copy_build_nullable_column(column, chunk, slot);
            }
        }
    }
}

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::_build_default_output(ChunkPtr* chunk, size_t count) {
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

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::_copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot,
                                              bool to_nullable) {
    if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
        if (to_nullable) {
            ColumnPtr dest_column =
                    NullableColumn::create((*src_column)->as_mutable_ptr(), NullColumn::create((*src_column)->size()));
            (*chunk)->append_column(std::move(dest_column), slot->id());
        } else {
            (*chunk)->append_column(*src_column, slot->id());
        }
    } else if (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE) {
        if (to_nullable) {
            (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
            ColumnPtr dest_column =
                    NullableColumn::create((*src_column)->as_mutable_ptr(), NullColumn::create((*src_column)->size()));
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

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::_copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk,
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

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::_copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot,
                                              bool to_nullable) {
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
        auto dest_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        (*chunk)->append_column(std::move(dest_column), slot->id());
    } else {
        auto dest_column = src_column->clone_empty();
        dest_column->append_selective(*src_column, _probe_state->build_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::_copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk,
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

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::_search_ht(RuntimeState* state, ChunkPtr* probe_chunk) {
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

template <LogicalType LT, typename Map>
void JoinHashMap<LT, Map>::_search_ht_remain(RuntimeState* state) {
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

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_search_ht_impl(RuntimeState* state, const Buffer<CppType>& build_data,
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

#define RETURN_IF_CHUNK_FULL()                \
    if (UNLIKELY(match_count > chunk_size)) { \
        _probe_state->count = chunk_size;     \
        return true;                          \
    }

#define RETURN_IF_CHUNK_FULL_FOR_COROUTINE()   \
    if (UNLIKELY(match_count >= chunk_size)) { \
        _probe_state->count = chunk_size;      \
        return true;                           \
    }

#define RETURN_IF_CHUNK_FULL_FOR_NULL_AWARE()            \
    if (UNLIKELY(match_count > chunk_size)) {            \
        _probe_state->count = chunk_size;                \
        _probe_state->cur_nullaware_build_index = j + 1; \
        return true;                                     \
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

#define PROBE_OVER()                   \
    _probe_state->has_remain = false;  \
    _probe_state->cur_probe_index = 0; \
    _probe_state->cur_build_index = 0; \
    _probe_state->count = match_count; \
    _probe_state->cur_row_match_count = 0;

/// TODO (fzh): calculate hash distribution, skew or not.
// NOTE: coroutine only SIMD code of SSE but not AVX
template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_coroutine(RuntimeState* state, const Buffer<CppType>& build_data,
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

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data,
                                          const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;

    const size_t chunk_size = state->chunk_size();
    size_t match_count = 0;
    bool one_to_many = false;

    if constexpr (first_probe) {
        memset(_probe_state->probe_match_filter.data(), 0, _probe_state->probe_row_count * sizeof(uint8_t));
    } else {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        if constexpr (first_probe) {
            _probe_state->probe_match_filter[probe_index] = 1;
        }
        match_count++;
        RETURN_IF_CHUNK_FULL();
        return false;
    };

    auto finish_probe_row_process = [&](const uint32_t probe_index, const uint32_t cur_row_match_count) {
        if constexpr (first_probe) {
            one_to_many |= cur_row_match_count > 1;
        }
        return false;
    };

    if (probe_chunk<first_probe>(build_data, probe_data, match_process, finish_probe_row_process)) {
        return;
    }

    if constexpr (first_probe) {
        CHECK_MATCH()
    }
    PROBE_OVER()
}

template <LogicalType LT, typename Map>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, Map>::_probe_from_ht(RuntimeState* state,
                                                                         const Buffer<CppType>& build_data,
                                                                         const Buffer<CppType>& probe_data) {
    const auto chunk_size = state->chunk_size();
    auto& match_count = _probe_state->match_count;

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        _probe_state->probe_match_filter[probe_index] = 1;
        match_count++;
        RETURN_IF_CHUNK_FULL_FOR_COROUTINE();
        return false;
    };

    auto finish_probe_row_process = [&](const uint32_t probe_index, const uint32_t cur_row_match_count) {
        if (cur_row_match_count > 1) {
            _probe_state->cur_row_match_count = cur_row_match_count; // means one_to_many match
        }
        return false;
    };

    auto finish_probe_func = [&]() {
        bool one_to_many = _probe_state->cur_row_match_count > 1;
        if (!_probe_state->has_remain) {
            CHECK_MATCH()
            REORDER_PROBE_INDEX()
        }
    };

    return probe_chunk_coroutine(build_data, probe_data, match_process, finish_probe_row_process, finish_probe_func);
}

template <LogicalType LT, typename Map>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, Map>::_probe_from_ht_for_left_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    const auto chunk_size = state->chunk_size();
    auto& match_count = _probe_state->match_count;

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        match_count++;
        RETURN_IF_CHUNK_FULL_FOR_COROUTINE();
        return false;
    };

    auto finish_probe_row_process = [&](const uint32_t probe_index, const uint32_t cur_row_match_count) {
        if (cur_row_match_count == 0) {
            _probe_state->probe_index[match_count] = probe_index;
            _probe_state->build_index[match_count] = 0;
            match_count++;
            RETURN_IF_CHUNK_FULL_FOR_COROUTINE();
        } else if (cur_row_match_count > 1) {
            // one key of left table match multi key of right table
            _probe_state->cur_row_match_count = cur_row_match_count;
        }

        return false;
    };

    auto finish_probe_func = [&]() {
        const bool one_to_many = _probe_state->cur_row_match_count > 1;
        if (!_probe_state->has_remain) {
            CHECK_ALL_MATCH()
            REORDER_PROBE_INDEX()
        }
        PROBE_OVER()
    };

    return probe_chunk_coroutine(build_data, probe_data, match_process, finish_probe_row_process, finish_probe_func);
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_left_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                                              const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;

    const size_t chunk_size = state->chunk_size();
    size_t match_count = 0;
    bool one_to_many = false;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        match_count++;
        RETURN_IF_CHUNK_FULL();
        return false;
    };

    auto finish_probe_row_process = [&](const uint32_t probe_index, const uint32_t cur_row_match_count) {
        if (cur_row_match_count == 0) {
            _probe_state->probe_index[match_count] = probe_index;
            _probe_state->build_index[match_count] = 0;
            match_count++;
            RETURN_IF_CHUNK_FULL();
            return false;
        }
        if constexpr (first_probe) {
            one_to_many |= cur_row_match_count > 1;
        }
        return false;
    };

    if (probe_chunk<first_probe>(build_data, probe_data, match_process, finish_probe_row_process)) {
        return;
    }

    if constexpr (first_probe) {
        CHECK_ALL_MATCH()
    }
    PROBE_OVER()
}

template <LogicalType LT, typename Map>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, Map>::_probe_from_ht_for_left_semi_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    auto& match_count = _probe_state->match_count;

    auto need_row_func = [](uint32_t probe_index) { return true; };
    auto contains_row_func = [&](uint32_t probe_index, bool contains) {
        if (contains) {
            _probe_state->probe_index[match_count] = probe_index;
            match_count++;
        }
    };
    auto finish_probe_func = [&]() { PROBE_OVER() };

    return contains_coroutine(build_data, probe_data, need_row_func, contains_row_func, finish_probe_func);
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_left_semi_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                                             const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    const size_t probe_row_count = _probe_state->probe_row_count;
    for (size_t i = 0; i < probe_row_count; i++) {
        if (contains(i, build_data, probe_data)) {
            _probe_state->probe_index[match_count] = i;
            match_count++;
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_left_anti_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                                             const Buffer<CppType>& probe_data) {
    DCHECK_LT(0, _table_items->row_count);
    const size_t probe_row_count = _probe_state->probe_row_count;
    size_t match_count = 0;

    if (_table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _probe_state->null_array != nullptr) {
        for (size_t i = 0; i < probe_row_count; i++) {
            if ((*_probe_state->null_array)[i] == 1) {
                continue;
            }

            if (!contains(i, build_data, probe_data)) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
            }
        }
    } else {
        for (size_t i = 0; i < probe_row_count; i++) {
            if (!contains(i, build_data, probe_data)) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
            }
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, typename Map>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, Map>::_probe_from_ht_for_left_anti_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    DCHECK_LT(0, _table_items->row_count);

    auto& match_count = _probe_state->match_count;
    auto contains_row_func = [&](const uint32_t probe_index, const bool contains) {
        if (!contains) {
            _probe_state->probe_index[match_count] = probe_index;
            match_count++;
        }
    };

    auto finish_probe_func = [&]() { PROBE_OVER() };

    if (_table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _probe_state->null_array != nullptr) {
        auto need_row_func = [&](uint32_t probe_index) { return (*_probe_state->null_array)[probe_index] != 1; };
        return contains_coroutine(build_data, probe_data, need_row_func, contains_row_func, finish_probe_func);
    } else {
        auto need_row_func = [](uint32_t probe_index) { return true; };
        return contains_coroutine(build_data, probe_data, need_row_func, contains_row_func, finish_probe_func);
    }
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_right_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                                               const Buffer<CppType>& probe_data) {
    const size_t chunk_size = state->chunk_size();
    size_t match_count = 0;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        _probe_state->build_match_index[build_index] = 1;
        match_count++;
        RETURN_IF_CHUNK_FULL();
        return false;
    };
    if (probe_chunk<first_probe>(build_data, probe_data, match_process)) {
        return;
    }

    // TODO: all match optimized
    PROBE_OVER()
}

template <LogicalType LT, typename Map>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, Map>::_probe_from_ht_for_right_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    const auto chunk_size = state->chunk_size();
    auto& match_count = _probe_state->match_count;

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        _probe_state->build_match_index[build_index] = 1;
        match_count++;
        RETURN_IF_CHUNK_FULL_FOR_COROUTINE();
        return false;
    };

    auto finish_probe_func = [&]() {
        // TODO: all match optimized
        PROBE_OVER()
    };

    return probe_chunk_coroutine(build_data, probe_data, match_process, finish_probe_func);
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_right_semi_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                                              const Buffer<CppType>& probe_data) {
    const size_t chunk_size = state->chunk_size();
    size_t match_count = 0;

    if constexpr (!first_probe) {
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        if (_probe_state->build_match_index[build_index] == 0) {
            _probe_state->build_match_index[build_index] = 1;
            _probe_state->build_index[match_count] = build_index;
            match_count++;
            RETURN_IF_CHUNK_FULL();
        }
        return false;
    };
    if (probe_chunk<first_probe>(build_data, probe_data, match_process)) {
        return;
    }

    PROBE_OVER()
}

template <LogicalType LT, typename Map>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, Map>::_probe_from_ht_for_right_semi_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    const auto chunk_size = state->chunk_size();
    auto& match_count = _probe_state->match_count;

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        if (_probe_state->build_match_index[build_index] == 0) {
            _probe_state->build_index[_probe_state->match_count] = build_index;
            _probe_state->build_match_index[build_index] = 1;
            match_count++;
            RETURN_IF_CHUNK_FULL_FOR_COROUTINE();
        }
        return false;
    };

    auto finish_probe_func = [&]() { PROBE_OVER() };

    return probe_chunk_coroutine(build_data, probe_data, match_process, finish_probe_func);
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_right_anti_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                                              const Buffer<CppType>& probe_data) {
    probe_chunk<first_probe>(build_data, probe_data, [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->build_match_index[build_index] = 1;
        return false;
    });
    _probe_state->count = 0;
}

template <LogicalType LT, typename Map>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, Map>::_probe_from_ht_for_right_anti_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->build_match_index[build_index] = 1;
        return false;
    };
    auto finish_probe_func = [&]() { _probe_state->count = 0; };
    return probe_chunk_coroutine(build_data, probe_data, match_process, finish_probe_func);
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_full_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                                              const Buffer<CppType>& probe_data) {
    const size_t chunk_size = state->chunk_size();
    size_t match_count = 0;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        _probe_state->build_match_index[build_index] = 1;
        match_count++;
        RETURN_IF_CHUNK_FULL();
        return false;
    };

    auto finish_probe_row_process = [&](const uint32_t probe_index, const uint32_t cur_row_match_count) {
        if (cur_row_match_count == 0) {
            _probe_state->probe_index[match_count] = probe_index;
            _probe_state->build_index[match_count] = 0;
            match_count++;
            RETURN_IF_CHUNK_FULL();
        }
        return false;
    };

    if (probe_chunk<first_probe>(build_data, probe_data, match_process, finish_probe_row_process)) {
        return;
    }

    PROBE_OVER()
}

template <LogicalType LT, typename Map>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, Map>::_probe_from_ht_for_full_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    const auto chunk_size = state->chunk_size();
    auto& match_count = _probe_state->match_count;

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        _probe_state->build_match_index[build_index] = 1;
        match_count++;
        RETURN_IF_CHUNK_FULL_FOR_COROUTINE();
        return false;
    };

    auto finish_probe_row_process = [&](const uint32_t probe_index, const uint32_t cur_row_match_count) {
        if (cur_row_match_count == 0) {
            _probe_state->probe_index[match_count] = probe_index;
            _probe_state->build_index[match_count] = 0;
            match_count++;
            RETURN_IF_CHUNK_FULL_FOR_COROUTINE();
        }

        return false;
    };

    auto finish_probe_func = [&]() { PROBE_OVER() };

    return probe_chunk_coroutine(build_data, probe_data, match_process, finish_probe_row_process, finish_probe_func);
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_left_semi_join_with_other_conjunct(RuntimeState* state,
                                                                                 const Buffer<CppType>& build_data,
                                                                                 const Buffer<CppType>& probe_data) {
    const size_t chunk_size = state->chunk_size();
    size_t match_count = 0;

    if constexpr (first_probe) {
        memset(_probe_state->probe_match_index.data(), 0, chunk_size * sizeof(uint32_t));
    } else {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        match_count++;

        RETURN_IF_CHUNK_FULL();
        return false;
    };

    if (probe_chunk<first_probe>(build_data, probe_data, match_process)) {
        return;
    }

    PROBE_OVER()
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_null_aware_anti_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    const size_t chunk_size = state->chunk_size();
    const size_t num_builder_rows = _table_items->row_count + 1;
    size_t match_count = 0;

    const bool builder_has_null = _table_items->key_columns[0]->has_null();
    auto finish_row_match_process = [&](const uint32_t probe_index, const uint32_t cur_row_match_count) {
        // Null row in the left table should match all the rows in the right table.
        if (_probe_state->null_array != nullptr && (*_probe_state->null_array)[probe_index] == 1) {
            for (size_t j = _probe_state->cur_nullaware_build_index; j < num_builder_rows; j++) {
                _probe_state->probe_index[match_count] = probe_index;
                _probe_state->build_index[match_count] = j;
                _probe_state->probe_match_index[probe_index]++;
                match_count++;
                RETURN_IF_CHUNK_FULL_FOR_NULL_AWARE();
            }
        } else if (builder_has_null) { // Any row in the left table should match all the null rows in the right table.
            const auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(_table_items->key_columns[0]);
            const auto& null_array = nullable_column->null_column()->get_data();
            for (size_t j = _probe_state->cur_nullaware_build_index; j < num_builder_rows; j++) {
                if (null_array[j] == 1) {
                    _probe_state->probe_index[match_count] = probe_index;
                    _probe_state->build_index[match_count] = j;
                    _probe_state->probe_match_index[probe_index]++;
                    match_count++;
                    RETURN_IF_CHUNK_FULL_FOR_NULL_AWARE();
                }
            }
        } else if (cur_row_match_count == 0) {
            _probe_state->probe_index[match_count] = probe_index;
            _probe_state->build_index[match_count] = 0;
            match_count++;
            RETURN_IF_CHUNK_FULL();
        }

        _probe_state->cur_nullaware_build_index = 1;

        return false;
    };

    if constexpr (first_probe) {
        _probe_state->cur_nullaware_build_index = 1;
        memset(_probe_state->probe_match_index.data(), 0, chunk_size * sizeof(uint32_t));
    } else {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;

        // The reason for the lastest probe paused is that the result chunk was full while processing null values
        // (cur_nullaware_build_index > 1) in finish_row_match_process (cur_build_index == 0), so now we need to
        // continue processing null values.
        if (_probe_state->cur_build_index == 0 && _probe_state->cur_nullaware_build_index > 1) {
            if (finish_row_match_process(_probe_state->cur_probe_index, _probe_state->cur_row_match_count)) {
                return;
            }
        }
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        _probe_state->probe_match_index[probe_index]++;
        match_count++;
        RETURN_IF_CHUNK_FULL();
        return false;
    };

    if (probe_chunk<first_probe>(build_data, probe_data, match_process, finish_row_match_process)) {
        return;
    }

    PROBE_OVER()
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    const size_t chunk_size = state->chunk_size();
    size_t match_count = 0;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        match_count++;
        RETURN_IF_CHUNK_FULL();
        return false;
    };

    if (probe_chunk<first_probe>(build_data, probe_data, match_process)) {
        return;
    }

    PROBE_OVER()
}

template <LogicalType LT, typename Map>
template <bool first_probe>
void JoinHashMap<LT, Map>::_probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    const size_t chunk_size = state->chunk_size();
    size_t match_count = 0;

    if constexpr (first_probe) {
        _probe_state->cur_row_match_count = 0;
        memset(_probe_state->probe_match_index.data(), 0, chunk_size * sizeof(uint32_t));
    } else {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    auto match_process = [&](const uint32_t probe_index, const uint32_t build_index) {
        _probe_state->probe_index[match_count] = probe_index;
        _probe_state->build_index[match_count] = build_index;
        match_count++;
        RETURN_IF_CHUNK_FULL();
        return false;
    };

    auto finish_probe_row_process = [&](const uint32_t probe_index, const uint32_t cur_row_match_count) {
        if (cur_row_match_count == 0) {
            _probe_state->probe_index[match_count] = probe_index;
            _probe_state->build_index[match_count] = 0;
            match_count++;
            RETURN_IF_CHUNK_FULL();
        }
        return false;
    };

    if (probe_chunk<first_probe>(build_data, probe_data, match_process, finish_probe_row_process)) {
        return;
    }

    PROBE_OVER()
}

template <bool is_remain>
Status JoinHashTable::lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) {
    _hash_map_variant.visit([&](auto& map) { map->template lazy_output<is_remain>(state, probe_chunk, result_chunk); });
    if (_table_items->has_large_column) {
        RETURN_IF_ERROR((*result_chunk)->downgrade());
    }
    return Status::OK();
}

#undef JOIN_HASH_MAP_TPP
} // namespace starrocks
