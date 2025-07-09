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

#include "util/runtime_profile.h"

#define JOIN_HASH_MAP_H

#include <gen_cpp/PlanNodes_types.h>
#include <runtime/descriptors.h>
#include <runtime/runtime_state.h>

#include <coroutine>
#include <cstdint>
#include <set>

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/join/bucket_chained_join_hash_map.hpp"
#include "exec/join/join_hash_map_helper.hpp"
#include "exec/join/join_hash_table_items.h"
#include "exec/join/join_hash_table_probe_state.h"
#include "simd/simd.h"
#include "util/phmap/phmap.h"

#if defined(__aarch64__)
#include "arm_acle.h"
#endif

namespace starrocks {

#define APPLY_FOR_JOIN_VARIANTS(M) \
    M(empty)                       \
    M(keyboolean)                  \
    M(key8)                        \
    M(key16)                       \
    M(key32)                       \
    M(key64)                       \
    M(key128)                      \
    M(keyfloat)                    \
    M(keydouble)                   \
    M(keystring)                   \
    M(keydate)                     \
    M(keydatetime)                 \
    M(keydecimal)                  \
    M(keydecimal32)                \
    M(keydecimal64)                \
    M(keydecimal128)               \
    M(slice)                       \
    M(fixed32)                     \
    M(fixed64)                     \
    M(fixed128)

struct HashTableParam {
    bool with_other_conjunct = false;
    bool enable_late_materialization = false;
    bool enable_partition_hash_join = false;
    long column_view_concat_rows_limit = -1L;
    long column_view_concat_bytes_limit = -1L;

    TJoinOp::type join_type = TJoinOp::INNER_JOIN;
    const RowDescriptor* build_row_desc = nullptr;
    const RowDescriptor* probe_row_desc = nullptr;
    std::set<SlotId> build_output_slots;
    std::set<SlotId> probe_output_slots;
    std::set<SlotId> predicate_slots;
    std::vector<JoinKeyDesc> join_keys;

    RuntimeProfile::Counter* search_ht_timer = nullptr;
    RuntimeProfile::Counter* output_build_column_timer = nullptr;
    RuntimeProfile::Counter* output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* probe_counter = nullptr;
};

// When hash table is empty, specific its implemention.
// TODO: Merge with JoinHashMap?
class JoinHashMapForEmpty {
public:
    explicit JoinHashMapForEmpty(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : _table_items(table_items), _probe_state(probe_state) {}

    void build_prepare(RuntimeState* state) {}
    void probe_prepare(RuntimeState* state) {}
    void build(RuntimeState* state) {}
    void probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
               bool* has_remain) {
        DCHECK_EQ(0, _table_items->row_count);
        *has_remain = false;
        _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;
        switch (_table_items->join_type) {
        case TJoinOp::FULL_OUTER_JOIN:
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
        case TJoinOp::LEFT_OUTER_JOIN: {
            _probe_state->count = (*probe_chunk)->num_rows();
            _probe_output<false>(probe_chunk, chunk);
            _build_output<false>(chunk);

            if (_table_items->enable_late_materialization) {
                _probe_index_output(chunk);
            }
            break;
        }
        default:
            break;
        }
    }
    void probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) {
        // For RIGHT ANTI-JOIN, RIGHT SEMI-JOIN, FULL OUTER-JOIN, right table is empty,
        // do nothing for probe_remain.
        DCHECK_EQ(0, _table_items->row_count);
        *has_remain = false;
    }

    template <bool is_remain>
    void lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) {
        if ((*result_chunk)->num_rows() < _probe_state->count) {
            _probe_state->match_flag = JoinMatchFlag::NORMAL;
            _probe_state->count = (*result_chunk)->num_rows();
        }

        (*result_chunk)->remove_column_by_slot_id(Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);

        _probe_output<true>(probe_chunk, result_chunk);
        _build_output<true>(result_chunk);
        _probe_state->count = 0;
    }

private:
    template <bool is_lazy>
    void _probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk) {
        SCOPED_TIMER(_probe_state->output_probe_column_timer);
        bool to_nullable = _table_items->left_to_nullable;
        for (size_t i = 0; i < _table_items->probe_column_count; i++) {
            HashTableSlotDescriptor hash_table_slot = _table_items->probe_slots[i];
            SlotDescriptor* slot = hash_table_slot.slot;

            bool output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
            if (output) {
                auto& column = (*probe_chunk)->get_column_by_slot_id(slot->id());
                if (!column->is_nullable()) {
                    _copy_probe_column(&column, chunk, slot, to_nullable);
                } else {
                    _copy_probe_nullable_column(&column, chunk, slot);
                }
            }
        }
    }

    void _copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable) {
        if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
            if (to_nullable) {
                MutableColumnPtr dest_column = NullableColumn::create((*src_column)->as_mutable_ptr(),
                                                                      NullColumn::create(_probe_state->count));
                (*chunk)->append_column(std::move(dest_column), slot->id());
            } else {
                (*chunk)->append_column(*src_column, slot->id());
            }
        } else {
            MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), to_nullable);
            dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
            (*chunk)->append_column(std::move(dest_column), slot->id());
        }
    }

    void _copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot) {
        if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
            (*chunk)->append_column(*src_column, slot->id());
        } else {
            MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
            dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
            (*chunk)->append_column(std::move(dest_column), slot->id());
        }
    }

    template <bool is_lazy>
    void _build_output(ChunkPtr* chunk) {
        SCOPED_TIMER(_probe_state->output_build_column_timer);

        for (size_t i = 0; i < _table_items->build_column_count; i++) {
            HashTableSlotDescriptor hash_table_slot = _table_items->build_slots[i];
            SlotDescriptor* slot = hash_table_slot.slot;

            bool output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
            if (output) {
                MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
                dest_column->append_nulls(_probe_state->count);
                (*chunk)->append_column(std::move(dest_column), slot->id());
            }
        }
    }

    void _probe_index_output(ChunkPtr* chunk) {
        _probe_state->probe_index_column->resize(_probe_state->count);
        auto* col = down_cast<UInt32Column*>(_probe_state->probe_index_column.get());
        std::iota(col->get_data().begin(), col->get_data().end(), 0);
        (*chunk)->append_column(_probe_state->probe_index_column, Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);
    }

    JoinHashTableItems* _table_items = nullptr;
    HashTableProbeState* _probe_state = nullptr;
};

template <LogicalType LT>
struct OneColumnKeyBuilder {
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    template <typename HashMap>
    static void build_key_and_append_to_map(HashMap& hash_map, RuntimeState* state, JoinHashTableItems* table_items,
                                            HashTableProbeState* probe_state);
    static const Buffer<CppType>& get_build_key_data(const JoinHashTableItems& table_items);
};

class SerializedKeyBuilder {
public:
    template <typename HashMap>
    static void build_key_and_append_to_map(HashMap& hash_map, RuntimeState* state, JoinHashTableItems* table_items,
                                            HashTableProbeState* probe_state);
    static const Buffer<Slice>& get_build_key_data(const JoinHashTableItems& table_items);

private:
    static void _build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                               const Columns& data_columns, uint32_t start, uint32_t count, uint8_t** ptr);

    static void _build_nullable_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                        const Columns& data_columns, const NullColumns& null_columns, uint32_t start,
                                        uint32_t count, uint8_t** ptr);
};

template <LogicalType LT>
struct FixedSizeSerializedKeyBuilder {
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void construct_build_key(RuntimeState* state, JoinHashTableItems* table_items,
                                    HashTableProbeState* probe_state);
    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items);
};

template <LogicalType LT, typename Map, typename Derived>
class JoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;

    explicit JoinHashMap(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : _table_items(table_items), _probe_state(probe_state) {}

    void build_prepare(RuntimeState* state);
    void probe_prepare(RuntimeState* state);

    void build(RuntimeState* state);
    void probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
               bool* has_remain);
    void probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain);
    template <bool is_remain>
    void lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk);

protected:
    template <bool is_lazy>
    void _probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk);
    template <bool is_lazy>
    void _probe_null_output(ChunkPtr* chunk, size_t count);

    template <bool is_lazy>
    void _build_output(ChunkPtr* chunk);
    void _build_default_output(ChunkPtr* chunk, size_t count);

    void _copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable);

    void _copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot);

    void _copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable);

    void _copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot);

    void _probe_index_output(ChunkPtr* chunk);
    void _build_index_output(ChunkPtr* chunk);

    void _search_ht(RuntimeState* state, ChunkPtr* probe_chunk);
    void _search_ht_remain(RuntimeState* state);

    template <bool first_probe>
    void _search_ht_impl(RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& data);

    template <bool first_probe>
    void _probe_coroutine(RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key inner join
    template <bool first_probe>
    void _probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data,
                                                       const Buffer<CppType>& probe_data);

    // for one key left outer join
    template <bool first_probe>
    void _probe_from_ht_for_left_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                            const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_outer_join(RuntimeState* state,
                                                                           const Buffer<CppType>& build_data,
                                                                           const Buffer<CppType>& probe_data);
    // for one key left semi join
    template <bool first_probe>
    void _probe_from_ht_for_left_semi_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                           const Buffer<CppType>& probe_data);

    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_semi_join(RuntimeState* state,
                                                                          const Buffer<CppType>& build_data,
                                                                          const Buffer<CppType>& probe_data);
    // for one key left anti join
    template <bool first_probe>
    void _probe_from_ht_for_left_anti_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                           const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_anti_join(RuntimeState* state,
                                                                          const Buffer<CppType>& build_data,
                                                                          const Buffer<CppType>& probe_data);

    // for one key right outer join
    template <bool first_probe>
    void _probe_from_ht_for_right_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                             const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_outer_join(RuntimeState* state,
                                                                            const Buffer<CppType>& build_data,
                                                                            const Buffer<CppType>& probe_data);

    // for one key right semi join
    template <bool first_probe>
    void _probe_from_ht_for_right_semi_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                            const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_semi_join(RuntimeState* state,
                                                                           const Buffer<CppType>& build_data,
                                                                           const Buffer<CppType>& probe_data);

    // for one key right anti join
    template <bool first_probe>
    void _probe_from_ht_for_right_anti_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                            const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_anti_join(RuntimeState* state,
                                                                           const Buffer<CppType>& build_data,
                                                                           const Buffer<CppType>& probe_data);

    // for one key full outer join
    template <bool first_probe>
    void _probe_from_ht_for_full_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                            const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_full_outer_join(RuntimeState* state,
                                                                           const Buffer<CppType>& build_data,
                                                                           const Buffer<CppType>& probe_data);

    // for left semi join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_left_semi_join_with_other_conjunct(RuntimeState* state, const Buffer<CppType>& build_data,
                                                               const Buffer<CppType>& probe_data);

    // for null aware anti join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_null_aware_anti_join_with_other_conjunct(RuntimeState* state,
                                                                     const Buffer<CppType>& build_data,
                                                                     const Buffer<CppType>& probe_data);

    // for one key right outer join with other conjunct
    template <bool first_probe>
    void _probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct(
            RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key full outer join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct(RuntimeState* state,
                                                                                     const Buffer<CppType>& build_data,
                                                                                     const Buffer<CppType>& probe_data);

    JoinHashTableItems* _table_items = nullptr;
    HashTableProbeState* _probe_state = nullptr;
    Map _map;
};

template <LogicalType LT, typename Map>
class JoinHashMapWithOneKey : public JoinHashMap<LT, Map, JoinHashMapWithOneKey<LT, Map>> {
public:
    using Base = JoinHashMap<LT, Map, JoinHashMapWithOneKey<LT, Map>>;

    JoinHashMapWithOneKey(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : Base(table_items, probe_state) {}

    void construct_hash_map(RuntimeState* state);
};

template <LogicalType LT, typename Map>
class JoinHashMapWithSerializedKey : public JoinHashMap<LT, Map, JoinHashMapWithSerializedKey<LT, Map>> {
public:
    using Base = JoinHashMap<LT, Map, JoinHashMapWithSerializedKey<LT, Map>>;

    JoinHashMapWithSerializedKey(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : Base(table_items, probe_state) {}

    void construct_hash_map(RuntimeState* state);

private:
    static void _build_columns(const Columns& data_columns, uint32_t start, uint32_t count, uint8_t** ptr);

    static void _build_nullable_columns(const Columns& data_columns, const NullColumns& null_columns, uint32_t start,
                                        uint32_t count, uint8_t** ptr);
};

template <LogicalType LT, typename Map>
class JoinHashMapWithSerializedKeyFixedSize
        : public JoinHashMap<LT, Map, JoinHashMapWithSerializedKeyFixedSize<LT, Map>> {
public:
    using Base = JoinHashMap<LT, Map, JoinHashMapWithSerializedKeyFixedSize<LT, Map>>;

    JoinHashMapWithSerializedKeyFixedSize(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : Base(table_items, probe_state) {}

    void construct_hash_map(RuntimeState* state);
};

#define JoinHashMapForOneKey(LT) JoinHashMap<LT, JoinBuildFunc<LT>, JoinProbeFunc<LT>>
#define JoinHashMapForDirectMapping(LT) JoinHashMap<LT, DirectMappingJoinBuildFunc<LT>, DirectMappingJoinProbeFunc<LT>>
#define JoinHashMapForFixedSizeKey(LT) JoinHashMap<LT, FixedSizeJoinBuildFunc<LT>, FixedSizeJoinProbeFunc<LT>>
#define JoinHashMapForSerializedKey(LT) JoinHashMap<LT, SerializedJoinBuildFunc, SerializedJoinProbeFunc>

struct JoinHashMapVariant {
    enum class Type {
        empty,
        keyboolean,
        key8,
        key16,
        key32,
        key64,
        key128,
        keyfloat,
        keydouble,
        keystring,
        keydate,
        keydatetime,
        keydecimal,
        keydecimal32,
        keydecimal64,
        keydecimal128,
        slice,
        fixed32, // 4 bytes
        fixed64, // 8 bytes
        fixed128 // 16 bytes
    };

    using Variant = std::variant<
            std::unique_ptr<JoinHashMapForEmpty>, std::unique_ptr<JoinHashMapForDirectMapping(TYPE_BOOLEAN)>,
            std::unique_ptr<JoinHashMapForDirectMapping(TYPE_TINYINT)>,
            std::unique_ptr<JoinHashMapForDirectMapping(TYPE_SMALLINT)>,
            std::unique_ptr<JoinHashMapForOneKey(TYPE_INT)>, std::unique_ptr<JoinHashMapForOneKey(TYPE_BIGINT)>,
            std::unique_ptr<JoinHashMapForOneKey(TYPE_LARGEINT)>, std::unique_ptr<JoinHashMapForOneKey(TYPE_FLOAT)>,
            std::unique_ptr<JoinHashMapForOneKey(TYPE_DOUBLE)>, std::unique_ptr<JoinHashMapForOneKey(TYPE_VARCHAR)>,
            std::unique_ptr<JoinHashMapForOneKey(TYPE_DATE)>, std::unique_ptr<JoinHashMapForOneKey(TYPE_DATETIME)>,
            std::unique_ptr<JoinHashMapForOneKey(TYPE_DECIMALV2)>,
            std::unique_ptr<JoinHashMapForOneKey(TYPE_DECIMAL32)>,
            std::unique_ptr<JoinHashMapForOneKey(TYPE_DECIMAL64)>,
            std::unique_ptr<JoinHashMapForOneKey(TYPE_DECIMAL128)>,
            std::unique_ptr<JoinHashMapForSerializedKey(TYPE_VARCHAR)>,
            std::unique_ptr<JoinHashMapForFixedSizeKey(TYPE_INT)>,
            std::unique_ptr<JoinHashMapForFixedSizeKey(TYPE_BIGINT)>,
            std::unique_ptr<JoinHashMapForFixedSizeKey(TYPE_LARGEINT)>>;

    Variant variant;
    Type type = Type::empty;

    template <typename Visitor>
    auto visit(Visitor&& visitor) const {
        return std::visit(std::forward<Visitor>(visitor), variant);
    }
};

class JoinHashTable {
public:
    JoinHashTable() = default;
    ~JoinHashTable() = default;

    // Disable copy ctor and assignment.
    JoinHashTable(const JoinHashTable&) = delete;
    JoinHashTable& operator=(const JoinHashTable&) = delete;
    // Enable move ctor and move assignment.
    JoinHashTable(JoinHashTable&&) = default;
    JoinHashTable& operator=(JoinHashTable&&) = default;

    // Clone a new hash table with the same hash table as this, and the different probe state from this.
    JoinHashTable clone_readable_table();
    void set_probe_profile(RuntimeProfile::Counter* search_ht_timer, RuntimeProfile::Counter* output_probe_column_timer,
                           RuntimeProfile::Counter* output_build_column_timer, RuntimeProfile::Counter* probe_counter);

    void create(const HashTableParam& param);
    void close();

    Status build(RuntimeState* state);
    void reset_probe_state(RuntimeState* state);
    Status probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk, bool* eos);
    Status probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    template <bool is_remain>
    Status lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk);

    void append_chunk(const ChunkPtr& chunk, const Columns& key_columns);
    void merge_ht(const JoinHashTable& ht);
    // convert input column to spill schema order
    ChunkPtr convert_to_spill_schema(const ChunkPtr& chunk) const;

    const ChunkPtr& get_build_chunk() const { return _table_items->build_chunk; }
    Columns& get_key_columns() { return _table_items->key_columns; }
    const Columns& get_key_columns() const { return _table_items->key_columns; }
    uint32_t get_row_count() const { return _table_items->row_count; }
    size_t get_probe_column_count() const { return _table_items->probe_column_count; }
    size_t get_output_probe_column_count() const { return _table_items->output_probe_column_count; }
    size_t get_build_column_count() const { return _table_items->build_column_count; }
    size_t get_output_build_column_count() const { return _table_items->output_build_column_count; }
    size_t get_bucket_size() const { return _table_items->bucket_size; }
    float get_keys_per_bucket() const;
    void remove_duplicate_index(Filter* filter);
    JoinHashTableItems* table_items() const { return _table_items.get(); }

    int64_t mem_usage() const;

private:
    void _init_probe_column(const HashTableParam& param);
    void _init_build_column(const HashTableParam& param);
    void _init_join_keys();

    JoinHashMapVariant::Type _choose_join_hash_map();
    static size_t _get_size_of_fixed_and_contiguous_type(LogicalType data_type);

    Status _upgrade_key_columns_if_overflow();

    void _remove_duplicate_index_for_left_outer_join(Filter* filter);
    void _remove_duplicate_index_for_left_semi_join(Filter* filter);
    void _remove_duplicate_index_for_left_anti_join(Filter* filter);
    void _remove_duplicate_index_for_right_outer_join(Filter* filter);
    void _remove_duplicate_index_for_right_semi_join(Filter* filter);
    void _remove_duplicate_index_for_right_anti_join(Filter* filter);
    void _remove_duplicate_index_for_full_outer_join(Filter* filter);

    JoinHashMapVariant _hash_map_variant;
    std::shared_ptr<JoinHashTableItems> _table_items;
    std::unique_ptr<HashTableProbeState> _probe_state = std::make_unique<HashTableProbeState>();
};
} // namespace starrocks

#ifndef JOIN_HASH_MAP_TPP
#include "join_hash_map.tpp"
#endif

#undef JOIN_HASH_MAP_H
