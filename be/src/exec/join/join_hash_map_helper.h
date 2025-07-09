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
#include "join_hash_table_items.h"
#include "join_hash_table_probe_state.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// SJoinHashMapHelper
// ------------------------------------------------------------------------------------

template <class T, size_t Size = sizeof(T)>
struct JoinKeyHash {
    static constexpr uint32_t CRC_SEED = 0x811C9DC5;
    uint32_t operator()(const T& value, uint32_t num_buckets, uint32_t num_log_buckets) const {
        const size_t hash = crc_hash_32(&value, sizeof(T), CRC_SEED);
        return hash & (num_buckets - 1);
    }
};

/// Apply multiplicative hashing for 4-byte or 8-byte keys.
/// It only needs to perform arithmetic operations on the key as a whole, so the compiler can automatically vectorize it.
template <typename T>
struct JoinKeyHash<T, 4> {
    uint32_t operator()(T value, uint32_t num_buckets, uint32_t num_log_buckets) const {
        static constexpr uint32_t a = 2654435761u;
        uint32_t v = *reinterpret_cast<uint32_t*>(&value);
        v ^= v >> (32 - num_log_buckets);
        const uint32_t fraction = v * a;
        return fraction >> (32 - num_log_buckets);
    }
};

template <typename T>
struct JoinKeyHash<T, 8> {
    uint32_t operator()(T value, uint32_t num_buckets, uint32_t num_log_buckets) const {
        static constexpr uint64_t a = 11400714819323198485ull;
        uint64_t v = *reinterpret_cast<uint64_t*>(&value);
        v ^= v >> (64 - num_log_buckets);
        const uint64_t fraction = v * a;
        return fraction >> (64 - num_log_buckets);
    }
};

template <>
struct JoinKeyHash<Slice> {
    static const uint32_t CRC_SEED = 0x811C9DC5;
    uint32_t operator()(const Slice& slice, uint32_t num_buckets, uint32_t num_log_buckets) const {
        const size_t hash = crc_hash_32(slice.data, slice.size, CRC_SEED);
        return hash & (num_buckets - 1);
    }
};

class JoinHashMapHelper {
public:
    // maxinum bucket size
    const static uint32_t MAX_BUCKET_SIZE = 1 << 31;

    static uint32_t calc_bucket_size(uint32_t size) {
        size_t expect_bucket_size = static_cast<size_t>(size) + (size - 1) / 4;
        // Limit the maximum hash table bucket size.
        if (expect_bucket_size >= MAX_BUCKET_SIZE) {
            return MAX_BUCKET_SIZE;
        }
        return phmap::priv::NormalizeCapacity(expect_bucket_size) + 1;
    }

    template <typename CppType>
    static uint32_t calc_bucket_num(const CppType& value, uint32_t bucket_size, uint32_t num_log_buckets) {
        using HashFunc = JoinKeyHash<CppType>;

        return HashFunc()(value, bucket_size, num_log_buckets);
    }

    template <typename CppType>
    static void calc_bucket_nums(const Buffer<CppType>& data, uint32_t bucket_size, uint32_t num_log_buckets,
                                 Buffer<uint32_t>* buckets, uint32_t start, uint32_t count) {
        DCHECK(count <= buckets->size());
        for (size_t i = 0; i < count; i++) {
            (*buckets)[i] = calc_bucket_num<CppType>(data[start + i], bucket_size, num_log_buckets);
        }
    }

    static Slice get_hash_key(const Columns& key_columns, size_t row_idx, uint8_t* buffer) {
        size_t byte_size = 0;
        for (const auto& key_column : key_columns) {
            byte_size += key_column->serialize(row_idx, buffer + byte_size);
        }
        return {buffer, byte_size};
    }

    // combine keys into fixed size key by column.
    template <LogicalType LT>
    static void serialize_fixed_size_key_column(const Columns& key_columns, Column* fixed_size_key_column,
                                                uint32_t start, uint32_t count) {
        using CppType = typename RunTimeTypeTraits<LT>::CppType;
        using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

        auto& data = reinterpret_cast<ColumnType*>(fixed_size_key_column)->get_data();
        auto* buf = reinterpret_cast<uint8_t*>(&data[start]);

        const size_t byte_interval = sizeof(CppType);
        size_t byte_offset = 0;
        for (const auto& key_col : key_columns) {
            size_t offset = key_col->serialize_batch_at_interval(buf, byte_offset, byte_interval, start, count);
            byte_offset += offset;
        }
    }
};

// ------------------------------------------------------------------------------------
// JoinBuildFunc and JoinProbeFunc
// ------------------------------------------------------------------------------------

template <LogicalType LT>
class JoinBuildFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* runtime, JoinHashTableItems* table_items);
    static void construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                     HashTableProbeState* probe_state);
    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items);
};

template <LogicalType LT>
class JoinProbeFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {}
    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);
    static const Buffer<CppType>& get_key_data(const HashTableProbeState& probe_state);
    static bool equal(const CppType& x, const CppType& y) { return x == y; }
};

// ------------------------------------------------------------------------------------
// DirectMappingJoinBuildFunc and DirectMappingJoinProbeFunc
// ------------------------------------------------------------------------------------

template <LogicalType LT>
class DirectMappingJoinBuildFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* runtime, JoinHashTableItems* table_items);
    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items);
    static void construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                     HashTableProbeState* probe_state);
};

template <LogicalType LT>
class DirectMappingJoinProbeFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {}
    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);
    static const Buffer<CppType>& get_key_data(const HashTableProbeState& probe_state);
    static bool equal(const CppType& x, const CppType& y) { return true; }
};

// ------------------------------------------------------------------------------------
// FixedSizeJoinBuildFunc and FixedSizeJoinProbeFunc
// ------------------------------------------------------------------------------------

template <LogicalType LT>
class FixedSizeJoinBuildFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, JoinHashTableItems* table_items);

    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items) {
        return ColumnHelper::as_raw_column<const ColumnType>(table_items.build_key_column)->get_data();
    }
    static void construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                     HashTableProbeState* probe_state);

private:
    static void _build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                               const Columns& data_columns, uint32_t start, uint32_t count);

    static void _build_nullable_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                        const Columns& data_columns, const NullColumns& null_columns, uint32_t start,
                                        uint32_t count);
};

template <LogicalType LT>
class FixedSizeJoinProbeFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {
        probe_state->is_nulls.resize(state->chunk_size());
        probe_state->probe_key_column = ColumnType::create(state->chunk_size());
    }

    // serialize and calculate hash values for probe keys.
    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

    static const Buffer<CppType>& get_key_data(const HashTableProbeState& probe_state) {
        return ColumnHelper::as_raw_column<ColumnType>(probe_state.probe_key_column)->get_data();
    }

    static bool equal(const CppType& x, const CppType& y) { return x == y; }

private:
    static void _probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                              const Columns& data_columns);
    static void _probe_nullable_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                       const Columns& data_columns, const NullColumns& null_columns);
};

// ------------------------------------------------------------------------------------
// SerializedJoinBuildFunc and SerializedJoinProbeFunc
// ------------------------------------------------------------------------------------

class SerializedJoinBuildFunc {
public:
    static void prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static const Buffer<Slice>& get_key_data(const JoinHashTableItems& table_items) { return table_items.build_slice; }
    static void construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                     HashTableProbeState* probe_state);

private:
    static void _build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                               const Columns& data_columns, uint32_t start, uint32_t count, uint8_t** ptr);

    static void _build_nullable_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                        const Columns& data_columns, const NullColumns& null_columns, uint32_t start,
                                        uint32_t count, uint8_t** ptr);
};

class SerializedJoinProbeFunc {
public:
    static const Buffer<Slice>& get_key_data(const HashTableProbeState& probe_state) { return probe_state.probe_slice; }

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {
        probe_state->probe_pool = std::make_unique<MemPool>();
        probe_state->probe_slice.resize(state->chunk_size());
        probe_state->is_nulls.resize(state->chunk_size());
    }

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

    static bool equal(const Slice& x, const Slice& y) { return x == y; }

private:
    static void _probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                              const Columns& data_columns, uint8_t* ptr);
    static void _probe_nullable_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                       const Columns& data_columns, const NullColumns& null_columns, uint8_t* ptr);
};

} // namespace starrocks
