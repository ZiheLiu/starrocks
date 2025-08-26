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

#include "join_hash_map_helper.h"
#include "join_hash_table_descriptor.h"

namespace starrocks {

template <class T>
struct JoinKeyHash {
    static constexpr uint32_t CRC_SEED = 0x811C9DC5;
    uint64_t operator()(const T& value) const { return crc_hash_64(&value, sizeof(T), CRC_SEED); }
};

template <>
struct JoinKeyHash<Slice> {
    static constexpr uint32_t CRC_SEED = 0x811C9DC5;
    uint64_t operator()(const Slice& slice) const { return crc_hash_64(slice.data, slice.size, CRC_SEED); }
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

        return HashFunc()(value) & (bucket_size - 1);
    }

    template <typename CppType>
    static std::pair<uint32_t, uint8_t> calc_bucket_num_and_fp(const CppType& value, uint32_t bucket_size,
                                                               uint32_t num_log_buckets) {
        static constexpr uint64_t FP_BITS = 7;
        using HashFunc = JoinKeyHash<CppType>;
        const uint64_t hash = HashFunc()(value);
        // , bucket_size << FP_BITS, num_log_buckets + FP_BITS
        // return {hash & (bucket_size - 1), (hash >> (64 - FP_BITS)) | 0x80};
        return {(hash >> FP_BITS) & (bucket_size - 1), (hash & 0x7F) | 0x80};
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

} // namespace starrocks