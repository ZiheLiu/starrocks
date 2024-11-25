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

#include "exprs/runtime_filter.h"

#include <storage/column_predicate.h>

#include "serde/column_array_serde.h"
#include "types/logical_type_infra.h"
#include "util/compression/stream_compression.h"

namespace starrocks {
// TODO: remove it
LogicalType RuntimeFilterSerializeType::from_serialize_type(RuntimeFilterSerializeType::PrimitiveType ptype) {
    switch (ptype) {
#define CONVERT_PTYPE(type_name)                       \
    case RuntimeFilterSerializeType::TYPE_##type_name: \
        return LogicalType::TYPE_##type_name;
        APPLY_FOR_SCALAR_THRIFT_TYPE(CONVERT_PTYPE);
#undef CONVERT_PTYPE
    default:
        return TYPE_UNKNOWN;
    }
}

RuntimeFilterSerializeType::PrimitiveType RuntimeFilterSerializeType::to_serialize_type(LogicalType type) {
    switch (type) {
#define CONVERT_TYPE(type_name)         \
    case LogicalType::TYPE_##type_name: \
        return RuntimeFilterSerializeType::TYPE_##type_name;
        APPLY_FOR_SCALAR_THRIFT_TYPE(CONVERT_TYPE);
#undef CONVERT_TYPE
    default:
        return RuntimeFilterSerializeType::TYPE_NULL;
    }
}

void SimdBlockFilter::init(size_t nums) {
    nums = std::max(MINIMUM_ELEMENT_NUM, nums);
    int log_heap_space = std::ceil(std::log2(nums));
    _log_num_buckets = std::max(1, log_heap_space - LOG_BUCKET_BYTE_SIZE);
    _directory_mask = (1ull << std::min(63, _log_num_buckets)) - 1;
    const size_t alloc_size = get_alloc_size();
    const int malloc_failed = posix_memalign(reinterpret_cast<void**>(&_directory), 64, alloc_size);
    if (malloc_failed) throw ::std::bad_alloc();
    memset(_directory, 0, alloc_size);
}

SimdBlockFilter::SimdBlockFilter(SimdBlockFilter&& bf) noexcept {
    _log_num_buckets = bf._log_num_buckets;
    _directory_mask = bf._directory_mask;
    _directory = bf._directory;
    bf._directory = nullptr;
}

size_t SimdBlockFilter::max_serialized_size() const {
    const size_t alloc_size = _directory == nullptr ? 0 : get_alloc_size();
    return sizeof(_log_num_buckets) + sizeof(_directory_mask) + // data size + max data size
           sizeof(int32_t) + alloc_size;
}

size_t SimdBlockFilter::serialize(uint8_t* data) const {
    size_t offset = 0;
#define SIMD_BF_COPY_FIELD(field)                 \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);
    SIMD_BF_COPY_FIELD(_log_num_buckets);
    // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter bf [_join_mode=" << _log_num_buckets << "] [offset=" << offset
    //              << "]";

    SIMD_BF_COPY_FIELD(_directory_mask);
    // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter bf [_directory_mask=" << _directory_mask << "] [offset=" << offset
    //              << "]";

    const size_t alloc_size = get_alloc_size();
    int32_t data_size = alloc_size;
    SIMD_BF_COPY_FIELD(data_size);
    // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter bf [data_size=" << data_size << "] [offset=" << offset << "]";

    if (LIKELY(data_size > 0)) {
        memcpy(data + offset, _directory, data_size);
        offset += data_size;
        // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter bf [_directory] [offset=" << offset << "]";
    }
    return offset;
#undef SIMD_BF_COPY_FIELD
}

size_t SimdBlockFilter::deserialize(const uint8_t* data) {
    size_t offset = 0;
    int32_t data_size = 0;

#define SIMD_BF_COPY_FIELD(field)                 \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    SIMD_BF_COPY_FIELD(_log_num_buckets);
    // LOG(WARNING) << "[RF] deserialize JoinRuntimeFilter bf [_log_num_buckets=" << _log_num_buckets
    //              << "] [offset=" << offset << "]";

    SIMD_BF_COPY_FIELD(_directory_mask);
    // LOG(WARNING) << "[RF] deserialize JoinRuntimeFilter bf [_directory_mask=" << _directory_mask
    //              << "] [offset=" << offset << "]";

    SIMD_BF_COPY_FIELD(data_size);
    // LOG(WARNING) << "[RF] deserialize JoinRuntimeFilter bf [data_size=" << data_size << "] [offset=" << offset << "]";

#undef SIMD_BF_COPY_FIELD
    const size_t alloc_size = get_alloc_size();
    DCHECK(data_size == alloc_size);
    // LOG(WARNING) << "[RF] deserialize JoinRuntimeFilter bf [alloc_size=" << alloc_size << "] [offset=" << offset << "]";

    if (LIKELY(data_size > 0)) {
        const int malloc_failed = posix_memalign(reinterpret_cast<void**>(&(_directory)), 64, alloc_size);
        if (malloc_failed) throw ::std::bad_alloc();
        memcpy(_directory, data + offset, data_size);
        offset += data_size;
        // LOG(WARNING) << "[RF] deserialize JoinRuntimeFilter bf [_directory] [offset=" << offset << "]";
    }
    return offset;
}

void SimdBlockFilter::merge(const SimdBlockFilter& bf) {
    if (_directory == nullptr || bf._directory == nullptr) {
        return;
    }
    DCHECK(_log_num_buckets == bf._log_num_buckets);
    for (int i = 0; i < (1 << _log_num_buckets); i++) {
#ifdef __AVX2__
        auto* const dst = reinterpret_cast<__m256i*>(_directory[i]);
        auto* const src = reinterpret_cast<__m256i*>(bf._directory[i]);
        const __m256i a = _mm256_load_si256(src);
        const __m256i b = _mm256_load_si256(dst);
        const __m256i c = _mm256_or_si256(a, b);
        _mm256_store_si256(dst, c);
#else
        for (int j = 0; j < BITS_SET_PER_BLOCK; j++) {
            _directory[i][j] |= bf._directory[i][j];
        }
#endif
    }
}

// For scalar version:
void SimdBlockFilter::make_mask(uint32_t key, uint32_t* masks) const {
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        // add some salt to key
        masks[i] = key * SALT[i];
        // masks[i] mod 32
        masks[i] = masks[i] >> 27;
        // set the masks[i]-th bit
        masks[i] = 0x1 << masks[i];
    }
}

bool SimdBlockFilter::check_equal(const SimdBlockFilter& bf) const {
    const size_t alloc_size = get_alloc_size();
    return _log_num_buckets == bf._log_num_buckets && _directory_mask == bf._directory_mask &&
           memcmp(_directory, bf._directory, alloc_size) == 0;
}

void SimdBlockFilter::clear() {
    if (_directory) {
        free(_directory);
        _directory = nullptr;
        _log_num_buckets = 0;
        _directory_mask = 0;
    }
}

size_t JoinRuntimeFilter::max_serialized_size() const {
    // todo(yan): noted that it's not serialize compatible with 32-bit and 64-bit.
    auto num_partitions = _hash_partition_bf.size();
    size_t size = sizeof(_has_null) + sizeof(_size) + sizeof(num_partitions) + sizeof(_join_mode);
    if (num_partitions == 0) {
        size += _bf.max_serialized_size();
    } else {
        for (const auto& bf : _hash_partition_bf) {
            size += bf.max_serialized_size();
        }
    }
    return size;
}

size_t JoinRuntimeFilter::serialize(int serialize_version, uint8_t* data) const {
    size_t offset = 0;
    const auto num_partitions = _hash_partition_bf.size();

#define JRF_COPY_FIELD(field)                     \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);
    // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter [_has_null=" << _has_null << "] [offset=" << offset << "]";

    JRF_COPY_FIELD(_size);
    // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter [_size=" << _size << "] [offset=" << offset << "]";

    JRF_COPY_FIELD(num_partitions);
    // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter [num_partitions=" << num_partitions << "] [offset=" << offset
    //              << "]";

    JRF_COPY_FIELD(_join_mode);
    // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter [_join_mode=" << _join_mode << "] [offset=" << offset << "]";

#undef JRF_COPY_FIELD

    if (num_partitions == 0) {
        offset += _bf.serialize(data + offset);
        // LOG(WARNING) << "[RF] serialize JoinRuntimeFilter [_bf] [offset=" << offset << "]";

    } else {
        for (const auto& bf : _hash_partition_bf) {
            offset += bf.serialize(data + offset);
        }
    }
    return offset;
}

size_t JoinRuntimeFilter::deserialize(int serialize_version, const uint8_t* data) {
    size_t offset = 0;
    size_t num_partitions = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);
    JRF_COPY_FIELD(_size);
    JRF_COPY_FIELD(num_partitions);
    JRF_COPY_FIELD(_join_mode);
#undef JRF_COPY_FIELD

    if (num_partitions == 0) {
        offset += _bf.deserialize(data + offset);
    } else {
        for (size_t i = 0; i < num_partitions; i++) {
            SimdBlockFilter bf;
            offset += bf.deserialize(data + offset);
            _hash_partition_bf.emplace_back(std::move(bf));
        }
    }

    return offset;
}

bool JoinRuntimeFilter::check_equal(const JoinRuntimeFilter& rf) const {
    auto lhs_num_partitions = _hash_partition_bf.size();
    auto rhs_num_partitions = rf._hash_partition_bf.size();
    bool first = (_has_null == rf._has_null && _size == rf._size && lhs_num_partitions == rhs_num_partitions &&
                  _join_mode == rf._join_mode);
    if (!first) return false;
    if (lhs_num_partitions == 0) {
        if (!_bf.check_equal(rf._bf)) return false;
    } else {
        for (size_t i = 0; i < lhs_num_partitions; ++i) {
            if (!_hash_partition_bf[i].check_equal(rf._hash_partition_bf[i])) {
                return false;
            }
        }
    }
    return true;
}

void JoinRuntimeFilter::clear_bf() {
    if (_hash_partition_bf.empty()) {
        _bf.clear();
    } else {
        for (size_t i = 0; i < _hash_partition_bf.size(); i++) {
            _hash_partition_bf[i].clear();
        }
    }
    _size = 0;
}

// ------------------------------------------------------------------------------------
// RuntimeInFilter
// ------------------------------------------------------------------------------------

template <LogicalType Type>
RuntimeInFilter<Type>::RuntimeInFilter() = default;
template <LogicalType Type>
RuntimeInFilter<Type>::~RuntimeInFilter() = default;

template <LogicalType Type>
Status RuntimeInFilter<Type>::insert(bool is_null_safe, const ColumnPtr& in_values, size_t in_values_offset) {
    if (_in_values == nullptr) {
        _in_values = in_values->clone_empty();
    }

    _in_values->append(*in_values, in_values_offset, in_values->size() - in_values_offset);
    _has_null |= (is_null_safe && in_values->has_null());

    if (in_values->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(in_values.get());
        const auto& input_data = GetContainer<Type>::get_data(nullable_column->data_column());
        for (size_t i = in_values_offset; i < input_data.size(); i++) {
            if (!nullable_column->is_null(i)) {
                _in_values_set.emplace(input_data[i]);
            }
        }
    } else {
        const auto& input_data = GetContainer<Type>::get_data(in_values.get());
        for (size_t i = in_values_offset; i < input_data.size(); i++) {
            _in_values_set.emplace(input_data[i]);
        }
    }
    return Status::OK();
}

template <LogicalType Type>
void RuntimeInFilter<Type>::clear_bf() {
    _in_values.reset();
    _in_values_set.clear();
    _pool.clear();
    JoinRuntimeFilter::clear_bf();
}

template <LogicalType Type>
bool RuntimeInFilter<Type>::can_use_bf() const {
    return _in_values != nullptr;
}

template <LogicalType Type>
size_t RuntimeInFilter<Type>::bf_alloc_size() const {
    return 0;
}

template <LogicalType Type>
size_t RuntimeInFilter<Type>::max_serialized_size() const {
    size_t size = sizeof(Type) + JoinRuntimeFilter::max_serialized_size();

    size += sizeof(bool); // has_in_values

    if (_in_values != nullptr) {
        size += sizeof(bool); // is_nullable
        size += serde::ColumnArraySerde::max_serialized_size(*_in_values);
    }

    return size;
}

template <LogicalType Type>
size_t RuntimeInFilter<Type>::serialize(int serialize_version, uint8_t* data) const {
    size_t offset = 0;
    if (serialize_version == RF_VERSION) {
        auto ltype = RuntimeFilterSerializeType::to_serialize_type(Type);
        memcpy(data + offset, &ltype, sizeof(ltype));
        offset += sizeof(ltype);

        // LOG(WARNING) << "[RF] serialize [ltype=" << ltype << "] [offset=" << offset << "]";
    } else {
        auto ltype = to_thrift(Type);
        memcpy(data + offset, &ltype, sizeof(ltype));
        offset += sizeof(ltype);

        // LOG(WARNING) << "[RF] serialize [ltype=" << ltype << "] [offset=" << offset << "]";
    }

    offset += JoinRuntimeFilter::serialize(serialize_version, data + offset);
    // LOG(WARNING) << "[RF] serialize [JoinRuntimeFilter::serialize] [offset=" << offset << "]";

#define JRF_COPY_FIELD(field)                     \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);

    if (_in_values == nullptr) {
        const bool has_in_values = false;
        JRF_COPY_FIELD(has_in_values);

        // LOG(WARNING) << "[RF] serialize [has_in_values=" << has_in_values << "] [offset=" << offset << "]";
    } else {
        const bool has_in_values = true;
        JRF_COPY_FIELD(has_in_values);

        // LOG(WARNING) << "[RF] serialize [has_in_values=" << has_in_values << "] [offset=" << offset << "]";

        const bool is_nullable = _in_values->is_nullable();
        JRF_COPY_FIELD(is_nullable);

        // LOG(WARNING) << "[RF] serialize [is_nullable=" << is_nullable << "] [offset=" << offset << "]";

        auto* buf = data + offset;
        offset += serde::ColumnArraySerde::serialize(*_in_values, buf) - buf;

        // LOG(WARNING) << "[RF] serialize [buf] [offset=" << offset << "]";
    }
#undef JRF_COPY_FIELD

    return offset;
}

template <LogicalType Type>
size_t RuntimeInFilter<Type>::deserialize(int serialize_version, const uint8_t* data) {
    size_t offset = 0;
    if (serialize_version == RF_VERSION) {
        RuntimeFilterSerializeType::PrimitiveType ltype = RuntimeFilterSerializeType::to_serialize_type(Type);
        memcpy(&ltype, data + offset, sizeof(ltype));
        offset += sizeof(ltype);

        // LOG(WARNING) << "[RF] deserialize [ltype=" << ltype << "] [offset=" << offset << "]";
    } else {
        auto ltype = to_thrift(Type);
        memcpy(&ltype, data + offset, sizeof(ltype));
        offset += sizeof(ltype);

        // LOG(WARNING) << "[RF] deserialize [ltype=" << ltype << "] [offset=" << offset << "]";
    }

    offset += JoinRuntimeFilter::deserialize(serialize_version, data + offset);
    // LOG(WARNING) << "[RF] deserialize [JoinRuntimeFilter::serialize] [offset=" << offset << "]";

    bool has_in_values;
    memcpy(&has_in_values, data + offset, sizeof(has_in_values));
    offset += sizeof(has_in_values);
    // LOG(WARNING) << "[RF] deserialize [has_in_values=" << has_in_values << "] [offset=" << offset << "]";
    if (!has_in_values) {
        return offset;
    }

    bool is_nullable;
    memcpy(&is_nullable, data + offset, sizeof(is_nullable));
    offset += sizeof(is_nullable);

    // LOG(WARNING) << "[RF] deserialize [is_nullable=" << is_nullable << "] [offset=" << offset << "]";

    _in_values = ColumnHelper::create_column(TypeDescriptor{Type}, is_nullable);
    auto* buf = data + offset;
    offset += serde::ColumnArraySerde::deserialize(buf, _in_values.get()) - buf;

    if (_in_values->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(_in_values.get());
        const auto& input_data = GetContainer<Type>::get_data(nullable_column->data_column());
        for (size_t i = 0; i < input_data.size(); i++) {
            if (!nullable_column->is_null(i)) {
                _in_values_set.emplace(input_data[i]);
            }
        }
    } else {
        const auto& input_data = GetContainer<Type>::get_data(_in_values.get());
        for (size_t i = 0; i < input_data.size(); i++) {
            _in_values_set.emplace(input_data[i]);
        }
    }

    return offset;
}

template <LogicalType Type>
void RuntimeInFilter<Type>::evaluate(Column* input_column, RunningContext* ctx) const {
    if (_in_values == nullptr) {
        return;
    }

    if (input_column->empty()) {
        return;
    }

    if (_has_null) {
        _evaluate_in_filter<true>(input_column, ctx);
    } else {
        _evaluate_in_filter<false>(input_column, ctx);
    }
}

template <LogicalType Type>
template <bool null_is_true>
void RuntimeInFilter<Type>::_evaluate_in_filter(Column* input_column, RunningContext* ctx) const {
    const size_t num_rows = input_column->size();

    Filter& selection_filter = ctx->use_merged_selection ? ctx->merged_selection : ctx->selection;
    selection_filter.resize(num_rows);
    uint8_t* selection = selection_filter.data();

    if (input_column->is_constant()) {
        const auto* const_column = down_cast<const ConstColumn*>(input_column);
        if (const_column->only_null()) {
            memset(selection, null_is_true, num_rows * sizeof(null_is_true));
        } else {
            const auto& input_data = GetContainer<Type>::get_data(const_column->data_column());
            const uint8_t sel = _in_values_set.contains(input_data[0]);
            memset(selection, sel, num_rows * sizeof(sel));
        }
    } else if (input_column->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(input_column);
        const auto& input_data = GetContainer<Type>::get_data(nullable_column->data_column());

        if (!input_column->has_null()) {
            for (int i = 0; i < num_rows; i++) {
                selection[i] = _in_values_set.contains(input_data[i]);
            }
        } else {
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            for (int i = 0; i < num_rows; i++) {
                if constexpr (null_is_true) {
                    selection[i] = null_data[i] || _in_values_set.contains(input_data[i]);
                } else {
                    selection[i] = (!null_data[i]) && _in_values_set.contains(input_data[i]);
                }
            }
        }
    } else {
        const auto& input_data = GetContainer<Type>::get_data(input_column);
        for (int i = 0; i < num_rows; i++) {
            selection[i] = _in_values_set.contains(input_data[i]);
        }
    }
}

template <LogicalType Type>
std::string RuntimeInFilter<Type>::debug_string() const {
    return "<RuntimeInFilter>";
}

template <LogicalType Type>
bool RuntimeInFilter<Type>::check_equal(const JoinRuntimeFilter& rf) const {
    // TODO: only used for multi partition?
    return JoinRuntimeFilter::check_equal(rf);
}

template <LogicalType Type>
JoinRuntimeFilter* RuntimeInFilter<Type>::create_empty(ObjectPool* pool) {
    return pool->add(new RuntimeInFilter());
}

#define M(Type) template class RuntimeInFilter<Type>;
APPLY_FOR_ALL_SCALAR_TYPE(M)
#undef M

} // namespace starrocks
