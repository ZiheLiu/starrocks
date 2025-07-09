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

class ColumnRef;

struct HashTableSlotDescriptor {
    SlotDescriptor* slot;
    bool need_output;
    bool need_lazy_materialize = false;
};

struct JoinKeyDesc {
    const TypeDescriptor* type = nullptr;
    bool is_null_safe_equal;
    ColumnRef* col_ref = nullptr;
};

struct JoinHashTableItems {
    //TODO: memory continues problem?
    ChunkPtr build_chunk = nullptr;
    Columns key_columns;
    Buffer<Slice> build_slice;
    ColumnPtr build_key_column = nullptr;

    std::vector<HashTableSlotDescriptor> build_slots;
    std::vector<HashTableSlotDescriptor> probe_slots;

    // A hash value is the bucket index of the hash map. "JoinHashTableItems.first" is the
    // buckets of the hash map, and it holds the index of the first key value saved in each bucket,
    // while other keys can be found by following the indices saved in
    // "JoinHashTableItems.next". "JoinHashTableItems.next[0]" represents the end of
    // the list of keys in a bucket.
    // A paper (https://dare.uva.nl/search?identifier=5ccbb60a-38b8-4eeb-858a-e7735dd37487) talks
    // about the bucket-chained hash table of this kind.
    Buffer<uint32_t> first;
    Buffer<uint32_t> next;

    uint32_t bucket_size = 0;
    uint32_t log_bucket_size = 0;
    uint32_t row_count = 0; // real row count

    size_t build_column_count = 0;
    size_t output_build_column_count = 0;
    size_t lazy_output_build_column_count = 0;
    size_t probe_column_count = 0;
    size_t output_probe_column_count = 0;
    size_t lazy_output_probe_column_count = 0;

    bool with_other_conjunct = false;
    bool left_to_nullable = false;
    bool right_to_nullable = false;
    bool has_large_column = false;

    float keys_per_bucket = 0;
    size_t used_buckets = 0;
    bool cache_miss_serious = false;
    bool enable_late_materialization = false;
    bool is_collision_free_and_unique = false;

    TJoinOp::type join_type = TJoinOp::INNER_JOIN;

    std::unique_ptr<MemPool> build_pool = nullptr;
    std::vector<JoinKeyDesc> join_keys;

    float get_keys_per_bucket() const { return keys_per_bucket; }
    bool ht_cache_miss_serious() const { return cache_miss_serious; }

    void calculate_ht_info(size_t key_bytes) {
        if (used_buckets == 0) { // to avoid redo
            used_buckets = SIMD::count_nonzero(first);
            keys_per_bucket = used_buckets == 0 ? 0 : row_count * 1.0 / used_buckets;
            size_t probe_bytes = key_bytes + row_count * sizeof(uint32_t);
            // cache miss is serious when
            // 1) the ht's size is enough large, for example, larger than (1UL << 27) bytes.
            // 2) smaller ht but most buckets have more than one keys
            cache_miss_serious = row_count > (1UL << 18) &&
                                 ((probe_bytes > (1UL << 25) && keys_per_bucket > 2) ||
                                  (probe_bytes > (1UL << 26) && keys_per_bucket > 1.5) || probe_bytes > (1UL << 27));
            VLOG_QUERY << "ht cache miss serious = " << cache_miss_serious << " row# = " << row_count
                       << " , bytes = " << probe_bytes << " , depth = " << keys_per_bucket;

            is_collision_free_and_unique = used_buckets == row_count;
        }
    }
};

} // namespace starrocks
