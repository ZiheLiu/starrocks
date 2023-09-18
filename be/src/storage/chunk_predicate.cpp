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

#include "storage/chunk_predicate.h"

#include "storage/column_predicate.h"

namespace starrocks {

enum class CompoundType { AND, OR };

template <CompoundType type>
class CompoundChunkPredicate final : public ChunkPredicate {
public:
    ~CompoundChunkPredicate() override = default;

    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;

private:
    std::vector<ChunkPredicate*> _chunk_preds;
    std::unordered_map<ColumnId, std::vector<ColumnPredicate*>> _cid_to_col_preds;
};

using ConjunctChunkPredicate = CompoundChunkPredicate<CompoundType::AND>;
using DisjunctChunkPredicate = CompoundChunkPredicate<CompoundType::OR>;

template <CompoundType type>
Status CompoundChunkPredicate<type>::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from,
                                              uint16_t to) const {
    for (const auto& [cid, col_preds] : _cid_to_col_preds) {
    }
}

template <CompoundType type>
Status CompoundChunkPredicate<type>::evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from,
                                                  uint16_t to) const {}

template <CompoundType type>
Status CompoundChunkPredicate<type>::evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from,
                                                 uint16_t to) const {}

} // namespace starrocks
