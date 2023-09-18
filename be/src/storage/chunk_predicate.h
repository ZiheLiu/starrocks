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

#include "column/chunk.h"
#include "column/column.h" // Column
#include "common/status.h"

namespace starrocks {

class ChunkPredicate {
public:
    virtual ~ChunkPredicate() = default;

    virtual Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    Status evaluate(const Chunk* chunk, uint8_t* selection) const {
        return evaluate(chunk, selection, 0, chunk->num_rows());
    }

    Status evaluate_and(const Chunk* chunk, uint8_t* selection) const {
        return evaluate_and(chunk, selection, 0, chunk->num_rows());
    }

    Status evaluate_or(const Chunk* chunk, uint8_t* selection) const {
        return evaluate_or(chunk, selection, 0, chunk->num_rows());
    }
};

} // namespace starrocks
