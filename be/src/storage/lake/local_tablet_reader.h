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

#include <memory>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {
class Tablet;

class LocalTabletReader {
public:
    LocalTabletReader() = default;

    Status init(std::shared_ptr<Tablet> tablet, TabletMetadataPtr metadata, TabletSchemaCSPtr tablet_schema,
                int64_t version);

    Status multi_get(const Chunk& keys, const std::vector<std::string>& value_field_names,
                     const std::vector<ColumnId>& value_column_ids, const Schema& value_schema,
                     std::vector<bool>* found, ChunkPtr* chunk);

private:
    std::shared_ptr<Tablet> _tablet;
    TabletMetadataPtr _metadata;
    TabletSchemaCSPtr _tablet_schema;
    int64_t _version{0};
};

} // namespace starrocks::lake
