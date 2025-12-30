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

#include "storage/lake/local_tablet_reader.h"

#include <map>

#include "storage/chunk_helper.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/primary_key_encoder.h"

namespace starrocks::lake {

using std::vector;

Status LocalTabletReader::init(std::shared_ptr<Tablet> tablet, TabletMetadataPtr metadata,
                               TabletSchemaCSPtr tablet_schema, int64_t version) {
    if (tablet == nullptr || metadata == nullptr || tablet_schema == nullptr) {
        return Status::InvalidArgument("invalid lake local tablet reader init params");
    }
    _tablet = std::move(tablet);
    _metadata = std::move(metadata);
    _tablet_schema = std::move(tablet_schema);
    _version = version;
    return Status::OK();
}

Status LocalTabletReader::multi_get(const Chunk& keys, const std::vector<std::string>& value_field_names,
                                    const std::vector<ColumnId>& value_column_ids, const Schema& value_schema,
                                    std::vector<bool>* found, ChunkPtr* chunk) {
    (void)value_field_names;
    if (_tablet == nullptr || _metadata == nullptr || _tablet_schema == nullptr) {
        return Status::InvalidArgument("lake local tablet reader not initialized");
    }
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }

    MutableColumnPtr pk_column;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(*_tablet_schema->schema(), &pk_column));
    PrimaryKeyEncoder::encode(*_tablet_schema->schema(), keys, 0, keys.num_rows(), pk_column.get());

    std::vector<uint64_t> rss_rowids;
    RETURN_IF_ERROR(
            _tablet->update_mgr()->get_rowids_from_pkindex(_tablet->id(), _version, pk_column, &rss_rowids, true));

    found->resize(rss_rowids.size());
    for (size_t i = 0; i < rss_rowids.size(); ++i) {
        (*found)[i] = (rss_rowids[i] >> 32) != UINT32_MAX;
    }

    size_t num_default = 0;
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    std::vector<uint32_t> idxes;
    RowsetUpdateState::plan_read_by_rssid(rss_rowids, &num_default, &rowids_by_rssid, &idxes);

    std::vector<uint32_t> read_column_ids(value_column_ids.begin(), value_column_ids.end());
    MutableColumns read_columns;
    read_columns.resize(read_column_ids.size());
    for (size_t i = 0; i < read_columns.size(); ++i) {
        auto column = ChunkHelper::column_from_field(*value_schema.field(i).get());
        read_columns[i] = column->clone_empty();
    }

    RssidFileInfoContainer container;
    container.add_rssid_to_file(*_metadata);
    TxnLogPB_OpWrite op_write;
    auto tablet_schema = std::make_shared<TabletSchema>(_metadata->schema());
    RowsetUpdateStateParams params{op_write, tablet_schema, _metadata, _tablet.get(), container};

    RETURN_IF_ERROR(_tablet->update_mgr()->get_column_values(params, read_column_ids, num_default > 0, rowids_by_rssid,
                                                             &read_columns));

    (*chunk)->reset();
    for (size_t col_idx = 0; col_idx < read_columns.size(); ++col_idx) {
        (*chunk)->get_column_by_index(col_idx)->append_selective(*read_columns[col_idx], idxes.data(), 0, idxes.size());
    }
    return Status::OK();
}

} // namespace starrocks::lake
