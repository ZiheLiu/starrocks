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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowFlightSqlBackendResult {
    private final long backendId;
    private final TUniqueId fragmentInstanceId;
    private final Schema schema;

    public ArrowFlightSqlBackendResult(long backendId, TUniqueId fragmentInstanceId, Schema schema) {
        this.backendId = backendId;
        this.fragmentInstanceId = fragmentInstanceId;
        this.schema = schema;
    }

    public long getBackendId() {
        return backendId;
    }

    public TUniqueId getFragmentInstanceId() {
        return fragmentInstanceId;
    }

    public Schema getSchema() {
        return schema;
    }
}
