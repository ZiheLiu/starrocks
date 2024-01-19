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

package com.starrocks.common.pool;

import com.starrocks.thrift.TNetworkAddress;

public interface ConnectionPool<V> {
    boolean reopen(V object, int timeoutMs);

    boolean reopen(V object);

    void clearPool(TNetworkAddress addr);

    boolean peak(V object);

    V borrowObject(TNetworkAddress address) throws Exception;

    V borrowObject(TNetworkAddress address, int timeoutMs) throws Exception;

    void returnObject(TNetworkAddress address, V object);

    void invalidateObject(TNetworkAddress address, V object);

    void setTimeoutMs(int timeoutMs);
}