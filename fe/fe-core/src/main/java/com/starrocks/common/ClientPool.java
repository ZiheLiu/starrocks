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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/ClientPool.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.starrocks.common.pool.DefaultGenericPool;
import com.starrocks.common.pool.GenericPool;
import com.starrocks.common.pool.PartitionedPool;
import com.starrocks.common.pool.Poolable;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TFileBrokerService;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class ClientPool {
    static GenericKeyedObjectPoolConfig heartbeatConfig = new GenericKeyedObjectPoolConfig();
    static int heartbeatTimeoutMs = Config.heartbeat_timeout_second * 1000;

    static GenericKeyedObjectPoolConfig backendConfig = new GenericKeyedObjectPoolConfig();
    static int backendTimeoutMs = 60000; // 1min

    static GenericKeyedObjectPoolConfig brokerPoolConfig = new GenericKeyedObjectPoolConfig();
    public static int brokerTimeoutMs = Config.broker_client_timeout_ms;

    static GenericKeyedObjectPoolConfig slotConfig = new GenericKeyedObjectPoolConfig();

    static {
        heartbeatConfig.setLifo(true);            // set Last In First Out strategy
        heartbeatConfig.setMaxIdlePerKey(2);      // (default 2)
        heartbeatConfig.setMinIdlePerKey(1);      // (default 1)
        heartbeatConfig.setMaxTotalPerKey(-1);    // (default -1)
        heartbeatConfig.setMaxTotal(-1);          // (default -1)
        heartbeatConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    static {
        backendConfig.setLifo(true);            // set Last In First Out strategy
        backendConfig.setMaxIdlePerKey(128);    // (default 128)
        backendConfig.setMinIdlePerKey(2);      // (default 2)
        backendConfig.setMaxTotalPerKey(-1);    // (default -1)
        backendConfig.setMaxTotal(-1);          // (default -1)
        backendConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    static {
        brokerPoolConfig.setLifo(true);            // set Last In First Out strategy
        brokerPoolConfig.setMaxIdlePerKey(128);    // (default 128)
        brokerPoolConfig.setMinIdlePerKey(2);      // (default 2)
        brokerPoolConfig.setMaxTotalPerKey(-1);    // (default -1)
        brokerPoolConfig.setMaxTotal(-1);          // (default -1)
        brokerPoolConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    static {
        slotConfig.setLifo(true);            // set Last In First Out strategy
        slotConfig.setMaxIdlePerKey(128);    // (default 128)
        slotConfig.setMinIdlePerKey(2);      // (default 2)
        slotConfig.setMaxTotalPerKey(-1);    // (default -1)
        slotConfig.setMaxTotal(-1);          // (default -1)
        slotConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    public static DefaultGenericPool<HeartbeatService.Client> beHeartbeatPool =
            new DefaultGenericPool<>("HeartbeatService", heartbeatConfig, heartbeatTimeoutMs);
    public static DefaultGenericPool<TFileBrokerService.Client> brokerHeartbeatPool =
            new DefaultGenericPool<>("TFileBrokerService", heartbeatConfig, heartbeatTimeoutMs);
    public static DefaultGenericPool<FrontendService.Client> frontendPool =
            new DefaultGenericPool<>("FrontendService", backendConfig, backendTimeoutMs);
    public static DefaultGenericPool<BackendService.Client> backendPool =
            new DefaultGenericPool<>("BackendService", backendConfig, backendTimeoutMs);
    public static DefaultGenericPool<TFileBrokerService.Client> brokerPool =
            new DefaultGenericPool<>("TFileBrokerService", brokerPoolConfig, brokerTimeoutMs);

    public static GenericPool<Poolable<FrontendService.Client>> slotPool =
            new PartitionedPool<>("FrontendService", slotConfig, backendTimeoutMs, Config.num_slot_partition_pools);
}
