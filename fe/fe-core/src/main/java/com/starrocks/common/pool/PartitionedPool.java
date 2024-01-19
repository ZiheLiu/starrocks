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

import com.starrocks.common.GenericPool;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;

public class PartitionedPool<V extends TServiceClient> implements ConnectionPool<Poolable<V>> {
    private final GenericPool<V>[] partitionPools;

    public PartitionedPool(String className, GenericKeyedObjectPoolConfig<V> config, int timeoutMs, int numPartitions) {
        partitionPools = new GenericPool[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitionPools[i] = new GenericPool<>(className, config, timeoutMs);
        }
    }

    @Override
    public boolean reopen(Poolable<V> object, int timeoutMs) {
        return partitionPools[object.getPartition()].reopen(object.getObject(), timeoutMs);
    }

    @Override
    public boolean reopen(Poolable<V> object) {
        return partitionPools[object.getPartition()].reopen(object.getObject());
    }

    @Override
    public void clearPool(TNetworkAddress addr) {
        for (GenericPool<V> pool : partitionPools) {
            pool.clearPool(addr);
        }
    }

    @Override
    public boolean peak(Poolable<V> object) {
        return partitionPools[object.getPartition()].peak(object.getObject());
    }

    @Override
    public Poolable<V> borrowObject(TNetworkAddress address) throws Exception {
        int partition = getPartition();
        V object = partitionPools[partition].borrowObject(address);
        return new Poolable<>(object, partition);
    }

    @Override
    public Poolable<V> borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
        int partition = getPartition();
        V object = partitionPools[partition].borrowObject(address, timeoutMs);
        return new Poolable<>(object, partition);
    }

    @Override
    public void returnObject(TNetworkAddress address, Poolable<V> object) {
        partitionPools[object.getPartition()].returnObject(address, object.getObject());
    }

    @Override
    public void invalidateObject(TNetworkAddress address, Poolable<V> object) {
        partitionPools[object.getPartition()].invalidateObject(address, object.getObject());
    }

    @Override
    public void setTimeoutMs(int timeoutMs) {
        for (GenericPool<V> pool : partitionPools) {
            pool.setTimeoutMs(timeoutMs);
        }
    }

    private int getPartition() {
        return (int) (Thread.currentThread().getId() % partitionPools.length);
    }
}
