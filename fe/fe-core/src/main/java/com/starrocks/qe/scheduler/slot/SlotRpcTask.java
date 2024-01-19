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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.common.Config;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.thrift.TException;

import java.util.List;

public abstract class SlotRpcTask<Req, Res> implements Runnable {
    public interface Factory {
        SlotRpcTask<?, ?> create(List<SlotRpcTask<?, ?>> tasks);
    }

    public enum RpcMethod {
        FINISH_SLOT_REQUIREMENT,
        REQUIRE_SLOT,
        RELEASE_SLOT
    }

    protected final TNetworkAddress address;

    public SlotRpcTask(TNetworkAddress address) {
        this.address = address;
    }

    public abstract boolean isMergeable();

    public abstract Factory getMergedTaskFactory();

    public abstract RpcMethod getRpcMethod();

    public abstract Req getRequest();

    public abstract Res sendRequest(FrontendService.Client client, Req request) throws TException;

    public abstract void onSuccess(Res response);

    public abstract void onFailure(Exception exception);

    public TNetworkAddress getAddress() {
        return address;
    }

    @Override
    public void run() {
        try {
            Req request = getRequest();
            Res response = FrontendServiceProxy.callWithPartition(address, Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times, client -> sendRequest(client, request));
            onSuccess(response);
        } catch (Exception e) {
            onFailure(e);
        }
    }
}
