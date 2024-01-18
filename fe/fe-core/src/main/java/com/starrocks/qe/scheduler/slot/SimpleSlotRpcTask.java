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

import com.starrocks.thrift.TNetworkAddress;

import java.util.function.Consumer;

public abstract class SimpleSlotRpcTask<Req, Res> extends SlotRpcTask<Req, Res> {

    private final Consumer<Res> onSuccessFunc;
    private final Consumer<Exception> onFailureFunc;

    private final Req request;

    public SimpleSlotRpcTask(Req request, TNetworkAddress address, Consumer<Res> onSuccessFunc,
                             Consumer<Exception> onFailureFunc) {
        super(address);
        this.request = request;
        this.onSuccessFunc = onSuccessFunc;
        this.onFailureFunc = onFailureFunc;
    }

    @Override
    public Req getRequest() {
        return request;
    }

    @Override
    public void onSuccess(Res response) {
        onSuccessFunc.accept(response);
    }

    @Override
    public void onFailure(Exception exception) {
        onFailureFunc.accept(exception);
    }
}
