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

import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRequireBatchSlotRequest;
import com.starrocks.thrift.TRequireBatchSlotResponse;
import com.starrocks.thrift.TRequireSlotRequest;
import com.starrocks.thrift.TRequireSlotResponse;
import org.apache.thrift.TException;

import java.util.List;
import java.util.function.Consumer;

public class RequireSlotRpcTask {
    public static class SimpleTask extends SimpleSlotRpcTask<TRequireSlotRequest, TRequireSlotResponse> {

        public SimpleTask(TRequireSlotRequest request, TNetworkAddress address,
                          Consumer<TRequireSlotResponse> onSuccessFunc, Consumer<Exception> onFailureFunc) {
            super(request, address, onSuccessFunc, onFailureFunc);
        }

        @Override
        public boolean isMergeable() {
            return true;
        }

        @Override
        public RpcMethod getRpcMethod() {
            return RpcMethod.REQUIRE_SLOT;
        }

        @Override
        public Factory getMergedTaskFactory() {
            return (tasks) -> {
                List<SlotRpcTask<TRequireSlotRequest, TRequireSlotResponse>> castTasks = tasks.stream()
                        .map(task -> (SlotRpcTask<TRequireSlotRequest, TRequireSlotResponse>) task)
                        .collect(java.util.stream.Collectors.toList());
                return new MergedTask(castTasks);
            };
        }

        @Override
        public TRequireSlotResponse sendRequest(FrontendService.Client client, TRequireSlotRequest request)
                throws TException {
            return client.requireSlotAsync(request);
        }
    }

    public static class MergedTask extends
            MergedSlotRpcTask<TRequireSlotRequest, TRequireSlotResponse,
                    TRequireBatchSlotRequest, TRequireBatchSlotResponse> {

        public MergedTask(List<SlotRpcTask<TRequireSlotRequest, TRequireSlotResponse>> tasks) {
            super(tasks);
        }

        @Override
        public RpcMethod getRpcMethod() {
            return RpcMethod.REQUIRE_SLOT;
        }

        @Override
        public TRequireBatchSlotRequest mergeRequests(List<TRequireSlotRequest> requests) {
            TRequireBatchSlotRequest mergedRequest = new TRequireBatchSlotRequest();
            mergedRequest.setRequests(requests);
            return mergedRequest;
        }

        @Override
        public List<TRequireSlotResponse> getResponses(TRequireBatchSlotResponse response) {
            return response.getResponses();
        }

        @Override
        public TRequireBatchSlotResponse sendRequest(FrontendService.Client client,
                                                     TRequireBatchSlotRequest request) throws TException {
            return client.requireBatchSlotAsync(request);
        }
    }
}
