// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless Released by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe.scheduler.slot;

import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReleaseBatchSlotRequest;
import com.starrocks.thrift.TReleaseBatchSlotResponse;
import com.starrocks.thrift.TReleaseSlotRequest;
import com.starrocks.thrift.TReleaseSlotResponse;
import org.apache.thrift.TException;

import java.util.List;
import java.util.function.Consumer;

public class ReleaseSlotRpcTask {
    public static class SimpleTask extends SimpleSlotRpcTask<TReleaseSlotRequest, TReleaseSlotResponse> {

        public SimpleTask(TReleaseSlotRequest request, TNetworkAddress address,
                          Consumer<TReleaseSlotResponse> onSuccessFunc, Consumer<Exception> onFailureFunc) {
            super(request, address, onSuccessFunc, onFailureFunc);
        }

        @Override
        public boolean isMergeable() {
            return true;
        }

        @Override
        public RpcMethod getRpcMethod() {
            return RpcMethod.RELEASE_SLOT;
        }

        @Override
        public Factory getMergedTaskFactory() {
            return (tasks) -> {
                List<SlotRpcTask<TReleaseSlotRequest, TReleaseSlotResponse>> castTasks = tasks.stream()
                        .map(task -> (SlotRpcTask<TReleaseSlotRequest, TReleaseSlotResponse>) task)
                        .collect(java.util.stream.Collectors.toList());
                return new MergedTask(castTasks);
            };
        }

        @Override
        public TReleaseSlotResponse sendRequest(FrontendService.Client client, TReleaseSlotRequest request)
                throws TException {
            return client.releaseSlot(request);
        }
    }

    public static class MergedTask extends
            MergedSlotRpcTask<TReleaseSlotRequest, TReleaseSlotResponse,
                    TReleaseBatchSlotRequest, TReleaseBatchSlotResponse> {

        public MergedTask(List<SlotRpcTask<TReleaseSlotRequest, TReleaseSlotResponse>> tasks) {
            super(tasks);
        }

        @Override
        public RpcMethod getRpcMethod() {
            return RpcMethod.REQUIRE_SLOT;
        }

        @Override
        public TReleaseBatchSlotRequest mergeRequests(List<TReleaseSlotRequest> requests) {
            TReleaseBatchSlotRequest mergedRequest = new TReleaseBatchSlotRequest();
            mergedRequest.setRequests(requests);
            return mergedRequest;
        }

        @Override
        public List<TReleaseSlotResponse> getResponses(TReleaseBatchSlotResponse response) {
            return response.getResponses();
        }

        @Override
        public TReleaseBatchSlotResponse sendRequest(FrontendService.Client client,
                                                     TReleaseBatchSlotRequest request) throws TException {
            return client.releaseBatchSlot(request);
        }
    }
}
