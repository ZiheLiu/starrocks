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
import com.starrocks.thrift.TFinishBatchSlotRequirementRequest;
import com.starrocks.thrift.TFinishBatchSlotRequirementResponse;
import com.starrocks.thrift.TFinishSlotRequirementRequest;
import com.starrocks.thrift.TFinishSlotRequirementResponse;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.thrift.TException;

import java.util.List;
import java.util.function.Consumer;

public class FinishSlotRequirementRpcTask {
    public static class SimpleTask extends SimpleSlotRpcTask<TFinishSlotRequirementRequest, TFinishSlotRequirementResponse> {

        public SimpleTask(TFinishSlotRequirementRequest request, TNetworkAddress address,
                          Consumer<TFinishSlotRequirementResponse> onSuccessFunc, Consumer<Exception> onFailureFunc) {
            super(request, address, onSuccessFunc, onFailureFunc);
        }

        @Override
        public boolean isMergeable() {
            return true;
        }

        @Override
        public RpcMethod getRpcMethod() {
            return RpcMethod.FINISH_SLOT_REQUIREMENT;
        }

        @Override
        public Factory getMergedTaskFactory() {
            return (tasks) -> {
                List<SlotRpcTask<TFinishSlotRequirementRequest, TFinishSlotRequirementResponse>> castTasks = tasks.stream()
                        .map(task -> (SlotRpcTask<TFinishSlotRequirementRequest, TFinishSlotRequirementResponse>) task)
                        .collect(java.util.stream.Collectors.toList());
                return new MergedTask(castTasks);
            };
        }

        @Override
        public TFinishSlotRequirementResponse sendRequest(FrontendService.Client client, TFinishSlotRequirementRequest request)
                throws TException {
            return client.finishSlotRequirement(request);
        }
    }

    public static class MergedTask extends
            MergedSlotRpcTask<TFinishSlotRequirementRequest, TFinishSlotRequirementResponse,
                    TFinishBatchSlotRequirementRequest, TFinishBatchSlotRequirementResponse> {

        public MergedTask(List<SlotRpcTask<TFinishSlotRequirementRequest, TFinishSlotRequirementResponse>> tasks) {
            super(tasks);
        }

        @Override
        public RpcMethod getRpcMethod() {
            return RpcMethod.FINISH_SLOT_REQUIREMENT;
        }

        @Override
        public TFinishBatchSlotRequirementRequest mergeRequests(List<TFinishSlotRequirementRequest> requests) {
            TFinishBatchSlotRequirementRequest mergedRequest = new TFinishBatchSlotRequirementRequest();
            mergedRequest.setRequests(requests);
            return mergedRequest;
        }

        @Override
        public List<TFinishSlotRequirementResponse> getResponses(TFinishBatchSlotRequirementResponse response) {
            return response.getResponses();
        }

        @Override
        public TFinishBatchSlotRequirementResponse sendRequest(FrontendService.Client client,
                                                               TFinishBatchSlotRequirementRequest request) throws TException {
            return client.finishBatchSlotRequirement(request);
        }
    }
}
