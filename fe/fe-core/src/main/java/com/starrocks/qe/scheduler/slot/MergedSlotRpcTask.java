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

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

public abstract class MergedSlotRpcTask<Req, Res, BatchReq, BatchRes> extends SlotRpcTask<BatchReq, BatchRes> {
    private final List<SlotRpcTask<Req, Res>> tasks;

    public MergedSlotRpcTask(List<SlotRpcTask<Req, Res>> tasks) {
        super(tasks.get(0).getAddress());
        Preconditions.checkArgument(!tasks.isEmpty(), "tasks size must be > 0");
        this.tasks = tasks;
    }

    @Override
    public boolean isMergeable() {
        return false;
    }

    @Override
    public Factory getMergedTaskFactory() {
        Preconditions.checkArgument(false, "Unreachable");
        return null;
    }

    @Override
    public BatchReq getRequest() {
        List<Req> requests = tasks.stream().map(SlotRpcTask::getRequest).collect(Collectors.toList());
        return mergeRequests(requests);
    }

    @Override
    public void onSuccess(BatchRes response) {
        List<Res> responses = getResponses(response);
        Preconditions.checkArgument(responses.size() == tasks.size(), "responses size must be equal to tasks size");
        for (int i = 0; i < tasks.size(); i++) {
            tasks.get(i).onSuccess(responses.get(i));
        }
    }

    @Override
    public void onFailure(Exception exception) {
        for (SlotRpcTask<Req, Res> task : tasks) {
            task.onFailure(exception);
        }
    }

    public abstract BatchReq mergeRequests(List<Req> requests);

    public abstract List<Res> getResponses(BatchRes response);
}
