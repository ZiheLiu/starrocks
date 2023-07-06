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

import com.google.common.collect.Maps;
import com.starrocks.common.Status;
import jline.internal.Log;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SharingSlotRequirement {
    private final Map<Long, SharingSlot> workerID2requirement;
    private final CompletableFuture<Status> future;

    private SharingSlotRequirement() {
        this.workerID2requirement = Maps.newHashMap();

        this.future = new CompletableFuture<>();
    }

    public boolean isEmpty() {
        return workerID2requirement.isEmpty();
    }

    public boolean containsWorker(Long workerID) {
        return workerID2requirement.containsKey(workerID);
    }

    @Override
    public String toString() {
        return "SharingSlotRequirement{" +
                "requirements=" + workerID2requirement.values() +
                ", future=" + future +
                '}';
    }

    public Status waitAllocated() {
        while (true) {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Log.info("waitAllocated gets interrupted exception");
            } catch (ExecutionException e) {
                Log.warn("waitAllocated gets execution exception", e);
                return Status.createInternalError(e.getMessage());
            }
        }
    }

    public void onAllocated() {
        future.complete(Status.OK);
    }

    public void onFailed(Status status) {
        future.complete(status);
    }

    public void onCancel() {
        future.complete(Status.CANCELLED);
    }

    public static class Builder {
        private final SharingSlotRequirement instance = new SharingSlotRequirement();

        public Builder add(Long workerID, int numPhysicalSlots) {
            instance.workerID2requirement.computeIfAbsent(workerID, k -> new SharingSlot(workerID, numPhysicalSlots));

            return this;
        }

        public SharingSlotRequirement build() {
            return instance;
        }
    }
}
