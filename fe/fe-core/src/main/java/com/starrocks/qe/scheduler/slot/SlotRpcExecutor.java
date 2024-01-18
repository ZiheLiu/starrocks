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

import com.starrocks.common.Pair;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlotRpcExecutor {
    private static final Logger LOG = LogManager.getLogger(SlotRpcExecutor.class);

    private final AtomicBoolean started = new AtomicBoolean();
    private final LockFreeBlockingQueue<SlotRpcTask<?, ?>> sendRpcQueue = new LockFreeBlockingQueue<>();
    private final LockFreeBlockingQueue<SlotRpcTask<?, ?>> mergeRpcQueue = new LockFreeBlockingQueue<>();
    private final Thread[] sendRpcWorkers;
    private final Thread mergeRpcWorker;

    public SlotRpcExecutor(int numThreads) {
        sendRpcWorkers = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            sendRpcWorkers[i] = new Thread(new SendRpcWorker(), "slot-rpc-" + i);
            sendRpcWorkers[i].setDaemon(true);
        }

        mergeRpcWorker = new Thread(new MergeRpcWorker(), "slot-rpc-merge");
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            for (Thread thread : sendRpcWorkers) {
                thread.start();
            }
            mergeRpcWorker.start();
        }
    }

    public void execute(SlotRpcTask<?, ?> job) {
        mergeRpcQueue.add(job);
    }

    private class SendRpcWorker implements Runnable {
        public void runOneCycle() {
            try {
                SlotRpcTask<?, ?> task = sendRpcQueue.take();
                task.run();
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("[Slot] SendRpcWorker thread interrupted", e);
            } catch (Exception e) {
                LOG.warn("[Slot] SendRpcWorker thread throws unexpected error", e);
            }
        }

        @Override
        public void run() {
            for (; ; ) {
                runOneCycle();
            }
        }
    }

    private class MergeRpcWorker implements Runnable {
        private final Map<Pair<TNetworkAddress, SlotRpcTask.RpcMethod>, List<SlotRpcTask<?, ?>>> tasksToMerge = new HashMap<>();

        private void mergeOrSendRpcTask(
                Map<Pair<TNetworkAddress, SlotRpcTask.RpcMethod>, List<SlotRpcTask<?, ?>>> tasksToMerge,
                SlotRpcTask<?, ?> task) {
            if (task.isMergeable()) {
                tasksToMerge
                        .computeIfAbsent(Pair.create(task.getAddress(), task.getRpcMethod()), (k) -> new ArrayList<>())
                        .add(task);
            } else {
                sendRpcQueue.add(task);
            }
        }

        private void runOneCycle() {
            SlotRpcTask<?, ?> task = null;
            try {
                try {
                    task = mergeRpcQueue.take();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.warn("[Slot] MergeRpcWorker thread interrupted", e);
                }
                if (task != null) {
                    mergeOrSendRpcTask(tasksToMerge, task);
                }

                while ((task = sendRpcQueue.poll()) != null) {
                    mergeOrSendRpcTask(tasksToMerge, task);
                }

                tasksToMerge.forEach((key, curTasks) -> {
                    SlotRpcTask<?, ?> mergedTask = curTasks.get(0).getMergedTaskFactory().create(curTasks);
                    sendRpcQueue.add(mergedTask);
                });

            } catch (Exception e) {
                LOG.warn("[Slot] MergeRpcWorker thread throws unexpected error", e);
            } finally {
                tasksToMerge.clear();
            }
        }

        @Override
        public void run() {
            for (; ; ) {
                runOneCycle();
            }
        }
    }
}
