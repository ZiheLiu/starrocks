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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.compress.utils.Lists;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SlotSelectionStrategyV2 implements SlotSelectionStrategy {
    private static final long UPDATE_OPTIONS_INTERVAL_MS = 1000;

    /**
     * These three members are initialized in the first call of {@link #updateOptionsPeriodically};
     */
    private long lastUpdateOptionsTime = 0;
    private QueryQueueOptions opts = null;
    private WeightedRoundRobinQueue requiringQueue = null;

    private final Map<TUniqueId, SlotContext> slotContexts = Maps.newHashMap();

    private final LinkedHashMap<TUniqueId, SlotContext> requiringSmallSlots = new LinkedHashMap<>();
    private int numAllocatedSmallSlots = 0;

    @Override
    public void onRequireSlot(LogicalSlot slot) {
        updateOptionsPeriodically();

        SlotContext slotContext = new SlotContext(slot);

        slotContexts.put(slot.getSlotId(), slotContext);
        if (isSmallSlot(slot)) {
            requiringSmallSlots.put(slot.getSlotId(), slotContext);
        }
        requiringQueue.add(slotContext);

        MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING.getMetric(String.valueOf(slotContext.getSubQueueIndex()))
                .increase((long) slot.getNumPhysicalSlots());
    }

    @Override
    public void onAllocateSlot(LogicalSlot slot) {
        updateOptionsPeriodically();

        SlotContext slotContext = slotContexts.get(slot.getSlotId());
        if (isSmallSlot(slot)) {
            requiringSmallSlots.remove(slot.getSlotId());
            if (slotContext.isAllocatedAsSmallSlot()) {
                numAllocatedSmallSlots += slot.getNumPhysicalSlots();
            }
        }
        requiringQueue.remove(slotContext);

        MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING.getMetric(String.valueOf(slotContext.getSubQueueIndex()))
                .increase((long) -slot.getNumPhysicalSlots());
        MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_RUNNING.getMetric(String.valueOf(slotContext.getSubQueueIndex()))
                .increase((long) slot.getNumPhysicalSlots());
    }

    @Override
    public void onReleaseSlot(LogicalSlot slot) {
        updateOptionsPeriodically();

        SlotContext slotContext = slotContexts.remove(slot.getSlotId());
        if (isSmallSlot(slot)) {
            requiringSmallSlots.remove(slot.getSlotId());
            if (slotContext.isAllocatedAsSmallSlot()) {
                numAllocatedSmallSlots -= slot.getNumPhysicalSlots();
            }
        }
        if (requiringQueue.remove(slotContext)) {
            MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING.getMetric(String.valueOf(slotContext.getSubQueueIndex()))
                    .increase((long) -slot.getNumPhysicalSlots());
        } else {
            MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_RUNNING.getMetric(String.valueOf(slotContext.getSubQueueIndex()))
                    .increase((long) -slot.getNumPhysicalSlots());
        }
    }

    @Override
    public List<LogicalSlot> peakSlotsToAllocate(SlotTracker slotTracker) {
        updateOptionsPeriodically();

        List<LogicalSlot> slotsToAllocate = Lists.newArrayList();

        int curNumAllocatedSmallSlots = numAllocatedSmallSlots;
        for (SlotContext slotContext : requiringSmallSlots.values()) {
            LogicalSlot slot = slotContext.getSlot();
            if (curNumAllocatedSmallSlots + slot.getNumPhysicalSlots() > opts.v2().getTotalSmallSlots()) {
                break;
            }

            requiringQueue.remove(slotContext);

            slotsToAllocate.add(slot);
            slotContext.setAllocateAsSmallSlot();
            curNumAllocatedSmallSlots += slot.getNumPhysicalSlots();
        }

        int numAllocatedSlots = slotTracker.getNumAllocatedSlots() - numAllocatedSmallSlots;
        while (!requiringQueue.isEmpty()) {
            SlotContext slotContext = requiringQueue.peak();
            if (!isGlobalSlotAvailable(numAllocatedSlots, slotContext.getSlot())) {
                break;
            }

            requiringQueue.poll();

            slotsToAllocate.add(slotContext.getSlot());
            numAllocatedSlots += slotContext.getSlot().getNumPhysicalSlots();
        }

        return slotsToAllocate;
    }

    private void updateOptionsPeriodically() {
        long now = System.currentTimeMillis();
        if (now - lastUpdateOptionsTime < UPDATE_OPTIONS_INTERVAL_MS) {
            return;
        }

        lastUpdateOptionsTime = now;
        QueryQueueOptions newOpts = QueryQueueOptions.createFromEnv();
        if (!newOpts.equals(opts)) {
            opts = newOpts;
            requiringQueue = new WeightedRoundRobinQueue(opts.v2().getTotalSlots(), opts.v2().getNumWorkers());
            slotContexts.values().stream()
                    .filter(slotContext -> slotContext.getSlot().getState() == LogicalSlot.State.REQUIRING)
                    .forEach(slotContext -> requiringQueue.add(slotContext));
        }
    }

    private boolean isGlobalSlotAvailable(int numAllocatedSlots, LogicalSlot slot) {
        final int numTotalSlots = opts.v2().getTotalSlots();
        return numAllocatedSlots == 0 || numAllocatedSlots + slot.getNumPhysicalSlots() <= numTotalSlots;
    }

    private static boolean isSmallSlot(LogicalSlot slot) {
        return slot.getNumPhysicalSlots() <= 1;
    }

    @VisibleForTesting
    static class WeightedRoundRobinQueue {
        private static final int NUM_SUB_QUEUES = 8;

        private final int lastSubQueueLog2Bound;
        private final int numWorkers;

        private final SlotSubQueue[] subQueues;
        private int size = 0;
        private final int totalWeight;

        private SlotContext nextSlotToPeak = null;

        public WeightedRoundRobinQueue(int totalSlots, int numWorkers) {
            this.numWorkers = numWorkers;

            final int numSlotsPerWorker = totalSlots / numWorkers;
            this.lastSubQueueLog2Bound = Utils.log2(numSlotsPerWorker);

            int curMinSlots = 1 << lastSubQueueLog2Bound;
            int curWeight = 1;
            this.subQueues = new SlotSubQueue[NUM_SUB_QUEUES];
            int totalWeight = 0;
            for (int i = subQueues.length - 1; i >= 0; i--) {
                subQueues[i] = new SlotSubQueue(curWeight);
                totalWeight += curWeight;

                String category = String.valueOf(i);
                LongCounterMetric m = MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING.getMetric(category);
                m.increase(-m.getValue());  // Reset the pending slots of this category.
                MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_WEIGHT.getMetric(category).setValue(curWeight);
                MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_MIN_SLOTS.getMetric(category).setValue(curMinSlots * numWorkers);

                if (curMinSlots > 1) {
                    curWeight = curWeight << 2;
                    curMinSlots = curMinSlots >>> 1;
                }
            }
            this.totalWeight = totalWeight;
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void add(SlotContext slotContext) {
            int queueIndex = getSubQueueIndex(slotContext);
            SlotSubQueue subQueue = subQueues[queueIndex];
            if (subQueue.slots.put(slotContext.getSlotId(), slotContext) == null) {
                slotContext.setSubQueueIndex(queueIndex);
                size++;

                if (subQueue.slots.size() == 1) {
                    if (subQueue.isPaused()) {
                        subQueue.resumeState();
                    }

                    if (nextSlotToPeak != null) {
                        // If the previous peaked slot is in a lower priority sub-queue due to this higher priority sub-queue is empty,
                        // peak the slot in this higher priority sub-queue instead.
                        SlotSubQueue prevPeakSubQueue = subQueues[nextSlotToPeak.getSubQueueIndex()];
                        if (prevPeakSubQueue.state + totalWeight < subQueue.state) {
                            nextSlotToPeak = slotContext;
                            subQueue.state -= totalWeight;
                            prevPeakSubQueue.state += totalWeight;
                        }
                    }
                }
            }
        }

        public boolean remove(SlotContext slotContext) {
            int queueIndex = getSubQueueIndex(slotContext);
            TUniqueId slotId = slotContext.getSlotId();
            boolean contains = subQueues[queueIndex].slots.remove(slotId) != null;
            if (contains) {
                size--;
                if (nextSlotToPeak != null && slotId.equals(nextSlotToPeak.getSlotId())) {
                    nextSlotToPeak = null;
                }
            }
            return contains;
        }

        public SlotContext peak() {
            if (size == 0) {
                return null;
            }

            if (nextSlotToPeak != null) {
                return nextSlotToPeak;
            }

            int queueIndex = nextSubQueueIndex();
            nextSlotToPeak = subQueues[queueIndex].slots.values().iterator().next();
            return nextSlotToPeak;
        }

        public SlotContext poll() {
            SlotContext slotContext = peak();
            if (slotContext != null) {
                remove(slotContext);
            }
            return slotContext;
        }

        private int nextSubQueueIndex() {
            int maxQueueIndex = -1;
            if (isEmpty()) {
                return maxQueueIndex;
            }

            for (int i = 0; i < subQueues.length; i++) {
                SlotSubQueue queue = subQueues[i];

                queue.state += queue.weight;

                if (queue.slots.isEmpty()) {
                    if (!queue.isPaused()) {
                        queue.pauseState();
                    }
                } else if (maxQueueIndex == -1 || queue.state > subQueues[maxQueueIndex].state) {
                    maxQueueIndex = i;
                }
            }

            subQueues[maxQueueIndex].state -= totalWeight;

            return maxQueueIndex;
        }

        private int getSubQueueIndex(SlotContext slotContext) {
            int numSlotsPerWorker = slotContext.getSlot().getNumPhysicalSlots() / numWorkers;
            int index = (NUM_SUB_QUEUES - 1) - (lastSubQueueLog2Bound - Utils.log2(numSlotsPerWorker));
            return Math.max(0, Math.min(index, NUM_SUB_QUEUES - 1));
        }

        private static class SlotSubQueue {
            private static final int PAUSED_WEIGHT = 0;

            private final LinkedHashMap<TUniqueId, SlotContext> slots = Maps.newLinkedHashMap();
            private final int staticWeight;
            private int weight;
            private int state;
            private int toResumeState;

            public SlotSubQueue(int weight) {
                this.staticWeight = weight;
                this.weight = weight;
                this.state = 0;
                this.toResumeState = 0;
            }

            public void resumeState() {
                state = toResumeState;
                toResumeState = 0;
                weight = staticWeight;
            }

            public void pauseState() {
                toResumeState = state;
                state = Integer.MIN_VALUE;
                weight = PAUSED_WEIGHT;
            }

            public boolean isPaused() {
                return weight == PAUSED_WEIGHT;
            }
        }
    }

    @VisibleForTesting
    static class SlotContext {
        private final LogicalSlot slot;
        private boolean allocatedAsSmallSlot = false;
        private int subQueueIndex = 0;

        public SlotContext(LogicalSlot slot) {
            this.slot = slot;
        }

        public boolean isAllocatedAsSmallSlot() {
            return allocatedAsSmallSlot;
        }

        public void setAllocateAsSmallSlot() {
            this.allocatedAsSmallSlot = true;
        }

        public LogicalSlot getSlot() {
            return slot;
        }

        public TUniqueId getSlotId() {
            return slot.getSlotId();
        }

        public int getSubQueueIndex() {
            return subQueueIndex;
        }

        public void setSubQueueIndex(int subQueueIndex) {
            this.subQueueIndex = subQueueIndex;
        }
    }
}
