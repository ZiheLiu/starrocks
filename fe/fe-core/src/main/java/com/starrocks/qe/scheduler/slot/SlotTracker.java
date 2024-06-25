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

import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SlotTracker {
    private final ConcurrentMap<TUniqueId, LogicalSlot> slots = new ConcurrentHashMap<>();

    private final Set<LogicalSlot> slotsOrderByExpiredTime = new TreeSet<>(
            Comparator.comparingLong(LogicalSlot::getExpiredPendingTimeMs)
                    .thenComparing(LogicalSlot::getSlotId));

    private final Map<TUniqueId, LogicalSlot> pendingSlots = new HashMap<>();
    private final Map<TUniqueId, LogicalSlot> allocatedSlots = new HashMap<>();

    private int numAllocatedSlots = 0;

    private final List<Listener> listeners;

    public SlotTracker(List<Listener> listeners) {
        this.listeners = listeners;
    }

    public boolean requireSlot(LogicalSlot slot) {
        if (GlobalVariable.isQueryQueueMaxQueuedQueriesEffective() &&
                pendingSlots.size() >= GlobalVariable.getQueryQueueMaxQueuedQueries()) {
            return false;
        }

        if (slots.containsKey(slot.getSlotId())) {
            return true;
        }

        slots.put(slot.getSlotId(), slot);
        slotsOrderByExpiredTime.add(slot);
        pendingSlots.put(slot.getSlotId(), slot);

        MetricRepo.COUNTER_QUERY_QUEUE_SLOT_PENDING.increase((long) slot.getNumPhysicalSlots());

        listeners.forEach(listener -> listener.onRequireSlot(slot));
        slot.onRequire();

        return true;
    }

    public void allocateSlot(LogicalSlot slot) {
        TUniqueId slotId = slot.getSlotId();
        if (!slots.containsKey(slotId)) {
            return;
        }

        if (pendingSlots.remove(slotId) == null) {
            return;
        }
        MetricRepo.COUNTER_QUERY_QUEUE_SLOT_PENDING.increase((long) -slot.getNumPhysicalSlots());

        if (allocatedSlots.put(slotId, slot) != null) {
            return;
        }

        MetricRepo.COUNTER_QUERY_QUEUE_SLOT_RUNNING.increase((long) slot.getNumPhysicalSlots());
        numAllocatedSlots += slot.getNumPhysicalSlots();

        listeners.forEach(listener -> listener.onAllocateSlot(slot));
        slot.onAllocate();
    }

    public LogicalSlot releaseSlot(TUniqueId slotId) {
        LogicalSlot slot = slots.remove(slotId);
        if (slot == null) {
            return null;
        }

        slotsOrderByExpiredTime.remove(slot);

        if (allocatedSlots.remove(slotId) != null) {
            numAllocatedSlots -= slot.getNumPhysicalSlots();
            MetricRepo.COUNTER_QUERY_QUEUE_SLOT_RUNNING.increase((long) -slot.getNumPhysicalSlots());
        } else {
            if (pendingSlots.remove(slotId) != null) {
                MetricRepo.COUNTER_QUERY_QUEUE_SLOT_PENDING.increase((long) -slot.getNumPhysicalSlots());
            }
        }

        listeners.forEach(listener -> listener.onReleaseSlot(slot));
        slot.onRelease();

        return slot;
    }

    public List<LogicalSlot> peakExpiredSlots() {
        final long nowMs = System.currentTimeMillis();
        List<LogicalSlot> expiredSlots = new ArrayList<>();
        for (LogicalSlot slot : slotsOrderByExpiredTime) {
            if (!slot.isAllocatedExpired(nowMs)) {
                break;
            }
            expiredSlots.add(slot);
        }
        return expiredSlots;
    }

    public long getMinExpiredTimeMs() {
        if (slotsOrderByExpiredTime.isEmpty()) {
            return 0;
        }
        return slotsOrderByExpiredTime.iterator().next().getExpiredPendingTimeMs();
    }

    public Collection<LogicalSlot> getSlots() {
        return slots.values();
    }

    public LogicalSlot getSlot(TUniqueId slotId) {
        return slots.get(slotId);
    }

    public int getNumAllocatedSlots() {
        return numAllocatedSlots;
    }

    public interface Listener {
        void onRequireSlot(LogicalSlot slot);

        void onAllocateSlot(LogicalSlot slot);

        void onReleaseSlot(LogicalSlot slot);
    }

}
