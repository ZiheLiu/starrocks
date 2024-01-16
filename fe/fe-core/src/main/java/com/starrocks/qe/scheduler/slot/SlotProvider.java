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

import com.staros.util.LockCloseable;
import com.starrocks.common.Config;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.qe.scheduler.RecoverableException;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReleaseBatchSlotRequest;
import com.starrocks.thrift.TReleaseBatchSlotResponse;
import com.starrocks.thrift.TReleaseSlotRequest;
import com.starrocks.thrift.TReleaseSlotResponse;
import com.starrocks.thrift.TRequireBatchSlotRequest;
import com.starrocks.thrift.TRequireBatchSlotResponse;
import com.starrocks.thrift.TRequireSlotRequest;
import com.starrocks.thrift.TRequireSlotResponse;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TApplicationException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * The slot manager view in the follower FEs. It receives the slot operations from {@link com.starrocks.qe.scheduler.Coordinator}
 * and sends it to {@link SlotManager} via RPC.
 *
 * @see SlotManager
 */
public class SlotProvider {
    private static final Logger LOG = LogManager.getLogger(SlotProvider.class);

    private final ConcurrentMap<TUniqueId, PendingSlotRequest> pendingSlots = new ConcurrentHashMap<>();
    private final ConcurrentMap<TNetworkAddress, SingleFlightRequireSlotRequest> singleFlightRequireSlotRequests =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<TNetworkAddress, SingleFlightReleaseSlotRequest> singleFlightReleaseSlotRequests =
            new ConcurrentHashMap<>();

    public CompletableFuture<LogicalSlot> requireSlot(LogicalSlot slot) {
        TNetworkAddress leaderEndpoint = GlobalStateMgr.getCurrentState().getNodeMgr().getLeaderRpcEndpoint();
        PendingSlotRequest slotRequest = new PendingSlotRequest(slot, leaderEndpoint);
        slotRequest.onRequire();

        pendingSlots.put(slot.getSlotId(), slotRequest);

        // The leader may be changed between getting leaderEndpoint and putting request to pendingSlots,
        // so check whether leader changed after putting request to pendingSlots.
        TNetworkAddress newLeaderEndpoint = GlobalStateMgr.getCurrentState().getNodeMgr().getLeaderRpcEndpoint();
        if (!newLeaderEndpoint.equals(leaderEndpoint)) {
            failSlotRequestByLeaderChange(slotRequest);
            return slotRequest.getSlotFuture();
        }

        try {
            requireSlotFromSlotManager(slotRequest);
        } catch (Exception e) {
            LOG.warn("[Slot] failed to require slot [slot={}]", slot, e);
            pendingSlots.remove(slot.getSlotId());
            slotRequest.onFailed(e);
        }

        return slotRequest.getSlotFuture();
    }

    public Status finishSlotRequirement(TUniqueId slotId, int pipelineDop, Status status) {
        PendingSlotRequest slotRequest = pendingSlots.remove(slotId);
        if (slotRequest == null) {
            LOG.warn("[Slot] finishSlotRequirement receives a response with non-exist slotId [slotId={}] [status={}]",
                    DebugUtil.printId(slotId), status);
            return Status.internalError("the slotId does not exist");
        }

        if (status.ok()) {
            slotRequest.onFinished(pipelineDop);
        } else {
            LOG.warn("[Slot] finishSlotRequirement receives a failed response [slot={}] [status={}]", slotRequest, status);
            slotRequest.onFailed(new UserException(status.getErrorMsg()));
        }

        return new Status();
    }

    public void cancelSlotRequirement(LogicalSlot slot) {
        if (slot == null) {
            return;
        }

        PendingSlotRequest slotRequest = pendingSlots.remove(slot.getSlotId());
        if (slotRequest == null) {
            return;
        }

        slotRequest.onCancel();
        releaseSlotToSlotManager(slot);
    }

    public void releaseSlot(LogicalSlot slot) {
        if (slot == null || slot.getState() != LogicalSlot.State.ALLOCATED) {
            return;
        }

        slot.onRelease();
        releaseSlotToSlotManager(slot);
    }

    public void leaderChangeListener(LeaderInfo leaderInfo) {
        pendingSlots.values().stream()
                .filter(slot -> !slot.getLeaderEndpoint().getHostname().equals(leaderInfo.getIp()))
                .collect(Collectors.toList())
                .forEach(this::failSlotRequestByLeaderChange);
    }

    private void failSlotRequestByLeaderChange(PendingSlotRequest slotRequest) {
        pendingSlots.remove(slotRequest.getSlot().getSlotId());
        slotRequest.onRetry(new RecoverableException("leader is changed and need require slot again"));
    }

    private abstract static class SingleFlightSlotRequest<Req, BatchReq, Res> {
        protected final TNetworkAddress leaderEndpoint;
        private final ReentrantLock lock = new ReentrantLock();
        private final ConcurrentMap<Req, CompletableFuture<Res>> reqToRes = new ConcurrentHashMap<>();

        protected abstract BatchReq createBatchRequest(List<Req> requests);

        protected abstract List<Res> sendBatchRequest(BatchReq batchReq) throws Exception;

        public SingleFlightSlotRequest(TNetworkAddress leaderEndpoint) {
            this.leaderEndpoint = leaderEndpoint;
        }

        public Res process(Req request) throws Exception {
            CompletableFuture<Res> resFuture = new CompletableFuture<>();
            reqToRes.put(request, resFuture);
            doProcess();
            try {
                return resFuture.get();
            } catch (ExecutionException executionException) {
                if (executionException.getCause() != null && executionException.getCause() instanceof Exception) {
                    throw (Exception) executionException.getCause();
                }
                throw executionException;
            }
        }

        public boolean isEmpty() {
            return reqToRes.isEmpty();
        }

        private void doProcess() {
            if (reqToRes.isEmpty()) {
                return;
            }
            try (LockCloseable ignored = new LockCloseable(lock)) {
                if (reqToRes.isEmpty()) {
                    return;
                }

                List<Req> requests = new ArrayList<>(reqToRes.keySet());
                List<CompletableFuture<Res>> responseFutures = new ArrayList<>();
                for (Req request : requests) {
                    responseFutures.add(reqToRes.remove(request));
                }

                BatchReq batchRequest = createBatchRequest(requests);
                try {
                    List<Res> responses = sendBatchRequest(batchRequest);
                    for (int i = 0; i < responses.size(); i++) {
                        Res response = responses.get(i);
                        responseFutures.get(i).complete(response);
                    }
                } catch (Exception e) {
                    for (CompletableFuture<Res> responseFuture : responseFutures) {
                        responseFuture.completeExceptionally(e);
                    }
                }
            }
        }
    }

    private static class SingleFlightRequireSlotRequest
            extends SingleFlightSlotRequest<TRequireSlotRequest, TRequireBatchSlotRequest, TRequireSlotResponse> {

        public SingleFlightRequireSlotRequest(TNetworkAddress leaderEndpoint) {
            super(leaderEndpoint);
        }

        @Override
        protected TRequireBatchSlotRequest createBatchRequest(List<TRequireSlotRequest> requests) {
            TRequireBatchSlotRequest batchRequest = new TRequireBatchSlotRequest();
            batchRequest.setRequests(requests);
            return batchRequest;
        }

        @Override
        protected List<TRequireSlotResponse> sendBatchRequest(TRequireBatchSlotRequest batchReq)
                throws Exception {
            TRequireBatchSlotResponse batchRes = FrontendServiceProxy.call(leaderEndpoint,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.requireBatchSlotAsync(batchReq));
            return batchRes.getResponses();
        }
    }

    private static class SingleFlightReleaseSlotRequest
            extends SingleFlightSlotRequest<TReleaseSlotRequest, TReleaseBatchSlotRequest, TReleaseSlotResponse> {

        public SingleFlightReleaseSlotRequest(TNetworkAddress leaderEndpoint) {
            super(leaderEndpoint);
        }

        @Override
        protected TReleaseBatchSlotRequest createBatchRequest(List<TReleaseSlotRequest> requests) {
            TReleaseBatchSlotRequest batchRequest = new TReleaseBatchSlotRequest();
            batchRequest.setRequests(requests);
            return batchRequest;
        }

        @Override
        protected List<TReleaseSlotResponse> sendBatchRequest(TReleaseBatchSlotRequest batchReq)
                throws Exception {
            TReleaseBatchSlotResponse batchRes = FrontendServiceProxy.call(leaderEndpoint,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.releaseBatchSlot(batchReq));
            return batchRes.getResponses();
        }
    }

    private void requireSlotFromSlotManager(PendingSlotRequest slotRequest) throws Exception {
        TRequireSlotRequest request = new TRequireSlotRequest();
        request.setSlot(slotRequest.getSlot().toThrift());

        SingleFlightRequireSlotRequest singleFlightRequireSlotRequest =
                singleFlightRequireSlotRequests.computeIfAbsent(slotRequest.getLeaderEndpoint(),
                        SingleFlightRequireSlotRequest::new);

        try {
            singleFlightRequireSlotRequest.process(request);
        } catch (TApplicationException e) {
            if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                LOG.warn("[Slot] leader doesn't have the RPC method [requireSlotAsync]. " +
                                "It is grayscale upgrading, so admit this query without requiring slots. [slot={}]",
                        slotRequest);
                pendingSlots.remove(slotRequest.getSlot().getSlotId());
                slotRequest.onFinished(0);
                slotRequest.getSlot().onRelease(); // Avoid sending releaseSlot RPC.
            } else {
                throw e;
            }
        }
    }

    private void releaseSlotToSlotManager(LogicalSlot slot) {
        TNetworkAddress leaderEndpoint = GlobalStateMgr.getCurrentState().getNodeMgr().getLeaderRpcEndpoint();
        TReleaseSlotRequest slotRequest = new TReleaseSlotRequest();
        slotRequest.setSlot_id(slot.getSlotId());


        SingleFlightReleaseSlotRequest singleFlightReleaseSlotRequest =
                singleFlightReleaseSlotRequests.computeIfAbsent(leaderEndpoint,
                        SingleFlightReleaseSlotRequest::new);

        try {
            TReleaseSlotResponse res = singleFlightReleaseSlotRequest.process(slotRequest);
            if (res.getStatus().getStatus_code() != TStatusCode.OK) {
                String errMsg = "";
                if (!CollectionUtils.isEmpty(res.getStatus().getError_msgs())) {
                    errMsg = res.getStatus().getError_msgs().get(0);
                }
                LOG.warn("[Slot] failed to release slot [slot={}] [errMsg={}]", slot, errMsg);
            }
        } catch (Exception e) {
            LOG.warn("[Slot] failed to release slot [slot={}]", slot, e);
        }
    }
}
