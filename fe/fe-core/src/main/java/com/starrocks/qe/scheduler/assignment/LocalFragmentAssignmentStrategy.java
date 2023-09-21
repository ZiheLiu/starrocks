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

package com.starrocks.qe.scheduler.assignment;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ListUtil;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.BackendSelector;
import com.starrocks.qe.ColocatedBackendSelector;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The assignment strategy for fragments whose left most node is a scan node.
 * <p> It firstly assigns scan ranges to workers, and then dispatches scan ranges, assigned to each worker, to fragment instances.
 */
public class LocalFragmentAssignmentStrategy implements FragmentAssignmentStrategy {
    private static final Logger LOG = LogManager.getLogger(LocalFragmentAssignmentStrategy.class);

    private final ConnectContext connectContext;
    private final WorkerProvider workerProvider;
    private final boolean usePipeline;
    private final boolean isLoadType;

    private final Set<Integer> replicatedScanIds = Sets.newHashSet();

    public LocalFragmentAssignmentStrategy(ConnectContext connectContext, WorkerProvider workerProvider, boolean usePipeline,
                                           boolean isLoadType) {
        this.connectContext = connectContext;
        this.workerProvider = workerProvider;
        this.usePipeline = usePipeline;
        this.isLoadType = isLoadType;
    }

    @Override
    public void assignFragmentToWorker(ExecutionFragment execFragment) throws UserException {
        for (ScanNode scanNode : execFragment.getScanNodes()) {
            assignScanRangesToWorker(execFragment, scanNode);
        }

        assignScanRangesToFragmentInstancePerWorker(execFragment);

        // The fragment which only contains scan nodes without scan ranges,
        // such as SchemaScanNode, is assigned to an arbitrary worker.
        if (execFragment.getInstances().isEmpty()) {
            long workerId = workerProvider.selectNextWorker();
            ComputeNode worker = workerProvider.getWorkerById(workerId);
            FragmentInstance instance = new FragmentInstance(worker, execFragment);
            execFragment.addInstance(instance);
        }
    }

    private void assignScanRangesToWorker(ExecutionFragment execFragment, ScanNode scanNode) throws UserException {
        BackendSelector backendSelector = BackendSelectorFactory.create(
                scanNode, isLoadType, execFragment, workerProvider, connectContext, replicatedScanIds);

        backendSelector.computeScanRangeAssignment();

        if (LOG.isDebugEnabled()) {
            LOG.debug(execFragment.getScanRangeAssignment().toDebugString());
        }
    }

    private void assignScanRangesToFragmentInstancePerWorker(ExecutionFragment execFragment) {
        ColocatedBackendSelector.Assignment colocatedAssignment = execFragment.getColocatedAssignment();
        boolean hasColocate = execFragment.isColocated()
                && colocatedAssignment != null && !colocatedAssignment.getSeqToWorkerId().isEmpty();
        boolean hasBucketShuffle = execFragment.isLocalBucketShuffleJoin() && colocatedAssignment != null;

        if (hasColocate || hasBucketShuffle) {
            assignScanRangesToColocateFragmentInstancePerWorker(execFragment,
                    colocatedAssignment.getSeqToWorkerId(),
                    colocatedAssignment.getSeqToScanRange());
        } else {
            assignScanRangesToNormalFragmentInstancePerWorker(execFragment);
        }
    }

    /**
     * This strategy assigns buckets to each driver sequence to avoid local shuffle.
     * If the number of buckets assigned to a fragment instance is less than pipelineDop,
     * pipelineDop will be set to num_buckets, which will reduce the degree of operator parallelism.
     * Therefore, when there are few buckets (<=pipeline_dop/2), insert local shuffle instead of using this strategy
     * to improve the degree of parallelism.
     *
     * @param scanRanges  The buckets assigned to a fragment instance.
     * @param pipelineDop The expected pipelineDop.
     * @return Whether using the strategy of assigning scanRanges to each driver sequence.
     */
    private <T> boolean enableAssignScanRangesPerDriverSeq(List<T> scanRanges, int pipelineDop) {
        boolean enableTabletInternalParallel =
                connectContext != null && connectContext.getSessionVariable().isEnableTabletInternalParallel();
        return !enableTabletInternalParallel || scanRanges.size() > pipelineDop / 2;
    }

    private boolean enableAssignScanRangesPerDriverSeq(PlanFragment fragment, List<TScanRangeParams> scanRanges) {
        if (!usePipeline) {
            return false;
        }

        if (fragment.isForceAssignScanRangesPerDriverSeq()) {
            return true;
        }

        return fragment.isAssignScanRangesPerDriverSeq() &&
                enableAssignScanRangesPerDriverSeq(scanRanges, fragment.getPipelineDop());
    }

    private boolean needAddScanRanges(Set<Integer> visitedReplicatedScanIds, Integer scanId) {
        if (!replicatedScanIds.contains(scanId)) {
            return true;
        }
        return visitedReplicatedScanIds.add(scanId);
    }

    private interface AssignBucketSeqToDriverSeqStrategy {
        void assign(FragmentInstance destInstance, Set<Integer> instanceReplicatedScanIds, List<Integer> bucketSeqs);

        boolean isAssignPerDriverSeq();
    }

    private static class AssignPerInstanceStrategy implements AssignBucketSeqToDriverSeqStrategy {
        private final ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange;

        public AssignPerInstanceStrategy(ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange) {
            this.bucketSeqToScanRange = bucketSeqToScanRange;
        }

        @Override
        public void assign(FragmentInstance destInstance, Set<Integer> instanceReplicatedScanIds, List<Integer> bucketSeqs) {
            bucketSeqs.forEach(bucketSeq -> {
                destInstance.addBucketSeq(bucketSeq);
                bucketSeqToScanRange.get(bucketSeq).forEach((scanId, scanRanges) -> {
                    if (needAddScanRanges(instanceReplicatedScanIds, scanId)) {
                        destInstance.addScanRanges(scanId, scanRanges);
                    }
                });
            });
        }

        @Override
        public boolean isAssignPerDriverSeq() {
            return false;
        }
    }

    private static class AssignBucketPerDriverSeqStrategy implements AssignBucketSeqToDriverSeqStrategy {
        private final ExecutionFragment execFragment;
        private final ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange;

        private AssignBucketPerDriverSeqStrategy(ExecutionFragment execFragment,
                                                 ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange) {
            this.execFragment = execFragment;
            this.bucketSeqToScanRange = bucketSeqToScanRange;
        }

        @Override
        public void assign(FragmentInstance destInstance, Set<Integer> instanceReplicatedScanIds, List<Integer> bucketSeqs) {
            final PlanFragment fragment = execFragment.getPlanFragment();
            final int pipelineDop = fragment.getPipelineDop();

            int expectedDop = Math.max(1, pipelineDop);
            List<List<Integer>> bucketSeqsPerDriverSeq = ListUtil.splitBySize(bucketSeqs, expectedDop);

            destInstance.setPipelineDop(bucketSeqsPerDriverSeq.size());

            for (int driverSeq = 0; driverSeq < bucketSeqsPerDriverSeq.size(); driverSeq++) {
                int finalDriverSeq = driverSeq;
                bucketSeqsPerDriverSeq.get(driverSeq).forEach(bucketSeq -> {
                    destInstance.addBucketSeqAndDriverSeq(bucketSeq, finalDriverSeq);
                    bucketSeqToScanRange.get(bucketSeq).forEach((scanId, scanRanges) -> {
                        if (needAddScanRanges(instanceReplicatedScanIds, scanId)) {
                            destInstance.addScanRanges(scanId, finalDriverSeq, scanRanges);
                        }
                    });
                });
            }

            destInstance.paddingScanRanges();
        }

        @Override
        public boolean isAssignPerDriverSeq() {
            return true;
        }
    }

    private static class BucketSeqAndPartition {
        private final Integer bucketSeq;
        private final Range<PartitionKey> partitionKeyRange;

        public BucketSeqAndPartition(Integer bucketSeq, Range<PartitionKey> partitionKeyRange) {
            this.bucketSeq = bucketSeq;
            this.partitionKeyRange = partitionKeyRange;
        }

        public Integer getBucketSeq() {
            return bucketSeq;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BucketSeqAndPartition that = (BucketSeqAndPartition) o;
            return bucketSeq.equals(that.bucketSeq) &&
                    partitionKeyRange.equals(that.partitionKeyRange);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucketSeq, partitionKeyRange);
        }
    }

    private static class AssignBucketPartitionPerDriverSeqStrategy implements AssignBucketSeqToDriverSeqStrategy {
        private final ExecutionFragment execFragment;

        private final Map<Integer, Map<BucketSeqAndPartition, Map<Integer, List<TScanRangeParams>>>> bucketSeqToPartitionToScanRanges;

        private AssignBucketPartitionPerDriverSeqStrategy(ExecutionFragment execFragment,
                                                          ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange) {
            this.execFragment = execFragment;
            this.bucketSeqToScanRange = bucketSeqToScanRange;
        }

        private static boolean useAssignPartitionPerDriverSeq(
                ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange, List<Integer> bucketSeqs, int pipelineDop) {
            int numTablets = bucketSeqs.stream()
                    .mapToInt(bucketSeq -> bucketSeqToScanRange.get(bucketSeq).values().stream()
                            .map(List::size)
                            .max(Comparator.naturalOrder())
                            .orElse(0))
                    .sum();
            return numTablets > pipelineDop / 2;
        }

        public static AssignBucketPartitionPerDriverSeqStrategy create(
                ExecutionFragment execFragment,
                ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange,
                Map<Long, List<Integer>> workerIdToBucketSeqs) {
            final PlanFragment fragment = execFragment.getPlanFragment();
            final int pipelineDop = fragment.getPipelineDop();

            boolean useAssignPartitionPerDriverSeq = workerIdToBucketSeqs.values().stream()
                    .allMatch(bucketSeqs -> useAssignPartitionPerDriverSeq(bucketSeqToScanRange, bucketSeqs, pipelineDop));
            if (!useAssignPartitionPerDriverSeq) {
                return null;
            }

            Map<Integer, Map<BucketSeqAndPartition, Map<Integer, List<TScanRangeParams>>>> bucketSeqToPartitionToScanRanges =
                    new HashMap<>();
            for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> kv : bucketSeqToScanRange.entrySet()) {
                Integer bucketSeq = kv.getKey();
                Map<Integer, List<TScanRangeParams>> scanIdToScanRanges = kv.getValue();

                for (Map.Entry<Integer, List<TScanRangeParams>> kv2 : scanIdToScanRanges.entrySet()) {
                    Integer scanId = kv2.getKey();
                    List<TScanRangeParams> scanRanges = kv2.getValue();

                    ScanNode scanNode = execFragment.getScanNode(new PlanNodeId(scanId));
                    if (!(scanNode instanceof OlapScanNode)) {
                        return null;
                    }
                    OlapScanNode olapScanNode = (OlapScanNode) scanNode;

                    PartitionInfo partitionInfo = olapScanNode.getOlapTable().getPartitionInfo();
                    if (!(partitionInfo instanceof RangePartitionInfo)) {
                        return null;
                    }
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;

                    for (TScanRangeParams scanRange : scanRanges) {
                        if (!scanRange.getScan_range().isSetInternal_scan_range() ||
                                !scanRange.getScan_range().getInternal_scan_range().isSetPartition_id()) {
                            return null;
                        }
                        long partitionId = scanRange.getScan_range().getInternal_scan_range().getPartition_id();

                        Range<PartitionKey> partitionKeyRange = rangePartitionInfo.getRange(partitionId);
                        if (partitionKeyRange == null) {
                            return null;
                        }
                        BucketSeqAndPartition bucketSeqPartition = new BucketSeqAndPartition(bucketSeq, partitionKeyRange);

                        bucketSeqToPartitionToScanRanges
                                .computeIfAbsent(bucketSeq, (k) -> new HashMap<>())
                                .computeIfAbsent(bucketSeqPartition, (k) -> new HashMap<>())
                                .computeIfAbsent(scanId, (k) -> new ArrayList<>())
                                .add(scanRange);
                    }
                }
            }

            return
        }

        @Override
        public void assign(FragmentInstance destInstance, Set<Integer> instanceReplicatedScanIds, List<Integer> bucketSeqs) {
            final PlanFragment fragment = execFragment.getPlanFragment();
            final int pipelineDop = fragment.getPipelineDop();

            int expectedDop = Math.max(1, pipelineDop);
            List<List<Integer>> bucketSeqsPerDriverSeq = ListUtil.splitBySize(bucketSeqs, expectedDop);

            destInstance.setPipelineDop(bucketSeqsPerDriverSeq.size());

            for (int driverSeq = 0; driverSeq < bucketSeqsPerDriverSeq.size(); driverSeq++) {
                int finalDriverSeq = driverSeq;
                bucketSeqsPerDriverSeq.get(driverSeq).forEach(bucketSeq -> {
                    destInstance.addBucketSeqAndDriverSeq(bucketSeq, finalDriverSeq);
                    bucketSeqToScanRange.get(bucketSeq).forEach((scanId, scanRanges) -> {
                        if (needAddScanRanges(instanceReplicatedScanIds, scanId)) {
                            destInstance.addScanRanges(scanId, finalDriverSeq, scanRanges);
                        }
                    });
                });
            }

            destInstance.paddingScanRanges();
        }

        @Override
        public boolean isAssignPerDriverSeq() {
            return true;
        }
    }

    private AssignBucketSeqToDriverSeqStrategy getAssignBucketSeqToDriverSeqStrategy(
            ExecutionFragment execFragment, ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange,
            Map<Long, List<Integer>> workerIdToBucketSeqs) {
        final PlanFragment fragment = execFragment.getPlanFragment();
        final int pipelineDop = fragment.getPipelineDop();

        boolean assignPerDriverSeq = usePipeline && workerIdToBucketSeqs.values().stream()
                .allMatch(bucketSeqs -> enableAssignScanRangesPerDriverSeq(bucketSeqs, pipelineDop));
        if (assignPerDriverSeq) {
            return new AssignBucketPerDriverSeqStrategy(execFragment, bucketSeqToScanRange);
        }

        boolean enablePartitionColocateJoin =
                connectContext != null && connectContext.getSessionVariable().isEnablePartitionColocateJoin()
                        && fragment.isContainsAllPartitionColumns();
        if (enablePartitionColocateJoin) {
        }

        return new AssignPerInstanceStrategy(bucketSeqToScanRange);
    }

    private void assignScanRangesToColocateFragmentInstancePerWorker(
            ExecutionFragment execFragment,
            Map<Integer, Long> bucketSeqToWorkerId,
            ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange) {
        final PlanFragment fragment = execFragment.getPlanFragment();
        final int parallelExecInstanceNum = fragment.getParallelExecNum();

        // 1. count each node in one fragment should scan how many tablet, gather them in one list
        Map<Long, List<Integer>> workerIdToBucketSeqs = bucketSeqToWorkerId.entrySet().stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())
                ));

        AssignBucketSeqToDriverSeqStrategy assignStrategy =
                getAssignBucketSeqToDriverSeqStrategy(execFragment, bucketSeqToScanRange, workerIdToBucketSeqs);

        if (!assignStrategy.isAssignPerDriverSeq()) {
            // these optimize depend on assignPerDriverSeq.
            fragment.disablePhysicalPropertyOptimize();
        }

        workerIdToBucketSeqs.forEach((workerId, bucketSeqsOfWorker) -> {
            ComputeNode worker = workerProvider.getWorkerById(workerId);

            // 2. split how many scanRange one instance should scan
            int expectedInstanceNum = Math.max(1, parallelExecInstanceNum);
            List<List<Integer>> bucketSeqsPerInstance = ListUtil.splitBySize(bucketSeqsOfWorker, expectedInstanceNum);

            // 3.construct instanceExecParam add the scanRange should be scanned by instance
            bucketSeqsPerInstance.forEach(bucketSeqsOfInstance -> {
                FragmentInstance instance = new FragmentInstance(worker, execFragment);
                execFragment.addInstance(instance);

                // record each instance replicate scan id in set, to avoid add replicate scan range repeatedly when they are in different buckets
                Set<Integer> instanceReplicatedScanIds = new HashSet<>();
                assignStrategy.assign(instance, instanceReplicatedScanIds, bucketSeqsOfInstance);
            });
        });
    }

    private void assignScanRangesToNormalFragmentInstancePerWorker(ExecutionFragment execFragment) {
        final PlanFragment fragment = execFragment.getPlanFragment();
        final int parallelExecInstanceNum = fragment.getParallelExecNum();
        final int pipelineDop = fragment.getPipelineDop();

        execFragment.getScanRangeAssignment().forEach((workerId, scanRangesPerWorker) -> {
            // 1. Handle normal scan node firstly
            scanRangesPerWorker.forEach((scanId, scanRangesOfNode) -> {
                if (replicatedScanIds.contains(scanId)) {
                    return;
                }

                int expectedInstanceNum = Math.max(1, parallelExecInstanceNum);
                List<List<TScanRangeParams>> scanRangesPerInstance = ListUtil.splitBySize(scanRangesOfNode, expectedInstanceNum);

                for (List<TScanRangeParams> scanRanges : scanRangesPerInstance) {
                    FragmentInstance instance = new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
                    execFragment.addInstance(instance);

                    if (!enableAssignScanRangesPerDriverSeq(fragment, scanRanges)) {
                        instance.addScanRanges(scanId, scanRanges);
                        fragment.disablePhysicalPropertyOptimize();
                    } else {
                        int expectedDop = Math.max(1, Math.min(pipelineDop, scanRanges.size()));
                        List<List<TScanRangeParams>> scanRangesPerDriverSeq;
                        if (Config.enable_schedule_insert_query_by_row_count && isLoadType
                                && !scanRanges.isEmpty()
                                && scanRanges.get(0).getScan_range().isSetInternal_scan_range()) {
                            scanRangesPerDriverSeq = splitScanRangeParamByRowCount(scanRanges, expectedDop);
                        } else {
                            scanRangesPerDriverSeq = ListUtil.splitBySize(scanRanges, expectedDop);
                        }

                        if (fragment.isForceAssignScanRangesPerDriverSeq() && scanRangesPerDriverSeq.size() != pipelineDop) {
                            fragment.setPipelineDop(scanRangesPerDriverSeq.size());
                        }
                        instance.setPipelineDop(scanRangesPerDriverSeq.size());

                        for (int driverSeq = 0; driverSeq < scanRangesPerDriverSeq.size(); ++driverSeq) {
                            instance.addScanRanges(scanId, driverSeq, scanRangesPerDriverSeq.get(driverSeq));
                        }
                        instance.paddingScanRanges();
                    }
                }
            });

            // 2. Handle replicated scan node, if needed
            boolean isReplicated = execFragment.isReplicated();
            if (isReplicated) {
                scanRangesPerWorker.forEach((scanId, scanRangesPerNode) -> {
                    if (!replicatedScanIds.contains(scanId)) {
                        return;
                    }

                    for (FragmentInstance instance : execFragment.getInstances()) {
                        instance.addScanRanges(scanId, scanRangesPerNode);
                    }
                });
            }
        });
    }

    /**
     * Split scan range params into groupNum groups by each group's row count.
     */
    private static List<List<TScanRangeParams>> splitScanRangeParamByRowCount(List<TScanRangeParams> scanRangeParams,
                                                                              int groupNum) {
        List<List<TScanRangeParams>> result = new ArrayList<>(groupNum);
        for (int i = 0; i < groupNum; i++) {
            result.add(new ArrayList<>());
        }
        long[] dataSizePerGroup = new long[groupNum];
        for (TScanRangeParams scanRangeParam : scanRangeParams) {
            int minIndex = 0;
            long minDataSize = dataSizePerGroup[0];
            for (int i = 1; i < groupNum; i++) {
                if (dataSizePerGroup[i] < minDataSize) {
                    minIndex = i;
                    minDataSize = dataSizePerGroup[i];
                }
            }
            dataSizePerGroup[minIndex] += Math.max(1, scanRangeParam.getScan_range().getInternal_scan_range().getRow_count());
            result.get(minIndex).add(scanRangeParam);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("dataSizePerGroup: {}", dataSizePerGroup);
        }

        return result;
    }
}
