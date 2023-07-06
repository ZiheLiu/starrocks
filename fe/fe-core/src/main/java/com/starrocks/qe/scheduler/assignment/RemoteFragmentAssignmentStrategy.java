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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.Reference;
import com.starrocks.common.UserException;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.thrift.TNetworkAddress;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class RemoteFragmentAssignmentStrategy implements FragmentAssignmentStrategy {

    private final Random random;

    private final ConnectContext connectContext;
    private final WorkerProvider workerProvider;
    private final ExecutionDAG executionDAG;
    private final boolean usePipeline;

    public RemoteFragmentAssignmentStrategy(ConnectContext connectContext, WorkerProvider workerProvider,
                                            ExecutionDAG executionDAG, Random random, boolean usePipeline) {
        this.connectContext = connectContext;
        this.workerProvider = workerProvider;
        this.executionDAG = executionDAG;
        this.random = random;
        this.usePipeline = usePipeline;
    }

    @Override
    public void assignToFragment(ExecutionFragment execFragment) throws UserException {
        final PlanFragment fragment = execFragment.getPlanFragment();

        // If left child is MultiCastDataFragment(only support left now), will keep same instance with child.
        boolean isCTEConsumerFragment =
                fragment.getChildren().size() > 0 && fragment.getChild(0) instanceof MultiCastPlanFragment;
        if (isCTEConsumerFragment) {
            assignToCTEConsumerFragment(execFragment);
            return;
        }

        boolean isGatherFragment = fragment.getDataPartition() == DataPartition.UNPARTITIONED;
        if (isGatherFragment) {
            assignToGatherFragment(execFragment);
            return;
        }

        assignToRemoteFragment(execFragment);
    }

    private void assignToCTEConsumerFragment(ExecutionFragment execFragment) {
        ExecutionFragment childFragment = execFragment.getChild(0);
        for (FragmentInstance childInstance : childFragment.getInstances()) {
            TNetworkAddress addr = childInstance.getAddress();
            FragmentInstance instance = new FragmentInstance(workerProvider.getUsingWorkerByAddr(addr), addr, execFragment);
            execFragment.addInstance(instance);
        }
    }

    private void assignToGatherFragment(ExecutionFragment execFragment) throws UserException {
        Reference<Long> backendIdRef = new Reference<>();
        TNetworkAddress addr = workerProvider.chooseNextWorker(backendIdRef);
        FragmentInstance instance = new FragmentInstance(workerProvider.getUsingWorkerByAddr(addr), addr, execFragment);
        execFragment.addInstance(instance);
    }

    private void assignToRemoteFragment(ExecutionFragment execFragment) {
        final boolean dopAdaptionEnabled = usePipeline && connectContext.getSessionVariable().isEnablePipelineAdaptiveDop();
        final boolean isGatherOutput = executionDAG.isGatherOutput();
        final PlanFragment fragment = execFragment.getPlanFragment();

        // hostSet contains target backends to whom fragment instances of the current PlanFragment will be
        // delivered. when pipeline parallelization is adopted, the number of instances should be the size
        // of hostSet, that it to say, each backend has exactly one fragment.
        Set<TNetworkAddress> hostSet = Sets.newHashSet();
        int maxParallelism = 0;

        List<TNetworkAddress> selectedComputedNodes = workerProvider.chooseAllComputedNodes();
        if (!selectedComputedNodes.isEmpty()) {
            hostSet.addAll(selectedComputedNodes);
            // make olapScan maxParallelism equals prefer compute node number
            maxParallelism = hostSet.size() * fragment.getParallelExecNum();
        } else if (fragment.isUnionFragment() && isGatherOutput) {
            // union fragment use all children's host
            // if output fragment isn't gather, all fragment must keep 1 instance
            for (int i = 0; i < execFragment.childrenSize(); i++) {
                ExecutionFragment childExecFragment = execFragment.getChild(i);
                childExecFragment.getInstances().stream()
                        .map(FragmentInstance::getAddress)
                        .forEach(hostSet::add);
            }
            maxParallelism = hostSet.size() * fragment.getParallelExecNum();
        } else {
            // there is no leftmost scan; we assign the same hosts as those of our
            // input fragment which has a higher instance_number
            int inputFragmentIndex = 0;
            for (int i = 0; i < execFragment.childrenSize(); i++) {
                int curInputFragmentParallelism = execFragment.getChild(i).getInstances().size();
                // when dop adaptation enabled, numInstances * pipelineDop is equivalent to numInstances in
                // non-pipeline engine and pipeline engine(dop adaptation disabled).
                if (dopAdaptionEnabled) {
                    curInputFragmentParallelism *= fragment.getChild(i).getPipelineDop();
                }

                if (curInputFragmentParallelism > maxParallelism) {
                    maxParallelism = curInputFragmentParallelism;
                    inputFragmentIndex = i;
                }
            }

            ExecutionFragment maxInputExecFragment = execFragment.getChild(inputFragmentIndex);
            maxInputExecFragment.getInstances().stream()
                    .map(FragmentInstance::getAddress)
                    .forEach(hostSet::add);
        }

        if (dopAdaptionEnabled) {
            maxParallelism = hostSet.size();
        }

        int exchangeInstances = -1;
        if (connectContext != null && connectContext.getSessionVariable() != null) {
            exchangeInstances = connectContext.getSessionVariable().getExchangeInstanceParallel();
        }
        List<TNetworkAddress> hosts;
        if (exchangeInstances > 0 && maxParallelism > exchangeInstances) {
            // random select some instance
            // get distinct host,  when parallel_fragment_exec_instance_num > 1, single host may execute several instances
            maxParallelism = exchangeInstances;
            hosts = Lists.newArrayList(hostSet);
            Collections.shuffle(hosts, random);
        } else {
            hosts = Lists.newArrayList(hostSet);
        }

        for (int i = 0; i < maxParallelism; i++) {
            TNetworkAddress host = hosts.get(i % hosts.size());
            FragmentInstance instance = new FragmentInstance(workerProvider.getUsingWorkerByAddr(host), host, execFragment);
            execFragment.addInstance(instance);
        }

        // When group by cardinality is smaller than number of backend, only some backends always
        // process while other has no data to process.
        // So we shuffle instances to make different backends handle different queries.
        execFragment.shuffleInstances(random);

        // TODO: switch to unpartitioned/coord execution if our input fragment
        // is executed that way (could have been downgraded from distributed)
    }
}
