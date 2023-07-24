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

package com.starrocks.qe.scheduler.dag;

import com.google.common.collect.Maps;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Map;

// fragment instance exec param, it is used to assemble
// the per-instance TPlanFragmentExecParas, as a member of
// FragmentExecParams
public class FragmentInstance {
    private static final int ABSENT_PIPELINE_DOP = -1;
    private static final int ABSENT_DRIVER_SEQUENCE = -1;

    private int indexInJob = -1;
    /**
     * The index of `execFragment.instances`, which is set when adding this instance to `execFragment`.
     */
    private int indexInFragment = -1;
    private TUniqueId instanceId = null;
    private ExecutionFragment execFragment = null;

    private final ComputeNode worker;

    private final Map<Integer, List<TScanRangeParams>> node2ScanRanges = Maps.newHashMap();
    private final Map<Integer, Map<Integer, List<TScanRangeParams>>> node2DriverSeqToScanRanges = Maps.newHashMap();

    private final Map<Integer, Integer> bucketSeqToDriverSeq = Maps.newHashMap();

    private int pipelineDop = ABSENT_PIPELINE_DOP;

    public FragmentInstance(ExecutionFragment execFragment, ComputeNode worker) {
        this.execFragment = execFragment;
        this.worker = worker;
    }

    public void addBucketSeqAndDriverSeq(int bucketSeq, int driverSeq) {
        this.bucketSeqToDriverSeq.putIfAbsent(bucketSeq, driverSeq);
    }

    public void addBucketSeq(int bucketSeq) {
        this.bucketSeqToDriverSeq.putIfAbsent(bucketSeq, ABSENT_DRIVER_SEQUENCE);
    }

    public PlanFragment fragment() {
        return execFragment.planFragment;
    }

    public boolean isSetPipelineDop() {
        return pipelineDop != ABSENT_PIPELINE_DOP;
    }

    public int getPipelineDop() {
        return pipelineDop;
    }

    public Map<Integer, Integer> getBucketSeqToDriverSeq() {
        return bucketSeqToDriverSeq;
    }

    public Map<Integer, List<TScanRangeParams>> getNode2ScanRanges() {
        return node2ScanRanges;
    }

    public Map<Integer, Map<Integer, List<TScanRangeParams>>> getNode2DriverSeqToScanRanges() {
        return node2DriverSeqToScanRanges;
    }

    public int getIndexInJob() {
        return indexInJob;
    }

    public void setIndexInJob(int indexInJob) {
        this.indexInJob = indexInJob;
    }

    public TUniqueId getInstanceId() {
        return instanceId;
    }

    public Long getWorkerId() {
        return worker.getId();
    }

    public int getTableSinkDop() {
        PlanFragment fragment = fragment();
        if (!fragment.forceSetTableSinkDop()) {
            return getPipelineDop(); // instance dop.
        }

        DataSink dataSink = fragment.getSink();
        int fragmentDop = fragment.getPipelineDop();
        if (!(dataSink instanceof IcebergTableSink)) {
            return fragmentDop;
        } else {
            int sessionVarSinkDop = ConnectContext.get().getSessionVariable().getPipelineSinkDop();
            if (sessionVarSinkDop > 0) {
                return Math.min(fragmentDop, sessionVarSinkDop);
            } else {
                return Math.min(fragmentDop, IcebergTableSink.ICEBERG_SINK_MAX_DOP);
            }
        }
    }
}
