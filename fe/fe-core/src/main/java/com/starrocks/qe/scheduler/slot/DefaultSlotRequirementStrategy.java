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

import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.FileTableScanNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeVisitor;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.qe.scheduler.SchedulerException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.JobInformation;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.List;

public class DefaultSlotRequirementStrategy implements SlotRequirementStrategy {

    @Override
    public SharingSlotRequirement calculateRequirement(JobInformation jobInformation, WorkerProvider workerProvider)
            throws SchedulerException {
        Context ctx = new Context(workerProvider);
        SharingSlotRequirementCalculator calculator = new SharingSlotRequirementCalculator();
        for (ScanNode scanNode : jobInformation.getScanNodes()) {
            scanNode.accept(calculator, ctx);
        }
        return ctx.reqBuilder.build();
    }

    private static class Context {
        private final WorkerProvider workerProvider;
        private final SharingSlotRequirement.Builder reqBuilder;

        public Context(WorkerProvider workerProvider) {
            this.workerProvider = workerProvider;
            this.reqBuilder = new SharingSlotRequirement.Builder();
        }
    }

    private static class SharingSlotRequirementCalculator implements PlanNodeVisitor<Void, Context> {

        @Override
        public Void visit(PlanNode node, Context ctx) throws SchedulerException {
            String msg = String.format("SharingSlotRequirementCalculator cannot handle plan node [%s]", node);
            throw new SchedulerException(msg);
        }

        @Override
        public Void visitScanNode(ScanNode node, Context ctx) throws SchedulerException {
            List<TScanRangeLocations> locations = node.getScanRangeLocations(0);
            if (locations == null) {
                return null;
            }

            for (TScanRangeLocations scanRangeLocations : locations) {
                boolean foundLocation = false;
                for (TScanRangeLocation location : scanRangeLocations.getLocations()) {
                    long workerID = location.getBackend_id();
                    if (ctx.workerProvider.containsBackend(workerID)) {
                        foundLocation = true;
                        ctx.reqBuilder.add(workerID, 1);
                    }
                }
                if (!foundLocation) {
                    if (scanRangeLocations.getLocations().isEmpty()) {
                        throw new SchedulerException("Scan range not found: " + ctx.workerProvider.backendsToString());
                    } else {
                        ctx.workerProvider.reportBackendNotFoundException();
                    }
                }
            }

            return null;
        }

        public Void visitConnectorNode(Context ctx) {
            ctx.workerProvider.getWorkersPreferringComputeNode().forEach(cn -> ctx.reqBuilder.add(cn.getId(), 1));
            return null;
        }

        @Override
        public Void visitSchemaScanNode(SchemaScanNode node, Context ctx) throws SchedulerException {
            return visitScanNode(node, ctx);
        }

        @Override
        public Void visitHdfsScanNode(HdfsScanNode node, Context ctx) {
            return visitConnectorNode(ctx);
        }

        @Override
        public Void visitIcebergScanNode(IcebergScanNode node, Context ctx) {
            return visitConnectorNode(ctx);
        }

        @Override
        public Void visitHudiScanNode(HudiScanNode node, Context ctx) {
            return visitConnectorNode(ctx);
        }

        @Override
        public Void visitDeltaLakeScanNode(DeltaLakeScanNode node, Context ctx) {
            return visitConnectorNode(ctx);
        }

        @Override
        public Void visitFileTableScanNode(FileTableScanNode node, Context ctx) {
            return visitConnectorNode(ctx);
        }

        @Override
        public Void visitPaimonScanNode(PaimonScanNode node, Context ctx) {
            return visitConnectorNode(ctx);
        }
    }
}
