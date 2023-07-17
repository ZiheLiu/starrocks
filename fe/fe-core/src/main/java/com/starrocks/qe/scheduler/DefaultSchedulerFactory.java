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

package com.starrocks.qe.scheduler;

import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.dag.JobInformation;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Map;

public class DefaultSchedulerFactory implements ICoordinator.Factory {
    @Override
    public DefaultScheduler createQueryScheduler(ConnectContext context,
                                                 List<PlanFragment> fragments,
                                                 List<ScanNode> scanNodes,
                                                 TDescriptorTable descTable) {

        JobInformation jobInfo =
                JobInformation.Factory.fromQueryInfo(context, fragments, scanNodes, descTable, TQueryType.SELECT);

        return new DefaultScheduler(context, jobInfo, context.getSessionVariable().isEnableProfile());
    }

    @Override
    public DefaultScheduler createInsertScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                  List<ScanNode> scanNodes,
                                                  TDescriptorTable descTable) {
        JobInformation jobInfo =
                JobInformation.Factory.fromQueryInfo(context, fragments, scanNodes, descTable, TQueryType.LOAD);

        return new DefaultScheduler(context, jobInfo, context.getSessionVariable().isEnableProfile());
    }

    @Override
    public DefaultScheduler createBrokerLoadScheduler(LoadPlanner loadPlanner) {
        ConnectContext context = loadPlanner.getContext();
        JobInformation jobInfo = JobInformation.Factory.fromBrokerLoadJobInfo(loadPlanner);

        return new DefaultScheduler(context, jobInfo, true);
    }

    @Override
    public DefaultScheduler createStreamLoadScheduler(LoadPlanner loadPlanner) {
        ConnectContext context = loadPlanner.getContext();
        JobInformation jobInfo = JobInformation.Factory.fromStreamLoadJobInfo(loadPlanner);

        return new DefaultScheduler(context, jobInfo, true);
    }

    @Override
    public DefaultScheduler createSyncStreamLoadScheduler(StreamLoadPlanner planner, TNetworkAddress address) {
        JobInformation jobInfo = JobInformation.Factory.fromSyncStreamLoadInfo(planner);

        return new DefaultScheduler(planner.getConnectContext(), jobInfo, address);
    }

    @Override
    public DefaultScheduler createNonPipelineBrokerLoadScheduler(Long jobId, TUniqueId queryId,
                                                                 DescriptorTable descTable,
                                                                 List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                                                 String timezone,
                                                                 long startTime, Map<String, String> sessionVariables,
                                                                 ConnectContext context,
                                                                 long execMemLimit) {
        JobInformation jobInfo =
                JobInformation.Factory.fromNonPipelineBrokerLoadJobInfo(context, jobId, queryId, descTable,
                        fragments, scanNodes, timezone,
                        startTime, sessionVariables, execMemLimit);

        return new DefaultScheduler(context, jobInfo, true);
    }

    @Override
    public DefaultScheduler createBrokerExportScheduler(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                                        List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                                        String timezone,
                                                        long startTime, Map<String, String> sessionVariables,
                                                        long execMemLimit) {
        ConnectContext context = new ConnectContext();
        context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        context.getSessionVariable().setEnablePipelineEngine(true);
        context.getSessionVariable().setPipelineDop(0);

        JobInformation jobInfo =
                JobInformation.Factory.fromBrokerExportInfo(context, jobId, queryId, descTable,
                        fragments, scanNodes, timezone,
                        startTime, sessionVariables, execMemLimit);

        return new DefaultScheduler(context, jobInfo, true);
    }
}
