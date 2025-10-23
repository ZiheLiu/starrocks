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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

/**
 * 对于单个 BE 的集群，Global Agg 与 Scan 之间的 Shuffle Exchange 可以使用 Local Shuffle 代替。
 * 为了鲁棒性，限制以下两种 plan 可以消除掉 Shuffle Exchange：
 * - OlapScan -> [Project] -> Local Agg -> Shuffle Exchange -> Global Agg
 * - OlapScan -> [Project] -> Shuffle Exchange -> Global Agg
 *
 * 限制：
 * - 查询中不能包含输入为多个 child 的算子，例如 Join。
 * - 必须是上述两种 plan。
 */
public class PruneShuffleDistributionNodeRule implements TreeRewriteRule {
    private static final Visitor VISITOR = new Visitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable sv = taskContext.getOptimizerContext().getConnectContext().getSessionVariable();
        if (!sv.isEnableLocalShuffleAgg() || !sv.isEnablePipelineEngine() ||
                !GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().isSingleBackendAndComputeNode()) {
            return root;
        }

        return root.getOp().accept(VISITOR, root, null);
    }

    private static class Visitor extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            // Don't use local shuffle agg when the query has join or other complex operators,
            // who accepts data from multiple children.
            if (optExpression.getInputs().size() > 1) {
                return optExpression;
            }

            for (int i = 0; i < optExpression.arity(); ++i) {
                OptExpression childOptExpr = optExpression.inputAt(i);
                optExpression.setChild(i, childOptExpr.getOp().accept(this, childOptExpr, null));
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            PhysicalHashAggregateOperator agg = optExpression.getOp().cast();
            if (!agg.getType().isGlobal()) {
                return visit(optExpression, context);
            }

            // Child should be a shuffle distribution.
            OptExpression childOptExpr = optExpression.inputAt(0);
            Operator childOperator = childOptExpr.getOp();
            if (!(childOperator instanceof PhysicalDistributionOperator)) {
                return visit(optExpression, context);
            }
            PhysicalDistributionOperator distribution = (PhysicalDistributionOperator) childOperator;
            if (distribution.getDistributionSpec().getType() != DistributionSpec.DistributionType.SHUFFLE) {
                return visit(optExpression, context);
            }

            if (!canPruneShuffleDistribution(childOptExpr)) {
                return visit(optExpression, context);
            }

            agg.setWithLocalShuffle(true);
            optExpression.setChild(0, childOptExpr.inputAt(0));
            return optExpression;
        }

        private boolean canPruneShuffleDistribution(OptExpression distributionOptExpr) {
            OptExpression childOptExpression = distributionOptExpr.inputAt(0);
            Operator childOperator = childOptExpression.getOp();

            if (childOperator instanceof PhysicalHashAggregateOperator) {
                PhysicalHashAggregateOperator childAgg = childOperator.cast();
                if (!childAgg.getType().isLocal() && !childAgg.getType().isDistinctLocal()) {
                    return false;
                }
                childOptExpression = childOptExpression.inputAt(0);
                childOperator = childOptExpression.getOp();
            }

            if (childOperator instanceof PhysicalProjectOperator) {
                childOptExpression = childOptExpression.inputAt(0);
                childOperator = childOptExpression.getOp();
            }

            return childOperator instanceof PhysicalOlapScanOperator;
        }

    }
}
