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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;

import java.util.List;
import java.util.Map;

public class IvmDeltaJoinRule extends TransformationRule {
    public IvmDeltaJoinRule() {
        super(RuleType.TF_OLAP_IVM_DELTA_JOIN, Pattern.create(OperatorType.LOGICAL_DELTA)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        OptExpression joinExpr = input.inputAt(0);
        LogicalJoinOperator join = (LogicalJoinOperator) joinExpr.getOp();
        if (!join.getJoinType().isInnerJoin()) {
            return List.of();
        }

        ColumnRefFactory factory = context.getColumnRefFactory();
        ColumnRefOperator actionColumn = delta.getActionColumn();
        List<ColumnRefOperator> finalOutputColumns = input.getOutputColumns().getColumnRefOperators(factory);
        List<ColumnRefOperator> joinOutputColumns = Lists.newArrayList();
        for (ColumnRefOperator outputColumn : finalOutputColumns) {
            if (actionColumn == null || outputColumn.getId() != actionColumn.getId()) {
                joinOutputColumns.add(outputColumn);
            }
        }

        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();

        // branch1: delta(t1) inner join t2@from_version
        BranchResult branch1 = buildBranch(factory, context, joinExpr,
                JoinOperator.INNER_JOIN, joinOutputColumns, actionColumn, true);
        if (!appendBranch(unionChildren, unionChildOutputs, branch1)) {
            return List.of();
        }

        // branch2: t1@to_version inner join delta(t2)
        BranchResult branch2 = buildBranch(factory, context, joinExpr,
                JoinOperator.INNER_JOIN, joinOutputColumns, actionColumn, false);
        if (!appendBranch(unionChildren, unionChildOutputs, branch2)) {
            return List.of();
        }

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
        return List.of(OptExpression.create(unionOperator, unionChildren));
    }

    private DuplicatedJoin duplicateJoin(ColumnRefFactory columnRefFactory,
                                         OptimizerContext context,
                                         OptExpression joinExpr) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(columnRefFactory, context);
        return new DuplicatedJoin(duplicator.duplicate(joinExpr), duplicator.getColumnMapping());
    }

    private BranchResult buildBranch(ColumnRefFactory columnRefFactory,
                                     OptimizerContext context,
                                     OptExpression joinExpr,
                                     JoinOperator branchJoinType,
                                     List<ColumnRefOperator> joinOutputColumns,
                                     ColumnRefOperator actionColumn,
                                     boolean isLeftDelta) {
        DuplicatedJoin duplicatedJoin = duplicateJoin(columnRefFactory, context, joinExpr);
        LogicalJoinOperator duplicatedJoinOp = (LogicalJoinOperator) duplicatedJoin.joinExpr().getOp();
        ColumnRefOperator branchActionColumn = duplicateActionColumn(columnRefFactory, actionColumn);
        OptExpression left;
        OptExpression right;
        if (isLeftDelta) {
            left = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn),
                    duplicatedJoin.joinExpr().inputAt(0));
            right = OptExpression.create(LogicalVersionOperator.fromVersion(), duplicatedJoin.joinExpr().inputAt(1));
        } else {
            left = OptExpression.create(LogicalVersionOperator.toVersion(), duplicatedJoin.joinExpr().inputAt(0));
            right = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn),
                    duplicatedJoin.joinExpr().inputAt(1));
        }

        List<ColumnRefOperator> outputs = deriveBranchOutputs(joinOutputColumns,
                branchActionColumn, duplicatedJoin.columnMapping());
        if (outputs == null) {
            return null;
        }

        LogicalJoinOperator newJoin = LogicalJoinOperator.builder()
                .withOperator(duplicatedJoinOp)
                .setJoinType(branchJoinType)
                .build();
        return new BranchResult(OptExpression.create(newJoin, left, right), outputs);
    }

    private boolean appendBranch(List<OptExpression> unionChildren,
                                 List<List<ColumnRefOperator>> unionChildOutputs,
                                 BranchResult branch) {
        if (branch == null || branch.outputs() == null) {
            return false;
        }
        unionChildren.add(branch.branchExpr());
        unionChildOutputs.add(branch.outputs());
        return true;
    }

    private ColumnRefOperator duplicateActionColumn(ColumnRefFactory columnRefFactory, ColumnRefOperator actionColumn) {
        if (actionColumn == null) {
            return null;
        }
        return columnRefFactory.create(actionColumn.getName(), actionColumn.getType(), actionColumn.isNullable());
    }

    private List<ColumnRefOperator> deriveBranchOutputs(List<ColumnRefOperator> joinOutputColumns,
                                                        ColumnRefOperator actionColumn,
                                                        Map<ColumnRefOperator, ColumnRefOperator> oldToNewColumnMapping) {
        List<ColumnRefOperator> outputs =
                Lists.newArrayListWithCapacity(joinOutputColumns.size() + (actionColumn == null ? 0 : 1));
        for (ColumnRefOperator output : joinOutputColumns) {
            ColumnRefOperator mappedOutput = oldToNewColumnMapping.get(output);
            if (mappedOutput == null) {
                return null;
            }
            outputs.add(mappedOutput);
        }
        if (actionColumn != null) {
            outputs.add(actionColumn);
        }
        return outputs;
    }

    private record DuplicatedJoin(OptExpression joinExpr,
                                  Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
    }

    private record BranchResult(OptExpression branchExpr, List<ColumnRefOperator> outputs) {
    }
}
