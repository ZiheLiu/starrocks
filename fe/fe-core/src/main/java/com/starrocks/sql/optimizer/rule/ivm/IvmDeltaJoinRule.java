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
        JoinOperator joinType = join.getJoinType();
        if (!joinType.isInnerJoin() && !joinType.isCrossJoin()) {
            return List.of();
        }

        OptExpression leftChild = joinExpr.inputAt(0);
        OptExpression rightChild = joinExpr.inputAt(1);
        ColumnRefFactory factory = context.getColumnRefFactory();

        List<ColumnRefOperator> joinOutputColumns =
                joinExpr.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        List<ColumnRefOperator> finalOutputColumns = Lists.newArrayList(joinOutputColumns);
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn != null) {
            finalOutputColumns.add(actionColumn);
        }

        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();

        // delta(t1) INNER JOIN t2@from_version
        BranchResult branch1 = buildBranch(factory, context, join, joinOutputColumns, actionColumn, leftChild, rightChild, true);
        if (branch1 == null || branch1.outputs == null) {
            return List.of();
        }
        unionChildren.add(branch1.branchExpr);
        unionChildOutputs.add(branch1.outputs);

        // t1@to_version INNER JOIN delta(t2)
        BranchResult branch2 = buildBranch(factory, context, join, joinOutputColumns, actionColumn, leftChild, rightChild, false);
        if (branch2 == null || branch2.outputs == null) {
            return List.of();
        }
        unionChildren.add(branch2.branchExpr);
        unionChildOutputs.add(branch2.outputs);

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
        return List.of(OptExpression.create(unionOperator, unionChildren));
    }

    private BranchInput duplicateChildren(ColumnRefFactory columnRefFactory,
                                          OptimizerContext context,
                                          OptExpression leftChild,
                                          OptExpression rightChild) {
        OptExpressionDuplicator leftDuplicator = new OptExpressionDuplicator(columnRefFactory, context);
        OptExpressionDuplicator rightDuplicator = new OptExpressionDuplicator(columnRefFactory, context);
        OptExpression newLeftChild = leftDuplicator.duplicate(leftChild);
        OptExpression newRightChild = rightDuplicator.duplicate(rightChild);
        return new BranchInput(newLeftChild, newRightChild,
                leftDuplicator.getColumnMapping(), rightDuplicator.getColumnMapping());
    }

    private BranchResult buildBranch(ColumnRefFactory columnRefFactory,
                                     OptimizerContext context,
                                     LogicalJoinOperator join,
                                     List<ColumnRefOperator> joinOutputColumns,
                                     ColumnRefOperator actionColumn,
                                     OptExpression leftChild,
                                     OptExpression rightChild,
                                     boolean isLeftDelta) {
        BranchInput branchInput = duplicateChildren(columnRefFactory, context, leftChild, rightChild);
        ColumnRefOperator branchActionColumn = duplicateActionColumn(columnRefFactory, actionColumn);
        OptExpression left;
        OptExpression right;
        if (isLeftDelta) {
            left = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn), branchInput.leftChild);
            right = OptExpression.create(LogicalVersionOperator.fromVersion(), branchInput.rightChild);
        } else {
            left = OptExpression.create(LogicalVersionOperator.toVersion(), branchInput.leftChild);
            right = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn), branchInput.rightChild);
        }

        List<ColumnRefOperator> outputs = deriveBranchOutputs(joinOutputColumns, branchActionColumn, branchInput);
        if (outputs == null) {
            return null;
        }

        LogicalJoinOperator newJoin = LogicalJoinOperator.builder().withOperator(join).build();
        return new BranchResult(OptExpression.create(newJoin, left, right), outputs);
    }

    private ColumnRefOperator duplicateActionColumn(ColumnRefFactory columnRefFactory, ColumnRefOperator actionColumn) {
        if (actionColumn == null) {
            return null;
        }
        return columnRefFactory.create(actionColumn.getName(), actionColumn.getType(), actionColumn.isNullable());
    }

    private List<ColumnRefOperator> deriveBranchOutputs(List<ColumnRefOperator> joinOutputColumns,
                                                        ColumnRefOperator actionColumn,
                                                        BranchInput branch) {
        List<ColumnRefOperator> outputs =
                Lists.newArrayListWithCapacity(joinOutputColumns.size() + (actionColumn == null ? 0 : 1));
        for (ColumnRefOperator output : joinOutputColumns) {
            ColumnRefOperator mappedOutput = branch.leftOldToNew.get(output);
            if (mappedOutput == null) {
                mappedOutput = branch.rightOldToNew.get(output);
            }
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

    private record BranchInput(OptExpression leftChild, OptExpression rightChild,
                               Map<ColumnRefOperator, ColumnRefOperator> leftOldToNew,
                               Map<ColumnRefOperator, ColumnRefOperator> rightOldToNew) {
    }

    private record BranchResult(OptExpression branchExpr, List<ColumnRefOperator> outputs) {
    }

}
