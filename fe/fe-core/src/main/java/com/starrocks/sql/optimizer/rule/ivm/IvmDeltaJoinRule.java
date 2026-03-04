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
import com.google.common.collect.Maps;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.IntegerType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        if (!joinType.isInnerJoin() && !joinType.isCrossJoin() && !joinType.isLeftOuterJoin()) {
            return List.of();
        }

        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        ColumnRefOperator actionColumn = delta.getActionColumn();
        List<ColumnRefOperator> finalOutputColumns =
                input.getOutputColumns().getColumnRefOperators(columnRefFactory);
        List<ColumnRefOperator> joinOutputColumns = Lists.newArrayList();
        for (ColumnRefOperator outputColumn : finalOutputColumns) {
            if (actionColumn == null || outputColumn.getId() != actionColumn.getId()) {
                joinOutputColumns.add(outputColumn);
            }
        }

        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();

        // branch1: delta(t1) join t2@from_version
        JoinOperator branch1JoinType = joinType.isLeftOuterJoin() ? JoinOperator.LEFT_OUTER_JOIN : joinType;
        BranchResult branch1 = buildBranch(columnRefFactory, context, joinExpr, branch1JoinType, joinOutputColumns,
                actionColumn, true);
        if (!appendBranch(unionChildren, unionChildOutputs, branch1)) {
            return List.of();
        }

        // branch2
        BranchResult branch2;
        if (joinType.isLeftOuterJoin()) {
            branch2 = buildLeftOuterRightDeltaBranch(columnRefFactory, context, joinExpr, joinOutputColumns, actionColumn);
        } else {
            branch2 = buildBranch(columnRefFactory, context, joinExpr, joinType, joinOutputColumns, actionColumn, false);
        }
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

        List<ColumnRefOperator> outputs =
                deriveBranchOutputs(joinOutputColumns, branchActionColumn, duplicatedJoin.columnMapping());
        if (outputs == null) {
            return null;
        }

        LogicalJoinOperator newJoin = LogicalJoinOperator.builder()
                .withOperator(duplicatedJoinOp)
                .setJoinType(branchJoinType)
                .build();
        return new BranchResult(OptExpression.create(newJoin, left, right), outputs);
    }

    private BranchResult buildLeftOuterRightDeltaBranch(ColumnRefFactory columnRefFactory,
                                                        OptimizerContext context,
                                                        OptExpression joinExpr,
                                                        List<ColumnRefOperator> joinOutputColumns,
                                                        ColumnRefOperator actionColumn) {
        if (actionColumn == null) {
            return null;
        }
        LogicalJoinOperator join = (LogicalJoinOperator) joinExpr.getOp();

        DuplicatedJoin positiveJoin = duplicateJoin(columnRefFactory, context, joinExpr);
        DuplicatedJoin negativeJoin = duplicateJoin(columnRefFactory, context, joinExpr);
        LogicalJoinOperator positiveJoinOp = (LogicalJoinOperator) positiveJoin.joinExpr().getOp();
        ColumnRefOperator branchActionColumn = duplicateActionColumn(columnRefFactory, actionColumn);

        OptExpression leftToVersion =
                OptExpression.create(LogicalVersionOperator.toVersion(), positiveJoin.joinExpr().inputAt(0));
        OptExpression rightDelta = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn),
                positiveJoin.joinExpr().inputAt(1));
        OptExpression negativeDelta = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn),
                negativeJoin.joinExpr().inputAt(1));

        List<ColumnRefOperator> originalRightOutputs =
                joinExpr.inputAt(1).getOutputColumns().getColumnRefOperators(columnRefFactory);
        Set<Integer> rightKeyColumnIds = deriveRightJoinKeyColumnIds(join, joinExpr.inputAt(0), joinExpr.inputAt(1));

        Map<ColumnRefOperator, ScalarOperator> negativeProjectMap = Maps.newLinkedHashMap();
        for (ColumnRefOperator originalRight : originalRightOutputs) {
            ColumnRefOperator positiveRight = positiveJoin.columnMapping().get(originalRight);
            ColumnRefOperator negativeRight = negativeJoin.columnMapping().get(originalRight);
            if (positiveRight == null || negativeRight == null) {
                return null;
            }
            if (rightKeyColumnIds.contains(originalRight.getId())) {
                negativeProjectMap.put(positiveRight, negativeRight);
            } else {
                negativeProjectMap.put(positiveRight, ConstantOperator.createNull(positiveRight.getType()));
            }
        }
        negativeProjectMap.put(branchActionColumn, buildReversedActionExpr(branchActionColumn));
        OptExpression negativeProject = OptExpression.create(new LogicalProjectOperator(negativeProjectMap), negativeDelta);

        List<ColumnRefOperator> rightUnionOutputs = Lists.newArrayListWithCapacity(originalRightOutputs.size() + 1);
        for (ColumnRefOperator originalRight : originalRightOutputs) {
            ColumnRefOperator positiveRight = positiveJoin.columnMapping().get(originalRight);
            if (positiveRight == null) {
                return null;
            }
            rightUnionOutputs.add(positiveRight);
        }
        rightUnionOutputs.add(branchActionColumn);
        LogicalUnionOperator rightUnion = new LogicalUnionOperator(rightUnionOutputs,
                List.of(rightUnionOutputs, rightUnionOutputs), true);
        OptExpression rightAugmented = OptExpression.create(rightUnion, rightDelta, negativeProject);

        List<ColumnRefOperator> outputs =
                deriveBranchOutputs(joinOutputColumns, branchActionColumn, positiveJoin.columnMapping());
        if (outputs == null) {
            return null;
        }

        LogicalJoinOperator newJoin = LogicalJoinOperator.builder()
                .withOperator(positiveJoinOp)
                .setJoinType(JoinOperator.INNER_JOIN)
                .build();
        return new BranchResult(OptExpression.create(newJoin, leftToVersion, rightAugmented), outputs);
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

    private Set<Integer> deriveRightJoinKeyColumnIds(LogicalJoinOperator join,
                                                     OptExpression leftChild,
                                                     OptExpression rightChild) {
        Set<Integer> rightKeyColumnIds = new HashSet<>();
        ScalarOperator onPredicate = join.getOnPredicate();
        if (onPredicate == null) {
            return rightKeyColumnIds;
        }
        ColumnRefSet leftColumns = leftChild.getOutputColumns();
        ColumnRefSet rightColumns = rightChild.getOutputColumns();
        List<BinaryPredicateOperator> eqPredicates =
                JoinHelper.getEqualsPredicate(leftColumns, rightColumns, Utils.extractConjuncts(onPredicate));
        for (BinaryPredicateOperator eqPredicate : eqPredicates) {
            ColumnRefSet lhsUsed = eqPredicate.getChild(0).getUsedColumns();
            ColumnRefSet rhsUsed = eqPredicate.getChild(1).getUsedColumns();
            if (rightColumns.containsAll(lhsUsed)) {
                for (int id : lhsUsed.getColumnIds()) {
                    rightKeyColumnIds.add(id);
                }
            } else if (rightColumns.containsAll(rhsUsed)) {
                for (int id : rhsUsed.getColumnIds()) {
                    rightKeyColumnIds.add(id);
                }
            }
        }
        return rightKeyColumnIds;
    }

    private ScalarOperator buildReversedActionExpr(ColumnRefOperator actionColumn) {
        ScalarOperator isDeleteAction = new BinaryPredicateOperator(BinaryType.LT, actionColumn,
                ConstantOperator.createTinyInt((byte) 0));
        return new CaseWhenOperator(IntegerType.TINYINT, null,
                ConstantOperator.createTinyInt((byte) -1),
                List.of(isDeleteAction, ConstantOperator.createTinyInt((byte) 1)));
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

    private record DuplicatedJoin(OptExpression joinExpr, Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
    }

    private record BranchResult(OptExpression branchExpr, List<ColumnRefOperator> outputs) {
    }
}
