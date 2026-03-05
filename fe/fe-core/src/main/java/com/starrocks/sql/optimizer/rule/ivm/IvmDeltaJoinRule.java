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
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        if (!joinType.isInnerJoin() && !joinType.isCrossJoin() && !joinType.isLeftOuterJoin()
                && !joinType.isLeftAntiJoin() && !joinType.isLeftSemiJoin()) {
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

        if (joinType.isLeftAntiJoin() || joinType.isLeftSemiJoin()) {
            LeftSemiAntiRewriteResult semiAntiRewrite = buildLeftSemiAntiJoinRewrite(factory, context, joinExpr,
                    joinOutputColumns, actionColumn, joinType.isLeftSemiJoin());
            if (semiAntiRewrite == null) {
                return List.of();
            }
            if (!appendBranch(unionChildren, unionChildOutputs, semiAntiRewrite.leftDeltaBranch())
                    || !appendBranch(unionChildren, unionChildOutputs, semiAntiRewrite.rightInsertBranch())
                    || !appendBranch(unionChildren, unionChildOutputs, semiAntiRewrite.rightDeleteBranch())) {
                return List.of();
            }
            LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
            OptExpression unionExpr = OptExpression.create(unionOperator, unionChildren);
            OptExpression cteAnchor = OptExpression.create(new LogicalCTEAnchorOperator(semiAntiRewrite.cteId()),
                    semiAntiRewrite.rightFlatProducer(), unionExpr);
            return List.of(cteAnchor);
        } else {
            // branch1: delta(t1) join t2@from_version
            JoinOperator branch1JoinType = joinType.isLeftOuterJoin() ? JoinOperator.LEFT_OUTER_JOIN : joinType;
            BranchResult branch1 =
                    buildBranch(factory, context, joinExpr, branch1JoinType, joinOutputColumns, actionColumn, true);
            if (!appendBranch(unionChildren, unionChildOutputs, branch1)) {
                return List.of();
            }

            // branch2
            BranchResult branch2;
            if (joinType.isLeftOuterJoin()) {
                branch2 = buildLeftOuterRightDeltaBranch(factory, context, joinExpr, joinOutputColumns, actionColumn);
            } else {
                branch2 = buildBranch(factory, context, joinExpr, joinType, joinOutputColumns, actionColumn, false);
            }
            if (!appendBranch(unionChildren, unionChildOutputs, branch2)) {
                return List.of();
            }
        }

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
        return List.of(OptExpression.create(unionOperator, unionChildren));
    }

    private LeftSemiAntiRewriteResult buildLeftSemiAntiJoinRewrite(ColumnRefFactory factory,
                                                                   OptimizerContext context,
                                                                   OptExpression joinExpr,
                                                                   List<ColumnRefOperator> joinOutputColumns,
                                                                   ColumnRefOperator actionColumn,
                                                                   boolean isSemiJoin) {
        if (actionColumn == null) {
            return null;
        }
        LogicalJoinOperator join = (LogicalJoinOperator) joinExpr.getOp();
        List<JoinKeyPair> joinKeyPairs = deriveJoinKeyPairs(join, joinExpr.inputAt(0), joinExpr.inputAt(1));
        if (joinKeyPairs.isEmpty()) {
            return null;
        }

        int cteId = context.getCteContext().getNextCteId();
        RightFlatProducerResult rightFlat =
                buildLeftAntiRightFlatProducer(factory, context, joinExpr, actionColumn, joinKeyPairs, cteId);
        if (rightFlat == null) {
            return null;
        }
        BranchResult leftDeltaBranch =
                buildBranch(factory, context, joinExpr,
                        isSemiJoin ? JoinOperator.LEFT_SEMI_JOIN : JoinOperator.LEFT_ANTI_JOIN,
                        joinOutputColumns, actionColumn, true);
        BranchResult rightInsertBranch = buildLeftAntiRightChangeBranchFromCte(factory, context, joinExpr,
                joinOutputColumns, actionColumn, joinKeyPairs, rightFlat, true, isSemiJoin);
        BranchResult rightDeleteBranch = buildLeftAntiRightChangeBranchFromCte(factory, context, joinExpr,
                joinOutputColumns, actionColumn, joinKeyPairs, rightFlat, false, isSemiJoin);
        if (leftDeltaBranch == null || rightInsertBranch == null || rightDeleteBranch == null) {
            return null;
        }
        return new LeftSemiAntiRewriteResult(cteId, rightFlat.producer(),
                leftDeltaBranch, rightInsertBranch, rightDeleteBranch);
    }

    private RightFlatProducerResult buildLeftAntiRightFlatProducer(ColumnRefFactory factory,
                                                                   OptimizerContext context,
                                                                   OptExpression joinExpr,
                                                                   ColumnRefOperator actionColumn,
                                                                   List<JoinKeyPair> joinKeyPairs,
                                                                   int cteId) {
        DuplicatedJoin producerJoin = duplicateJoin(factory, context, joinExpr);
        List<ColumnRefOperator> mappedRightKeys = mapRightJoinKeys(joinKeyPairs, producerJoin.columnMapping());
        if (mappedRightKeys.isEmpty() || mappedRightKeys.stream().anyMatch(k -> k == null)) {
            return null;
        }
        ColumnRefOperator branchActionColumn = duplicateActionColumn(factory, actionColumn);

        OptExpression rightDeltaInput = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn),
                producerJoin.joinExpr().inputAt(1));
        ColumnRefOperator deltaCntRef = factory.create("__delta_cnt", IntegerType.BIGINT, false);
        Map<ColumnRefOperator, CallOperator> rightDeltaAggMap = Maps.newHashMap();
        rightDeltaAggMap.put(deltaCntRef, createBuiltinCall(FunctionSet.SUM, IntegerType.BIGINT,
                List.of(branchActionColumn)));
        LogicalAggregationOperator rightDeltaAggOp = new LogicalAggregationOperator(AggType.GLOBAL,
                mappedRightKeys, rightDeltaAggMap);
        OptExpression rightDeltaAgg = OptExpression.create(rightDeltaAggOp, rightDeltaInput);

        DuplicatedJoin fromJoin = duplicateJoin(factory, context, joinExpr);
        DuplicatedJoin semiDeltaJoin = duplicateJoin(factory, context, joinExpr);
        List<ColumnRefOperator> fromRightKeys = mapRightJoinKeys(joinKeyPairs, fromJoin.columnMapping());
        List<ColumnRefOperator> semiDeltaKeys = mapRightJoinKeys(joinKeyPairs, semiDeltaJoin.columnMapping());
        if (fromRightKeys.isEmpty() || semiDeltaKeys.isEmpty()) {
            return null;
        }
        ScalarOperator rightSemiOn = buildEquiPredicate(fromRightKeys, semiDeltaKeys);
        if (rightSemiOn == null) {
            return null;
        }
        OptExpression fromVersionRight = OptExpression.create(LogicalVersionOperator.fromVersion(),
                fromJoin.joinExpr().inputAt(1));
        ColumnRefOperator semiAction = duplicateActionColumn(factory, actionColumn);
        OptExpression semiDeltaRight = OptExpression.create(new LogicalDeltaOperator(false, semiAction),
                semiDeltaJoin.joinExpr().inputAt(1));
        OptExpression rightSemiJoin = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, rightSemiOn),
                fromVersionRight, semiDeltaRight);

        ColumnRefOperator cnt0Ref = factory.create("__cnt0", IntegerType.BIGINT, false);
        Map<ColumnRefOperator, CallOperator> rightFromAggMap = Maps.newHashMap();
        rightFromAggMap.put(cnt0Ref, createBuiltinCall(FunctionSet.COUNT, IntegerType.BIGINT,
                List.of(ConstantOperator.createInt(1))));
        LogicalAggregationOperator rightFromAggOp = new LogicalAggregationOperator(AggType.GLOBAL,
                fromRightKeys, rightFromAggMap);
        OptExpression rightFromAgg = OptExpression.create(rightFromAggOp, rightSemiJoin);

        ScalarOperator rightFlatOn = buildEquiPredicate(mappedRightKeys, fromRightKeys);
        if (rightFlatOn == null) {
            return null;
        }
        OptExpression rightFlatJoin = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, rightFlatOn),
                rightDeltaAgg, rightFromAgg);

        ColumnRefOperator flatCnt0Ref = factory.create("__flat_cnt0", IntegerType.BIGINT, false);
        ColumnRefOperator flatCnt1Ref = factory.create("__flat_cnt1", IntegerType.BIGINT, false);
        ScalarOperator cnt0Coalesce = createBuiltinCall(FunctionSet.COALESCE, IntegerType.BIGINT,
                List.of(cnt0Ref, ConstantOperator.createBigint(0L)));
        ScalarOperator cnt1Expr = createBuiltinCall(FunctionSet.ADD, IntegerType.BIGINT,
                List.of(cnt0Coalesce, deltaCntRef));
        Map<ColumnRefOperator, ScalarOperator> rightFlatProjectMap = Maps.newLinkedHashMap();
        for (ColumnRefOperator mappedRightKey : mappedRightKeys) {
            rightFlatProjectMap.put(mappedRightKey, mappedRightKey);
        }
        rightFlatProjectMap.put(flatCnt0Ref, cnt0Coalesce);
        rightFlatProjectMap.put(flatCnt1Ref, cnt1Expr);
        OptExpression rightFlatProject = OptExpression.create(new LogicalProjectOperator(rightFlatProjectMap), rightFlatJoin);
        OptExpression rightFlatProducer = OptExpression.create(new LogicalCTEProduceOperator(cteId), rightFlatProject);
        return new RightFlatProducerResult(cteId, rightFlatProducer, mappedRightKeys, flatCnt0Ref, flatCnt1Ref);
    }

    // For left semi join: right insert -> left insert(+1), right delete -> left delete(-1).
    // For left anti join: right insert -> left delete(-1), right delete -> left insert(+1).
    private BranchResult buildLeftAntiRightChangeBranchFromCte(ColumnRefFactory factory,
                                                               OptimizerContext context,
                                                               OptExpression joinExpr,
                                                               List<ColumnRefOperator> joinOutputColumns,
                                                               ColumnRefOperator actionColumn,
                                                               List<JoinKeyPair> joinKeyPairs,
                                                               RightFlatProducerResult rightFlat,
                                                               boolean onRightInsert,
                                                               boolean isSemiJoin) {
        DuplicatedJoin branchJoin = duplicateJoin(factory, context, joinExpr);
        LogicalJoinOperator branchJoinOp = (LogicalJoinOperator) branchJoin.joinExpr().getOp();
        List<ColumnRefOperator> branchRightKeys = mapRightJoinKeys(joinKeyPairs, branchJoin.columnMapping());
        if (branchRightKeys.isEmpty() || branchRightKeys.stream().anyMatch(k -> k == null)) {
            return null;
        }
        ColumnRefOperator branchActionColumn = duplicateActionColumn(factory, actionColumn);
        OptExpression leftToVersion = OptExpression.create(LogicalVersionOperator.toVersion(), branchJoin.joinExpr().inputAt(0));

        Map<ColumnRefOperator, ColumnRefOperator> consumeMap = Maps.newLinkedHashMap();
        for (int i = 0; i < branchRightKeys.size(); i++) {
            consumeMap.put(branchRightKeys.get(i), rightFlat.rightKeys().get(i));
        }
        ColumnRefOperator branchCnt0Ref = factory.create("__branch_cnt0", IntegerType.BIGINT, false);
        ColumnRefOperator branchCnt1Ref = factory.create("__branch_cnt1", IntegerType.BIGINT, false);
        consumeMap.put(branchCnt0Ref, rightFlat.cnt0Ref());
        consumeMap.put(branchCnt1Ref, rightFlat.cnt1Ref());
        OptExpression rightFlatConsume = OptExpression.create(new LogicalCTEConsumeOperator(rightFlat.cteId(), consumeMap));

        ScalarOperator filterPredicate;
        if (onRightInsert) {
            filterPredicate = Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.EQ, branchCnt0Ref, ConstantOperator.createBigint(0L)),
                    new BinaryPredicateOperator(BinaryType.GT, branchCnt1Ref, ConstantOperator.createBigint(0L)));
        } else {
            filterPredicate = Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.GT, branchCnt0Ref, ConstantOperator.createBigint(0L)),
                    new BinaryPredicateOperator(BinaryType.EQ, branchCnt1Ref, ConstantOperator.createBigint(0L)));
        }
        OptExpression rightFlatFiltered = OptExpression.create(new LogicalFilterOperator(filterPredicate), rightFlatConsume);

        LogicalJoinOperator leftSemiJoin = LogicalJoinOperator.builder()
                .withOperator(branchJoinOp)
                .setJoinType(JoinOperator.LEFT_SEMI_JOIN)
                .build();
        OptExpression semiJoinExpr = OptExpression.create(leftSemiJoin, leftToVersion, rightFlatFiltered);

        byte actionValue;
        if (isSemiJoin) {
            actionValue = onRightInsert ? (byte) 1 : (byte) -1;
        } else {
            actionValue = onRightInsert ? (byte) -1 : (byte) 1;
        }
        Map<ColumnRefOperator, ScalarOperator> branchProjectMap = Maps.newLinkedHashMap();
        for (ColumnRefOperator output : joinOutputColumns) {
            ColumnRefOperator mappedOutput = branchJoin.columnMapping().get(output);
            if (mappedOutput == null) {
                return null;
            }
            branchProjectMap.put(mappedOutput, mappedOutput);
        }
        branchProjectMap.put(branchActionColumn, ConstantOperator.createTinyInt(actionValue));
        OptExpression branchExpr = OptExpression.create(new LogicalProjectOperator(branchProjectMap), semiJoinExpr);

        List<ColumnRefOperator> outputs = deriveBranchOutputs(joinOutputColumns, branchActionColumn,
                branchJoin.columnMapping());
        if (outputs == null) {
            return null;
        }
        return new BranchResult(branchExpr, outputs);
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

    private List<JoinKeyPair> deriveJoinKeyPairs(LogicalJoinOperator join,
                                                 OptExpression leftChild,
                                                 OptExpression rightChild) {
        ScalarOperator onPredicate = join.getOnPredicate();
        if (onPredicate == null) {
            return List.of();
        }
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(onPredicate);
        ColumnRefSet leftColumns = leftChild.getOutputColumns();
        ColumnRefSet rightColumns = rightChild.getOutputColumns();
        List<BinaryPredicateOperator> eqPredicates =
                JoinHelper.getEqualsPredicate(leftColumns, rightColumns, conjuncts);
        if (eqPredicates.isEmpty() || eqPredicates.size() != conjuncts.size()) {
            return List.of();
        }

        List<JoinKeyPair> pairs = new ArrayList<>(eqPredicates.size());
        for (BinaryPredicateOperator eqPredicate : eqPredicates) {
            if (!(eqPredicate.getChild(0) instanceof ColumnRefOperator)
                    || !(eqPredicate.getChild(1) instanceof ColumnRefOperator)) {
                return List.of();
            }
            ColumnRefOperator lhs = (ColumnRefOperator) eqPredicate.getChild(0);
            ColumnRefOperator rhs = (ColumnRefOperator) eqPredicate.getChild(1);
            if (leftColumns.contains(lhs) && rightColumns.contains(rhs)) {
                pairs.add(new JoinKeyPair(lhs, rhs));
            } else if (leftColumns.contains(rhs) && rightColumns.contains(lhs)) {
                pairs.add(new JoinKeyPair(rhs, lhs));
            } else {
                return List.of();
            }
        }
        return pairs;
    }

    private List<ColumnRefOperator> mapRightJoinKeys(List<JoinKeyPair> keyPairs,
                                                     Map<ColumnRefOperator, ColumnRefOperator> oldToNew) {
        return keyPairs.stream().map(JoinKeyPair::rightKey).map(oldToNew::get).collect(Collectors.toList());
    }

    private ScalarOperator buildEquiPredicate(List<ColumnRefOperator> leftKeys,
                                              List<ColumnRefOperator> rightKeys) {
        if (leftKeys.size() != rightKeys.size() || leftKeys.isEmpty()) {
            return null;
        }
        List<ScalarOperator> conjuncts = Lists.newArrayListWithCapacity(leftKeys.size());
        for (int i = 0; i < leftKeys.size(); i++) {
            if (leftKeys.get(i) == null || rightKeys.get(i) == null) {
                return null;
            }
            conjuncts.add(new BinaryPredicateOperator(BinaryType.EQ, leftKeys.get(i), rightKeys.get(i)));
        }
        return Utils.compoundAnd(conjuncts);
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

    private CallOperator createBuiltinCall(String fnName, Type returnType, List<ScalarOperator> args) {
        Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = ExprUtils.getBuiltinFunction(fnName, argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            return new CallOperator(fnName, returnType, args);
        }
        return new CallOperator(fnName, returnType, args, fn.copy());
    }

    private record DuplicatedJoin(OptExpression joinExpr, Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
    }

    private record RightFlatProducerResult(int cteId,
                                           OptExpression producer,
                                           List<ColumnRefOperator> rightKeys,
                                           ColumnRefOperator cnt0Ref,
                                           ColumnRefOperator cnt1Ref) {
    }

    private record LeftSemiAntiRewriteResult(int cteId,
                                             OptExpression rightFlatProducer,
                                             BranchResult leftDeltaBranch,
                                             BranchResult rightInsertBranch,
                                             BranchResult rightDeleteBranch) {
    }

    private record JoinKeyPair(ColumnRefOperator leftKey, ColumnRefOperator rightKey) {
    }

    private record BranchResult(OptExpression branchExpr, List<ColumnRefOperator> outputs) {
    }
}
