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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IvmDeltaIntersectRule extends TransformationRule {
    public IvmDeltaIntersectRule() {
        super(RuleType.TF_OLAP_IVM_DELTA_INTERSECT,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_INTERSECT)
                                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF))));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        if (delta.getActionColumn() == null) {
            return List.of();
        }

        OptExpression intersectExpr = input.inputAt(0);
        LogicalIntersectOperator intersect = intersectExpr.getOp().cast();
        List<ColumnRefOperator> finalOutputs = input.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        List<ColumnRefOperator> detailOutputs = finalOutputs.stream()
                .filter(output -> !output.equals(delta.getActionColumn()))
                .toList();
        if (detailOutputs.isEmpty() || intersectExpr.arity() < 2) {
            return List.of();
        }

        ColumnRefFactory factory = context.getColumnRefFactory();
        int affectedKeyCteId = context.getCteContext().getNextCteId();
        OptExpression affectedKeyProducer =
                buildAffectedKeyProducer(context, affectedKeyCteId, intersectExpr, intersect, detailOutputs);
        if (affectedKeyProducer == null) {
            return List.of();
        }

        OptExpression oldPart = buildVersionedIntersect(context, affectedKeyCteId, detailOutputs,
                intersectExpr, intersect, LogicalVersionOperator.VersionRefType.FROM_VERSION);
        OptExpression newPart = buildVersionedIntersect(context, affectedKeyCteId, detailOutputs,
                intersectExpr, intersect, LogicalVersionOperator.VersionRefType.TO_VERSION);
        if (oldPart == null || newPart == null) {
            return List.of();
        }

        int oldPartCteId = context.getCteContext().getNextCteId();
        int newPartCteId = context.getCteContext().getNextCteId();
        OptExpression oldProducer = OptExpression.create(new LogicalCTEProduceOperator(oldPartCteId), oldPart);
        OptExpression newProducer = OptExpression.create(new LogicalCTEProduceOperator(newPartCteId), newPart);

        DiffBranch plusPart = buildDiffBranch(
                factory, newPartCteId, oldPartCteId, detailOutputs, delta.getActionColumn(), (byte) 1);
        DiffBranch minusPart = buildDiffBranch(
                factory, oldPartCteId, newPartCteId, detailOutputs, delta.getActionColumn(), (byte) -1);

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(
                finalOutputs, List.of(plusPart.outputs(), minusPart.outputs()), true);
        OptExpression unionExpr = OptExpression.create(unionOperator, plusPart.optExpression(), minusPart.optExpression());

        OptExpression newAnchor = OptExpression.create(new LogicalCTEAnchorOperator(newPartCteId), newProducer, unionExpr);
        OptExpression oldAnchor = OptExpression.create(new LogicalCTEAnchorOperator(oldPartCteId), oldProducer, newAnchor);
        return List.of(OptExpression.create(new LogicalCTEAnchorOperator(affectedKeyCteId), affectedKeyProducer, oldAnchor));
    }

    private OptExpression buildAffectedKeyProducer(OptimizerContext context,
                                                   int cteId,
                                                   OptExpression intersectExpr,
                                                   LogicalIntersectOperator intersect,
                                                   List<ColumnRefOperator> detailOutputs) {
        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();
        for (int i = 0; i < intersectExpr.arity(); i++) {
            ChildClone childClone = cloneChild(context, intersectExpr.inputAt(i), intersect.getChildOutputColumns().get(i));
            unionChildren.add(OptExpression.create(new LogicalDeltaOperator(false), childClone.optExpression()));
            unionChildOutputs.add(childClone.outputColumns());
        }

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(detailOutputs, unionChildOutputs, true);
        OptExpression unionExpr = OptExpression.create(unionOperator, unionChildren);
        LogicalAggregationOperator distinctAgg =
                new LogicalAggregationOperator(AggType.GLOBAL, detailOutputs, Maps.newHashMap());
        return OptExpression.create(new LogicalCTEProduceOperator(cteId), OptExpression.create(distinctAgg, unionExpr));
    }

    private OptExpression buildVersionedIntersect(OptimizerContext context,
                                                  int affectedKeyCteId,
                                                  List<ColumnRefOperator> detailOutputs,
                                                  OptExpression intersectExpr,
                                                  LogicalIntersectOperator intersect,
                                                  LogicalVersionOperator.VersionRefType versionRefType) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        List<OptExpression> children = Lists.newArrayListWithCapacity(intersectExpr.arity());
        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayListWithCapacity(intersectExpr.arity());
        for (int i = 0; i < intersectExpr.arity(); i++) {
            ChildClone childClone = cloneChild(context, intersectExpr.inputAt(i), intersect.getChildOutputColumns().get(i));
            OptExpression versionExpr =
                    OptExpression.create(new LogicalVersionOperator(versionRefType), childClone.optExpression());
            OptExpression filteredExpr =
                    createLeftSemiJoin(factory, affectedKeyCteId, childClone.outputColumns(), detailOutputs, versionExpr);
            if (filteredExpr == null) {
                return null;
            }
            children.add(filteredExpr);
            childOutputColumns.add(childClone.outputColumns());
        }

        LogicalIntersectOperator intersectOperator = new LogicalIntersectOperator.Builder()
                .setOutputColumnRefOp(detailOutputs)
                .setChildOutputColumns(childOutputColumns)
                .build();
        return OptExpression.create(intersectOperator, children);
    }

    private DiffBranch buildDiffBranch(ColumnRefFactory factory,
                                       int leftCteId,
                                       int rightCteId,
                                       List<ColumnRefOperator> detailOutputs,
                                       ColumnRefOperator actionColumn,
                                       byte actionValue) {
        CteConsumer left = createCteConsumer(factory, leftCteId, detailOutputs);
        CteConsumer right = createCteConsumer(factory, rightCteId, detailOutputs);
        ScalarOperator onPredicate = buildEqualityPredicate(left.outputColumns(), right.outputColumns());
        OptExpression antiJoinExpr = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_ANTI_JOIN, onPredicate),
                left.optExpression(), right.optExpression());

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newLinkedHashMap();
        List<ColumnRefOperator> projectOutputs = Lists.newArrayListWithCapacity(left.outputColumns().size() + 1);
        for (ColumnRefOperator output : left.outputColumns()) {
            projectMap.put(output, output);
            projectOutputs.add(output);
        }
        ColumnRefOperator branchAction =
                factory.create(actionColumn.getName(), actionColumn.getType(), actionColumn.isNullable());
        projectMap.put(branchAction, ConstantOperator.createTinyInt(actionValue));
        projectOutputs.add(branchAction);

        return new DiffBranch(OptExpression.create(new LogicalProjectOperator(projectMap), antiJoinExpr), projectOutputs);
    }

    private OptExpression createLeftSemiJoin(ColumnRefFactory factory,
                                             int cteId,
                                             List<ColumnRefOperator> leftOutputs,
                                             List<ColumnRefOperator> rightProducerOutputs,
                                             OptExpression leftChild) {
        CteConsumer rightConsumer = createCteConsumer(factory, cteId, rightProducerOutputs);
        ScalarOperator onPredicate = buildEqualityPredicate(leftOutputs, rightConsumer.outputColumns());
        if (onPredicate == null) {
            return null;
        }
        return OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, onPredicate), leftChild, rightConsumer.optExpression());
    }

    private CteConsumer createCteConsumer(ColumnRefFactory factory, int cteId, List<ColumnRefOperator> producerOutputs) {
        List<ColumnRefOperator> consumerOutputs = Lists.newArrayListWithCapacity(producerOutputs.size());
        Map<ColumnRefOperator, ColumnRefOperator> consumerMap = Maps.newLinkedHashMap();
        for (ColumnRefOperator producerOutput : producerOutputs) {
            ColumnRefOperator consumerOutput =
                    factory.create(producerOutput.getName(), producerOutput.getType(), producerOutput.isNullable());
            consumerOutputs.add(consumerOutput);
            consumerMap.put(consumerOutput, producerOutput);
        }
        return new CteConsumer(OptExpression.create(new LogicalCTEConsumeOperator(cteId, consumerMap)), consumerOutputs);
    }

    private ScalarOperator buildEqualityPredicate(List<ColumnRefOperator> leftOutputs,
                                                 List<ColumnRefOperator> rightOutputs) {
        if (leftOutputs.size() != rightOutputs.size()) {
            return null;
        }
        if (leftOutputs.isEmpty()) {
            return ConstantOperator.TRUE;
        }
        List<ScalarOperator> predicates = new ArrayList<>(leftOutputs.size());
        for (int i = 0; i < leftOutputs.size(); i++) {
            predicates.add(new BinaryPredicateOperator(BinaryType.EQ, leftOutputs.get(i), rightOutputs.get(i)));
        }
        return Utils.compoundAnd(predicates);
    }

    private ChildClone cloneChild(OptimizerContext context, OptExpression child, List<ColumnRefOperator> oldOutputs) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
        OptExpression newChild = duplicator.duplicate(child);
        return new ChildClone(newChild, duplicator.getMappedColumns(oldOutputs));
    }

    private record ChildClone(OptExpression optExpression, List<ColumnRefOperator> outputColumns) {
    }

    private record CteConsumer(OptExpression optExpression, List<ColumnRefOperator> outputColumns) {
    }

    private record DiffBranch(OptExpression optExpression, List<ColumnRefOperator> outputs) {
    }
}
