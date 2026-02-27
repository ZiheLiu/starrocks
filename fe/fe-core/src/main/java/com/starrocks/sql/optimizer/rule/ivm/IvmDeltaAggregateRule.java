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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.IntegerType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IvmDeltaAggregateRule extends TransformationRule {
    public IvmDeltaAggregateRule() {
        super(RuleType.TF_OLAP_IVM_DELTA_AGGREGATE,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.inputAt(0).getOp();
        OptExpression aggChild = input.inputAt(0).inputAt(0);
        if (!isSupportedAggregate(agg)) {
            return List.of();
        }

        LogicalOlapScanOperator boundScan = findCandidateOlapScan(aggChild);
        if (boundScan == null || !(boundScan.getTable() instanceof OlapTable olapTable)) {
            return List.of();
        }
        Long fromVersion = boundScan.getTableVersion();
        if (fromVersion == null) {
            return List.of();
        }
        long toVersion = IvmRuleUtils.getLatestVisibleVersion(olapTable);
        if (toVersion <= fromVersion) {
            return List.of();
        }

        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        List<ColumnRefOperator> originalChildOutputs = aggChild.getOutputColumns().getColumnRefOperators(columnRefFactory);
        List<ColumnRefOperator> originalGroupingKeys = agg.getGroupingKeys();

        if (originalGroupingKeys.stream().anyMatch(k -> !originalChildOutputs.contains(k))) {
            return List.of();
        }

        // Build +R@toVersion and -R@fromVersion branches by version markers, then push version down by rewrite rules.
        OptExpressionDuplicator toDuplicator = new OptExpressionDuplicator(columnRefFactory, context);
        OptExpression toChild = toDuplicator.duplicate(aggChild);
        List<ColumnRefOperator> toMappedOutputs = toDuplicator.getMappedColumns(originalChildOutputs);
        OptExpressionDuplicator fromDuplicator = new OptExpressionDuplicator(columnRefFactory, context);
        OptExpression fromChild = fromDuplicator.duplicate(aggChild);
        List<ColumnRefOperator> fromMappedOutputs = fromDuplicator.getMappedColumns(originalChildOutputs);

        Map<ColumnRefOperator, ColumnRefOperator> originalToUnionCols = Maps.newHashMap();
        List<ColumnRefOperator> unionOutputColumns = Lists.newArrayListWithCapacity(originalChildOutputs.size() + 1);
        for (ColumnRefOperator originalOutput : originalChildOutputs) {
            ColumnRefOperator unionOutput = columnRefFactory.create(
                    originalOutput.getName(), originalOutput.getType(), originalOutput.isNullable());
            unionOutputColumns.add(unionOutput);
            originalToUnionCols.put(originalOutput, unionOutput);
        }
        ColumnRefOperator actionColumn = columnRefFactory.create(
                IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        unionOutputColumns.add(actionColumn);

        Map<ColumnRefOperator, ScalarOperator> plusProjectMap = Maps.newLinkedHashMap();
        Map<ColumnRefOperator, ScalarOperator> minusProjectMap = Maps.newLinkedHashMap();
        for (int i = 0; i < originalChildOutputs.size(); i++) {
            ColumnRefOperator unionOutput = unionOutputColumns.get(i);
            plusProjectMap.put(unionOutput, toMappedOutputs.get(i));
            minusProjectMap.put(unionOutput, fromMappedOutputs.get(i));
        }
        plusProjectMap.put(actionColumn, ConstantOperator.createTinyInt((byte) 1));
        minusProjectMap.put(actionColumn, ConstantOperator.createTinyInt((byte) -1));

        OptExpression plusProject = OptExpression.create(new LogicalProjectOperator(plusProjectMap), toChild);
        OptExpression minusProject = OptExpression.create(new LogicalProjectOperator(minusProjectMap), fromChild);
        OptExpression toSnapshot = OptExpression.create(
                new LogicalVersionOperator(toVersion, (byte) 1), plusProject);
        OptExpression fromSnapshot = OptExpression.create(
                new LogicalVersionOperator(fromVersion, (byte) -1), minusProject);

        OptExpression deltaInputForPlus = OptExpression.create(new LogicalDeltaOperator(), aggChild);
        LogicalAggregationOperator affectedKeysAggForPlus = new LogicalAggregationOperator(
                AggType.GLOBAL, originalGroupingKeys, Maps.newHashMap());
        OptExpression affectedKeysForPlus = OptExpression.create(affectedKeysAggForPlus, deltaInputForPlus);
        int cteId = context.getCteContext().getNextCteId();
        OptExpression affectedKeysProduce = OptExpression.create(new LogicalCTEProduceOperator(cteId), affectedKeysForPlus);

        Map<ColumnRefOperator, ColumnRefOperator> plusConsumeMap = Maps.newHashMap();
        for (ColumnRefOperator groupKey : originalGroupingKeys) {
            plusConsumeMap.put(groupKey, groupKey);
        }
        OptExpression affectedKeysConsumeForPlus = OptExpression.create(new LogicalCTEConsumeOperator(cteId, plusConsumeMap));

        Map<ColumnRefOperator, ColumnRefOperator> minusConsumeMap = Maps.newHashMap();
        List<ColumnRefOperator> affectedGroupingKeysForMinus = Lists.newArrayListWithCapacity(originalGroupingKeys.size());
        for (ColumnRefOperator groupKey : originalGroupingKeys) {
            ColumnRefOperator minusConsumeKey = columnRefFactory.create(
                    groupKey.getName(), groupKey.getType(), groupKey.isNullable());
            minusConsumeMap.put(minusConsumeKey, groupKey);
            affectedGroupingKeysForMinus.add(minusConsumeKey);
        }
        OptExpression affectedKeysConsumeForMinus = OptExpression.create(new LogicalCTEConsumeOperator(cteId, minusConsumeMap));

        ScalarOperator plusOnPredicate = buildSemiJoinPredicate(originalGroupingKeys, originalGroupingKeys, originalToUnionCols);
        ScalarOperator minusOnPredicate = buildSemiJoinPredicate(
                originalGroupingKeys, affectedGroupingKeysForMinus, originalToUnionCols);
        if (plusOnPredicate == null || minusOnPredicate == null) {
            return List.of();
        }

        OptExpression plusSemiJoin = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, plusOnPredicate),
                toSnapshot,
                affectedKeysConsumeForPlus);
        OptExpression minusSemiJoin = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, minusOnPredicate),
                fromSnapshot,
                affectedKeysConsumeForMinus);

        List<List<ColumnRefOperator>> unionChildrenOutputs = List.of(unionOutputColumns, unionOutputColumns);
        OptExpression semiJoinedUnion = OptExpression.create(
                new LogicalUnionOperator(unionOutputColumns, unionChildrenOutputs, true),
                plusSemiJoin, minusSemiJoin);

        // Align rewritten child slots back to original aggregate input slots so upper operators can keep old slot ids.
        Map<ColumnRefOperator, ScalarOperator> alignedProjectMap = Maps.newHashMap();
        for (ColumnRefOperator originalGroupingKey : originalGroupingKeys) {
            ColumnRefOperator mappedGroupKey = originalToUnionCols.get(originalGroupingKey);
            Preconditions.checkState(mappedGroupKey != null,
                    "Missing mapped group key for %s in IVM delta aggregate rewrite", originalGroupingKey);
            alignedProjectMap.put(originalGroupingKey, mappedGroupKey);
        }
        for (CallOperator call : agg.getAggregations().values()) {
            for (ColumnRefOperator usedColumn : call.getUsedColumns().getColumnRefOperators(columnRefFactory)) {
                if (alignedProjectMap.containsKey(usedColumn)) {
                    continue;
                }
                ColumnRefOperator mappedUsedColumn = originalToUnionCols.get(usedColumn);
                if (mappedUsedColumn != null) {
                    alignedProjectMap.put(usedColumn, mappedUsedColumn);
                }
            }
        }
        alignedProjectMap.put(actionColumn, actionColumn);
        OptExpression alignedInput = OptExpression.create(new LogicalProjectOperator(alignedProjectMap), semiJoinedUnion);

        List<ColumnRefOperator> rewrittenGroupingKeys = Lists.newArrayListWithCapacity(originalGroupingKeys.size() + 1);
        rewrittenGroupingKeys.addAll(originalGroupingKeys);
        rewrittenGroupingKeys.add(actionColumn);
        List<ColumnRefOperator> rewrittenPartitionBys = new ArrayList<>(originalGroupingKeys);

        LogicalAggregationOperator rewrittenAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setGroupingKeys(rewrittenGroupingKeys)
                .setPartitionByColumns(rewrittenPartitionBys)
                .build();
        OptExpression rewrittenAggExpr = OptExpression.create(rewrittenAgg, alignedInput);
        OptExpression cteAnchor = OptExpression.create(new LogicalCTEAnchorOperator(cteId),
                affectedKeysProduce,
                rewrittenAggExpr);
        return List.of(cteAnchor);
    }

    private boolean isSupportedAggregate(LogicalAggregationOperator agg) {
        if (!agg.getType().isGlobal()) {
            return false;
        }
        if (CollectionUtils.isEmpty(agg.getGroupingKeys()) || CollectionUtils.isEmpty(agg.getAggregations().entrySet())) {
            return false;
        }
        if (agg.getPredicate() != null) {
            return false;
        }
        return agg.getAggregations().values().stream().noneMatch(CallOperator::isDistinct);
    }

    private ScalarOperator buildSemiJoinPredicate(List<ColumnRefOperator> leftGroupingKeys,
                                                  List<ColumnRefOperator> rightGroupingKeys,
                                                  Map<ColumnRefOperator, ColumnRefOperator> originalToUnionCols) {
        if (leftGroupingKeys.size() != rightGroupingKeys.size()) {
            return null;
        }
        List<ScalarOperator> joinConjuncts = Lists.newArrayListWithCapacity(leftGroupingKeys.size());
        for (int i = 0; i < leftGroupingKeys.size(); i++) {
            ColumnRefOperator leftKey = leftGroupingKeys.get(i);
            ColumnRefOperator rightKey = rightGroupingKeys.get(i);
            ColumnRefOperator mappedLeft = originalToUnionCols.get(leftKey);
            if (mappedLeft == null) {
                return null;
            }
            joinConjuncts.add(new BinaryPredicateOperator(BinaryType.EQ, mappedLeft, rightKey));
        }
        return Utils.compoundAnd(joinConjuncts);
    }

    private LogicalOlapScanOperator findCandidateOlapScan(OptExpression root) {
        List<LogicalOlapScanOperator> scans = Lists.newArrayList();
        collectOlapScans(root, scans);
        if (scans.isEmpty()) {
            return null;
        }
        return scans.get(0);
    }

    private void collectOlapScans(OptExpression root, List<LogicalOlapScanOperator> scans) {
        if (root.getOp() instanceof LogicalOlapScanOperator scan) {
            scans.add(scan);
        }
        for (OptExpression child : root.getInputs()) {
            collectOlapScans(child, scans);
        }
    }

}
