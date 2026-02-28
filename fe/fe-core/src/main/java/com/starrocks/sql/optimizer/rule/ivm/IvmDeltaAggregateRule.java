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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
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
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IvmDeltaAggregateRule extends TransformationRule {
    public IvmDeltaAggregateRule() {
        super(RuleType.TF_OLAP_IVM_DELTA_AGGREGATE,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.inputAt(0).getOp();
        OptExpression aggChild = input.inputAt(0).inputAt(0);
        if (!isSupportedAggregate(agg)) {
            return List.of();
        }

        OptExpression optimized = tryRewriteByChangesAndMv(context, delta, agg, aggChild);
        if (optimized != null) {
            return List.of(optimized);
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

        OptExpression deltaInputForPlus = OptExpression.create(new LogicalDeltaOperator(false), aggChild);
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

    private OptExpression tryRewriteByChangesAndMv(OptimizerContext context,
                                                   LogicalDeltaOperator delta,
                                                   LogicalAggregationOperator agg, OptExpression aggChild) {
        // This fast path is only valid for the root IVM aggregate refresh on primary-key MV.
        if (!delta.isRootDelta()) {
            return null;
        }
        MaterializedView targetMv = resolveTargetMv(context);
        if (targetMv == null || targetMv.getKeysType() != KeysType.PRIMARY_KEYS) {
            return null;
        }

        // Align aggregate input/grouping columns to the duplicated "changes" subtree.
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        List<ColumnRefOperator> originalChildOutputs = aggChild.getOutputColumns().getColumnRefOperators(columnRefFactory);
        List<ColumnRefOperator> originalGroupingKeys = agg.getGroupingKeys();
        if (originalGroupingKeys.stream().anyMatch(k -> !originalChildOutputs.contains(k))) {
            return null;
        }

        OptExpressionDuplicator childDuplicator = new OptExpressionDuplicator(columnRefFactory, context);
        OptExpression clonedChild = childDuplicator.duplicate(aggChild);
        List<ColumnRefOperator> mappedOutputs = childDuplicator.getMappedColumns(originalChildOutputs);
        Map<ColumnRefOperator, ColumnRefOperator> originalToMapped = Maps.newHashMap();
        for (int i = 0; i < originalChildOutputs.size(); i++) {
            originalToMapped.put(originalChildOutputs.get(i), mappedOutputs.get(i));
        }
        List<ColumnRefOperator> mappedGroupingKeys = Lists.newArrayListWithCapacity(originalGroupingKeys.size());
        for (ColumnRefOperator groupingKey : originalGroupingKeys) {
            ColumnRefOperator mapped = originalToMapped.get(groupingKey);
            if (mapped == null) {
                return null;
            }
            mappedGroupingKeys.add(mapped);
        }

        // Rewrite base scans to CHANGES scans and ensure __ACTION__ is available in the subtree.
        OptExpression changesInput = rewriteOlapScansToChanges(clonedChild);
        if (changesInput == null) {
            return null;
        }
        IvmActionColumnDeriver.Result actionResult = IvmActionColumnDeriver.deriveAndRewrite(changesInput, context);
        if (!actionResult.success()) {
            return null;
        }
        changesInput = actionResult.rewrittenRoot();
        ColumnRefOperator actionColumn = IvmRuleUtils.findActionColumn(changesInput).orElse(null);
        if (actionColumn == null) {
            return null;
        }

        // Build delta aggregate over changes:
        // 1) total row-count delta (sum(action))
        // 2) per-output retractable deltas (COUNT/SUM/AVG supported).
        ColumnRefOperator deltaCount1Ref = columnRefFactory.create("__delta_count1", IntegerType.BIGINT, false);
        Map<ColumnRefOperator, CallOperator> deltaAggCalls = Maps.newHashMap();
        deltaAggCalls.put(deltaCount1Ref, sumCall(IntegerType.BIGINT, actionColumn));
        List<RetractableAggInfo> aggInfos = Lists.newArrayListWithCapacity(agg.getAggregations().size());
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            RetractableAggInfo info = buildRetractableAggInfo(entry.getKey(), entry.getValue(), originalToMapped,
                    actionColumn, deltaCount1Ref, columnRefFactory);
            if (info == null) {
                return null;
            }
            if (info.deltaAggCall != null) {
                deltaAggCalls.put(info.deltaOutputRef, info.deltaAggCall);
            }
            deltaAggCalls.putAll(info.extraDeltaCalls);
            aggInfos.add(info);
        }
        LogicalAggregationOperator deltaAgg = new LogicalAggregationOperator(
                AggType.GLOBAL, mappedGroupingKeys, deltaAggCalls);
        OptExpression deltaAggExpr = OptExpression.create(deltaAgg, changesInput);

        // Scan current MV state and bind visible/hidden state columns for each aggregate output.
        MvScanInfo mvScanInfo = buildMvScan(
                targetMv, originalGroupingKeys, aggInfos, columnRefFactory, delta.getMvColumnMapping());
        if (mvScanInfo == null) {
            return null;
        }
        // Merge "delta state" with current MV state on grouping keys.
        ScalarOperator joinOn = buildJoinOnByKeys(mappedGroupingKeys, mvScanInfo.groupingKeyRefs);
        if (joinOn == null) {
            return null;
        }
        OptExpression mergeJoin = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, joinOn),
                deltaAggExpr,
                mvScanInfo.scanExpr);

        Map<ColumnRefOperator, ScalarOperator> deleteProjectMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> insertProjectMap = Maps.newHashMap();
        for (int i = 0; i < originalGroupingKeys.size(); i++) {
            deleteProjectMap.put(originalGroupingKeys.get(i), mappedGroupingKeys.get(i));
            insertProjectMap.put(originalGroupingKeys.get(i), mappedGroupingKeys.get(i));
        }

        // Materialize old/new visible values and hidden states:
        // - delete branch emits old state with action=-1 when old row exists
        // - insert branch emits new state with action=+1 when new row exists
        List<ColumnRefOperator> hiddenOutputRefs = Lists.newArrayList();
        for (RetractableAggInfo info : aggInfos) {
            ScalarOperator oldVisible = info.computeOldVisible(mvScanInfo, columnRefFactory);
            ScalarOperator newVisible = info.computeNewVisible(mvScanInfo, columnRefFactory);
            deleteProjectMap.put(info.outputRef, oldVisible);
            insertProjectMap.put(info.outputRef, newVisible);
            for (HiddenStateRef hiddenStateRef : info.hiddenStateRefs) {
                hiddenOutputRefs.add(hiddenStateRef.outputRef);
                deleteProjectMap.put(hiddenStateRef.outputRef, hiddenStateRef.oldExpr);
                insertProjectMap.put(hiddenStateRef.outputRef, hiddenStateRef.newExpr);
            }
        }

        ColumnRefOperator finalActionColumn = columnRefFactory.create(
                IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        deleteProjectMap.put(finalActionColumn, ConstantOperator.createTinyInt((byte) -1));
        insertProjectMap.put(finalActionColumn, ConstantOperator.createTinyInt((byte) 1));

        ScalarOperator oldExists = new BinaryPredicateOperator(BinaryType.GT,
                coalesceZero(mvScanInfo.totalCountRef), ConstantOperator.createBigint(0));
        ScalarOperator newExists = new BinaryPredicateOperator(BinaryType.GT,
                addOperator(coalesceZero(mvScanInfo.totalCountRef), deltaCount1Ref, IntegerType.BIGINT),
                ConstantOperator.createBigint(0));

        // Build DELETE and INSERT change streams, then union them as final MV changes.
        // Keep mergeJoin under one CTE producer so two branches consume the same affected rows snapshot.
        Set<ColumnRefOperator> consumeRefs = new HashSet<>();
        for (ScalarOperator expr : deleteProjectMap.values()) {
            consumeRefs.addAll(expr.getUsedColumns().getColumnRefOperators(columnRefFactory));
        }
        for (ScalarOperator expr : insertProjectMap.values()) {
            consumeRefs.addAll(expr.getUsedColumns().getColumnRefOperators(columnRefFactory));
        }
        consumeRefs.addAll(oldExists.getUsedColumns().getColumnRefOperators(columnRefFactory));
        consumeRefs.addAll(newExists.getUsedColumns().getColumnRefOperators(columnRefFactory));

        int mergeCteId = context.getCteContext().getNextCteId();
        OptExpression mergeProduce = OptExpression.create(new LogicalCTEProduceOperator(mergeCteId), mergeJoin);
        Map<ColumnRefOperator, ColumnRefOperator> deleteConsumeMap = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> insertConsumeMap = Maps.newHashMap();
        for (ColumnRefOperator ref : consumeRefs) {
            deleteConsumeMap.put(ref, ref);
            insertConsumeMap.put(ref, ref);
        }
        OptExpression deleteConsume = OptExpression.create(new LogicalCTEConsumeOperator(mergeCteId, deleteConsumeMap));
        OptExpression insertConsume = OptExpression.create(new LogicalCTEConsumeOperator(mergeCteId, insertConsumeMap));

        OptExpression deleteBranch = OptExpression.create(new LogicalProjectOperator(deleteProjectMap),
                OptExpression.create(new LogicalFilterOperator(oldExists), deleteConsume));
        OptExpression insertBranch = OptExpression.create(new LogicalProjectOperator(insertProjectMap),
                OptExpression.create(new LogicalFilterOperator(newExists), insertConsume));

        List<ColumnRefOperator> unionOutputs = Lists.newArrayList();
        unionOutputs.addAll(originalGroupingKeys);
        unionOutputs.addAll(agg.getAggregations().keySet());
        unionOutputs.addAll(hiddenOutputRefs);
        unionOutputs.add(finalActionColumn);
        List<List<ColumnRefOperator>> childOutputCols = List.of(unionOutputs, unionOutputs);
        OptExpression unionChanges = OptExpression.create(new LogicalUnionOperator(unionOutputs, childOutputCols, true),
                deleteBranch, insertBranch);
        return OptExpression.create(new LogicalCTEAnchorOperator(mergeCteId), mergeProduce, unionChanges);
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

    private ScalarOperator buildJoinOnByKeys(List<ColumnRefOperator> leftKeys, List<ColumnRefOperator> rightKeys) {
        if (leftKeys.size() != rightKeys.size()) {
            return null;
        }
        List<ScalarOperator> predicates = Lists.newArrayListWithCapacity(leftKeys.size());
        for (int i = 0; i < leftKeys.size(); i++) {
            predicates.add(new BinaryPredicateOperator(BinaryType.EQ, leftKeys.get(i), rightKeys.get(i)));
        }
        return Utils.compoundAnd(predicates);
    }

    private OptExpression rewriteOlapScansToChanges(OptExpression root) {
        if (root.getOp() instanceof LogicalOlapScanOperator scan) {
            if (!(scan.getTable() instanceof OlapTable olapTable)) {
                return null;
            }
            Long fromVersion = scan.getTableVersion();
            if (fromVersion == null) {
                return null;
            }
            long toVersion = IvmRuleUtils.getLatestVisibleVersion(olapTable);
            if (toVersion <= fromVersion) {
                return null;
            }
            LogicalOlapScanOperator rewrittenScan = LogicalOlapScanOperator.builder()
                    .withOperator(scan)
                    .setTableVersion(null)
                    .setChangesVersionRange(fromVersion, toVersion)
                    .build();
            return OptExpression.create(rewrittenScan);
        }

        if (root.getInputs().isEmpty()) {
            return root;
        }
        List<OptExpression> rewrittenChildren = Lists.newArrayListWithCapacity(root.getInputs().size());
        for (OptExpression child : root.getInputs()) {
            OptExpression rewrittenChild = rewriteOlapScansToChanges(child);
            if (rewrittenChild == null) {
                return null;
            }
            rewrittenChildren.add(rewrittenChild);
        }
        return OptExpression.create(root.getOp(), root.getTvrMeta(), rewrittenChildren);
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

    private MaterializedView resolveTargetMv(OptimizerContext context) {
        if (!(context.getStatement() instanceof InsertStmt insertStmt)) {
            return null;
        }
        if (!insertStmt.isSystem() || !(insertStmt.getTargetTable() instanceof MaterializedView targetMv)) {
            return null;
        }
        return targetMv;
    }

    private RetractableAggInfo buildRetractableAggInfo(ColumnRefOperator outputRef, CallOperator call,
                                                       Map<ColumnRefOperator, ColumnRefOperator> originalToMapped,
                                                       ColumnRefOperator actionColumn,
                                                       ColumnRefOperator deltaCount1Ref,
                                                       ColumnRefFactory columnRefFactory) {
        String fnName = call.getFnName().toLowerCase();
        if (call.isDistinct()) {
            return null;
        }
        if (FunctionSet.COUNT.equals(fnName)) {
            if (isCountStarOrOne(call)) {
                RetractableAggInfo info = new RetractableAggInfo(outputRef, AggKind.COUNT_ONE, deltaCount1Ref,
                        null);
                info.totalCountDeltaRef = deltaCount1Ref;
                return info;
            }
            if (call.getArguments().size() != 1 || !(call.getChild(0) instanceof ColumnRefOperator arg)) {
                return null;
            }
            ColumnRefOperator mappedArg = originalToMapped.get(arg);
            if (mappedArg == null) {
                return null;
            }
            ColumnRefOperator deltaRef = columnRefFactory.create(
                    "__delta_" + outputRef.getName(), IntegerType.BIGINT, false);
            ScalarOperator countExpr = nullToZero(mappedArg, actionColumn, IntegerType.BIGINT);
            RetractableAggInfo info = new RetractableAggInfo(outputRef, AggKind.COUNT_COLUMN, deltaRef,
                    sumCall(IntegerType.BIGINT, countExpr));
            info.totalCountDeltaRef = deltaCount1Ref;
            return info;
        }
        if (FunctionSet.SUM.equals(fnName)) {
            if (call.getArguments().size() != 1 || !(call.getChild(0) instanceof ColumnRefOperator arg)) {
                return null;
            }
            ColumnRefOperator mappedArg = originalToMapped.get(arg);
            if (mappedArg == null) {
                return null;
            }
            ColumnRefOperator deltaRef = columnRefFactory.create(
                    "__delta_" + outputRef.getName(), outputRef.getType(), true);
            ScalarOperator scaled = nullToZero(mappedArg,
                    multiplyOperator(mappedArg, actionColumn, outputRef.getType()), outputRef.getType());
            RetractableAggInfo info = new RetractableAggInfo(outputRef, AggKind.SUM_COLUMN, deltaRef,
                    sumCall(outputRef.getType(), scaled));
            info.totalCountDeltaRef = deltaCount1Ref;
            return info;
        }
        if (FunctionSet.AVG.equals(fnName)) {
            if (call.getArguments().size() != 1 || !(call.getChild(0) instanceof ColumnRefOperator arg)) {
                return null;
            }
            ColumnRefOperator mappedArg = originalToMapped.get(arg);
            if (mappedArg == null) {
                return null;
            }
            ColumnRefOperator deltaRef = columnRefFactory.create(
                    "__delta_" + outputRef.getName(), outputRef.getType(), true);
            ColumnRefOperator deltaSumRef = columnRefFactory.create(
                    "__delta_sum_" + outputRef.getName(), outputRef.getType(), true);
            ColumnRefOperator deltaCountRef = columnRefFactory.create(
                    "__delta_count_" + outputRef.getName(), IntegerType.BIGINT, false);
            ScalarOperator scaled = nullToZero(mappedArg,
                    multiplyOperator(mappedArg, actionColumn, outputRef.getType()), outputRef.getType());
            RetractableAggInfo info = new RetractableAggInfo(outputRef, AggKind.AVG_COLUMN, deltaRef,
                    sumCall(outputRef.getType(), scaled));
            info.avgDeltaSumRef = deltaSumRef;
            info.avgDeltaCountRef = deltaCountRef;
            info.totalCountDeltaRef = deltaCount1Ref;
            info.extraDeltaCalls.put(deltaSumRef, sumCall(outputRef.getType(), scaled));
            info.extraDeltaCalls.put(deltaCountRef, sumCall(IntegerType.BIGINT, nullToZero(mappedArg, actionColumn,
                    IntegerType.BIGINT)));
            return info;
        }
        return null;
    }

    private MvScanInfo buildMvScan(MaterializedView targetMv, List<ColumnRefOperator> groupingKeys,
                                   List<RetractableAggInfo> infos, ColumnRefFactory columnRefFactory,
                                   Map<ColumnRefOperator, Column> mvColumnMapping) {
        Map<String, Column> mvColumnByName = Maps.newHashMap();
        for (Column column : targetMv.getFullSchema()) {
            mvColumnByName.put(column.getName().toLowerCase(), column);
        }

        Map<ColumnRefOperator, Column> colRefToMeta = Maps.newHashMap();
        Map<Column, ColumnRefOperator> metaToColRef = Maps.newHashMap();
        List<ColumnRefOperator> groupKeyRefs = Lists.newArrayListWithCapacity(groupingKeys.size());
        for (ColumnRefOperator groupingKey : groupingKeys) {
            Column mvColumn = mvColumnMapping.get(groupingKey);
            if (mvColumn == null) {
                return null;
            }
            ColumnRefOperator ref = createScanColumnRef(columnRefFactory, targetMv, mvColumn, colRefToMeta, metaToColRef);
            groupKeyRefs.add(ref);
        }

        Column totalCountColumn = null;
        for (RetractableAggInfo info : infos) {
            Column mvColumn = mvColumnMapping.get(info.outputRef);
            if (mvColumn == null) {
                return null;
            }
            info.mvVisibleRef = createScanColumnRef(columnRefFactory, targetMv, mvColumn, colRefToMeta, metaToColRef);
            String visibleName = mvColumn.getName();

            if (info.kind == AggKind.COUNT_COLUMN || info.kind == AggKind.SUM_COLUMN || info.kind == AggKind.AVG_COLUMN) {
                Column cnt1 = mvColumnByName.get(IvmRuleUtils.count1StateColumnName(visibleName).toLowerCase());
                if (cnt1 == null) {
                    return null;
                }
                info.mvCount1StateRef = createScanColumnRef(columnRefFactory, targetMv, cnt1, colRefToMeta, metaToColRef);
                if (totalCountColumn == null) {
                    totalCountColumn = cnt1;
                }
            }
            if (info.kind == AggKind.AVG_COLUMN) {
                Column sum = mvColumnByName.get(IvmRuleUtils.sumStateColumnName(visibleName).toLowerCase());
                Column count = mvColumnByName.get(IvmRuleUtils.countStateColumnName(visibleName).toLowerCase());
                if (sum == null || count == null) {
                    return null;
                }
                info.mvSumStateRef = createScanColumnRef(columnRefFactory, targetMv, sum, colRefToMeta, metaToColRef);
                info.mvCountStateRef = createScanColumnRef(columnRefFactory, targetMv, count, colRefToMeta, metaToColRef);
            }
            if (info.kind == AggKind.COUNT_ONE && totalCountColumn == null) {
                totalCountColumn = mvColumn;
            }
        }
        if (totalCountColumn == null) {
            return null;
        }

        ColumnRefOperator totalCountRef = createScanColumnRef(columnRefFactory, targetMv, totalCountColumn,
                colRefToMeta, metaToColRef);
        LogicalOlapScanOperator scan = new LogicalOlapScanOperator(targetMv, colRefToMeta, metaToColRef, null,
                Operator.DEFAULT_LIMIT, null, targetMv.getBaseIndexMetaId(), targetMv.getAllPartitionIds(), null,
                false, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(), false, null);
        return new MvScanInfo(OptExpression.create(scan), groupKeyRefs, totalCountRef);
    }

    private ColumnRefOperator createScanColumnRef(ColumnRefFactory factory, MaterializedView targetMv, Column column,
                                                  Map<ColumnRefOperator, Column> colRefToMeta,
                                                  Map<Column, ColumnRefOperator> metaToColRef) {
        if (metaToColRef.containsKey(column)) {
            return metaToColRef.get(column);
        }
        ColumnRefOperator ref = factory.create(column.getName(), column.getType(), column.isAllowNull());
        factory.updateColumnRefToColumns(ref, column, targetMv);
        colRefToMeta.put(ref, column);
        metaToColRef.put(column, ref);
        return ref;
    }

    private static boolean isCountStarOrOne(CallOperator call) {
        if (call.getArguments().isEmpty()) {
            return true;
        }
        if (call.getArguments().size() != 1 || !(call.getChild(0) instanceof ConstantOperator constant)) {
            return false;
        }
        Optional<ConstantOperator> asBigint = constant.castTo(IntegerType.BIGINT);
        return asBigint.isPresent() && !asBigint.get().isNull() && asBigint.get().getBigint() == 1L;
    }

    private static CallOperator sumCall(Type returnType, ScalarOperator arg) {
        return createBuiltinCall(FunctionSet.SUM, returnType, List.of(arg));
    }

    private static ScalarOperator coalesceZero(ScalarOperator input) {
        return new com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator(input.getType(), null,
                input, List.of(new IsNullPredicateOperator(input), zeroConstant(input.getType())));
    }

    private static ScalarOperator nullToZero(ScalarOperator nullableExpr, ScalarOperator nonNullExpr, Type type) {
        return new com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator(type, null,
                nonNullExpr, List.of(new IsNullPredicateOperator(nullableExpr), zeroConstant(type)));
    }

    private static ScalarOperator addOperator(ScalarOperator left, ScalarOperator right, Type type) {
        return createBuiltinCall(FunctionSet.ADD, type, List.of(left, right));
    }

    private static ScalarOperator multiplyOperator(ScalarOperator left, ScalarOperator right, Type type) {
        return createBuiltinCall(FunctionSet.MULTIPLY, type, List.of(left, right));
    }

    private static ScalarOperator divideOperator(ScalarOperator left, ScalarOperator right, Type type) {
        return createBuiltinCall(FunctionSet.DIVIDE, type, List.of(left, right));
    }

    private static CallOperator createBuiltinCall(String fnName, Type returnType, List<ScalarOperator> args) {
        Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = ExprUtils.getBuiltinFunction(fnName, argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            return new CallOperator(fnName, returnType, args);
        }
        Function copied = fn.copy();
        copied = copied.updateArgType(argTypes);
        copied.setRetType(returnType);
        return new CallOperator(fnName, returnType, args, copied);
    }

    private static ConstantOperator zeroConstant(Type type) {
        Optional<ConstantOperator> casted = ConstantOperator.createBigint(0L).castTo(type);
        if (casted.isPresent()) {
            return casted.get();
        }
        return ConstantOperator.createInt(0);
    }

    private enum AggKind {
        COUNT_ONE,
        COUNT_COLUMN,
        SUM_COLUMN,
        AVG_COLUMN
    }

    private static final class HiddenStateRef {
        private final ColumnRefOperator outputRef;
        private final ScalarOperator oldExpr;
        private final ScalarOperator newExpr;

        private HiddenStateRef(ColumnRefOperator outputRef, ScalarOperator oldExpr, ScalarOperator newExpr) {
            this.outputRef = outputRef;
            this.oldExpr = oldExpr;
            this.newExpr = newExpr;
        }
    }

    private static final class RetractableAggInfo {
        private final ColumnRefOperator outputRef;
        private final AggKind kind;
        private final ColumnRefOperator deltaOutputRef;
        private final CallOperator deltaAggCall;
        private final Map<ColumnRefOperator, CallOperator> extraDeltaCalls = Maps.newHashMap();
        private ColumnRefOperator totalCountDeltaRef;
        private ColumnRefOperator avgDeltaSumRef;
        private ColumnRefOperator avgDeltaCountRef;
        private ColumnRefOperator mvVisibleRef;
        private ColumnRefOperator mvCount1StateRef;
        private ColumnRefOperator mvSumStateRef;
        private ColumnRefOperator mvCountStateRef;
        private final List<HiddenStateRef> hiddenStateRefs = Lists.newArrayList();

        private RetractableAggInfo(ColumnRefOperator outputRef, AggKind kind, ColumnRefOperator deltaOutputRef,
                                   CallOperator deltaAggCall) {
            this.outputRef = outputRef;
            this.kind = kind;
            this.deltaOutputRef = deltaOutputRef;
            this.deltaAggCall = deltaAggCall;
        }

        private ScalarOperator computeOldVisible(MvScanInfo scanInfo, ColumnRefFactory factory) {
            return switch (kind) {
                case COUNT_ONE -> coalesceZero(mvVisibleRef);
                case COUNT_COLUMN -> {
                    ScalarOperator oldCnt1 = coalesceZero(mvCount1StateRef);
                    ScalarOperator oldCntCol = coalesceZero(mvVisibleRef);
                    ColumnRefOperator stateRef = factory.create(IvmRuleUtils.count1StateColumnName(outputRef.getName()),
                            IntegerType.BIGINT, false);
                    hiddenStateRefs.add(new HiddenStateRef(stateRef, oldCnt1,
                            addOperator(oldCnt1, totalCountDeltaRef, IntegerType.BIGINT)));
                    yield oldCntCol;
                }
                case SUM_COLUMN -> {
                    ScalarOperator oldCnt1 = coalesceZero(mvCount1StateRef);
                    ScalarOperator oldSum = coalesceZero(mvVisibleRef);
                    ColumnRefOperator stateRef = factory.create(IvmRuleUtils.count1StateColumnName(outputRef.getName()),
                            IntegerType.BIGINT, false);
                    hiddenStateRefs.add(new HiddenStateRef(stateRef, oldCnt1,
                            addOperator(oldCnt1, totalCountDeltaRef, IntegerType.BIGINT)));
                    yield oldSum;
                }
                case AVG_COLUMN -> {
                    ScalarOperator oldCnt1 = coalesceZero(mvCount1StateRef);
                    ScalarOperator oldSum = coalesceZero(mvSumStateRef);
                    ScalarOperator oldCnt = coalesceZero(mvCountStateRef);
                    ScalarOperator oldAvg = new com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator(
                            outputRef.getType(), null, divideOperator(oldSum, oldCnt, outputRef.getType()),
                            List.of(new BinaryPredicateOperator(BinaryType.LE, oldCnt, ConstantOperator.createBigint(0L)),
                                    ConstantOperator.createNull(outputRef.getType())));
                    ScalarOperator newCnt1 = addOperator(oldCnt1, totalCountDeltaRef, IntegerType.BIGINT);
                    ScalarOperator newSum = addOperator(oldSum, avgDeltaSumRef, outputRef.getType());
                    ScalarOperator newCnt = addOperator(oldCnt, avgDeltaCountRef, IntegerType.BIGINT);
                    ScalarOperator newAvg = new com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator(
                            outputRef.getType(), null, divideOperator(newSum, newCnt, outputRef.getType()),
                            List.of(new BinaryPredicateOperator(BinaryType.LE, newCnt, ConstantOperator.createBigint(0L)),
                                    ConstantOperator.createNull(outputRef.getType())));
                    hiddenStateRefs.add(new HiddenStateRef(factory.create(IvmRuleUtils.count1StateColumnName(outputRef.getName()),
                            IntegerType.BIGINT, false), oldCnt1, newCnt1));
                    hiddenStateRefs.add(new HiddenStateRef(factory.create(IvmRuleUtils.sumStateColumnName(outputRef.getName()),
                            outputRef.getType(), true), oldSum, newSum));
                    hiddenStateRefs.add(new HiddenStateRef(factory.create(IvmRuleUtils.countStateColumnName(outputRef.getName()),
                            IntegerType.BIGINT, false), oldCnt, newCnt));
                    this.cachedNewVisible = newAvg;
                    yield oldAvg;
                }
            };
        }

        private ScalarOperator cachedNewVisible;

        private ScalarOperator computeNewVisible(MvScanInfo scanInfo, ColumnRefFactory factory) {
            if (cachedNewVisible != null) {
                return cachedNewVisible;
            }
            return switch (kind) {
                case COUNT_ONE -> addOperator(coalesceZero(mvVisibleRef), deltaOutputRef, IntegerType.BIGINT);
                case COUNT_COLUMN -> addOperator(coalesceZero(mvVisibleRef), deltaOutputRef, IntegerType.BIGINT);
                case SUM_COLUMN -> addOperator(coalesceZero(mvVisibleRef), deltaOutputRef, outputRef.getType());
                case AVG_COLUMN -> cachedNewVisible;
            };
        }
    }

    private static final class MvScanInfo {
        private final OptExpression scanExpr;
        private final List<ColumnRefOperator> groupingKeyRefs;
        private final ColumnRefOperator totalCountRef;

        private MvScanInfo(OptExpression scanExpr, List<ColumnRefOperator> groupingKeyRefs, ColumnRefOperator totalCountRef) {
            this.scanExpr = scanExpr;
            this.groupingKeyRefs = groupingKeyRefs;
            this.totalCountRef = totalCountRef;
        }
    }

}
