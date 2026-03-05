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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRowIdContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IvmRowIdDeriver {
    public record Result(boolean success, OptExpression rewrittenRoot, String unsupportedReason,
                         List<ColumnRefOperator> rootRowIdColumnRefs) {
    }

    private IvmRowIdDeriver() {
    }

    public static Result deriveAndRewrite(OptExpression root, OptimizerContext optimizerContext) {
        IvmRowIdContext context = new IvmRowIdContext(optimizerContext.getColumnRefFactory());

        root.getOp().accept(new CollectorVisitor(context), root, null);
        if (!context.isSupported()) {
            return new Result(false, root, context.getUnsupportedReason().orElse("row-id derive failed"), List.of());
        }

        OptExpression rewritten = root.getOp().accept(new RewriteVisitor(context), root, null);
        List<ColumnRefOperator> rootRowIds = context.getRowIds(root).orElse(List.of());
        return new Result(true, rewritten, null, rootRowIds);
    }

    private static class CollectorVisitor extends OptExpressionVisitor<Void, Void> {
        private final IvmRowIdContext context;

        private CollectorVisitor(IvmRowIdContext context) {
            this.context = context;
        }

        @Override
        public Void visit(OptExpression expression, Void context) {
            collectChildren(expression);
            if (!this.context.isSupported()) {
                return null;
            }
            this.context.markUnsupported("unsupported operator for OLAP IVM row-id derive: "
                    + expression.getOp().getOpType());
            return null;
        }

        @Override
        public Void visitLogicalTableScan(OptExpression expression, Void context) {
            if (!(expression.getOp() instanceof LogicalOlapScanOperator scan)) {
                this.context.markUnsupported("only OlapScanOperator is supported in OLAP IVM row-id derive");
                return null;
            }
            if (!(scan.getTable() instanceof OlapTable table)) {
                this.context.markUnsupported("only OlapTable is supported in OLAP IVM row-id derive");
                return null;
            }
            if (table.getKeysType() != KeysType.PRIMARY_KEYS) {
                this.context.markUnsupported("only PRIMARY KEY table is supported in OLAP IVM row-id derive");
                return null;
            }
            List<Column> keyColumns = table.getKeyColumnsInOrder();
            if (keyColumns.isEmpty()) {
                this.context.markUnsupported("primary key column is missing in OLAP IVM row-id derive");
                return null;
            }

            List<ColumnRefOperator> rowIds = keyColumns.stream()
                    .map(col -> getOrCreateKeyRef(scan, col))
                    .collect(Collectors.toList());
            this.context.putRowIds(expression, rowIds);
            return null;
        }

        @Override
        public Void visitLogicalFilter(OptExpression expression, Void context) {
            collectChildren(expression);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("filter must be unary in OLAP IVM row-id derive");
                return null;
            }
            List<ColumnRefOperator> childRowIds = this.context.getRowIds(expression.inputAt(0)).orElse(null);
            if (childRowIds == null || childRowIds.isEmpty()) {
                this.context.markUnsupported("filter child row-id is missing in OLAP IVM row-id derive");
                return null;
            }

            LogicalFilterOperator filter = (LogicalFilterOperator) expression.getOp();
            if (filter.getProjection() == null) {
                this.context.putRowIds(expression, childRowIds);
                return null;
            }

            List<ColumnRefOperator> outputRowIds =
                    mapRowIdsThroughProjection(filter.getProjection().getColumnRefMap(), childRowIds);
            this.context.putRowIds(expression, outputRowIds);
            return null;
        }

        @Override
        public Void visitLogicalProject(OptExpression expression, Void context) {
            collectChildren(expression);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("project must be unary in OLAP IVM row-id derive");
                return null;
            }
            List<ColumnRefOperator> childRowIds = this.context.getRowIds(expression.inputAt(0)).orElse(null);
            if (childRowIds == null || childRowIds.isEmpty()) {
                this.context.markUnsupported("project child row-id is missing in OLAP IVM row-id derive");
                return null;
            }

            LogicalProjectOperator project = (LogicalProjectOperator) expression.getOp();
            List<ColumnRefOperator> outputRowIds = mapRowIdsThroughProjection(project.getColumnRefMap(), childRowIds);
            this.context.putRowIds(expression, outputRowIds);
            return null;
        }

        @Override
        public Void visitLogicalAggregate(OptExpression expression, Void context) {
            collectChildren(expression);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("aggregate must be unary in OLAP IVM row-id derive");
                return null;
            }
            LogicalAggregationOperator agg = (LogicalAggregationOperator) expression.getOp();
            if (agg.getGroupingKeys().isEmpty()) {
                this.context.markUnsupported("aggregate without group by is not supported in OLAP IVM row-id derive");
                return null;
            }
            this.context.putRowIds(expression, agg.getGroupingKeys());
            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression expression, Void context) {
            collectChildren(expression);
            if (expression.getInputs().size() != 2) {
                this.context.markUnsupported("join must be binary in OLAP IVM row-id derive");
                return null;
            }

            LogicalJoinOperator join = (LogicalJoinOperator) expression.getOp();
            JoinOperator joinType = join.getJoinType();
            if (!joinType.isInnerJoin() && !joinType.isCrossJoin()
                    && !joinType.isLeftOuterJoin() && !joinType.isLeftAntiJoin() && !joinType.isLeftSemiJoin()) {
                this.context.markUnsupported(
                        "only inner/cross/left outer/left semi/left anti join is supported in OLAP IVM row-id derive");
                return null;
            }

            List<ColumnRefOperator> leftRowIds = this.context.getRowIds(expression.inputAt(0)).orElse(null);
            List<ColumnRefOperator> rightRowIds = this.context.getRowIds(expression.inputAt(1)).orElse(null);
            if (leftRowIds == null || leftRowIds.isEmpty() || rightRowIds == null || rightRowIds.isEmpty()) {
                this.context.markUnsupported("join child row-id is missing in OLAP IVM row-id derive");
                return null;
            }

            List<ColumnRefOperator> inputRowIds = getJoinInputRowIds(joinType, leftRowIds, rightRowIds);
            if (join.getProjection() == null) {
                this.context.putRowIds(expression, inputRowIds);
                return null;
            }

            List<ColumnRefOperator> outputRowIds =
                    mapRowIdsThroughProjection(join.getProjection().getColumnRefMap(), inputRowIds);
            this.context.putRowIds(expression, outputRowIds);
            return null;
        }

        private List<ColumnRefOperator> getJoinInputRowIds(JoinOperator joinType,
                                                            List<ColumnRefOperator> leftRowIds,
                                                            List<ColumnRefOperator> rightRowIds) {
            if (joinType.isLeftAntiJoin() || joinType.isLeftSemiJoin()) {
                return Lists.newArrayList(leftRowIds);
            }
            List<ColumnRefOperator> inputRowIds = Lists.newArrayListWithCapacity(leftRowIds.size() + rightRowIds.size());
            inputRowIds.addAll(leftRowIds);
            inputRowIds.addAll(rightRowIds);
            return inputRowIds;
        }

        private List<ColumnRefOperator> mapRowIdsThroughProjection(
                Map<ColumnRefOperator, ScalarOperator> projectionMap,
                List<ColumnRefOperator> inputRowIds) {
            List<ColumnRefOperator> outputRowIds = new ArrayList<>(inputRowIds.size());
            for (int i = 0; i < inputRowIds.size(); i++) {
                ColumnRefOperator input = inputRowIds.get(i);
                final int idx = i;
                ColumnRefOperator output = findOutputRef(projectionMap, input).orElseGet(
                        () -> this.context.getColumnRefFactory().create(
                                "__row_id_" + idx, input.getType(), input.isNullable()));
                outputRowIds.add(output);
            }
            return outputRowIds;
        }

        private ColumnRefOperator getOrCreateKeyRef(LogicalOlapScanOperator scan, Column keyColumn) {
            ColumnRefOperator keyRef = scan.getColumnReference(keyColumn);
            if (keyRef != null) {
                return keyRef;
            }
            return this.context.getColumnRefFactory()
                    .create(keyColumn.getName(), keyColumn.getType(), keyColumn.isAllowNull());
        }

        private void collectChildren(OptExpression expression) {
            for (OptExpression child : expression.getInputs()) {
                child.getOp().accept(this, child, null);
                if (!context.isSupported()) {
                    return;
                }
            }
        }

        private Optional<ColumnRefOperator> findOutputRef(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                                          ColumnRefOperator input) {
            return columnRefMap.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(input))
                    .map(Map.Entry::getKey)
                    .findFirst();
        }
    }

    private static class RewriteVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final IvmRowIdContext context;

        private RewriteVisitor(IvmRowIdContext context) {
            this.context = context;
        }

        @Override
        public OptExpression visit(OptExpression expression, Void context) {
            return expression;
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression expression, Void context) {
            LogicalOlapScanOperator scan = (LogicalOlapScanOperator) expression.getOp();
            List<ColumnRefOperator> rowIds = this.context.getRowIds(expression).orElse(null);
            if (rowIds == null || rowIds.isEmpty()) {
                return expression;
            }
            if (!(scan.getTable() instanceof OlapTable table)) {
                return expression;
            }
            List<Column> keyColumns = table.getKeyColumnsInOrder();
            if (keyColumns.isEmpty() || keyColumns.size() != rowIds.size()) {
                return expression;
            }

            Map<ColumnRefOperator, Column> newColRefToMeta = Maps.newHashMap(scan.getColRefToColumnMetaMap());
            Map<Column, ColumnRefOperator> newMetaToColRef = Maps.newHashMap(scan.getColumnMetaToColRefMap());
            boolean scanChanged = false;
            for (int i = 0; i < keyColumns.size(); i++) {
                Column keyColumn = keyColumns.get(i);
                ColumnRefOperator rowId = rowIds.get(i);
                if (!newColRefToMeta.containsKey(rowId)) {
                    newColRefToMeta.put(rowId, keyColumn);
                    scanChanged = true;
                }
                ColumnRefOperator existing = newMetaToColRef.get(keyColumn);
                if (existing == null || !existing.equals(rowId)) {
                    newMetaToColRef.put(keyColumn, rowId);
                    scanChanged = true;
                }
            }

            boolean projectionChanged = false;
            Projection newProjection = scan.getProjection();
            if (scan.getProjection() != null) {
                Map<ColumnRefOperator, ScalarOperator> projectionMap =
                        Maps.newHashMap(scan.getProjection().getColumnRefMap());
                for (ColumnRefOperator rowId : rowIds) {
                    if (!projectionMap.containsKey(rowId)) {
                        projectionMap.put(rowId, rowId);
                        projectionChanged = true;
                    }
                }
                if (projectionChanged) {
                    newProjection = new Projection(
                            projectionMap,
                            Maps.newHashMap(scan.getProjection().getCommonSubOperatorMap()));
                }
            }

            if (!scanChanged && !projectionChanged) {
                return expression;
            }

            LogicalOlapScanOperator.Builder builder = LogicalOlapScanOperator.builder()
                    .withOperator(scan)
                    .setColRefToColumnMetaMap(newColRefToMeta)
                    .setColumnMetaToColRefMap(newMetaToColRef);
            if (projectionChanged) {
                builder.setProjection(newProjection);
            }
            return OptExpression.create(builder.build());
        }

        @Override
        public OptExpression visitLogicalFilter(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);
            LogicalFilterOperator filter = (LogicalFilterOperator) expression.getOp();
            if (filter.getProjection() == null) {
                if (rewrittenChild == child) {
                    return expression;
                }
                return OptExpression.create(filter, rewrittenChild);
            }

            List<ColumnRefOperator> rowIds = this.context.getRowIds(expression).orElse(null);
            List<ColumnRefOperator> childRowIds = this.context.getRowIds(child).orElse(null);
            if (rowIds == null || childRowIds == null || rowIds.size() != childRowIds.size()) {
                if (rewrittenChild == child) {
                    return expression;
                }
                return OptExpression.create(filter, rewrittenChild);
            }

            Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap(filter.getProjection().getColumnRefMap());
            boolean projectionChanged = false;
            for (int i = 0; i < rowIds.size(); i++) {
                ColumnRefOperator rowId = rowIds.get(i);
                if (!projectionMap.containsKey(rowId)) {
                    projectionMap.put(rowId, childRowIds.get(i));
                    projectionChanged = true;
                }
            }
            if (!projectionChanged && rewrittenChild == child) {
                return expression;
            }

            LogicalFilterOperator newFilter = new LogicalFilterOperator.Builder()
                    .withOperator(filter)
                    .setProjection(new Projection(
                            projectionMap,
                            Maps.newHashMap(filter.getProjection().getCommonSubOperatorMap())))
                    .build();
            return OptExpression.create(newFilter, rewrittenChild);
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);
            LogicalProjectOperator project = (LogicalProjectOperator) expression.getOp();

            List<ColumnRefOperator> rowIds = this.context.getRowIds(expression).orElse(null);
            List<ColumnRefOperator> childRowIds = this.context.getRowIds(child).orElse(null);
            if (rowIds == null || childRowIds == null || rowIds.size() != childRowIds.size()) {
                if (rewrittenChild == child) {
                    return expression;
                }
                return OptExpression.create(project, rewrittenChild);
            }

            Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap(project.getColumnRefMap());
            boolean projectChanged = false;
            for (int i = 0; i < rowIds.size(); i++) {
                ColumnRefOperator rowId = rowIds.get(i);
                if (!projectMap.containsKey(rowId)) {
                    projectMap.put(rowId, childRowIds.get(i));
                    projectChanged = true;
                }
            }
            if (!projectChanged && rewrittenChild == child) {
                return expression;
            }
            LogicalProjectOperator newProject = LogicalProjectOperator.builder()
                    .withOperator(project)
                    .setColumnRefMap(projectMap)
                    .build();
            return OptExpression.create(newProject, rewrittenChild);
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);
            if (rewrittenChild == child) {
                return expression;
            }
            return OptExpression.create(expression.getOp(), rewrittenChild);
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression expression, Void context) {
            OptExpression leftChild = expression.inputAt(0);
            OptExpression rightChild = expression.inputAt(1);
            OptExpression rewrittenLeft = leftChild.getOp().accept(this, leftChild, null);
            OptExpression rewrittenRight = rightChild.getOp().accept(this, rightChild, null);
            LogicalJoinOperator join = (LogicalJoinOperator) expression.getOp();

            if (join.getProjection() == null) {
                if (rewrittenLeft == leftChild && rewrittenRight == rightChild) {
                    return expression;
                }
                return OptExpression.create(join, rewrittenLeft, rewrittenRight);
            }

            List<ColumnRefOperator> rowIds = this.context.getRowIds(expression).orElse(null);
            List<ColumnRefOperator> leftRowIds = this.context.getRowIds(leftChild).orElse(null);
            List<ColumnRefOperator> rightRowIds = this.context.getRowIds(rightChild).orElse(null);
            if (rowIds == null || leftRowIds == null || rightRowIds == null) {
                if (rewrittenLeft == leftChild && rewrittenRight == rightChild) {
                    return expression;
                }
                return OptExpression.create(join, rewrittenLeft, rewrittenRight);
            }

            List<ColumnRefOperator> inputRowIds = getJoinInputRowIds(join.getJoinType(), leftRowIds, rightRowIds);
            if (rowIds.size() != inputRowIds.size()) {
                if (rewrittenLeft == leftChild && rewrittenRight == rightChild) {
                    return expression;
                }
                return OptExpression.create(join, rewrittenLeft, rewrittenRight);
            }

            Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap(join.getProjection().getColumnRefMap());
            boolean projectionChanged = false;
            for (int i = 0; i < rowIds.size(); i++) {
                ColumnRefOperator rowId = rowIds.get(i);
                if (!projectionMap.containsKey(rowId)) {
                    projectionMap.put(rowId, inputRowIds.get(i));
                    projectionChanged = true;
                }
            }
            if (!projectionChanged && rewrittenLeft == leftChild && rewrittenRight == rightChild) {
                return expression;
            }

            LogicalJoinOperator newJoin = LogicalJoinOperator.builder()
                    .withOperator(join)
                    .setProjection(new Projection(
                            projectionMap,
                            Maps.newHashMap(join.getProjection().getCommonSubOperatorMap())))
                    .build();
            return OptExpression.create(newJoin, rewrittenLeft, rewrittenRight);
        }

        private List<ColumnRefOperator> getJoinInputRowIds(JoinOperator joinType,
                                                            List<ColumnRefOperator> leftRowIds,
                                                            List<ColumnRefOperator> rightRowIds) {
            if (joinType.isLeftAntiJoin() || joinType.isLeftSemiJoin()) {
                return Lists.newArrayList(leftRowIds);
            }
            List<ColumnRefOperator> inputRowIds = Lists.newArrayListWithCapacity(leftRowIds.size() + rightRowIds.size());
            inputRowIds.addAll(leftRowIds);
            inputRowIds.addAll(rightRowIds);
            return inputRowIds;
        }
    }
}
