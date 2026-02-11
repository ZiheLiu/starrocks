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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRowIdContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IvmRowIdDeriver {
    public record Result(boolean success, OptExpression rewrittenRoot, String unsupportedReason) {
    }

    private IvmRowIdDeriver() {
    }

    public static Result deriveAndRewrite(OptExpression root, OptimizerContext optimizerContext) {
        IvmRowIdContext context = new IvmRowIdContext(optimizerContext.getColumnRefFactory());

        root.getOp().accept(new CollectorVisitor(context), root, null);
        if (!context.isSupported()) {
            return new Result(false, root, context.getUnsupportedReason().orElse("row-id derive failed"));
        }

        OptExpression rewritten = root.getOp().accept(new RewriteVisitor(context), root, null);
        return new Result(true, rewritten, null);
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
            if (keyColumns.size() != 1) {
                this.context.markUnsupported("only single primary key column is supported in OLAP IVM row-id derive");
                return null;
            }
            Column keyColumn = keyColumns.get(0);
            ColumnRefOperator rowId = scan.getColumnReference(keyColumn);
            if (rowId == null) {
                rowId = this.context.getColumnRefFactory()
                        .create(keyColumn.getName(), keyColumn.getType(), keyColumn.isAllowNull());
            }
            this.context.putRowId(expression, rowId);
            return null;
        }

        @Override
        public Void visitLogicalFilter(OptExpression expression, Void context) {
            collectChildren(expression);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("filter must be unary in OLAP IVM row-id derive");
                return null;
            }
            ColumnRefOperator childRowId = this.context.getRowId(expression.inputAt(0)).orElse(null);
            if (childRowId == null) {
                this.context.markUnsupported("filter child row-id is missing in OLAP IVM row-id derive");
                return null;
            }
            LogicalFilterOperator filter = (LogicalFilterOperator) expression.getOp();
            if (filter.getProjection() == null) {
                this.context.putRowId(expression, childRowId);
                return null;
            }
            ColumnRefOperator rowIdOutput = findOutputRef(filter.getProjection(), childRowId).orElseGet(
                    () -> this.context.getColumnRefFactory().create(
                            "__row_id", childRowId.getType(), childRowId.isNullable()));
            this.context.putRowId(expression, rowIdOutput);
            return null;
        }

        @Override
        public Void visitLogicalProject(OptExpression expression, Void context) {
            collectChildren(expression);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("project must be unary in OLAP IVM row-id derive");
                return null;
            }
            ColumnRefOperator childRowId = this.context.getRowId(expression.inputAt(0)).orElse(null);
            if (childRowId == null) {
                this.context.markUnsupported("project child row-id is missing in OLAP IVM row-id derive");
                return null;
            }
            LogicalProjectOperator project = (LogicalProjectOperator) expression.getOp();
            ColumnRefOperator rowIdOutput = findOutputRef(project.getColumnRefMap(), childRowId).orElseGet(
                    () -> this.context.getColumnRefFactory().create(
                            "__row_id", childRowId.getType(), childRowId.isNullable()));
            this.context.putRowId(expression, rowIdOutput);
            return null;
        }

        private void collectChildren(OptExpression expression) {
            for (OptExpression child : expression.getInputs()) {
                child.getOp().accept(this, child, null);
                if (!context.isSupported()) {
                    return;
                }
            }
        }

        private Optional<ColumnRefOperator> findOutputRef(Projection projection, ColumnRefOperator input) {
            return findOutputRef(projection.getColumnRefMap(), input);
        }

        private Optional<ColumnRefOperator> findOutputRef(
                Map<ColumnRefOperator, com.starrocks.sql.optimizer.operator.scalar.ScalarOperator> columnRefMap,
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
            ColumnRefOperator rowId = this.context.getRowId(expression).orElse(null);
            if (rowId == null) {
                return expression;
            }
            if (scan.getColRefToColumnMetaMap().containsKey(rowId)) {
                return expression;
            }
            if (!(scan.getTable() instanceof OlapTable table)) {
                return expression;
            }
            List<Column> keyColumns = table.getKeyColumnsInOrder();
            if (keyColumns.size() != 1) {
                return expression;
            }

            Map<ColumnRefOperator, Column> newColRefToMeta = Maps.newHashMap(scan.getColRefToColumnMetaMap());
            Map<Column, ColumnRefOperator> newMetaToColRef = Maps.newHashMap(scan.getColumnMetaToColRefMap());
            Column keyColumn = keyColumns.get(0);
            newColRefToMeta.put(rowId, keyColumn);
            newMetaToColRef.put(keyColumn, rowId);

            LogicalOlapScanOperator newScan = LogicalOlapScanOperator.builder()
                    .withOperator(scan)
                    .setColRefToColumnMetaMap(newColRefToMeta)
                    .setColumnMetaToColRefMap(newMetaToColRef)
                    .build();
            return OptExpression.create(newScan);
        }

        @Override
        public OptExpression visitLogicalFilter(OptExpression expression, Void context) {
            OptExpression rewrittenChild = expression.inputAt(0).getOp().accept(this, expression.inputAt(0), null);
            LogicalFilterOperator filter = (LogicalFilterOperator) expression.getOp();
            if (filter.getProjection() == null) {
                if (rewrittenChild == expression.inputAt(0)) {
                    return expression;
                }
                return OptExpression.create(filter, rewrittenChild);
            }

            ColumnRefOperator rowId = this.context.getRowId(expression).orElse(null);
            ColumnRefOperator childRowId = this.context.getRowId(expression.inputAt(0)).orElse(null);
            if (rowId == null || childRowId == null) {
                return expression;
            }
            Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap(filter.getProjection().getColumnRefMap());
            boolean projectionChanged = !projectionMap.containsKey(rowId);
            if (projectionChanged) {
                projectionMap.put(rowId, childRowId);
            }
            if (!projectionChanged && rewrittenChild == expression.inputAt(0)) {
                return expression;
            }
            LogicalFilterOperator newFilter = new LogicalFilterOperator.Builder()
                    .withOperator(filter)
                    .setProjection(
                            new Projection(projectionMap, Maps.newHashMap(filter.getProjection().getCommonSubOperatorMap())))
                    .build();
            return OptExpression.create(newFilter, rewrittenChild);
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression expression, Void context) {
            OptExpression rewrittenChild = expression.inputAt(0).getOp().accept(this, expression.inputAt(0), null);
            LogicalProjectOperator project = (LogicalProjectOperator) expression.getOp();
            ColumnRefOperator rowId = this.context.getRowId(expression).orElse(null);
            ColumnRefOperator childRowId = this.context.getRowId(expression.inputAt(0)).orElse(null);
            if (rowId == null || childRowId == null) {
                if (rewrittenChild == expression.inputAt(0)) {
                    return expression;
                }
                return OptExpression.create(project, rewrittenChild);
            }
            Map<ColumnRefOperator, com.starrocks.sql.optimizer.operator.scalar.ScalarOperator> projectMap =
                    Maps.newHashMap(project.getColumnRefMap());
            boolean projectChanged = !projectMap.containsKey(rowId);
            if (projectChanged) {
                projectMap.put(rowId, childRowId);
            }
            if (!projectChanged && rewrittenChild == expression.inputAt(0)) {
                return expression;
            }
            LogicalProjectOperator newProject = LogicalProjectOperator.builder()
                    .withOperator(project)
                    .setColumnRefMap(projectMap)
                    .build();
            return OptExpression.create(newProject, rewrittenChild);
        }
    }
}
