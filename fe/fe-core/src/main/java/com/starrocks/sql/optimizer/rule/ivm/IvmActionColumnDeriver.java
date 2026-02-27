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

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IvmActionColumnDeriver {
    public record Result(boolean success, OptExpression rewrittenRoot, String unsupportedReason) {
    }

    private IvmActionColumnDeriver() {
    }

    public static Result deriveAndRewrite(OptExpression root, OptimizerContext optimizerContext) {
        OptExpression newRoot = root.getOp().accept(new RewriteVisitor(optimizerContext), root, null);
        return new Result(true, newRoot, null);
    }

    private static class RewriteVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final OptimizerContext optimizerContext;

        private RewriteVisitor(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        @Override
        public OptExpression visit(OptExpression expression, Void context) {
            boolean hasChildChanged = false;
            List<OptExpression> children = expression.getInputs();
            List<OptExpression> rewrittenChildren = Lists.newArrayListWithCapacity(children.size());
            for (OptExpression child : children) {
                OptExpression rewrittenChild = child.getOp().accept(this, child, null);
                rewrittenChildren.add(rewrittenChild);
                hasChildChanged = hasChildChanged || rewrittenChild != child;
            }
            if (!hasChildChanged) {
                return expression;
            }
            return OptExpression.create(expression.getOp(), expression.getTvrMeta(), rewrittenChildren);
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression expression, Void context) {
            if (!(expression.getOp() instanceof LogicalOlapScanOperator scan)) {
                return expression;
            }
            if (!scan.isChangesQuery()) {
                return expression;
            }
            if (IvmRuleUtils.findActionColumn(expression).isPresent()) {
                return expression;
            }
            Column actionColumn = new Column(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
            ColumnRefOperator actionRef = optimizerContext.getColumnRefFactory()
                    .create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
            optimizerContext.getColumnRefFactory().updateColumnRefToColumns(actionRef, actionColumn, scan.getTable());

            Map<ColumnRefOperator, Column> colRefToMeta = Maps.newHashMap(scan.getColRefToColumnMetaMap());
            Map<Column, ColumnRefOperator> metaToColRef = Maps.newHashMap(scan.getColumnMetaToColRefMap());
            colRefToMeta.put(actionRef, actionColumn);
            metaToColRef.put(actionColumn, actionRef);

            LogicalOlapScanOperator rewrittenScan = LogicalOlapScanOperator.builder()
                    .withOperator(scan)
                    .setColRefToColumnMetaMap(colRefToMeta)
                    .setColumnMetaToColRefMap(metaToColRef)
                    .build();
            return OptExpression.create(rewrittenScan);
        }

        @Override
        public OptExpression visitLogicalFilter(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);

            LogicalFilterOperator filter = (LogicalFilterOperator) expression.getOp();
            ColumnRefOperator childAction = IvmRuleUtils.findActionColumn(rewrittenChild).orElse(null);
            if (filter.getProjection() == null || childAction == null) {
                if (rewrittenChild == child) {
                    return expression;
                }
                return OptExpression.create(filter, rewrittenChild);
            }
            if (IvmRuleUtils.findActionColumn(expression).isPresent()) {
                if (rewrittenChild == child) {
                    return expression;
                }
                return OptExpression.create(filter, rewrittenChild);
            }
            Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap(filter.getProjection().getColumnRefMap());
            projectionMap.put(childAction, childAction);
            LogicalFilterOperator rewrittenFilter = new LogicalFilterOperator.Builder()
                    .withOperator(filter)
                    .setProjection(new Projection(
                            projectionMap,
                            Maps.newHashMap(filter.getProjection().getCommonSubOperatorMap())))
                    .build();
            return OptExpression.create(rewrittenFilter, rewrittenChild);
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);

            LogicalProjectOperator project = (LogicalProjectOperator) expression.getOp();
            ColumnRefOperator childAction = IvmRuleUtils.findActionColumn(rewrittenChild).orElse(null);
            if (childAction == null || IvmRuleUtils.findActionColumn(expression).isPresent()) {
                if (rewrittenChild == child) {
                    return expression;
                }
                return OptExpression.create(project, rewrittenChild);
            }
            Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap(project.getColumnRefMap());
            projectMap.put(childAction, childAction);
            LogicalProjectOperator rewrittenProject = LogicalProjectOperator.builder()
                    .withOperator(project)
                    .setColumnRefMap(projectMap)
                    .build();
            return OptExpression.create(rewrittenProject, rewrittenChild);
        }
    }
}
