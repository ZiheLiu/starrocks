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

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.property.DomainProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A logical marker operator for incremental maintenance rewrite.
 * It should be eliminated by IVM delta rewrite rules before physical optimization.
 */
public class LogicalDeltaOperator extends LogicalOperator {
    private final boolean isRootDelta;
    private final Map<ColumnRefOperator, Column> mvColumnMapping;

    public LogicalDeltaOperator() {
        this(false, Maps.newHashMap());
    }

    public LogicalDeltaOperator(boolean isRootDelta) {
        this(isRootDelta, Maps.newHashMap());
    }

    public LogicalDeltaOperator(boolean isRootDelta, Map<ColumnRefOperator, Column> mvColumnMapping) {
        super(OperatorType.LOGICAL_DELTA);
        this.isRootDelta = isRootDelta;
        this.mvColumnMapping = Collections.unmodifiableMap(
                mvColumnMapping == null ? Maps.newHashMap() : Maps.newHashMap(mvColumnMapping));
    }

    public boolean isRootDelta() {
        return isRootDelta;
    }

    public Map<ColumnRefOperator, Column> getMvColumnMapping() {
        return mvColumnMapping;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return expressionContext.getChildLogicalProperty(0).getOutputColumns();
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(0).getRowOutputInfo());
    }

    @Override
    public DomainProperty deriveDomainProperty(List<OptExpression> inputs) {
        return inputs.get(0).getDomainProperty();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDelta(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalDelta(optExpression, context);
    }
}
