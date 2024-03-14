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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MergeConstantUnionRule extends TransformationRule {
    public MergeConstantUnionRule() {
        super(RuleType.TF_MERGE_CONSTANT_UNION,
                Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return input.getInputs().stream()
                .filter(MergeConstantUnionRule::isMergable)
                .count() > 1;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalUnionOperator unionOp = (LogicalUnionOperator) input.getOp();

        List<OptExpression> newInputs = Lists.newArrayList();
        List<List<ColumnRefOperator>> newChildOutputColumns = Lists.newArrayList();
        List<List<ScalarOperator>> newRows = Lists.newArrayList();
        int firstConstantChildIndex = -1;
        for (int i = 0; i < input.getInputs().size(); i++) {
            OptExpression child = input.getInputs().get(i);
            if (!isMergable(child)) {
                newInputs.add(child);
                newChildOutputColumns.add(unionOp.getChildOutputColumns().get(i));
                continue;
            }

            if (firstConstantChildIndex < 0) {
                firstConstantChildIndex = i;
            }

            LogicalValuesOperator childValuesOp = (LogicalValuesOperator) child.getOp();
            if (isConstantValues(childValuesOp)) {
                newRows.addAll(childValuesOp.getRows());
            } else {
                Map<ColumnRefOperator, ScalarOperator> columnRefMap = childValuesOp.getProjection().getColumnRefMap();
                newRows.add(unionOp.getChildOutputColumns().get(i).stream().map(columnRefMap::get).collect(Collectors.toList()));
            }
        }

        Preconditions.checkState(firstConstantChildIndex >= 0, "firstConstantChildIndex should not be non-positive");
        OptExpression firstConstantChild = input.getInputs().get(firstConstantChildIndex);
        LogicalValuesOperator newValuesOp = new LogicalValuesOperator.Builder()
                .withOperator((LogicalValuesOperator) firstConstantChild.getOp())
                .setProjection(null)
                .setRows(newRows)
                .setColumnRefSet(unionOp.getChildOutputColumns().get(firstConstantChildIndex))
                .build();
        OptExpression newValuesExpr = OptExpression.create(newValuesOp);
        newInputs.add(newValuesExpr);
        newChildOutputColumns.add(newValuesOp.getColumnRefSet());

        LogicalUnionOperator newUnionOp = new LogicalUnionOperator.Builder()
                .withOperator(unionOp)
                .setChildOutputColumns(newChildOutputColumns)
                .build();
        OptExpression newUnionExpr = OptExpression.create(newUnionOp, newInputs);

        return List.of(newUnionExpr);
    }

    private static boolean isMergable(OptExpression input) {
        if (input.getOp().getOpType() != OperatorType.LOGICAL_VALUES) {
            return false;
        }

        if (input.getOp().hasLimit()) {
            return false;
        }

        LogicalValuesOperator values = (LogicalValuesOperator) input.getOp();
        return isConstantValues(values) || isConstantUnion(values);
    }

    private static boolean isConstantUnion(LogicalValuesOperator valuesOp) {
        List<List<ScalarOperator>> rows = valuesOp.getRows();
        if (!(valuesOp.getProjection() != null && rows.size() == 1 && rows.get(0).size() == 1)) {
            return false;
        }
        ScalarOperator value = rows.get(0).get(0);
        return value.equals(ConstantOperator.createNull(value.getType()));
    }

    private static boolean isConstantValues(LogicalValuesOperator valuesOp) {
        return valuesOp.getProjection() == null;
    }
}
