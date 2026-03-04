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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;
import java.util.Map;

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
        if (!joinType.isInnerJoin() && !joinType.isCrossJoin()) {
            return List.of();
        }

        OptExpression leftChild = joinExpr.inputAt(0);
        OptExpression rightChild = joinExpr.inputAt(1);

        List<ColumnRefOperator> joinOutputColumns =
                joinExpr.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        List<ColumnRefOperator> finalOutputColumns = Lists.newArrayList(joinOutputColumns);
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn != null) {
            finalOutputColumns.add(actionColumn);
        }

        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();

        // delta(t1) INNER JOIN t2@from_version
        OptExpression leftDelta = OptExpression.create(new LogicalDeltaOperator(false, actionColumn), leftChild);
        OptExpression rightFromVersion = OptExpression.create(LogicalVersionOperator.fromVersion(), rightChild);
        OptExpression leftDeltaJoin = createBranch(join, leftDelta, rightFromVersion, finalOutputColumns, actionColumn);
        unionChildren.add(leftDeltaJoin);
        unionChildOutputs.add(finalOutputColumns);

        // t1@to_version INNER JOIN delta(t2)
        OptExpression leftToVersion = OptExpression.create(LogicalVersionOperator.toVersion(), leftChild);
        OptExpression rightDelta = OptExpression.create(new LogicalDeltaOperator(false, actionColumn), rightChild);
        OptExpression rightDeltaJoin = createBranch(join, leftToVersion, rightDelta, finalOutputColumns, actionColumn);
        unionChildren.add(rightDeltaJoin);
        unionChildOutputs.add(finalOutputColumns);

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
        return List.of(OptExpression.create(unionOperator, unionChildren));
    }

    private OptExpression createBranch(LogicalJoinOperator join,
                                       OptExpression left,
                                       OptExpression right,
                                       List<ColumnRefOperator> finalOutputColumns,
                                       ColumnRefOperator actionColumn) {
        LogicalJoinOperator newJoin = LogicalJoinOperator.builder().withOperator(join).build();
        OptExpression joinExpr = OptExpression.create(newJoin, left, right);

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newLinkedHashMap();
        for (ColumnRefOperator output : finalOutputColumns) {
            if (actionColumn != null && actionColumn.equals(output)) {
                projectMap.put(output, actionColumn);
            } else {
                projectMap.put(output, output);
            }
        }
        return OptExpression.create(new LogicalProjectOperator(projectMap), joinExpr);
    }
}
