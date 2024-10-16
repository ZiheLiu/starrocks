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

import com.google.api.client.util.Sets;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PruneUKFKAggregateColumns extends TransformationRule {
    public PruneUKFKAggregateColumns() {
        super(RuleType.TF_PRUNE_UKFK_AGG_COLUMNS, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.LOGICAL_PROJECT));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableUKFKOpt()) {
            return false;
        }
        UKFKConstraintsCollector.collectColumnConstraints(input);
        return input.getConstraints() != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression aggOpt, OptimizerContext context) {
        LogicalAggregationOperator aggOp = aggOpt.getOp().cast();
        OptExpression projectOpt = aggOpt.getInputs().get(0);
        LogicalProjectOperator projectOp = projectOpt.getOp().cast();

        UKFKConstraints constraints = aggOpt.getConstraints();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();
        List<ColumnRefOperator> groupBys = aggOp.getGroupingKeys();

        // Retrieve the set of non-UK columns from the table that contains the UK column used in the GROUP BY clause.
        ColumnRefSet nonUKColumnRefs = new ColumnRefSet();
        Set<ColumnRefOperator> usedUkGroupBys = Sets.newHashSet();
        Set<ColumnRefOperator> uselessUkGroupBys = Sets.newHashSet();
        for (ColumnRefOperator groupBy : groupBys) {
            UKFKConstraints.UniqueConstraintWrapper constraint = constraints.getRelaxedUniqueConstraint(groupBy.getId());
            if (constraint != null) {
                nonUKColumnRefs.union(constraint.nonUKColumnRefs);
                if (requiredOutputColumns.contains(groupBy)) {
                    usedUkGroupBys.add(groupBy);
                } else {
                    uselessUkGroupBys.add(groupBy);
                }
            }
        }
        if (usedUkGroupBys.isEmpty() && uselessUkGroupBys.isEmpty()) {
            return Lists.newArrayList();
        }

        // Remove group by columns that are non-UK columns and not used in the parent project operator.
        Set<ColumnRefOperator> removedGroupBys = Sets.newHashSet();
        for (ColumnRefOperator groupBy : groupBys) {
            if (requiredOutputColumns.contains(groupBy)) {
                continue;
            }

            if (nonUKColumnRefs.contains(groupBy)) {
                removedGroupBys.add(groupBy);
            } else {
                ScalarOperator inputOp = projectOp.getColumnRefMap().get(groupBy);
                ColumnRefSet usedColumns = inputOp.getUsedColumns();
                if (usedColumns.size() == 1 && nonUKColumnRefs.contains(usedColumns.getFirstId())
                        && !Utils.hasNonDeterministicFunc(inputOp)) {
                    removedGroupBys.add(groupBy);
                }
            }
        }
        if (usedUkGroupBys.isEmpty()) {
            uselessUkGroupBys.stream().skip(1).forEach(removedGroupBys::add);
        } else {
            removedGroupBys.addAll(uselessUkGroupBys);
        }
        if (removedGroupBys.isEmpty()) {
            return Lists.newArrayList();
        }

        List<ColumnRefOperator> newPartitionColumns = aggOp.getPartitionByColumns().stream()
                .filter(columnRefOperator -> !removedGroupBys.contains(columnRefOperator))
                .collect(Collectors.toList());
        List<ColumnRefOperator> newGroupBys = aggOp.getGroupingKeys().stream()
                .filter(columnRefOperator -> !removedGroupBys.contains(columnRefOperator))
                .collect(Collectors.toList());

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator.Builder().withOperator(aggOp)
                .setType(AggType.GLOBAL)
                .setGroupingKeys(newGroupBys)
                .setPartitionByColumns(newPartitionColumns)
                .build();
        OptExpression result = OptExpression.create(newAggOperator, aggOpt.getInputs());

        return Lists.newArrayList(result);
    }
}
