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

package com.starrocks.sql.optimizer.rule.ivm.common;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.Map;
import java.util.Optional;

public class IvmRuleUtils {
    public static final String ACTION_COLUMN_NAME = StatisticStorage.CHANGES_ACTION_COLUMN;
    public static final Type ACTION_COLUMN_TYPE = IntegerType.TINYINT;

    private IvmRuleUtils() {
    }

    public static boolean containsLogicalDelta(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_DELTA) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (containsLogicalDelta(child)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsLogicalVersion(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_VERSION) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (containsLogicalVersion(child)) {
                return true;
            }
        }
        return false;
    }

    public static long getLatestVisibleVersion(OlapTable table) {
        long maxVisibleVersion = 0;
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            maxVisibleVersion = Math.max(maxVisibleVersion, partition.getVisibleVersion());
        }
        return maxVisibleVersion;
    }

    public static Optional<ColumnRefOperator> findActionColumn(OptExpression expression) {
        if (expression == null || expression.getOp() == null) {
            return Optional.empty();
        }
        if (expression.getOp() instanceof LogicalOlapScanOperator scan) {
            return scan.getColRefToColumnMetaMap().entrySet().stream()
                    .filter(entry -> isActionColumn(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst();
        }
        if (expression.getOp() instanceof LogicalProjectOperator project) {
            return project.getColumnRefMap().keySet().stream()
                    .filter(IvmRuleUtils::isActionColumn)
                    .findFirst();
        }
        if (expression.getOp() instanceof LogicalAggregationOperator agg) {
            return agg.getGroupingKeys().stream()
                    .filter(IvmRuleUtils::isActionColumn)
                    .findFirst();
        }
        if (expression.getOp() instanceof LogicalJoinOperator join) {
            if (join.getJoinType().isLeftSemiJoin() && !expression.getInputs().isEmpty()) {
                return findActionColumn(expression.inputAt(0));
            }
            return Optional.empty();
        }
        if (expression.getOp() instanceof LogicalFilterOperator filter) {
            Projection projection = filter.getProjection();
            if (projection != null) {
                return projection.getColumnRefMap().keySet().stream()
                        .filter(IvmRuleUtils::isActionColumn)
                        .findFirst();
            }
        }
        if (expression.getOp() instanceof LogicalCTEAnchorOperator) {
            if (expression.getInputs().size() >= 2) {
                return findActionColumn(expression.inputAt(1));
            }
            return Optional.empty();
        }
        if (expression.getInputs().size() == 1) {
            return findActionColumn(expression.inputAt(0));
        }
        return Optional.empty();
    }

    public static boolean isActionColumn(Column column) {
        return column != null && ACTION_COLUMN_NAME.equalsIgnoreCase(column.getName());
    }

    public static boolean isActionColumn(ColumnRefOperator columnRef) {
        return columnRef != null && ACTION_COLUMN_NAME.equalsIgnoreCase(columnRef.getName());
    }
}
