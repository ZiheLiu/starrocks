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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IvmDeltaAggregateRuleTest {
    @Test
    public void testRewriteDeltaAggregate(@Mocked OlapTable table, @Mocked PhysicalPartition partition) {
        new Expectations() {
            {
                table.getAllPhysicalPartitions();
                result = List.of(partition);
                partition.getVisibleVersion();
                result = 10L;
            }
        };

        ColumnRefOperator kRef = new ColumnRefOperator(1, IntegerType.INT, "k", false);
        ColumnRefOperator vRef = new ColumnRefOperator(2, IntegerType.INT, "v", true);
        Column kCol = new Column("k", IntegerType.INT, false);
        Column vCol = new Column("v", IntegerType.INT, true);

        Map<ColumnRefOperator, Column> colRefToMeta = Maps.newHashMap();
        colRefToMeta.put(kRef, kCol);
        colRefToMeta.put(vRef, vCol);
        Map<Column, ColumnRefOperator> metaToColRef = Maps.newHashMap();
        metaToColRef.put(kCol, kRef);
        metaToColRef.put(vCol, vRef);

        LogicalOlapScanOperator scan = new LogicalOlapScanOperator(
                table, colRefToMeta, metaToColRef, null, -1, null,
                1, null, null, false, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(),
                false, 1L);
        OptExpression scanExpr = OptExpression.create(scan);

        ColumnRefOperator cntRef = new ColumnRefOperator(3, IntegerType.BIGINT, "cnt", false);
        CallOperator countOp = new CallOperator(FunctionSet.COUNT, IntegerType.BIGINT,
                List.of(ConstantOperator.createInt(1)));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(
                AggType.GLOBAL, List.of(kRef), Map.of(cntRef, countOp));
        OptExpression aggExpr = OptExpression.create(agg, scanExpr);
        OptExpression deltaAggExpr = OptExpression.create(new LogicalDeltaOperator(), aggExpr);

        IvmDeltaAggregateRule rule = new IvmDeltaAggregateRule();
        List<OptExpression> result = rule.transform(deltaAggExpr, OptimizerFactory.mockContext(new ColumnRefFactory()));

        assertEquals(1, result.size());
        LogicalAggregationOperator rewrittenAgg = (LogicalAggregationOperator) result.get(0).getOp();
        assertEquals(2, rewrittenAgg.getGroupingKeys().size());
        assertTrue(rewrittenAgg.getGroupingKeys().stream().anyMatch(IvmRuleUtils::isActionColumn));
        assertEquals(1, rewrittenAgg.getAggregations().size());

        OptExpression joinExpr = result.get(0).inputAt(0);
        LogicalJoinOperator joinOp = (LogicalJoinOperator) joinExpr.getOp();
        assertEquals(JoinOperator.LEFT_SEMI_JOIN, joinOp.getJoinType());

        OptExpression unionExpr = joinExpr.inputAt(0);
        LogicalUnionOperator unionOp = (LogicalUnionOperator) unionExpr.getOp();
        assertTrue(unionOp.isUnionAll());
        assertEquals(2, unionExpr.getInputs().size());

        LogicalVersionOperator plusVersion = (LogicalVersionOperator) unionExpr.inputAt(0).getOp();
        LogicalVersionOperator minusVersion = (LogicalVersionOperator) unionExpr.inputAt(1).getOp();
        assertEquals(10L, plusVersion.getTableVersion());
        assertEquals((byte) 1, plusVersion.getAction());
        assertEquals(1L, minusVersion.getTableVersion());
        assertEquals((byte) -1, minusVersion.getAction());

        LogicalProjectOperator plusProject = (LogicalProjectOperator) unionExpr.inputAt(0).inputAt(0).getOp();
        LogicalProjectOperator minusProject = (LogicalProjectOperator) unionExpr.inputAt(1).inputAt(0).getOp();
        ColumnRefOperator actionCol = plusProject.getColumnRefMap().keySet().stream()
                .filter(IvmRuleUtils::isActionColumn)
                .findFirst()
                .orElseThrow();
        assertEquals((byte) 1, ((ConstantOperator) plusProject.getColumnRefMap().get(actionCol)).getTinyInt());
        assertEquals((byte) -1, ((ConstantOperator) minusProject.getColumnRefMap().get(actionCol)).getTinyInt());

        OptExpression affectedKeyExpr = joinExpr.inputAt(1);
        assertInstanceOf(LogicalAggregationOperator.class, affectedKeyExpr.getOp());
        assertEquals(OperatorType.LOGICAL_DELTA, affectedKeyExpr.inputAt(0).getOp().getOpType());
    }
}
