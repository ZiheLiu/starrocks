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
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IvmRowIdDeriverTest {
    @Test
    public void testWindowAppendsRowIdToOrderBy(@Mocked OlapTable table) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator partitionRef = columnRefFactory.create("k1", IntegerType.INT, false);
        ColumnRefOperator orderRef = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator windowRef = columnRefFactory.create("w1", IntegerType.BIGINT, false);

        Column partitionColumn = new Column("k1", IntegerType.INT, false);
        Column orderColumn = new Column("v1", IntegerType.INT, true);
        new Expectations() {
            {
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                table.getKeyColumnsInOrder();
                result = List.of(partitionColumn);
                table.getBaseIndexMetaId();
                result = 1L;
            }
        };

        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        Maps.newHashMap(Map.of(partitionRef, partitionColumn, orderRef, orderColumn)),
                        Maps.newHashMap(Map.of(partitionColumn, partitionRef, orderColumn, orderRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();

        LogicalWindowOperator window = LogicalWindowOperator.builder()
                .setPartitionExpressions(List.of(partitionRef))
                .setOrderByElements(List.of(new Ordering(orderRef, true, true)))
                .setEnforceSortColumns(List.of(new Ordering(orderRef, true, true)))
                .setWindowCall(Map.of(windowRef, new CallOperator("row_number", IntegerType.BIGINT, List.of())))
                .build();
        OptExpression root = OptExpression.create(window, OptExpression.create(scan));

        IvmRowIdDeriver.Result result = IvmRowIdDeriver.deriveAndRewrite(root, context);
        Assertions.assertTrue(result.success());
        Assertions.assertEquals(List.of(partitionRef), result.rootRowIdColumnRefs());

        LogicalWindowOperator rewrittenWindow = (LogicalWindowOperator) result.rewrittenRoot().getOp();
        Assertions.assertEquals(2, rewrittenWindow.getOrderByElements().size());
        Assertions.assertEquals(orderRef, rewrittenWindow.getOrderByElements().get(0).getColumnRef());
        Assertions.assertEquals(partitionRef, rewrittenWindow.getOrderByElements().get(1).getColumnRef());
        Assertions.assertEquals(2, rewrittenWindow.getEnforceSortColumns().size());
        Assertions.assertEquals(partitionRef, rewrittenWindow.getEnforceSortColumns().get(1).getColumnRef());
    }

    @Test
    public void testWindowWithoutPartitionByIsUnsupported(@Mocked OlapTable table) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator orderRef = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator windowRef = columnRefFactory.create("w1", IntegerType.BIGINT, false);
        Column orderColumn = new Column("v1", IntegerType.INT, true);
        new Expectations() {
            {
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                table.getKeyColumnsInOrder();
                result = List.of(new Column("k1", IntegerType.INT, false));
                table.getBaseIndexMetaId();
                result = 1L;
            }
        };

        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        Maps.newHashMap(Map.of(orderRef, orderColumn)),
                        Maps.newHashMap(Map.of(orderColumn, orderRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalWindowOperator window = LogicalWindowOperator.builder()
                .setOrderByElements(List.of(new Ordering(orderRef, true, true)))
                .setWindowCall(Map.of(windowRef, new CallOperator("row_number", IntegerType.BIGINT, List.of())))
                .build();

        IvmRowIdDeriver.Result result = IvmRowIdDeriver.deriveAndRewrite(
                OptExpression.create(window, OptExpression.create(scan)), context);
        Assertions.assertFalse(result.success());
    }
}
