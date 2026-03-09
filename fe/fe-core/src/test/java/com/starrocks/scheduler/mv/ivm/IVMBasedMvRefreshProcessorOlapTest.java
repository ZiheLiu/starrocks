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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IVMBasedMvRefreshProcessorOlapTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(false);
    }

    @Test
    public void testIntersect() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `ivm_intersect_t1` (\n" +
                "  `pk` bigint NOT NULL,\n" +
                "  `v1` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `ivm_intersect_t2` (\n" +
                "  `pk` bigint NOT NULL,\n" +
                "  `v1` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        executeInsertSql("insert into ivm_intersect_t1 values (1, 10), (2, 20)");
        executeInsertSql("insert into ivm_intersect_t2 values (1, 10), (3, 30)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_intersect_mv`\n" +
                "DISTRIBUTED BY HASH(`pk`)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"refresh_mode\" = \"incremental\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT pk FROM ivm_intersect_t1\n" +
                "INTERSECT\n" +
                "SELECT pk FROM ivm_intersect_t2;");

        starRocksAssert.refreshMV("refresh materialized view ivm_intersect_mv with sync mode");

        executeInsertSql("insert into ivm_intersect_t1 values (3, 30)");
        executeInsertSql("insert into ivm_intersect_t2 values (2, 20)");

        MaterializedView mv = getMv("ivm_intersect_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_intersect_mv");
        PlanTestBase.assertContains(plan, "INTERSECT");
        PlanTestBase.assertContains(plan, "LEFT ANTI JOIN");
        PlanTestBase.assertContains(plan, "UNION");
        PlanTestBase.assertContains(plan, "TABLE: ivm_intersect_mv");
    }

    @Test
    public void testExcept() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `ivm_except_t1` (\n" +
                "  `pk` bigint NOT NULL,\n" +
                "  `v1` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `ivm_except_t2` (\n" +
                "  `pk` bigint NOT NULL,\n" +
                "  `v1` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        executeInsertSql("insert into ivm_except_t1 values (1, 10), (2, 20)");
        executeInsertSql("insert into ivm_except_t2 values (2, 20)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_except_mv`\n" +
                "DISTRIBUTED BY HASH(`pk`)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"refresh_mode\" = \"incremental\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT pk FROM ivm_except_t1\n" +
                "EXCEPT\n" +
                "SELECT pk FROM ivm_except_t2;");

        starRocksAssert.refreshMV("refresh materialized view ivm_except_mv with sync mode");

        executeInsertSql("insert into ivm_except_t1 values (3, 30)");
        executeInsertSql("insert into ivm_except_t2 values (1, 10)");

        MaterializedView mv = getMv("ivm_except_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_except_mv");
        PlanTestBase.assertContains(plan, "EXCEPT");
        PlanTestBase.assertContains(plan, "LEFT ANTI JOIN");
        PlanTestBase.assertContains(plan, "UNION");
        PlanTestBase.assertContains(plan, "TABLE: ivm_except_mv");
    }
}
