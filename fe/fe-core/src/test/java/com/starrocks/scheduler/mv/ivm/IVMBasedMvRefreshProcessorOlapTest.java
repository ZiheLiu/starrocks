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
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IVMBasedMvRefreshProcessorOlapTest extends MVTestBase {
    private static final String CREATE_T1 = "CREATE TABLE `t1` (\n" +
            "  `pk` bigint NOT NULL,\n" +
            "  `v1` int NOT NULL,\n" +
            "  `v2` int NOT NULL,\n" +
            "  `v3` int NOT NULL\n" +
            ") ENGINE=OLAP\n" +
            "PRIMARY KEY(`pk`)\n" +
            "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\"\n" +
            ");";

    private static final String CREATE_T2 = "CREATE TABLE `t2` (\n" +
            "  `pk` bigint NOT NULL,\n" +
            "  `v1` int NOT NULL,\n" +
            "  `v2` int NOT NULL,\n" +
            "  `v3` int NOT NULL\n" +
            ") ENGINE=OLAP\n" +
            "PRIMARY KEY(`pk`)\n" +
            "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\"\n" +
            ");";

    private final List<String> createdMVs = new ArrayList<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(false);
    }

    @BeforeEach
    public void setUpTables() throws Exception {
        createdMVs.clear();
        starRocksAssert.withTable(CREATE_T1);
        starRocksAssert.withTable(CREATE_T2);
    }

    @AfterEach
    public void tearDownObjects() throws Exception {
        for (String mvName : createdMVs) {
            connectContext.executeSql("drop materialized view if exists test." + mvName);
        }
        connectContext.executeSql("drop table if exists test.t1 force");
        connectContext.executeSql("drop table if exists test.t2 force");
    }

    @Test
    public void testIntersect() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 20, 200, 2000)");
        executeInsertSql("insert into t2 values (1, 10, 100, 1000), (3, 30, 300, 3000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_intersect_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v1, v2, v3 FROM t1\n" +
                "INTERSECT\n" +
                "SELECT v1, v2, v3 FROM t2;");
        createdMVs.add("ivm_intersect_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_intersect_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (v1,v2,v3)",
                        "AS SELECT v1, v2, v3 FROM t1\n" +
                                "INTERSECT\n" +
                                "SELECT v1, v2, v3 FROM t2");

        starRocksAssert.refreshMV("refresh materialized view ivm_intersect_mv with sync mode");
        executeInsertSql("insert into t1 values (3, 30, 300, 3000)");
        executeInsertSql("insert into t2 values (2, 20, 200, 2000)");

        MaterializedView mv = getMv("ivm_intersect_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_intersect_mv");
        assertThat(plan)
                .contains("INTERSECT", "LEFT ANTI JOIN", "UNION", "TABLE: ivm_intersect_mv");
    }

    @Test
    public void testExcept() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 20, 200, 2000)");
        executeInsertSql("insert into t2 values (2, 20, 200, 2000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_except_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v1, v2, v3 FROM t1\n" +
                "EXCEPT\n" +
                "SELECT v1, v2, v3 FROM t2;");
        createdMVs.add("ivm_except_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_except_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (v1,v2,v3)",
                        "AS SELECT v1, v2, v3 FROM t1\n" +
                                "EXCEPT\n" +
                                "SELECT v1, v2, v3 FROM t2");

        starRocksAssert.refreshMV("refresh materialized view ivm_except_mv with sync mode");
        executeInsertSql("insert into t1 values (3, 30, 300, 3000)");
        executeInsertSql("insert into t2 values (1, 10, 100, 1000)");

        MaterializedView mv = getMv("ivm_except_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_except_mv");
        assertThat(plan)
                .contains("EXCEPT", "LEFT ANTI JOIN", "UNION", "TABLE: ivm_except_mv");
    }

    private String getShowCreateMaterializedView(String mvName) throws Exception {
        String showCreateSql = "show create materialized view test." + mvName + ";";
        ShowCreateTableStmt stmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateSql, connectContext);
        ShowResultSet showResultSet = ShowExecutor.execute(stmt, connectContext);
        List<List<String>> resultRows = showResultSet.getResultRows();
        Assertions.assertEquals(1, resultRows.size());
        return resultRows.get(0).get(1);
    }
}
