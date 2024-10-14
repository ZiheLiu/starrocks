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

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.common.DdlException;
import com.starrocks.planner.TpchSQL;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HiveTPCHPlanTest extends ConnectorPlanTestBase {
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(temp.toURI().toString());
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        connectContext.changeCatalogDb("hive0.tpch");
        connectContext.getSessionVariable().setEnableStatsToOptimizeSkewJoin(true);
    }

    @AfterAll
    public static void afterClass() {
        try {
            UtFrameUtils.dropMockBackend(10002);
            UtFrameUtils.dropMockBackend(10003);
        } catch (DdlException e) {
            e.printStackTrace();
        }
    }

    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql, String resultFile) {
        runFileUnitTest(sql, resultFile);
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue(), "external/hive/tpch/" + entry.getKey()));
        }
        return cases.stream();
    }

    @Test
    public void test() throws Exception {
        connectContext.changeCatalogDb("hive0.tpch");
        connectContext.getSessionVariable().setEnableUKFKOpt(true);
        connectContext.getSessionVariable().setMockPK("hive0.tpch.customer.c_custkey");
        connectContext.getSessionVariable().setMockFK("hive0.tpch.orders(o_custkey) REFERENCES hive0.tpch.customer(c_custkey)");

        String plan = getFragmentPlan("select\n" +
                "    sum(o_shippriority) as revenue,\n" +
                "    c_custkey\n" +
                "from\n" +
                "    customer,\n" +
                "    orders\n" +
                " where \n" +
                "  c_custkey = o_custkey\n" +
                "group by\n" +
                "    c_custkey, c_name\n" +
                "order by\n" +
                "    revenue desc,\n" +
                "    c_custkey limit 10;\n" +
                "\n");
        System.out.println(plan);
    }
}