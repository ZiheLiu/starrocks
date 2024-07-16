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

import org.junit.BeforeClass;
import org.junit.Test;

public class PlanTimeoutTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(5000);
        connectContext.getSessionVariable().setCboCteReuse(false);
    }

    @Test
    public void testQueryTimeout() throws Exception {
        String sql = "with \n" +
                "    w1 as (select vt1.v1 from t0 vt1 join t0 vt2 using(v1)),\n" +
                "    w2 as (select vt1.v1 from w1 vt1 join w1 vt2 using(v1)),\n" +
                "    w3 as (select vt1.v1 from w2 vt1 join w2 vt2 using(v1)),\n" +
                "    w4 as (select vt1.v1 from w3 vt1 join w3 vt2 using(v1)),\n" +
                "    w5 as (select vt1.v1 from w4 vt1 join w4 vt2 using(v1)),\n" +
                "    w6 as (select vt1.v1 from w5 vt1 join w5 vt2 using(v1)),\n" +
                "    w7 as (select vt1.v1 from w6 vt1 join w6 vt2 using(v1)),\n" +
                "    w8 as (select vt1.v1 from w7 vt1 join w7 vt2 using(v1)),\n" +
                "    w9 as (select vt1.v1 from w8 vt1 join w8 vt2 using(v1)),\n" +
                "    w10 as (select vt1.v1 from w9 vt1 join w9 vt2 using(v1)),\n" +
                "    w11 as (select vt1.v1 from w10 vt1 join w10 vt2 using(v1)),\n" +
                "    w12 as (select vt1.v1 from w11 vt1 join w11 vt2 using(v1)),\n" +
                "    w13 as (select vt1.v1 from w12 vt1 join w12 vt2 using(v1)),\n" +
                "    w14 as (select vt1.v1 from w13 vt1 join w13 vt2 using(v1)),\n" +
                "    w15 as (select vt1.v1 from w14 vt1 join w14 vt2 using(v1)),\n" +
                "    w16 as (select vt1.v1 from w15 vt1 join w15 vt2 using(v1)),\n" +
                "    w17 as (select vt1.v1 from w16 vt1 join w16 vt2 using(v1))\n" +
                "select * from w17";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
    }
}
