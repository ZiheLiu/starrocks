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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.CTEProperty;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumper;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.transformation.JoinAssociativityRule;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.stream.Stream;

public class ReplayFromDumpTest extends ReplayFromDumpTestBase {
    @Test
    public void testForceRuleBasedRewrite() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(true);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("partition_flat_consumptions_partition_drinks_dates"));
    }

    @Test
    public void testForceRuleBasedRewriteMonth() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_month"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(true);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_month"), sessionVariable);
        Assert.assertTrue(replayPair.second,
                replayPair.second.contains("partition_flat_consumptions_partition_drinks_roll_month"));
    }

    @Test
    public void testForceRuleBasedRewriteYear() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_year"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(true);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_year"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("flat_consumptions_drinks_dates_roll_year"));
    }

    @Test
    public void testTPCH17WithUseAnalytic() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/tpch17"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setNewPlanerAggStage(2);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/tpch17"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  8:ANALYTIC\n" +
                "  |  functions: [, avg[([5: L_QUANTITY, DOUBLE, false]); args: DOUBLE; " +
                "result: DOUBLE; args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [18: P_PARTKEY, INT, false]"));
    }

    @Test
    public void testReplyOnlineCase_JoinEliminateNulls() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/join_eliminate_nulls"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setNewPlanerAggStage(2);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/join_eliminate_nulls"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("11:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: CASE 174: type WHEN '1' THEN concat('ocms_', 90: name) = " +
                "'ocms_fengyang56' " +
                "WHEN '0' THEN TRUE ELSE FALSE END\n" +
                "  |  limit: 10"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [tid, BIGINT, true] = [5: customer_id, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (5: customer_id), remote = true"));
        sessionVariable.setNewPlanerAggStage(0);
    }

    @Test
    public void testReplayTPCDS02() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds02"));
        SessionVariable replaySessionVariable = replayPair.first.getSessionVariable();
        Assert.assertEquals(replaySessionVariable.getParallelExecInstanceNum(), 4);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  |----24:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 65744\n" +
                "  |    \n" +
                "  18:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [321, INT, true] | [322, DECIMAL64(7,2), true]\n" +
                "  |  child exprs:\n" +
                "  |      [255: ws_sold_date_sk, INT, true] | [276: ws_ext_sales_price, DECIMAL64(7,2), true]\n" +
                "  |      [289: cs_sold_date_sk, INT, true] | [310: cs_ext_sales_price, DECIMAL64(7,2), true]\n" +
                "  |  pass-through-operands: all\n" +
                "  |  cardinality: 194398472\n" +
                "  |  column statistics: \n" +
                "  |  * ws_sold_date_sk-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                "  |  * ws_ext_sales_price-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"));
    }

    @Test
    public void testSSB10() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/ssb10"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  14:Project\n" +
                "  |  output columns:\n" +
                "  |  13 <-> [13: lo_revenue, INT, false]\n" +
                "  |  22 <-> [22: d_year, INT, false]\n" +
                "  |  38 <-> [38: c_city, VARCHAR, false]\n" +
                "  |  46 <-> [46: s_city, VARCHAR, false]\n" +
                "  |  cardinality: 28532"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  |----7:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 30"));
    }

    @Test
    public void testTPCDS54() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds54"));
        // Check the size of the left and right tables
        Assert.assertTrue(replayPair.second, replayPair.second.contains("51:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: cast([208: d_month_seq, INT, true] as BIGINT) <= [291: expr, BIGINT, " +
                "true]\n" +
                "  |  cardinality: 18262\n" +
                "  |  column statistics: \n" +
                "  |  * d_date_sk-->[2415022.0, 2488070.0, 0.0, 4.0, 18262.25] ESTIMATE\n" +
                "  |  * d_month_seq-->[0.0, 2400.0, 0.0, 4.0, 2398.0] ESTIMATE\n" +
                "  |  * expr-->[3.0, 2403.0, 0.0, 8.0, 30.135726072607262] ESTIMATE\n"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  |----18:EXCHANGE\n" +
                "  |       distribution type: SHUFFLE\n" +
                "  |       partition exprs: [70: cs_bill_customer_sk, INT, true]\n" +
                "  |       cardinality: 6304\n" +
                "  |    \n" +
                "  2:OlapScanNode"));
    }

    @Test
    public void testTPCDS23_1() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpcds23_1"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 56\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 77\n" +
                "    RANDOM\n" +
                "\n" +
                "  40:Project\n" +
                "  |  <slot 99> : 99: c_customer_sk\n" +
                "  |  \n" +
                "  39:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: CAST(118: sum AS DOUBLE) > CAST(0.5 * 190: max AS DOUBLE)"));
    }

    @Test
    public void testGroupByLimit() throws Exception {
        // check can generate 1 phase with limit 1
        // This test has column statistics and accurate table row count
        SessionVariable sessionVariable = GlobalStateMgr.getCurrentState().getVariableMgr().newSessionVariable();
        sessionVariable.setNewPlanerAggStage(1);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/groupby_limit"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(
                "aggregate: multi_distinct_count[([1: LO_ORDERKEY, INT, false])"));
    }

    @Test
    public void testTPCDS78() throws Exception {
        // check outer join with isNull predicate on inner table
        // The estimate cardinality of join should not be 0.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds78"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [2: ss_ticket_number, INT, false] = [25: sr_ticket_number, INT, true]\n" +
                "  |  equal join conjunct: [1: ss_item_sk, INT, false] = [24: sr_item_sk, INT, true]\n" +
                "  |  other predicates: 25: sr_ticket_number IS NULL\n" +
                "  |  output columns: 1, 3, 5, 11, 12, 14\n" +
                "  |  cardinality: 37372757"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [76: ws_order_number, INT, false] = [110: wr_order_number, INT, true]\n" +
                "  |  equal join conjunct: [75: ws_item_sk, INT, false] = [109: wr_item_sk, INT, true]\n" +
                "  |  other predicates: 110: wr_order_number IS NULL\n" +
                "  |  output columns: 75, 77, 80, 93, 94, 96\n" +
                "  |  cardinality: 7914602"));
    }

    @Test
    public void testTPCDS94() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds94"));
        // check ANTI JOIN cardinality is not 0
        Assert.assertTrue(replayPair.second, replayPair.second.contains("21:HASH JOIN\n" +
                "  |    |  join op: RIGHT ANTI JOIN (PARTITIONED)\n" +
                "  |    |  equal join conjunct: [138: wr_order_number, INT, false] = [2: ws_order_number, INT, " +
                "false]\n" +
                "  |    |  build runtime filters:\n" +
                "  |    |  - filter_id = 3, build_expr = (2: ws_order_number), remote = true\n" +
                "  |    |  output columns: 2, 17, 29, 34\n" +
                "  |    |  cardinality: 26765"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("23:HASH JOIN\n" +
                "  |  join op: RIGHT SEMI JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  equal join conjunct: [103: ws_order_number, INT, false] = [2: ws_order_number, INT, false]\n" +
                "  |  other join predicates: [17: ws_warehouse_sk, INT, true] != [118: ws_warehouse_sk, INT, true]\n" +
                "  |  build runtime filters:"));
    }

    @Test
    public void testTPCDS22() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds22"));
        // check d_date_sk distinct values has adjusted according to the cardinality
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [1: inv_date_sk, INT, false] = [5: d_date_sk, INT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (5: d_date_sk), remote = false\n" +
                "  |  output columns: 2, 4\n" +
                "  |  cardinality: 399330000\n" +
                "  |  column statistics: \n" +
                "  |  * inv_date_sk-->[2450815.0, 2452635.0, 0.0, 4.0, 260.0] ESTIMATE\n" +
                "  |  * inv_item_sk-->[1.0, 204000.0, 0.0, 4.0, 200414.0] ESTIMATE\n" +
                "  |  * inv_quantity_on_hand-->[0.0, 1000.0, 0.05000724964315228, 4.0, 1006.0] ESTIMATE\n" +
                "  |  * d_date_sk-->[2450815.0, 2452635.0, 0.0, 4.0, 260.0] ESTIMATE\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 335"));
    }

    @Test
    public void testTPCDS64() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpcds64"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  86:SELECT\n" +
                "  |  predicates: 457: d_year = 1999"));
    }

    @Test
    public void testCrossReorder() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(false);
        RuleSet mockRule = new RuleSet() {
            @Override
            public void addJoinTransformationRules() {
                this.getTransformRules().clear();
                this.getTransformRules().add(JoinAssociativityRule.INNER_JOIN_ASSOCIATIVITY_RULE);
            }
        };

        new MockUp<OptimizerContext>() {
            @Mock
            public RuleSet getRuleSet() {
                return mockRule;
            }
        };

        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/cross_reorder"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  13:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: CAST(CASE WHEN CAST(6: v3 AS BOOLEAN) THEN CAST(11: v2 AS VARCHAR) " +
                "WHEN CAST(3: v3 AS BOOLEAN) THEN '123' ELSE CAST(12: v3 AS VARCHAR) END AS DOUBLE) > " +
                "1.0, (CAST(2: v2 AS DECIMAL128(38,9)) = CAST(8: v2 AS DECIMAL128(38,9))) OR (3: v3 = 8: v2)\n"));
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
    }

    @Test
    public void testJoinReorderPushColumnsNoHandleProject() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_reorder"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second,
                replayPair.second.contains("  |  <slot 40> : CAST(15: id_smallint AS INT)\n" +
                        "  |  <slot 41> : CAST(23: id_date AS DATETIME)\n" +
                        "  |  \n" +
                        "  6:OlapScanNode\n" +
                        "     TABLE: external_es_table_without_null"));
    }

    @Test
    public void testMultiCountDistinct() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_count_distinct"), null, TExplainLevel.NORMAL);
        String plan = replayPair.second;
        Assert.assertTrue(plan, plan.contains("AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: multi_distinct_count(6: order_id), multi_distinct_count(11: delivery_phone)," +
                " multi_distinct_count(128: case), max(103: count)\n" +
                "  |  group by: 40: city, 116: division_en, 104: department, 106: category, 126: concat, " +
                "127: concat, 9: upc, 108: upc_desc"));
    }

    @Test
    public void testDecodeLimitWithProject() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/decode_limit_with_project"), null,
                        TExplainLevel.NORMAL);
        String plan = replayPair.second;
        Assert.assertTrue(plan, plan.contains(" 11:Decode\n" +
                "  |  <dict id 41> : <string id 18>\n" +
                "  |  <dict id 42> : <string id 23>"));
        FeConstants.USE_MOCK_DICT_MANAGER = false;
    }

    @Test
    public void testCountDistinctWithLimit() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/count_distinct_limit"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 5: lo_suppkey, 10: lo_extendedprice, 13: lo_revenue"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("4:AGGREGATE (update finalize)\n" +
                "  |  output: count(5: lo_suppkey)\n" +
                "  |  group by: 10: lo_extendedprice, 13: lo_revenue\n" +
                "  |  limit: 1"));
    }

    @Test
    public void testEighteenTablesJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/eighteen_tables_join"), null, TExplainLevel.NORMAL);
        // check optimizer finish task
        Assert.assertTrue(replayPair.second, replayPair.second.contains("52:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)"));
    }

    @Test
    public void testLocalAggregateWithoutTableRowCount() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/local_agg_without_table_rowcount"), null,
                        TExplainLevel.NORMAL);
        // check local aggregate
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(4: lo_partkey)"));
    }

    @Test
    public void testLogicalAggWithOneTablet() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/local_agg_with_one_tablet"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(4: t0d)"));
    }

    @Test
    public void testSelectSubqueryWithMultiJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/select_sbuquery_with_multi_join"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("20:Project\n" +
                "  |  <slot 31> : bitmap_and(20: bitmap_agg, 27: bitmap_agg)\n" +
                "  |  \n" +
                "  19:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----18:EXCHANGE\n" +
                "  |    \n" +
                "  11:Project\n" +
                "  |  <slot 20> : 17: bitmap_agg"));
    }

    @Test
    public void testTPCHRandom() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpch_random"), null, TExplainLevel.NORMAL);
        // check optimizer could extract best plan
        Assert.assertTrue(replayPair.second, replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)"));
    }

    @Test
    public void testInsertWithView() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/insert_view"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, UtFrameUtils.matchPlanWithoutId(" 2:Project\n" +
                "  |  <slot 2> : 2: t2_c2\n" +
                "  |  <slot 11> : CAST(CAST(1: t2_c1 AS BIGINT) + 1 AS INT)", replayPair.second));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("OLAP TABLE SINK"));
    }

    @Test
    public void testMergeGroupWithDeleteBestExpression() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/merge_group_delete_best_expression"), null,
                        TExplainLevel.NORMAL);
        // check without exception
        Assert.assertTrue(replayPair.second, replayPair.second.contains("14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testJoinReOrderPruneColumns() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_reorder_prune_columns"), null,
                        TExplainLevel.NORMAL);
        // check without exception
        Assert.assertTrue(replayPair.second, replayPair.second.contains("<slot 186> : 186: S_SUPPKEY"));
    }

    @Test
    public void testMultiViewWithDbName() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_with_db"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testMultiViewCrossJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_cross_join"), null, TExplainLevel.NORMAL);
        // check without exception
        Assert.assertTrue(replayPair.second, replayPair.second.contains("40:Project\n" +
                "  |  <slot 1> : 1: c_0_0"));
    }

    @Test
    public void testMultiViewPruneColumns() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_prune_columns"), null, TExplainLevel.NORMAL);
        // check without exception
        Assert.assertTrue(replayPair.second, replayPair.second.contains("<slot 1> : 1: c_1_0"));
    }

    @Test
    public void testCorrelatedSubqueryWithEqualsExpressions() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/correlated_subquery_with_equals_expression"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  22:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: if(19: c_0_0 != 1: c_0_0, 4: c_0_3, 20: c_0_3) = '1969-12-28', " +
                "if(((1: c_0_0 IS NULL) AND (NOT ((21: countRows IS NULL) OR (21: countRows = 0)))) OR " +
                "((22: countNotNulls < 21: countRows) AND (((NOT ((21: countRows IS NULL) OR (21: countRows = 0))) " +
                "AND (1: c_0_0 IS NOT NULL)) AND (16: c_0_0 IS NULL))), TRUE, FALSE)\n"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  20:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: c_0_0 = 16: c_0_0\n" +
                "  |  other join predicates: if(16: c_0_0 != 1: c_0_0, 4: c_0_3, 17: c_0_3) = '1969-12-28'"));
    }

    @Test
    public void testGatherWindowCTE2() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/gather_window_cte"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, UtFrameUtils.matchPlanWithoutId("  0:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [16, DATE, false] | [17, BIGINT, true] | [18, DECIMAL128(27,19), true]\n" +
                "  |  child exprs:\n" +
                "  |      [2: c_0_0, DATE, false] | [7: row_number(), BIGINT, true] " +
                "| [8: last_value(4: c_0_2), DECIMAL128(27,19), true]\n" +
                "  |      [9: c_0_0, DATE, false] | [14: row_number(), BIGINT, true] " +
                "| [15: last_value(11: c_0_2), DECIMAL128(27,19), true]", replayPair.second));
    }

    @Test
    public void testMultiSubqueries() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/subquery_statistics"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  96:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]\n" +
                "  |  hasNullableGenerateChild: true\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * count-->[0.0, 1.0420273298435367, 0.0, 8.0, 1.0] ESTIMATE\n" +
                "  |  \n" +
                "  95:Project\n" +
                "  |  output columns:\n" +
                "  |  549 <-> 1\n" +
                "  |  hasNullableGenerateChild: true\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * auto_fill_col-->[1.0, 1.0, 0.0, 1.0, 1.0] ESTIMATE"));
    }

    @Test
    public void testCorrelatedPredicateRewrite() throws Exception {
        connectContext.getSessionVariable().setSemiJoinDeduplicateMode(-1);
        connectContext.getSessionVariable().setEnableInnerJoinToSemi(false);
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/union_with_subquery"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1201:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  equal join conjunct: [3802: ref_id, BIGINT, true] = [3681: customer_id, BIGINT, true]"));
    }

    @Test
    public void testGroupByDistinctColumnSkewHint() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/group_by_count_distinct_skew_hint"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  9:Project\n" +
                "  |  <slot 39> : 39: year\n" +
                "  |  <slot 42> : 42: case\n" +
                "  |  <slot 45> : CAST(murmur_hash3_32(CAST(42: case AS VARCHAR)) % 512 AS SMALLINT)"));
    }

    @Test
    public void testGroupByDistinctColumnOptimization() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/group_by_count_distinct_optimize"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  9:Project\n" +
                "  |  <slot 39> : 39: year\n" +
                "  |  <slot 42> : 42: case\n" +
                "  |  <slot 45> : CAST(murmur_hash3_32(CAST(42: case AS VARCHAR)) % 512 AS SMALLINT)"));
    }

    @Test
    public void testPushDownDistinctAggBelowWindowRewrite() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/pushdown_distinct_agg_below_window"), null,
                        TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[([3: gross, DECIMAL128(10,2), false]); args: DECIMAL128; " +
                "result: DECIMAL128(38,2); args nullable: false; result nullable: true]\n" +
                "  |  group by: [1: country, VARCHAR, true], [2: trans_date, DATE, false]\n" +
                "  |  cardinality: 49070\n"));
    }

    @Test
    public void testSSBRightOuterJoinCase() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/right_outer_join_case"), null,
                        TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second.contains("4:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN"));
    }

    @Test
    public void testHiveTPCH02UsingResource() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch02_resource"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [24: n_regionkey, INT, true] = [26: r_regionkey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (26: r_regionkey), remote = false\n" +
                "  |  output columns: 22, 23\n" +
                "  |  cardinality: 23"));
    }

    @Test
    public void testHiveTPCH05UsingResource() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch05_resource"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  20:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [10: o_custkey, INT, true] = [1: c_custkey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 3, build_expr = (1: c_custkey), remote = false\n" +
                "  |  output columns: 4, 9\n" +
                "  |  cardinality: 22765073"));
    }

    @Test
    public void testHiveTPCH08UsingResource() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch08_resource"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("5:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [52: n_regionkey, INT, true] = [58: r_regionkey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (58: r_regionkey), remote = false\n" +
                "  |  output columns: 50\n" +
                "  |  cardinality: 5"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH02UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch02_catalog"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("19:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 17: ps_partkey = 1: p_partkey"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH05UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch05_catalog"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 20: l_suppkey = 34: s_suppkey\n" +
                "  |  equal join conjunct: 4: c_nationkey = 37: s_nationkey\n" +
                "  |  \n" +
                "  |----14:EXCHANGE\n" +
                "  |    \n" +
                "  12:EXCHANGE"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH08UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch08_catalog"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 33:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 52: n_regionkey = 58: r_regionkey"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testPruneCTEProperty() throws Exception {
        String jsonStr = getDumpInfoFromFile("query_dump/cte_reuse");
        connectContext.getSessionVariable().disableJoinReorder();
        Pair<String, ExecPlan> result = UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext,
                getDumpInfoFromJson(jsonStr));
        OptExpression expression = result.second.getPhysicalPlan().inputAt(1);
        Assert.assertEquals(new CTEProperty(1), expression.getLogicalProperty().getUsedCTEs());
        Assert.assertEquals(4, result.second.getCteProduceFragments().size());
    }

    @Test
    public void testReduceJoinTransformation1() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/reduce_transformation_1"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("33:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(212: case)\n" +
                "  |  group by: 34: cast, 33: cast, 38: handle, 135: concat, 136: case, 36: cast"));
    }

    @Test
    public void testReduceJoinTransformation2() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/reduce_transformation_2"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("38:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 398: substring = 361: date\n" +
                "  |  other join predicates: 209: zid = 298: zid"));
    }

    @Test
    public void testReduceJoinTransformation3() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/reduce_transformation_3"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 71: order_id = 2: orderid\n" +
                "  |  \n" +
                "  |----24:EXCHANGE"));
    }

    @Test
    public void testUnionAllWithTopNRuntimeFilter() throws Exception {
        QueryDumpInfo queryDumpInfo =
                getDumpInfoFromJson(getDumpInfoFromFile("query_dump/union_all_with_topn_runtime_filter"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setScanOrToUnionThreshold(-1);
        sessionVariable.setScanOrToUnionLimit(10);
        sessionVariable.setSelectRatioThreshold(20.0);
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/union_all_with_topn_runtime_filter"),
                        sessionVariable, TExplainLevel.VERBOSE);
        String plan = replayPair.second;

        // tbl_mock_015
        Assert.assertTrue(plan, plan.contains("probe runtime filters:\n" +
                "     - filter_id = 4, probe_expr = (<slot 79> 79: mock_004)"));
        Assert.assertTrue(plan, plan.contains("probe runtime filters:\n" +
                "     - filter_id = 3, probe_expr = (<slot 62> 62: mock_004)"));

        // table: tbl_mock_001, rollup: tbl_mock_001
        Assert.assertTrue(plan, plan.contains("probe runtime filters:\n" +
                "     - filter_id = 1, probe_expr = (<slot 110> 110: mock_004)"));
        Assert.assertTrue(plan, plan.contains("probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (<slot 96> 96: mock_004)\n"));

    }

    @Test
    public void testJoinWithArray() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_with_array"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 64: mock_007 = 1: mock_007\n" +
                "  |  equal join conjunct: 108: any_value = 41: mock_008\n" +
                "  |  \n" +
                "  |----3:OlapScanNode\n" +
                "  |       TABLE: tbl_mock_024"));
    }

    @Test
    public void testTwoStageAgg() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/two_stage_agg"),
                        null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING"));

        Assert.assertTrue(replayPair.second, replayPair.second.contains("0:OlapScanNode\n" +
                "     table: lineorder_2, rollup: lineorder_2"));
    }

    @Test
    public void testPushDistinctAggDownWindow() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/pushdown_distinct_agg_below_window2"),
                        null, TExplainLevel.NORMAL);
        System.out.println(replayPair.second);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  3:ANALYTIC\n" +
                "  |  functions: [, sum(5: sum), ]\n" +
                "  |  partition by: 1: TIME\n" +
                "  |  \n" +
                "  2:SORT\n" +
                "  |  order by: <slot 1> 1: TIME ASC\n" +
                "  |  analytic partition by: 1: TIME\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: NUM)\n" +
                "  |  group by: 1: TIME"));
    }

    @Test
    public void testNestedViewWithCTE() throws Exception {

        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/nested_view_with_cte"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("Project\n" +
                "  |  <slot 7363> : 7363: count\n" +
                "  |  limit: 100\n"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("AGGREGATE (merge finalize)\n" +
                "  |  output: count(7363: count)\n" +
                "  |  group by: 24: mock_038, 15: mock_003, 108: mock_109, 4: mock_005, 2: mock_110, 2133: case\n" +
                "  |  limit: 100"));
    }

    @Test
    public void testRBOMvOnView() throws Exception {
        String dumpInfo = getDumpInfoFromFile("query_dump/mv_on_view");
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpInfo);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("mv_LEAF_ACC_CUBE_SHADOW_VIEW_fb70da80"));
    }

    @Test
    public void testCBOMvOnView() throws Exception {
        String dumpInfo = getDumpInfoFromFile("query_dump/mv_on_view");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpInfo);
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableCBOViewBasedMvRewrite(true);
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpInfo, sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("mv_LEAF_ACC_CUBE_SHADOW_VIEW_fb70da80"));
        sessionVariable.setEnableCBOViewBasedMvRewrite(false);
    }

    @Test
    public void testCBONestedMvRewriteDrinks() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_drinks"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(false);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_drinks"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("partition_flat_consumptions_partition_drinks"));
    }

    @Test
    public void testCBONestedMvRewriteDates() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(false);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("partition_flat_consumptions_partition_drinks_dates"));
    }

    @Test
    public void testCBONestedMvRewriteMonth() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_month"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(false);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_month"), sessionVariable);
        Assert.assertTrue(replayPair.second,
                replayPair.second.contains("partition_flat_consumptions_partition_drinks_roll_month"));
    }

    @Test
    public void testCBONestedMvRewriteYear() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_year"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(false);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_year"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("flat_consumptions_drinks_dates_roll_year"));
    }

    @Test
    public void testNormalizeNonTrivialProject() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setPipelineDop(1);
        sv.setEnableQueryCache(true);
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            sv.setEnableLowCardinalityOptimize(true);
            Pair<QueryDumpInfo, String> replayPair =
                    getPlanFragment(getDumpInfoFromFile("query_dump/normalize_non_trivial_project"), sv,
                            TExplainLevel.NORMAL);
            Assert.assertTrue(replayPair.second,
                    replayPair.second != null && replayPair.second.contains("TABLE: tbl_mock_017"));
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    @Test
    public void testListPartitionPrunerWithNEExpr() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/list_partition_prune_dump"));
        // partitions should not be pruned
        Assert.assertTrue(replayPair.second, !replayPair.second.contains("partitionsRatio=2/3, tabletsRatio=20/20"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("0:OlapScanNode\n" +
                "     table: partitions2_keys311, rollup: partitions2_keys311\n" +
                "     preAggregation: on\n" +
                "     Predicates: [7: undef_signed_not_null, VARCHAR, false] != 'j'\n" +
                "     partitionsRatio=3/3, tabletsRatio=30/30"));
    }

    @Test
    public void testTopNPushDownBelowUnionAll() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/topn_push_down_union"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);

        // Topn should be pushed down below union all and contains no duplicated ording columns
        PlanTestBase.assertContains(replayPair.second, "  26:TOP-N\n" +
                "  |  order by: <slot 240> 240: expr ASC, <slot 241> 241: cast DESC, <slot 206> 206: mock_025 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 200");
        PlanTestBase.assertContains(replayPair.second, "17:TOP-N\n" +
                "  |  order by: <slot 165> 165: cast ASC, <slot 153> 153: cast DESC, <slot 166> 166: expr ASC, " +
                "<slot 167> 167: cast DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 200");
    }

    @Test
    public void testNoCTEOperatorPropertyDerived() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/no_cte_operator_test"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("23:Project\n" +
                "  |  <slot 193> : 193: mock_081\n" +
                "  |  <slot 194> : 194: mock_089\n" +
                "  |  <slot 391> : 391: case\n" +
                "  |  <slot 396> : 396: rank()"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 20:SORT\n" +
                "  |  order by: <slot 194> 194: mock_089 ASC," +
                " <slot 395> 395: case ASC, <slot 193> 193: mock_081 ASC, " +
                "<slot 233> 233: mock_065 ASC\n" +
                "  |  analytic partition by: 194: mock_089, 395: case, 193: mock_081\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  19:EXCHANGE"));
    }

    @Test
    public void testTimeoutDeepJoinCostPrune() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMER, "optimizer");
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);

        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/deep_join_cost"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        String ss = Tracers.printScopeTimer();
        int start = ss.indexOf("EnforceAndCostTask[") + "EnforceAndCostTask[".length();
        int end = ss.indexOf("]", start);
        long count = Long.parseLong(ss.substring(start, end));
        Assert.assertTrue(ss, count < 10000);
    }

    @Test
    public void testDistinctConstantRewrite() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/distinct_constant"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("4:AGGREGATE (update serialize)\n" +
                "  |  output: multi_distinct_count(1)"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("9:AGGREGATE (update serialize)\n" +
                "  |  output: multi_distinct_count(NULL)"));
    }

    @Test
    public void testSplitOrderBy() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/split_order_by"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("21:MERGING-EXCHANGE"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("20:TOP-N"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("15:MERGING-EXCHANGE"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("14:TOP-N"));

    }

    @Test
    public void testQueryCacheSetOperator() throws Exception {

        String savedSv = connectContext.getSessionVariable().getJsonString();
        try {
            connectContext.getSessionVariable().setEnableQueryCache(true);
            QueryDumpInfo dumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/query_cache_set_operator"));
            ExecPlan execPlan = UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dumpInfo);
            Assert.assertTrue(execPlan.getFragments().stream().anyMatch(frag -> frag.getCacheParam() != null));
        } finally {
            connectContext.getSessionVariable().replayFromJson(savedSv);
        }
    }

    @Test
    @Ignore
    public void testQueryTimeout() {
        Assert.assertThrows(StarRocksPlannerException.class,
                () -> getPlanFragment(getDumpInfoFromFile("query_dump/query_timeout"), null, TExplainLevel.NORMAL));
    }

    @Test
    public void testQueryCacheMisuseExogenousRuntimeFilter() throws Exception {
        String savedSv = connectContext.getSessionVariable().getJsonString();
        try {
            connectContext.getSessionVariable().setEnableQueryCache(true);
            QueryDumpInfo dumpInfo =
                    getDumpInfoFromJson(getDumpInfoFromFile("query_dump/query_cache_misuse_exogenous_runtime_filter"));
            ExecPlan execPlan = UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dumpInfo);
            Assert.assertTrue(execPlan.getFragments().stream().noneMatch(frag -> frag.getCacheParam() != null));
            Assert.assertTrue(
                    execPlan.getFragments().stream().anyMatch(frag -> !frag.getProbeRuntimeFilters().isEmpty()));
        } finally {
            connectContext.getSessionVariable().replayFromJson(savedSv);
        }
    }

    @Test
    public void testPruneTableNPE() throws Exception {
        String savedSv = connectContext.getSessionVariable().getJsonString();
        try {
            connectContext.getSessionVariable().setEnableCboTablePrune(true);
            connectContext.getSessionVariable().setEnableRboTablePrune(true);
            Pair<QueryDumpInfo, String> replayPair =
                    getPlanFragment(getDumpInfoFromFile("query_dump/prune_table_npe"),
                            null, TExplainLevel.NORMAL);
            long numHashJoins = Stream.of(replayPair.second.split("\n"))
                    .filter(ln -> ln.contains("HASH JOIN")).count();
            Assert.assertEquals(numHashJoins, 2);
        } finally {
            connectContext.getSessionVariable().replayFromJson(savedSv);
        }
    }

    @Test
    public void testJoinInitError() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/join_init_error"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("HASH JOIN"));
    }

    @Test
    public void testJTemp() throws Exception {

        Config.max_scalar_operator_optimize_depth = -100;

        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMER, "optimizer");
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);

        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/lzh_dump_file"));
        //        System.out.println(replayPair.second);

        String ss = Tracers.printScopeTimer();
        System.out.println(ss);

        String sql = "select cast(`t36`.`__fcol_923` as date) as `__fcol_969`, `t36`.`__fcol_918` as `__fcol_970`, `t36`.`__fcol_917` as `__fcol_971`, `t36`.`__fcol_919` as `__fcol_972`, `t36`.`__fcol_938` as `__fcol_973`, `t36`.`__fcol_911` as `__fcol_974`, `t36`.`__fcol_916` as `__fcol_975`, `t36`.`__fcol_914` as `__fcol_976`, `t36`.`__fcol_913` as `__fcol_977`, `t36`.`__fcol_915` as `__fcol_978`, `t36`.`__fcol_921` as `__fcol_979`, `t36`.`__fcol_941` as `__fcol_980`, cast(`t36`.`__fcol_920` as date) as `__fcol_982`, `t36`.`__fcol_912` as `__fcol_983`, `t36`.`__fcol_922` as `__fcol_984`, `t36`.`__fcol_942` as `__fcol_985`, `t36`.`__fcol_944` as `__fcol_986`, `t36`.`__fcol_936` as `__fcol_987`, `t36`.`__fcol_935` as `__fcol_988`, `t36`.`__fcol_928` as `__fcol_989`, `t36`.`__fcol_940` as `__fcol_990`, `t36`.`__fcol_927` as `__fcol_991`, `t36`.`__fcol_925` as `__fcol_992`, `t36`.`__fcol_930` as `__fcol_993`, `t36`.`__fcol_924` as `__fcol_994`, `t36`.`__fcol_929` as `__fcol_995`, `t36`.`__fcol_937` as `__fcol_996`, `t36`.`__fcol_931` as `__fcol_997`, `t36`.`__fcol_932` as `__fcol_998`, `t36`.`__fcol_933` as `__fcol_999`, `t36`.`__fcol_926` as `__fcol_1000`, `t36`.`__fcol_934` as `__fcol_1001`, `t36`.`__fcol_939` as `__fcol_1002`, `t36`.`__fcol_943` as `__fcol_1003`, `t36`.`__fcol_948` as `__fcol_1004`, `t36`.`__fcol_947` as `__fcol_1005`, `t36`.`__fcol_949` as `__fcol_1006`, `t36`.`__fcol_946` as `__fcol_1007`, `t36`.`__fcol_951` as `__fcol_1008`, `t36`.`__fcol_952` as `__fcol_1009`, `t36`.`__fcol_953` as `__fcol_1010`, `t36`.`__fcol_964` as `__fcol_1011`, `t36`.`__fcol_960` as `__fcol_1012`, `t36`.`__fcol_957` as `__fcol_1013`, `t36`.`__fcol_950` as `__fcol_1014`, `t36`.`__fcol_967` as `__fcol_1015`, `t36`.`__fcol_945` as `__fcol_1016` from ( select `t35`.`__fcol_859` as `__fcol_911`, `t35`.`__fcol_860` as `__fcol_912`, `t35`.`__fcol_861` as `__fcol_913`, `t35`.`__fcol_862` as `__fcol_914`, `t35`.`__fcol_863` as `__fcol_915`, `t35`.`__fcol_865` as `__fcol_916`, `t35`.`__fcol_867` as `__fcol_917`, `t35`.`__fcol_868` as `__fcol_918`, `t35`.`__fcol_869` as `__fcol_919`, `t35`.`__fcol_872` as `__fcol_920`, `t35`.`__fcol_874` as `__fcol_921`, `t35`.`__fcol_875` as `__fcol_922`, `t35`.`__fcol_876` as `__fcol_923`, `t35`.`__fcol_877` as `__fcol_924`, `t35`.`__fcol_878` as `__fcol_925`, `t35`.`__fcol_879` as `__fcol_926`, `t35`.`__fcol_880` as `__fcol_927`, `t35`.`__fcol_881` as `__fcol_928`, `t35`.`__fcol_882` as `__fcol_929`, `t35`.`__fcol_883` as `__fcol_930`, `t35`.`__fcol_884` as `__fcol_931`, `t35`.`__fcol_885` as `__fcol_932`, `t35`.`__fcol_886` as `__fcol_933`, `t35`.`__fcol_887` as `__fcol_934`, `t35`.`__fcol_888` as `__fcol_935`, `t35`.`__fcol_889` as `__fcol_936`, `t35`.`__fcol_890` as `__fcol_937`, `t35`.`__fcol_891` as `__fcol_938`, `t35`.`__fcol_892` as `__fcol_939`, `t35`.`__fcol_893` as `__fcol_940`, `t35`.`__fcol_894` as `__fcol_941`, `t35`.`__fcol_895` as `__fcol_942`, `t35`.`__fcol_896` as `__fcol_943`, `t35`.`__fcol_897` as `__fcol_944`, `t35`.`__fcol_898` as `__fcol_945`, `t35`.`__fcol_900` as `__fcol_946`, `t35`.`__fcol_901` as `__fcol_947`, `t35`.`__fcol_902` as `__fcol_948`, `t35`.`__fcol_903` as `__fcol_949`, `t35`.`__fcol_905` as `__fcol_950`, `t35`.`__fcol_906` as `__fcol_951`, `t35`.`__fcol_907` as `__fcol_952`, `t35`.`__fcol_910` as `__fcol_953`, case when ( `t35`.`__fcol_873` = '现车' and `t35`.`__fcol_891` >= '2025-02-12' and `t35`.`__fcol_866` <> '2025' ) then case when 1.13 = 0.0 then null else (((case when 2 = 0 then null else ((((case when `t35`.`__fcol_899` = 0 then null else ((((`t35`.`__fcol_899` - 20000) + 0.0)) / `t35`.`__fcol_899`) end * `t35`.`__fcol_907`) + 0.0)) / 2) end + 0.0)) / 1.13) end else 0 end as `__fcol_957`, case when `t35`.`__fcol_865` in ( '问界 M9 五座 增程 Max', '问界 M9 五座 增程 Ultra', '问界 M9 五座 纯电 Ultra' ) then -1327.43 else 0.0 end as `__fcol_960`, case when ( `t35`.`__fcol_864` in ('M7') and `t35`.`__fcol_866` not in ('2025') and `t35`.`__fcol_870` >= timestamp('2025-01-16 00:00:00.0') ) then 5309.73 else 0.0 end as `__fcol_964`, case when ( `t35`.`__fcol_865` in ( '问界 新M5 增程 Max', '问界 新M5 纯电 Max', '问界 新M5 增程 Max RS' ) and ( `t35`.`__fcol_870` >= timestamp('2025-04-30 00:00:00.0') or `t35`.`__fcol_871` >= timestamp('2025-04-30 00:00:00.0') or `t35`.`__fcol_891` like '%空%' ) ) then ((`t35`.`__fcol_907` + `t35`.`__fcol_903`) * 0.5) else 0.0 end as `__fcol_967` from ( select `t34`.`__fcol_812` as `__fcol_859`, `t34`.`__fcol_813` as `__fcol_860`, `t34`.`__fcol_814` as `__fcol_861`, `t34`.`__fcol_815` as `__fcol_862`, `t34`.`__fcol_816` as `__fcol_863`, `t34`.`__fcol_817` as `__fcol_864`, `t34`.`__fcol_818` as `__fcol_865`, `t34`.`__fcol_819` as `__fcol_866`, `t34`.`__fcol_820` as `__fcol_867`, `t34`.`__fcol_821` as `__fcol_868`, `t34`.`__fcol_822` as `__fcol_869`, `t34`.`__fcol_823` as `__fcol_870`, `t34`.`__fcol_824` as `__fcol_871`, `t34`.`__fcol_825` as `__fcol_872`, `t34`.`__fcol_826` as `__fcol_873`, `t34`.`__fcol_827` as `__fcol_874`, `t34`.`__fcol_828` as `__fcol_875`, `t34`.`__fcol_829` as `__fcol_876`, `t34`.`__fcol_830` as `__fcol_877`, `t34`.`__fcol_831` as `__fcol_878`, `t34`.`__fcol_832` as `__fcol_879`, `t34`.`__fcol_833` as `__fcol_880`, `t34`.`__fcol_834` as `__fcol_881`, `t34`.`__fcol_835` as `__fcol_882`, `t34`.`__fcol_836` as `__fcol_883`, `t34`.`__fcol_837` as `__fcol_884`, `t34`.`__fcol_838` as `__fcol_885`, `t34`.`__fcol_839` as `__fcol_886`, `t34`.`__fcol_840` as `__fcol_887`, `t34`.`__fcol_841` as `__fcol_888`, `t34`.`__fcol_842` as `__fcol_889`, `t34`.`__fcol_843` as `__fcol_890`, `t34`.`__fcol_845` as `__fcol_891`, `t34`.`__fcol_846` as `__fcol_892`, `t34`.`__fcol_847` as `__fcol_893`, `t34`.`__fcol_848` as `__fcol_894`, `t34`.`__fcol_849` as `__fcol_895`, `t34`.`__fcol_850` as `__fcol_896`, `t34`.`__fcol_851` as `__fcol_897`, `t34`.`__fcol_852` as `__fcol_898`, `t34`.`__fcol_853` as `__fcol_899`, `t34`.`__fcol_854` as `__fcol_900`, `t34`.`__fcol_855` as `__fcol_901`, `t34`.`__fcol_858` as `__fcol_902`, case when 1.13 = 0.0 then null else (((`t34`.`__fcol_855` + 0.0)) / 1.13) end as `__fcol_903`, case when ( `t34`.`__fcol_817` = 'M9' and `t34`.`__fcol_845` >= '2025-03-01' and `t34`.`__fcol_819` <> '2025' and ( case when '现金优惠' = '' then 1 else locate('现金优惠', `t34`.`__fcol_844`) end <> 0 or `t34`.`__fcol_826` = '现车' ) ) then case when 1.13 = 0.0 then null else ((((10000 * 0.5) + 0.0)) / 1.13) end else 0 end as `__fcol_905`, case when `t34`.`__fcol_819` <= '2022' then 0 else case when ( `t34`.`__fcol_817` = 'M5' or `t34`.`__fcol_817` = 'M7' ) then case when 1.13 = 0.0 then null else ((((((((((`t34`.`__fcol_842` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.4) + ((`t34`.`__fcol_841` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.4)) + ((`t34`.`__fcol_847` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.4)) + ((`t34`.`__fcol_830` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.4)) + ((`t34`.`__fcol_843` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.4)) + ((`t34`.`__fcol_831` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.4)) + ((`t34`.`__fcol_832` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.15) + 0.0)) / 1.13) end else case when `t34`.`__fcol_817` = 'M9' then case when 1.13 = 0.0 then null else (((((((((((((`t34`.`__fcol_842` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3) + ((`t34`.`__fcol_841` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3)) + ((`t34`.`__fcol_847` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.5)) + ((`t34`.`__fcol_836` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.5)) + ((`t34`.`__fcol_835` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.5)) + ((`t34`.`__fcol_834` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3)) + ((`t34`.`__fcol_833` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3)) + ((`t34`.`__fcol_839` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3)) + ((`t34`.`__fcol_840` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3)) + (((`t34`.`__fcol_837` + `t34`.`__fcol_838`) * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.15) + 0.0)) / 1.13) end else case when 1.13 = 0.0 then null else (((((((((((`t34`.`__fcol_842` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3) + ((`t34`.`__fcol_841` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3)) + ((`t34`.`__fcol_847` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.5)) + ((`t34`.`__fcol_831` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.5)) + ((`t34`.`__fcol_835` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.5)) + ((`t34`.`__fcol_834` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.3)) + (((`t34`.`__fcol_837` + `t34`.`__fcol_838`) * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.15)) + ((`t34`.`__fcol_846` * (1 - case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end)) * 0.5) + 0.0)) / 1.13) end end end end as `__fcol_906`, case when ( `t34`.`__fcol_817` = 'M5' or `t34`.`__fcol_817` = 'M7' ) then case when 1.13 = 0.0 then null else ((((((((((`t34`.`__fcol_842` + `t34`.`__fcol_841`) + `t34`.`__fcol_847`) + `t34`.`__fcol_830`) + `t34`.`__fcol_843`) + `t34`.`__fcol_831`) * case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end) * 0.5) + ((`t34`.`__fcol_832` * case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end) * 0.65) + 0.0)) / 1.13) end else case when 1.13 = 0.0 then null else (((((((((((`t34`.`__fcol_842` + `t34`.`__fcol_841`) + `t34`.`__fcol_834`) + `t34`.`__fcol_833`) + `t34`.`__fcol_839`) + `t34`.`__fcol_840`) * case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end) * 0.5) + ((((((`t34`.`__fcol_847` + `t34`.`__fcol_836`) + `t34`.`__fcol_835`) + `t34`.`__fcol_846`) + `t34`.`__fcol_831`) * case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end) * 0.3)) + (((`t34`.`__fcol_837` + `t34`.`__fcol_838`) * case when `t34`.`__fcol_849` = 0 then null else (((`t34`.`__fcol_854` + 0.0)) / `t34`.`__fcol_849`) end) * 0.65) + 0.0)) / 1.13) end end as `__fcol_907`, case when `t34`.`__fcol_818` in ( '后驱智驾版（M5智驾款EVR）', '后驱智驾版（M5智驾款EV）', '四驱智驾版（M5智驾款EVR）', '四驱智驾版（M5智驾款EV）' ) then -1300.0 else 0.0 end as `__fcol_910` from ( select `t33`.`__fcol_764` as `__fcol_812`, `t33`.`__fcol_765` as `__fcol_813`, `t33`.`__fcol_766` as `__fcol_814`, `t33`.`__fcol_767` as `__fcol_815`, `t33`.`__fcol_768` as `__fcol_816`, `t33`.`__fcol_769` as `__fcol_817`, `t33`.`__fcol_770` as `__fcol_818`, `t33`.`__fcol_771` as `__fcol_819`, `t33`.`__fcol_772` as `__fcol_820`, `t33`.`__fcol_773` as `__fcol_821`, `t33`.`__fcol_774` as `__fcol_822`, `t33`.`__fcol_775` as `__fcol_823`, `t33`.`__fcol_776` as `__fcol_824`, `t33`.`__fcol_777` as `__fcol_825`, `t33`.`__fcol_778` as `__fcol_826`, `t33`.`__fcol_779` as `__fcol_827`, `t33`.`__fcol_780` as `__fcol_828`, `t33`.`__fcol_781` as `__fcol_829`, `t33`.`__fcol_782` as `__fcol_830`, `t33`.`__fcol_783` as `__fcol_831`, `t33`.`__fcol_784` as `__fcol_832`, `t33`.`__fcol_785` as `__fcol_833`, `t33`.`__fcol_786` as `__fcol_834`, `t33`.`__fcol_787` as `__fcol_835`, `t33`.`__fcol_788` as `__fcol_836`, `t33`.`__fcol_789` as `__fcol_837`, `t33`.`__fcol_790` as `__fcol_838`, `t33`.`__fcol_791` as `__fcol_839`, `t33`.`__fcol_792` as `__fcol_840`, `t33`.`__fcol_793` as `__fcol_841`, `t33`.`__fcol_794` as `__fcol_842`, `t33`.`__fcol_795` as `__fcol_843`, `t33`.`__fcol_796` as `__fcol_844`, `t33`.`__fcol_797` as `__fcol_845`, `t33`.`__fcol_800` as `__fcol_846`, `t33`.`__fcol_801` as `__fcol_847`, `t33`.`__fcol_802` as `__fcol_848`, `t33`.`__fcol_803` as `__fcol_849`, `t33`.`__fcol_804` as `__fcol_850`, `t33`.`__fcol_805` as `__fcol_851`, `t33`.`__fcol_806` as `__fcol_852`, `t33`.`__fcol_807` as `__fcol_853`, `t33`.`__fcol_811` as `__fcol_854`, case when `t33`.`__fcol_771` = '2022' then 0 else case when ( `t33`.`__fcol_769` <> 'M9' and `t33`.`__fcol_797` < '2024-01-01' ) then 0 else (`t33`.`__fcol_805` - `t33`.`__fcol_811`) end end as `__fcol_855`, case when 1.13 = 0.0 then null else ((((`t33`.`__fcol_798` * `t33`.`__fcol_799`) + 0.0)) / 1.13) end as `__fcol_858` from ( select `t32`.`__fcol_717` as `__fcol_764`, `t32`.`__fcol_718` as `__fcol_765`, `t32`.`__fcol_719` as `__fcol_766`, `t32`.`__fcol_720` as `__fcol_767`, `t32`.`__fcol_721` as `__fcol_768`, `t32`.`__fcol_722` as `__fcol_769`, `t32`.`__fcol_723` as `__fcol_770`, `t32`.`__fcol_724` as `__fcol_771`, `t32`.`__fcol_725` as `__fcol_772`, `t32`.`__fcol_726` as `__fcol_773`, `t32`.`__fcol_727` as `__fcol_774`, `t32`.`__fcol_728` as `__fcol_775`, `t32`.`__fcol_729` as `__fcol_776`, `t32`.`__fcol_730` as `__fcol_777`, `t32`.`__fcol_731` as `__fcol_778`, `t32`.`__fcol_732` as `__fcol_779`, `t32`.`__fcol_733` as `__fcol_780`, `t32`.`__fcol_734` as `__fcol_781`, `t32`.`__fcol_735` as `__fcol_782`, `t32`.`__fcol_736` as `__fcol_783`, `t32`.`__fcol_737` as `__fcol_784`, `t32`.`__fcol_738` as `__fcol_785`, `t32`.`__fcol_739` as `__fcol_786`, `t32`.`__fcol_740` as `__fcol_787`, `t32`.`__fcol_741` as `__fcol_788`, `t32`.`__fcol_742` as `__fcol_789`, `t32`.`__fcol_743` as `__fcol_790`, `t32`.`__fcol_744` as `__fcol_791`, `t32`.`__fcol_745` as `__fcol_792`, `t32`.`__fcol_746` as `__fcol_793`, `t32`.`__fcol_747` as `__fcol_794`, `t32`.`__fcol_748` as `__fcol_795`, `t32`.`__fcol_749` as `__fcol_796`, `t32`.`__fcol_750` as `__fcol_797`, `t32`.`__fcol_751` as `__fcol_798`, `t32`.`__fcol_752` as `__fcol_799`, `t32`.`__fcol_753` as `__fcol_800`, `t32`.`__fcol_754` as `__fcol_801`, `t32`.`__fcol_755` as `__fcol_802`, `t32`.`__fcol_756` as `__fcol_803`, `t32`.`__fcol_757` as `__fcol_804`, `t32`.`__fcol_758` as `__fcol_805`, `t32`.`__fcol_759` as `__fcol_806`, `t32`.`__fcol_761` as `__fcol_807`, case when (((`t32`.`__fcol_760` + `t32`.`__fcol_762`) + `t32`.`__fcol_761`) + `t32`.`__fcol_763`) > `t32`.`__fcol_756` then `t32`.`__fcol_756` else (((`t32`.`__fcol_760` + `t32`.`__fcol_762`) + `t32`.`__fcol_761`) + `t32`.`__fcol_763`) end as `__fcol_811` from ( select `t31`.`__fcol_657` as `__fcol_717`, `t31`.`__fcol_658` as `__fcol_718`, `t31`.`__fcol_659` as `__fcol_719`, `t31`.`__fcol_660` as `__fcol_720`, `t31`.`__fcol_661` as `__fcol_721`, `t31`.`__fcol_662` as `__fcol_722`, `t31`.`__fcol_663` as `__fcol_723`, `t31`.`__fcol_664` as `__fcol_724`, `t31`.`__fcol_665` as `__fcol_725`, `t31`.`__fcol_666` as `__fcol_726`, `t31`.`__fcol_667` as `__fcol_727`, `t31`.`__fcol_668` as `__fcol_728`, `t31`.`__fcol_669` as `__fcol_729`, `t31`.`__fcol_670` as `__fcol_730`, `t31`.`__fcol_671` as `__fcol_731`, `t31`.`__fcol_672` as `__fcol_732`, `t31`.`__fcol_673` as `__fcol_733`, `t31`.`__fcol_674` as `__fcol_734`, `t31`.`__fcol_675` as `__fcol_735`, `t31`.`__fcol_676` as `__fcol_736`, `t31`.`__fcol_677` as `__fcol_737`, `t31`.`__fcol_678` as `__fcol_738`, `t31`.`__fcol_679` as `__fcol_739`, `t31`.`__fcol_680` as `__fcol_740`, `t31`.`__fcol_681` as `__fcol_741`, `t31`.`__fcol_682` as `__fcol_742`, `t31`.`__fcol_683` as `__fcol_743`, `t31`.`__fcol_684` as `__fcol_744`, `t31`.`__fcol_685` as `__fcol_745`, `t31`.`__fcol_686` as `__fcol_746`, `t31`.`__fcol_687` as `__fcol_747`, `t31`.`__fcol_688` as `__fcol_748`, `t31`.`__fcol_689` as `__fcol_749`, `t31`.`__fcol_690` as `__fcol_750`, `t31`.`__fcol_691` as `__fcol_751`, `t31`.`__fcol_692` as `__fcol_752`, `t31`.`__fcol_693` as `__fcol_753`, `t31`.`__fcol_694` as `__fcol_754`, `t31`.`__fcol_695` as `__fcol_755`, `t31`.`__fcol_696` as `__fcol_756`, `t31`.`__fcol_697` as `__fcol_757`, `t31`.`__fcol_700` as `__fcol_758`, `t31`.`__fcol_716` as `__fcol_759`, case when `t31`.`__fcol_696` = 0 then 0 else case when `t31`.`__fcol_662` <> 'M5' then 0 else case when `t31`.`__fcol_700` <= 0 then 0 else case when `t31`.`__fcol_716` = '选配全赠送' then `t31`.`__fcol_696` else case when ( `t31`.`__fcol_716` = '赠0.6万选配金+1.5万科技包' and `t31`.`__fcol_696` >= 21000 ) then 21000 else case when ( `t31`.`__fcol_716` = '赠0.6万选配金+1.5万科技包' and `t31`.`__fcol_696` = 15000 ) then 15000 else case when `t31`.`__fcol_716` = '赠0.6万选配金' then 6000 else case when ( `t31`.`__fcol_716` = '赠1.2万选配金' and `t31`.`__fcol_696` >= 12000 ) then 12000 else case when ( `t31`.`__fcol_716` = '赠1.2万选配金' and `t31`.`__fcol_696` < 12000 ) then `t31`.`__fcol_696` else 0 end end end end end end end end end as `__fcol_760`, case when `t31`.`__fcol_662` <> 'M9' then 0 else case when ( `t31`.`__fcol_700` <= 0 or `t31`.`__fcol_696` = 0 ) then 0 else case when `t31`.`__fcol_664` = '2025' then 0 else case when `t31`.`__fcol_690` >= '2025-03-16' then `t31`.`__fcol_696` else case when ( `t31`.`__fcol_696` >= 40000 and case when '内购' = '' then 1 else locate('内购', `t31`.`__fcol_689`) end <> 0 and `t31`.`__fcol_716` = '赠送1万选配金+送电池1万' ) then 40000 else case when ( `t31`.`__fcol_696` < 40000 and `t31`.`__fcol_696` > 30000 and case when '内购' = '' then 1 else locate('内购', `t31`.`__fcol_689`) end <> 0 and `t31`.`__fcol_716` = '赠送1万选配金+送电池1万' ) then `t31`.`__fcol_696` else case when ( `t31`.`__fcol_696` >= 30000 and case when '内购' = '' then 1 else locate('内购', `t31`.`__fcol_689`) end <> 0 ) then 30000 else case when ( `t31`.`__fcol_696` < 30000 and case when '内购' = '' then 1 else locate('内购', `t31`.`__fcol_689`) end <> 0 ) then `t31`.`__fcol_696` else case when ( `t31`.`__fcol_696` >= 30000 and `t31`.`__fcol_716` = '赠送1万选配金+送电池1万' ) then 20000 else case when ( `t31`.`__fcol_696` < 30000 and `t31`.`__fcol_716` = '赠送1万选配金+送电池1万' ) then ((`t31`.`__fcol_696` - `t31`.`__fcol_678`) + 10000) else case when ( `t31`.`__fcol_696` >= 20000 and `t31`.`__fcol_700` >= 20000 and `t31`.`__fcol_716` = '赠送2万选配金' ) then 20000 else case when ( `t31`.`__fcol_696` < 20000 and `t31`.`__fcol_716` = '赠送2万选配金' ) then `t31`.`__fcol_696` else case when ( `t31`.`__fcol_696` >= 30000 and `t31`.`__fcol_700` >= 30000 and `t31`.`__fcol_716` = '赠送3万选配金' ) then 30000 else case when ( `t31`.`__fcol_696` < 30000 and `t31`.`__fcol_716` = '赠送3万选配金' ) then `t31`.`__fcol_696` else case when ( `t31`.`__fcol_696` >= 10000 and `t31`.`__fcol_716` = '赠送1万选配金' ) then 10000 else case when `t31`.`__fcol_696` > `t31`.`__fcol_700` then `t31`.`__fcol_700` else `t31`.`__fcol_696` end end end end end end end end end end end end end end end end as `__fcol_761`, case when `t31`.`__fcol_696` = 0 then 0 else case when `t31`.`__fcol_662` <> 'M7' then 0 else case when `t31`.`__fcol_700` <= 0 then 0 else case when `t31`.`__fcol_663` = 'M7 Plus 四驱版（5座）' then ((`t31`.`__fcol_687` + `t31`.`__fcol_686`) + `t31`.`__fcol_694`) else case when case when 'M7 Plus 后驱版' = '' then 1 else locate('M7 Plus 后驱版', `t31`.`__fcol_663`) end <> 0 then ((`t31`.`__fcol_687` + `t31`.`__fcol_686`) + `t31`.`__fcol_688`) else 12000 end end end end end as `__fcol_762`, case when `t31`.`__fcol_716` = '赠送2000黑曜轮毂' then 0 else 0 end as `__fcol_763` from ( select `t30`.`__fcol_613` as `__fcol_657`, `t30`.`__fcol_614` as `__fcol_658`, `t30`.`__fcol_616` as `__fcol_659`, `t30`.`__fcol_617` as `__fcol_660`, `t30`.`__fcol_618` as `__fcol_661`, `t30`.`__fcol_619` as `__fcol_662`, `t30`.`__fcol_621` as `__fcol_663`, `t30`.`__fcol_622` as `__fcol_664`, `t30`.`__fcol_623` as `__fcol_665`, `t30`.`__fcol_624` as `__fcol_666`, `t30`.`__fcol_625` as `__fcol_667`, `t30`.`__fcol_626` as `__fcol_668`, `t30`.`__fcol_627` as `__fcol_669`, `t30`.`__fcol_628` as `__fcol_670`, `t30`.`__fcol_629` as `__fcol_671`, `t30`.`__fcol_631` as `__fcol_672`, `t30`.`__fcol_632` as `__fcol_673`, `t30`.`__fcol_633` as `__fcol_674`, `t30`.`__fcol_634` as `__fcol_675`, `t30`.`__fcol_635` as `__fcol_676`, `t30`.`__fcol_636` as `__fcol_677`, `t30`.`__fcol_637` as `__fcol_678`, `t30`.`__fcol_638` as `__fcol_679`, `t30`.`__fcol_639` as `__fcol_680`, `t30`.`__fcol_640` as `__fcol_681`, `t30`.`__fcol_641` as `__fcol_682`, `t30`.`__fcol_642` as `__fcol_683`, `t30`.`__fcol_643` as `__fcol_684`, `t30`.`__fcol_644` as `__fcol_685`, `t30`.`__fcol_645` as `__fcol_686`, `t30`.`__fcol_646` as `__fcol_687`, `t30`.`__fcol_647` as `__fcol_688`, `t30`.`__fcol_648` as `__fcol_689`, `t30`.`__fcol_649` as `__fcol_690`, `t30`.`__fcol_650` as `__fcol_691`, `t30`.`__fcol_651` as `__fcol_692`, `t30`.`__fcol_652` as `__fcol_693`, `t30`.`__fcol_653` as `__fcol_694`, `t30`.`__fcol_654` as `__fcol_695`, `t30`.`__fcol_655` as `__fcol_696`, `t30`.`__fcol_656` as `__fcol_697`, (`t30`.`__fcol_656` - case when `t30`.`__fcol_628` > cast(str_to_date(concat( cast(2025 as varchar), '-', cast(6 as varchar), '-', cast(1 as varchar) ), '%Y-%m-%d') as datetime) then `t30`.`__fcol_615` else case when `t30`.`__fcol_628` is null then `t30`.`__fcol_615` else `t30`.`__fcol_630` end end) as `__fcol_700`, case when ( `t30`.`__fcol_621` in ( '问界 新M5 增程 Max', '问界 新M5 纯电 Max' ) and `t30`.`__fcol_626` >= timestamp('2024-11-01 00:00:00.0') and `t30`.`__fcol_626` < timestamp('2025-04-30 00:00:00.0') and `t30`.`__fcol_618` like '%科技%' ) then '赠0.6万选配金+1.5万科技包' when ( ( `t30`.`__fcol_619` in ('M5') and `t30`.`__fcol_622` in ('2025') ) or ( ( `t30`.`__fcol_621` in ('问界 新M5 增程 Max RS') or ( `t30`.`__fcol_621` in ( '问界 新M5 纯电 Max', '问界 新M5 增程 Max' ) and `t30`.`__fcol_618` not like '%科技%' ) ) and `t30`.`__fcol_626` < timestamp('2025-04-30 00:00:00.0') ) ) then '赠0.6万选配金' when ( ( `t30`.`__fcol_621` in ( '问界 新M5 增程 Max', '问界 新M5 纯电 Max' ) and ( `t30`.`__fcol_626` < timestamp('2024-11-01 00:00:00.0') or `t30`.`__fcol_649` like '%空%' ) ) or `t30`.`__fcol_621` like '%新M7%' ) then '赠1.2万选配金' when ( ( `t30`.`__fcol_619` in ('M5') and `t30`.`__fcol_622` in ('2023') ) or ( `t30`.`__fcol_621` in ( 'M7 Plus 后驱版（5座）', 'M7 Plus 后驱版（6座）' ) and `t30`.`__fcol_618` like '%科技%' and ( `t30`.`__fcol_626` >= timestamp('2024-05-01 00:00:00.0') or `t30`.`__fcol_649` like '%空%' ) ) ) then '赠送内外饰+科技包' when ( ( `t30`.`__fcol_619` in ('M7') and `t30`.`__fcol_622` in ('2023') and `t30`.`__fcol_621` in ( 'M7 Plus 后驱版（5座）', 'M7 Plus 后驱版（6座）', 'M7 Max 后驱智驾版（5座）', 'M7 Max 后驱智驾版（6座）', 'M7 Max 四驱智驾版（5座）', 'M7 Max 四驱智驾版（6座）' ) and `t30`.`__fcol_626` < timestamp('2024-05-01 00:00:00.0') ) or ( `t30`.`__fcol_621` in ( 'M7 Plus 后驱版（5座）', 'M7 Plus 后驱版（6座）' ) and ( `t30`.`__fcol_618` not like '%科技%' or `t30`.`__fcol_618` = '' or `t30`.`__fcol_618` is null ) and ( `t30`.`__fcol_626` >= timestamp('2024-05-01 00:00:00.0') or `t30`.`__fcol_649` like '%空%' ) ) or `t30`.`__fcol_621` in ( 'M7 Max 后驱智驾版（5座）', 'M7 Max 后驱智驾版（6座）', 'M7 Max 四驱智驾版（5座）', 'M7 Max 四驱智驾版（6座）' ) or ( `t30`.`__fcol_621` in ('M7 Plus 四驱版（5座）') and ( `t30`.`__fcol_618` not like '%21%' or `t30`.`__fcol_618` = '' or `t30`.`__fcol_618` is null ) ) ) then '赠送内外饰' when ( `t30`.`__fcol_619` in ('M7') and `t30`.`__fcol_622` in ('2023') and `t30`.`__fcol_621` in ('M7 Plus 四驱版（5座）') and `t30`.`__fcol_618` like '%21%' ) then '赠送内外饰+赠轮毂' when ( `t30`.`__fcol_619` in ('M9') and `t30`.`__fcol_626` >= timestamp('2024-11-01 00:00:00.0') and `t30`.`__fcol_626` < timestamp('2024-12-01 00:00:00.0') and `t30`.`__fcol_618` like '%52度电池%' and `t30`.`__fcol_621` like '%六座%' ) then '赠送1万选配金+送电池1万' when ( ( `t30`.`__fcol_619` in ('M9') and `t30`.`__fcol_621` like '%六座%' and `t30`.`__fcol_626` < timestamp('2024-11-01 00:00:00.0') ) or ( `t30`.`__fcol_621` like '%六座%' and ( `t30`.`__fcol_618` not like '%52度电池%' or `t30`.`__fcol_618` = '' or `t30`.`__fcol_618` is null ) and `t30`.`__fcol_626` >= timestamp('2024-11-01 00:00:00.0') and `t30`.`__fcol_626` < timestamp('2024-12-01 00:00:00.0') ) or ( `t30`.`__fcol_621` in ( '问界 M9 五座 增程 Max', '问界 M9 五座 增程 Ultra', '问界 M9 五座 纯电 Ultra', '六座增程 Max', '六座增程 Ultra', '六座纯电 Ultra' ) and `t30`.`__fcol_626` < timestamp('2024-12-01 00:00:00.0') ) ) then '赠送1万选配金' when ( ( `t30`.`__fcol_619` in ('M9') and ( ( `t30`.`__fcol_626` >= timestamp('2024-12-01 00:00:00.0') and `t30`.`__fcol_626` < timestamp('2025-02-12 00:00:00.0') ) or ( `t30`.`__fcol_626` >= timestamp('2025-02-12 00:00:00.0') and `t30`.`__fcol_626` < timestamp('2025-03-01 00:00:00.0') and `t30`.`__fcol_648` not like '%现车%' ) ) ) or ( `t30`.`__fcol_619` in ('M9') and ( ( `t30`.`__fcol_626` >= timestamp('2024-11-06 00:00:00.0') and `t30`.`__fcol_626` < timestamp('2025-02-12 00:00:00.0') ) or ( `t30`.`__fcol_627` >= timestamp('2024-11-06 00:00:00.0') and `t30`.`__fcol_627` < timestamp('2025-02-12 00:00:00.0') ) ) and `t30`.`__fcol_632` in ('华为大客户') ) ) then '赠送2万选配金' when ( ( `t30`.`__fcol_619` in ('M9') and `t30`.`__fcol_622` in ( '2023', '2022', '2024' ) and `t30`.`__fcol_626` >= timestamp('2025-03-01 00:00:00.0') and `t30`.`__fcol_626` < timestamp('2025-03-16 00:00:00.0') ) or `t30`.`__fcol_619` in ('M9') or ( `t30`.`__fcol_619` in ('M9') and `t30`.`__fcol_626` >= timestamp('2025-02-12 00:00:00.0') and `t30`.`__fcol_626` < timestamp('2025-03-01 00:00:00.0') and `t30`.`__fcol_648` like '%现车%' ) ) then '赠送3万选配金' when ( ( `t30`.`__fcol_620` in ( '问界M9 EV', '问界M9' ) and `t30`.`__fcol_622` in ('2025') ) or `t30`.`__fcol_620` in ('问界M8') ) then '选配无赠送' when ( `t30`.`__fcol_620` in ('问界M8') and `t30`.`__fcol_618` like '%黑曜%' and ( `t30`.`__fcol_626` >= timestamp('2025-04-16 00:00:00.0') or `t30`.`__fcol_649` in ('空') ) ) then '赠送2000黑曜轮毂' when ( ( `t30`.`__fcol_620` in ( '问界M9 EV', '问界M9' ) and `t30`.`__fcol_622` in ( '2022', '2024', '2023' ) and ( `t30`.`__fcol_626` >= timestamp('2025-03-16 00:00:00.0') or `t30`.`__fcol_649` in ('空') ) ) or ( `t30`.`__fcol_621` in ( '问界 新M5 增程 Max', '问界 新M5 纯电 Max', '问界 新M5 增程 Max RS' ) and ( `t30`.`__fcol_626` >= timestamp('2025-04-30 00:00:00.0') or `t30`.`__fcol_627` >= timestamp('2025-04-30 00:00:00.0') or `t30`.`__fcol_649` like '%空%' ) ) ) then '选配全赠送' else '其他' end as `__fcol_716` from ( select `t29`.`__fcol_569` as `__fcol_613`, `t29`.`__fcol_570` as `__fcol_614`, `t29`.`__fcol_571` as `__fcol_615`, `t29`.`__fcol_572` as `__fcol_616`, `t29`.`__fcol_573` as `__fcol_617`, `t29`.`__fcol_574` as `__fcol_618`, `t29`.`__fcol_575` as `__fcol_619`, `t29`.`__fcol_576` as `__fcol_620`, `t29`.`__fcol_577` as `__fcol_621`, `t29`.`__fcol_578` as `__fcol_622`, `t29`.`__fcol_579` as `__fcol_623`, `t29`.`__fcol_580` as `__fcol_624`, `t29`.`__fcol_581` as `__fcol_625`, `t29`.`__fcol_582` as `__fcol_626`, `t29`.`__fcol_583` as `__fcol_627`, `t29`.`__fcol_584` as `__fcol_628`, `t29`.`__fcol_585` as `__fcol_629`, `t29`.`__fcol_586` as `__fcol_630`, `t29`.`__fcol_587` as `__fcol_631`, `t29`.`__fcol_588` as `__fcol_632`, `t29`.`__fcol_589` as `__fcol_633`, `t29`.`__fcol_590` as `__fcol_634`, `t29`.`__fcol_591` as `__fcol_635`, `t29`.`__fcol_592` as `__fcol_636`, `t29`.`__fcol_593` as `__fcol_637`, `t29`.`__fcol_594` as `__fcol_638`, `t29`.`__fcol_595` as `__fcol_639`, `t29`.`__fcol_596` as `__fcol_640`, `t29`.`__fcol_597` as `__fcol_641`, `t29`.`__fcol_598` as `__fcol_642`, `t29`.`__fcol_599` as `__fcol_643`, `t29`.`__fcol_600` as `__fcol_644`, `t29`.`__fcol_601` as `__fcol_645`, `t29`.`__fcol_602` as `__fcol_646`, `t29`.`__fcol_603` as `__fcol_647`, `t29`.`__fcol_604` as `__fcol_648`, `t29`.`__fcol_605` as `__fcol_649`, `t29`.`__fcol_606` as `__fcol_650`, `t29`.`__fcol_607` as `__fcol_651`, `t29`.`__fcol_608` as `__fcol_652`, `t29`.`__fcol_609` as `__fcol_653`, `t29`.`__fcol_610` as `__fcol_654`, `t29`.`__fcol_612` as `__fcol_655`, (`t29`.`__fcol_612` + `t29`.`__fcol_606`) as `__fcol_656` from ( select `t27`.`__fcol_487` as `__fcol_569`, `t27`.`__fcol_488` as `__fcol_570`, `t27`.`__fcol_489` as `__fcol_571`, `t27`.`__fcol_490` as `__fcol_572`, `t27`.`__fcol_491` as `__fcol_573`, `t27`.`__fcol_492` as `__fcol_574`, `t27`.`__fcol_493` as `__fcol_575`, `t27`.`__fcol_494` as `__fcol_576`, `t27`.`__fcol_495` as `__fcol_577`, `t27`.`__fcol_496` as `__fcol_578`, `t27`.`__fcol_497` as `__fcol_579`, `t27`.`__fcol_498` as `__fcol_580`, `t27`.`__fcol_499` as `__fcol_581`, `t27`.`__fcol_500` as `__fcol_582`, `t27`.`__fcol_501` as `__fcol_583`, `t27`.`__fcol_502` as `__fcol_584`, `t27`.`__fcol_503` as `__fcol_585`, `t27`.`__fcol_504` as `__fcol_586`, `t27`.`__fcol_505` as `__fcol_587`, `t27`.`__fcol_506` as `__fcol_588`, `t27`.`__fcol_524` as `__fcol_589`, `t27`.`__fcol_507` as `__fcol_590`, `t27`.`__fcol_508` as `__fcol_591`, `t27`.`__fcol_509` as `__fcol_592`, `t27`.`__fcol_510` as `__fcol_593`, `t27`.`__fcol_511` as `__fcol_594`, `t27`.`__fcol_512` as `__fcol_595`, `t27`.`__fcol_513` as `__fcol_596`, `t27`.`__fcol_514` as `__fcol_597`, `t27`.`__fcol_515` as `__fcol_598`, `t27`.`__fcol_516` as `__fcol_599`, `t27`.`__fcol_517` as `__fcol_600`, `t27`.`__fcol_518` as `__fcol_601`, `t27`.`__fcol_519` as `__fcol_602`, `t27`.`__fcol_520` as `__fcol_603`, `t27`.`__fcol_525` as `__fcol_604`, `t27`.`__fcol_526` as `__fcol_605`, `OS_CC276CF11A374040946D`.`field2` as `__fcol_606`, `OS_CC276CF11A374040946D`.`field3` as `__fcol_607`, `t27`.`__fcol_521` as `__fcol_608`, `t27`.`__fcol_522` as `__fcol_609`, case when `t27`.`__fcol_502` > cast(str_to_date(concat( cast(2025 as varchar), '-', cast(6 as varchar), '-', cast(1 as varchar) ), '%Y-%m-%d') as datetime) then `t27`.`__fcol_489` else case when `t27`.`__fcol_502` is null then `t27`.`__fcol_489` else `t27`.`__fcol_504` end end as `__fcol_610`, ((`t27`.`__fcol_519` + `t27`.`__fcol_518`) + `t27`.`__fcol_523`) as `__fcol_612` from ( select `t23`.`__fcol_406` as `__fcol_487`, `t23`.`__fcol_407` as `__fcol_488`, `t23`.`__fcol_408` as `__fcol_489`, `t23`.`__fcol_409` as `__fcol_490`, `t23`.`__fcol_410` as `__fcol_491`, `t23`.`__fcol_411` as `__fcol_492`, `t23`.`__fcol_412` as `__fcol_493`, `t23`.`__fcol_413` as `__fcol_494`, `t23`.`__fcol_414` as `__fcol_495`, `t23`.`__fcol_415` as `__fcol_496`, `t23`.`__fcol_416` as `__fcol_497`, `t23`.`__fcol_417` as `__fcol_498`, `t23`.`__fcol_418` as `__fcol_499`, `t23`.`__fcol_419` as `__fcol_500`, `t23`.`__fcol_420` as `__fcol_501`, `t23`.`__fcol_421` as `__fcol_502`, `t23`.`__fcol_422` as `__fcol_503`, `t23`.`__fcol_423` as `__fcol_504`, `t23`.`__fcol_424` as `__fcol_505`, `t23`.`__fcol_425` as `__fcol_506`, `t23`.`__fcol_426` as `__fcol_507`, `t23`.`__fcol_427` as `__fcol_508`, `t23`.`__fcol_428` as `__fcol_509`, `t23`.`__fcol_429` as `__fcol_510`, `t23`.`__fcol_430` as `__fcol_511`, `t23`.`__fcol_431` as `__fcol_512`, `t23`.`__fcol_432` as `__fcol_513`, `t23`.`__fcol_433` as `__fcol_514`, `t23`.`__fcol_434` as `__fcol_515`, `t23`.`__fcol_435` as `__fcol_516`, `t23`.`__fcol_436` as `__fcol_517`, `t23`.`__fcol_437` as `__fcol_518`, `t23`.`__fcol_438` as `__fcol_519`, `t23`.`__fcol_439` as `__fcol_520`, `t23`.`__fcol_440` as `__fcol_521`, `t23`.`__fcol_441` as `__fcol_522`, `t23`.`__fcol_442` as `__fcol_523`, `t23`.`__fcol_443` as `__fcol_524`, `t25`.`__fcol_447` as `__fcol_525`, case when `t23`.`__fcol_417` = 'PMALL' then left(date_format(`t23`.`__fcol_420`,'%Y-%m-%d %H:%i:%s'), 10) else case when `t23`.`__fcol_419` is null then '空' else left(date_format(`t23`.`__fcol_419`,'%Y-%m-%d %H:%i:%s'), 10) end end as `__fcol_526` from ( select `t19`.`__fcol_323` as `__fcol_406`, `t19`.`__fcol_324` as `__fcol_407`, `t19`.`__fcol_325` as `__fcol_408`, `t19`.`__fcol_326` as `__fcol_409`, `t19`.`__fcol_327` as `__fcol_410`, `t19`.`__fcol_328` as `__fcol_411`, `t19`.`__fcol_329` as `__fcol_412`, `t19`.`__fcol_330` as `__fcol_413`, `t19`.`__fcol_331` as `__fcol_414`, `t19`.`__fcol_332` as `__fcol_415`, `t19`.`__fcol_333` as `__fcol_416`, `t19`.`__fcol_334` as `__fcol_417`, `t19`.`__fcol_335` as `__fcol_418`, `t19`.`__fcol_336` as `__fcol_419`, `t19`.`__fcol_337` as `__fcol_420`, `t19`.`__fcol_338` as `__fcol_421`, `t19`.`__fcol_339` as `__fcol_422`, `t19`.`__fcol_340` as `__fcol_423`, `t19`.`__fcol_341` as `__fcol_424`, `t19`.`__fcol_342` as `__fcol_425`, `t19`.`__fcol_343` as `__fcol_426`, `t19`.`__fcol_344` as `__fcol_427`, `t19`.`__fcol_345` as `__fcol_428`, `t19`.`__fcol_346` as `__fcol_429`, `t19`.`__fcol_347` as `__fcol_430`, `t19`.`__fcol_348` as `__fcol_431`, `t19`.`__fcol_349` as `__fcol_432`, `t19`.`__fcol_350` as `__fcol_433`, `t19`.`__fcol_351` as `__fcol_434`, `t19`.`__fcol_352` as `__fcol_435`, `t19`.`__fcol_353` as `__fcol_436`, `t19`.`__fcol_354` as `__fcol_437`, `t19`.`__fcol_355` as `__fcol_438`, `t19`.`__fcol_356` as `__fcol_439`, `t19`.`__fcol_357` as `__fcol_440`, `t19`.`__fcol_358` as `__fcol_441`, `t19`.`__fcol_359` as `__fcol_442`, `t21`.`__fcol_366` as `__fcol_443` from ( select `t15`.`__fcol_236` as `__fcol_323`, `t15`.`__fcol_237` as `__fcol_324`, `t15`.`__fcol_238` as `__fcol_325`, `t15`.`__fcol_239` as `__fcol_326`, `t15`.`__fcol_240` as `__fcol_327`, `t15`.`__fcol_241` as `__fcol_328`, `t15`.`__fcol_242` as `__fcol_329`, `t15`.`__fcol_243` as `__fcol_330`, `t15`.`__fcol_244` as `__fcol_331`, `t15`.`__fcol_245` as `__fcol_332`, `t17`.`__fcol_275` as `__fcol_333`, `t17`.`__fcol_276` as `__fcol_334`, `t17`.`__fcol_277` as `__fcol_335`, `t17`.`__fcol_278` as `__fcol_336`, `t17`.`__fcol_280` as `__fcol_337`, `t17`.`__fcol_281` as `__fcol_338`, `t17`.`__fcol_282` as `__fcol_339`, `t17`.`__fcol_283` as `__fcol_340`, `t17`.`__fcol_284` as `__fcol_341`, `t17`.`__fcol_285` as `__fcol_342`, `t15`.`__fcol_246` as `__fcol_343`, `t15`.`__fcol_247` as `__fcol_344`, `t15`.`__fcol_248` as `__fcol_345`, `t15`.`__fcol_249` as `__fcol_346`, `t15`.`__fcol_250` as `__fcol_347`, `t15`.`__fcol_251` as `__fcol_348`, `t15`.`__fcol_252` as `__fcol_349`, `t15`.`__fcol_253` as `__fcol_350`, `t15`.`__fcol_254` as `__fcol_351`, `t15`.`__fcol_255` as `__fcol_352`, `t15`.`__fcol_256` as `__fcol_353`, `t15`.`__fcol_257` as `__fcol_354`, `t15`.`__fcol_258` as `__fcol_355`, `t15`.`__fcol_259` as `__fcol_356`, `t15`.`__fcol_260` as `__fcol_357`, `t15`.`__fcol_261` as `__fcol_358`, `t15`.`__fcol_262` as `__fcol_359` from ( select `t0`.`__fcol_0` as `__fcol_236`, `t0`.`__fcol_2` as `__fcol_237`, `t0`.`__fcol_3` as `__fcol_238`, `t13`.`__fcol_185` as `__fcol_239`, `t13`.`__fcol_186` as `__fcol_240`, `t13`.`__fcol_187` as `__fcol_241`, `t13`.`__fcol_188` as `__fcol_242`, `t13`.`__fcol_189` as `__fcol_243`, `t13`.`__fcol_190` as `__fcol_244`, `t13`.`__fcol_191` as `__fcol_245`, `t13`.`__fcol_192` as `__fcol_246`, `t13`.`__fcol_193` as `__fcol_247`, `t13`.`__fcol_194` as `__fcol_248`, `t13`.`__fcol_195` as `__fcol_249`, `t13`.`__fcol_196` as `__fcol_250`, `t13`.`__fcol_197` as `__fcol_251`, `t13`.`__fcol_198` as `__fcol_252`, `t13`.`__fcol_199` as `__fcol_253`, `t13`.`__fcol_200` as `__fcol_254`, `t13`.`__fcol_201` as `__fcol_255`, `t13`.`__fcol_202` as `__fcol_256`, `t13`.`__fcol_203` as `__fcol_257`, `t13`.`__fcol_204` as `__fcol_258`, `t13`.`__fcol_205` as `__fcol_259`, `t13`.`__fcol_206` as `__fcol_260`, `t13`.`__fcol_207` as `__fcol_261`, `t13`.`__fcol_208` as `__fcol_262` from ( select `T_A22E82CF513440DFB4D73`.`VIN` as `__fcol_0`, `T_A22E82CF513440DFB4D73`.`整车物料码` as `__fcol_1`, `T_A22E82CF513440DFB4D73`.`销售公司代码` as `__fcol_2`, `T_A22E82CF513440DFB4D73`.`总价` as `__fcol_3` from (select t.order_no as \"批售单号\", --t.status as \"批售单状态代码\",\n t.status_desc as \"批售单状态\", t.vin as \"VIN\", --t.vehicle_type as \"车辆类型代码\",\n t.vehicle_type_desc as \"车辆类型\", t.company_code as \"结算厅店代码\", t.receiving_company_code as \"收货厅店代码\", t.submit_date as \"提报时间\", t.activation_date as \"激活时间\", t.audit_date as \"审核通过时间\", t.match_vehicle_date as \"配车时间\", t.delivery_notice_date as \"配车确认时间\", t.out_date as \"出库时间\", t.grou_board_date as \"组板时间\", t.departure_confirm_date as \"发运单确认时间\", t.acceptance_date as \"签收时间\", t.promise_date as \"交付计划确认时间\", t.promise_out_date as \"承诺出库时间\", t.so_type as \"批售类型\", t.receiving_address as \"收货地址\", t.receiving_province_name as \"收货省\", t.receiving_city_name as \"收货市\", t.receiving_county_name as \"收货区\", t.assign_no as \"分派单号\", t.assign_date as \"分派单分派时间\", t.material_code as \"整车物料码\", t.warehouse_name as \"库房名称\", t.is_invoiced as \"是否开票\", t.shipping_state_name as \"物流在途状态\", t.sales_company_code as \"销售公司代码\", t.last_operation_date as \"最新开票操作时间\", t.expect_out_date as \"期望出库日期\", t.order_amount as \"总价\", t.bank_name as \"银行名称\", t.capital_type as \"资金类型\", t.stock_dealer_code as \"库存厅店\", t.transfer_no as \"调拨单号\", t.shipping_time as \"物流定位时间\", t.order_type_desc as \"订单类型\", t.logistics_name as \"物流商名称\", t.plan_operate_time AS \"预排确认时间\", t.claim_time as \"认领时间\", t.source as \"操作终端\", t.activate_source as \"订单激活审核操作来源\", t.cancel_source as \"订单取消审核操作来源\", t.return_date as \"发运单回厂确认时间\", t.driver_name as \"驾驶员姓名\", t.xcx_detailed_address as \"小程序在途位置\" from aito_sa_ads.ads_sa_fact_so_main t where exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.role_type ='1') or exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.dealer_code = t.company_code) or exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.dealer_code = t.stock_dealer_code) ) as `T_A22E82CF513440DFB4D73` where ( `T_A22E82CF513440DFB4D73`.`是否开票` in ( '是', '否' ) and `T_A22E82CF513440DFB4D73`.`VIN` in ( 'LM8F7G495SA021145', 'LM8F7E892SA021375', 'LM8F7E891SA008312', 'LM8F7E997SB004449', 'LM8F7E894SA015688', 'LM8F7E894SA024648', 'LM8F7G392SA024876', 'LM8F7G492SE011620', 'LM8F7G491SA026231', 'LM8F7E893SA026004', 'LM8F7E893RA147545', 'LM8F7E890SA024663', 'LM8F7G492SA017196', 'LM8F7G495SE011398', 'LM8F7E995SA024706', 'LM8F7G494SA021525', 'LM8F7E895SA015621', 'LM8F7E898SA013586', 'LM8F7E899SA008431', 'LM8F7G399SE011695', 'LM8F7G394SE011734', 'LM8F7G394SE011684', 'LM8F7E992SA024498', 'LM8F7G493SA024562', 'LM8F7E893SA024852', 'LM8F7G491SA014256', 'LM8F7G492SA019014', 'LM8F7E898SA015631', 'LM8F7G497SA015539', 'LM8F7E897SA021257', 'LM8F7G496SE011667', 'LM8F7G493SA024917', 'LM8F7E897SA021260', 'LM8F7G491SA024799', 'LM8F7G494SA001534', 'LM8F7E897SE011420', 'LM8F7G495SE011417', 'LM8F7E893RA454617', 'LM8F7G496SA019078', 'LM8F7E894SA006893', 'LM8F7G497SA019137', 'LM8F7E897SA017368', 'LM8F7E991SA024704', 'LM8F7G490SA014216', 'LM8F7E897RA473204', 'LM8F7G394SA023289', 'LM8F7E89XRA452167', 'LM8F7G497SE011709', 'LM8F7E899SA015637', 'LM8F7E895SA005879', 'LM8F7E896SA024604', 'LM8F7E898SA023034', 'LM8F7E89XSA019373', 'LM8F7E890SA024646', 'LM8F7G493SA017062', 'LM8F7E99XSA021106', 'LM8F7E898SE011376', 'LM8F7E892SA024521', 'LM8F7E896SE011389', 'LM8F7G495SA024823', 'LM8F7E891SA023148', 'LM8F7G494SA023095', 'LM8F7G499SE011632', 'LM8F7E999SA022988', 'LM8F7G390SA024780', 'LM8F7E891SE013129', 'LM8F7G493SA011312', 'LM8F7G490SA026110', 'LM8F7E899SA021440', 'LM8F7E893SE012077', 'LM8F7G493SA026344', 'LM8F7E890SA002887', 'LM8F7G398SE011736', 'LM8F7G492SE012315', 'LM8F7G497SA001530', 'LM8F7G492SA026349', 'LM8F7G395SE011855', 'LM8F7G39XSE011690', 'LM8F7E996SE012122', 'LM8F7E89XSA026002', 'LM8F7E897SE011997', 'LM8F7E999SE012082', 'LM8F7E895SE012131', 'LM8F7G492SA026092', 'LM8F7G492SE011715', 'LM8F7G495SE011627', 'LM8F7E994SA026169', 'LM8F7G492SA026089', 'LM8F7G394SE011720', 'LM8F7G498SE011704', 'LM8F7G493SE011707', 'LM8F7E99XSA024703', 'LM8F7E892SA024714', 'LM8F7G493SE011917', 'LM8F7E890SA026252', 'LM8F7E991SE012531' ) and `T_A22E82CF513440DFB4D73`.`批售单状态` not in ( '审核驳回', '已取消' ) ) ) as `t0` left outer join ( select `t9`.`__fcol_130` as `__fcol_184`, `t9`.`__fcol_131` as `__fcol_185`, `t9`.`__fcol_132` as `__fcol_186`, `t9`.`__fcol_133` as `__fcol_187`, `t9`.`__fcol_134` as `__fcol_188`, `t9`.`__fcol_135` as `__fcol_189`, `t9`.`__fcol_136` as `__fcol_190`, `t9`.`__fcol_137` as `__fcol_191`, `t9`.`__fcol_139` as `__fcol_192`, `t9`.`__fcol_141` as `__fcol_193`, `t9`.`__fcol_142` as `__fcol_194`, `t9`.`__fcol_143` as `__fcol_195`, `t9`.`__fcol_144` as `__fcol_196`, `t9`.`__fcol_145` as `__fcol_197`, `t9`.`__fcol_146` as `__fcol_198`, `t9`.`__fcol_147` as `__fcol_199`, `t9`.`__fcol_148` as `__fcol_200`, `t9`.`__fcol_149` as `__fcol_201`, `t9`.`__fcol_150` as `__fcol_202`, `t9`.`__fcol_154` as `__fcol_203`, `t11`.`__fcol_158` as `__fcol_204`, `t9`.`__fcol_140` as `__fcol_205`, `t9`.`__fcol_151` as `__fcol_206`, `t9`.`__fcol_152` as `__fcol_207`, `t9`.`__fcol_153` as `__fcol_208` from ( select `t5`.`__fcol_76` as `__fcol_130`, `t5`.`__fcol_77` as `__fcol_131`, `t5`.`__fcol_78` as `__fcol_132`, `t5`.`__fcol_79` as `__fcol_133`, `t5`.`__fcol_80` as `__fcol_134`, `t5`.`__fcol_81` as `__fcol_135`, `t5`.`__fcol_82` as `__fcol_136`, `t5`.`__fcol_83` as `__fcol_137`, `t5`.`__fcol_84` as `__fcol_138`, `t5`.`__fcol_86` as `__fcol_139`, `t5`.`__fcol_87` as `__fcol_140`, `t5`.`__fcol_88` as `__fcol_141`, `t5`.`__fcol_89` as `__fcol_142`, `t5`.`__fcol_90` as `__fcol_143`, `t5`.`__fcol_91` as `__fcol_144`, `t5`.`__fcol_92` as `__fcol_145`, `t5`.`__fcol_93` as `__fcol_146`, `t5`.`__fcol_94` as `__fcol_147`, `t5`.`__fcol_95` as `__fcol_148`, `t5`.`__fcol_96` as `__fcol_149`, `t5`.`__fcol_97` as `__fcol_150`, `t5`.`__fcol_98` as `__fcol_151`, `t5`.`__fcol_99` as `__fcol_152`, `t5`.`__fcol_100` as `__fcol_153`, `t7`.`__fcol_104` as `__fcol_154` from ( select `t1`.`__fcol_4` as `__fcol_76`, `t1`.`__fcol_5` as `__fcol_77`, `t1`.`__fcol_6` as `__fcol_78`, `t1`.`__fcol_7` as `__fcol_79`, `t1`.`__fcol_8` as `__fcol_80`, `t1`.`__fcol_9` as `__fcol_81`, `t1`.`__fcol_10` as `__fcol_82`, `t1`.`__fcol_11` as `__fcol_83`, `t1`.`__fcol_14` as `__fcol_84`, `t1`.`__fcol_16` as `__fcol_85`, `t3`.`__fcol_36` as `__fcol_86`, `t3`.`__fcol_37` as `__fcol_87`, `t3`.`__fcol_38` as `__fcol_88`, `t3`.`__fcol_39` as `__fcol_89`, `t3`.`__fcol_40` as `__fcol_90`, `t3`.`__fcol_41` as `__fcol_91`, `t3`.`__fcol_42` as `__fcol_92`, `t3`.`__fcol_43` as `__fcol_93`, `t3`.`__fcol_44` as `__fcol_94`, `t3`.`__fcol_45` as `__fcol_95`, `t3`.`__fcol_46` as `__fcol_96`, `t3`.`__fcol_47` as `__fcol_97`, `t3`.`__fcol_48` as `__fcol_98`, `t3`.`__fcol_49` as `__fcol_99`, `t3`.`__fcol_50` as `__fcol_100` from ( select `T_83FF4AA2074E45D29A32E`.`整车代码` as `__fcol_4`, `T_83FF4AA2074E45D29A32E`.`外饰` as `__fcol_5`, `T_83FF4AA2074E45D29A32E`.`内饰` as `__fcol_6`, `T_83FF4AA2074E45D29A32E`.`选装包` as `__fcol_7`, `T_83FF4AA2074E45D29A32E`.`车系` as `__fcol_8`, `T_83FF4AA2074E45D29A32E`.`车型` as `__fcol_9`, `T_83FF4AA2074E45D29A32E`.`版型` as `__fcol_10`, `T_83FF4AA2074E45D29A32E`.`车型年` as `__fcol_11`, concat( `T_83FF4AA2074E45D29A32E`.`车型代码`, `T_83FF4AA2074E45D29A32E`.`内饰代码` ) as `__fcol_14`, concat( `T_83FF4AA2074E45D29A32E`.`车型代码`, `T_83FF4AA2074E45D29A32E`.`外饰代码` ) as `__fcol_16`, concat( `T_83FF4AA2074E45D29A32E`.`车型代码`, `T_83FF4AA2074E45D29A32E`.`选装包代码` ) as `__fcol_18` from (select t.material_code as \"整车代码\", t.base_material_code as \"车型代码\", t.out_color_code as \"外饰代码\", t.in_color_code as \"内饰代码\", t.package_code as \"选装包代码\", t.out_color_value as \"外饰\", t.in_color_value as \"内饰\", t.package_value as \"选装包\", t.car_series as \"车系\", t.cartype_platform as \"车型\", t.car_drive_type as \"版型\", t.model_year as \"车型年\", t.power_type as \"动力类型\", t.driving_power as \"驱动动力\", t.sale_area as \"销售区域\" from aito_sa_ads.ads_sa_dim_sale_material t ) as `T_83FF4AA2074E45D29A32E` ) as `t1` left outer join ( select `t2`.`__fcol_19` as `__fcol_35`, `t2`.`__fcol_20` as `__fcol_36`, `t2`.`__fcol_21` as `__fcol_37`, `t2`.`__fcol_22` as `__fcol_38`, `t2`.`__fcol_23` as `__fcol_39`, `t2`.`__fcol_24` as `__fcol_40`, `t2`.`__fcol_25` as `__fcol_41`, `t2`.`__fcol_26` as `__fcol_42`, `t2`.`__fcol_27` as `__fcol_43`, `t2`.`__fcol_28` as `__fcol_44`, `t2`.`__fcol_29` as `__fcol_45`, `t2`.`__fcol_30` as `__fcol_46`, `t2`.`__fcol_31` as `__fcol_47`, `t2`.`__fcol_32` as `__fcol_48`, `t2`.`__fcol_33` as `__fcol_49`, `t2`.`__fcol_34` as `__fcol_50` from ( select `OS_42769A2AC3AB44BDAB82`.`field2` as `__fcol_19`, sum(`OS_42769A2AC3AB44BDAB82`.`field25`) as `__fcol_20`, sum(`OS_42769A2AC3AB44BDAB82`.`field26`) as `__fcol_21`, sum(`OS_42769A2AC3AB44BDAB82`.`field27`) as `__fcol_22`, sum(`OS_42769A2AC3AB44BDAB82`.`field28`) as `__fcol_23`, sum(`OS_42769A2AC3AB44BDAB82`.`field30`) as `__fcol_24`, sum(`OS_42769A2AC3AB44BDAB82`.`field31`) as `__fcol_25`, sum(`OS_42769A2AC3AB44BDAB82`.`field32`) as `__fcol_26`, sum(`OS_42769A2AC3AB44BDAB82`.`field33`) as `__fcol_27`, sum(`OS_42769A2AC3AB44BDAB82`.`field34`) as `__fcol_28`, sum(`OS_42769A2AC3AB44BDAB82`.`field35`) as `__fcol_29`, sum(`OS_42769A2AC3AB44BDAB82`.`field36`) as `__fcol_30`, sum(`OS_42769A2AC3AB44BDAB82`.`field37`) as `__fcol_31`, sum(`OS_42769A2AC3AB44BDAB82`.`field40`) as `__fcol_32`, sum(`OS_42769A2AC3AB44BDAB82`.`field41`) as `__fcol_33`, sum(`OS_42769A2AC3AB44BDAB82`.`field43`) as `__fcol_34` from `T_BI_OS_42769A2AC3AB44BDAB821A1C1CE0C074` as `OS_42769A2AC3AB44BDAB82` group by 1 ) as `t2` ) as `t3` on `t1`.`__fcol_18` = `t3`.`__fcol_35` ) as `t5` left outer join ( select `t6`.`__fcol_101` as `__fcol_103`, `t6`.`__fcol_102` as `__fcol_104` from ( select `OS_4A648442FDBF488B9C71`.`field2` as `__fcol_101`, sum(`OS_4A648442FDBF488B9C71`.`field4`) as `__fcol_102` from `T_BI_OS_4A648442FDBF488B9C714CBB608E4E41` as `OS_4A648442FDBF488B9C71` group by 1 ) as `t6` ) as `t7` on `t5`.`__fcol_85` = `t7`.`__fcol_103` ) as `t9` left outer join ( select `t10`.`__fcol_155` as `__fcol_157`, `t10`.`__fcol_156` as `__fcol_158` from ( select `OS_825573B210B7490AA4C2`.`field2` as `__fcol_155`, sum(`OS_825573B210B7490AA4C2`.`field4`) as `__fcol_156` from `T_BI_OS_825573B210B7490AA4C2A5E42808ACC8` as `OS_825573B210B7490AA4C2` group by 1 ) as `t10` ) as `t11` on `t9`.`__fcol_138` = `t11`.`__fcol_157` ) as `t13` on `t0`.`__fcol_1` = `t13`.`__fcol_184` ) as `t15` left outer join ( select `t16`.`__fcol_263` as `__fcol_275`, `t16`.`__fcol_264` as `__fcol_276`, `t16`.`__fcol_265` as `__fcol_277`, `t16`.`__fcol_267` as `__fcol_278`, `t16`.`__fcol_266` as `__fcol_279`, `t16`.`__fcol_268` as `__fcol_280`, `t16`.`__fcol_269` as `__fcol_281`, `t16`.`__fcol_270` as `__fcol_282`, `t16`.`__fcol_271` as `__fcol_283`, `t16`.`__fcol_272` as `__fcol_284`, `t16`.`__fcol_273` as `__fcol_285` from ( select `T_5759443B85E344B8984E6`.`交车单号` as `__fcol_263`, `T_5759443B85E344B8984E6`.`订单来源` as `__fcol_264`, `T_5759443B85E344B8984E6`.`Vmall订单编号` as `__fcol_265`, `T_5759443B85E344B8984E6`.`VIN` as `__fcol_266`, `T_5759443B85E344B8984E6`.`大定支付时间` as `__fcol_267`, `T_5759443B85E344B8984E6`.`大定接收时间` as `__fcol_268`, `T_5759443B85E344B8984E6`.`交付时间` as `__fcol_269`, `T_5759443B85E344B8984E6`.`交车类型` as `__fcol_270`, `T_5759443B85E344B8984E6`.`优惠后总金额` as `__fcol_271`, `T_5759443B85E344B8984E6`.`商品名称` as `__fcol_272`, `T_5759443B85E344B8984E6`.`定单车辆类型` as `__fcol_273`, row_number() over (partition by `T_5759443B85E344B8984E6`.`VIN`) as `__fcol_274` from (select t.delivery_car_no as \"交车单号\", --t.delivery_status as \"交车单状态\",\n t.delivery_status_desc as \"交车单状态\", --t.whether_normal as \"是否正常提车\",\n t.whether_normal_desc as \"是否正常提车\", --t.handover_status as \"灰水池状态\",\n t.handover_status_desc as \"灰水池状态\", t.is_sleep as \"是否休眠\", t.channel as \"订单来源\", t.large_order_no as \"大定单号JK\", t.external_order_no as \"Vmall订单编号\", t.require_vin as \"现车VIN\", t.warehouse_name as \"仓库名称\", t.vin as \"VIN\", t.delivery_store_code as \"厅店代码\", t.registration_city as \"上牌城市\", t.material_code as \"整车物料码\", t.large_order_pay_time as \"大定支付时间\", t.created_at as \"大定接收时间\", t.saler_submit_time as \"销售顾问补充信息后提交时间\", t.max_update_time as \"分配门店时间\", t.min_operation_time as \"最新分配专员时间\", t.submitted_at_one as \"首次方案提交时间\", t.submitted_at_last as \"最新方案维护时间\", t.audited_at as \"最新方案审核时间\", t.audit_result as \"最新方案审核状态\", t.first_audited_at as \"首次方案审核通过时间\", t.delivery_commissioner as \"交付专员ID\", t.delivery_commissioner_name as \"交付专员姓名\", t.last_delivery_time as \"最新默认交车时间\", t.delivery_date as \"交付时间\", t.invoice_date as \"开票日期\", t.invoice_no as \"发票号码\", t.order_lock_time as \"订单锁定时间\", t.finance_type as \"是否金融贷款\", t.hw_sale_dealer_code as \"销售厅店代码HW\", t.sale_dealer_code as \"销售厅店代码\", t.lock_car_time as \"锁车时间\", --t.delivery_type as \"交车类型（期车/现车）\",\n t.delivery_type_name as \"交车类型\", t.activity_amount as \"优惠金额\", --t.delivery_plan as \"交付方案状态代码\",\n t.delivery_plan_desc as \"交付方案状态描述\", t.duty_main as \"责任主体\", t.first_operation_time as \"首次分配专员时间\", t.total_amount as \"优惠后总金额\", t.deposit_amount as \"小定金额\", t.final_deposit_amount as \"大定金额\", t.received_amount as \"已收定单金额\", t.item_name as \"商品名称\", t.customer_id as \"实销客户ID\", t.priority_delivery_code as \"VIP带新\", t.vehicle_type_desc as \"定单车辆类型\", t.cancel_sleep_time AS \"唤醒时间\", t.sleep_time AS \"休眠时间\", t.delivery_by as \"交车专员\", t.delivery_by_name as \"交车专员姓名\", --用户中心JK代码\n t.delivery_jk_store_code as \"首次大定厅店代码\", t.order_first_store_time as \"门店首次接收定单时间\", t.order_latest_store_time as \"门店最新接收定单时间\", t.week_push_hw_date as \"推送透明化日期\" from aito_sa_ads.ads_sa_fact_do_main t where exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.role_type ='1') or exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.dealer_code = t.delivery_store_code) ) as `T_5759443B85E344B8984E6` ) as `t16` where `t16`.`__fcol_274` = 1 ) as `t17` on `t15`.`__fcol_236` = `t17`.`__fcol_279` where ( ( `t17`.`__fcol_276` is null or `t17`.`__fcol_276` in ( 'PMALL', 'HW', 'JKSales' ) ) and 1 = 1 ) ) as `t19` left outer join ( select `t20`.`__fcol_360` as `__fcol_364`, `t20`.`__fcol_361` as `__fcol_365`, `t20`.`__fcol_362` as `__fcol_366` from ( select `T_6555D002334D490E9E45B`.`VIN` as `__fcol_360`, `T_6555D002334D490E9E45B`.`单据类型` as `__fcol_361`, `T_6555D002334D490E9E45B`.`开票日期` as `__fcol_362`, row_number() over (partition by `T_6555D002334D490E9E45B`.`VIN`) as `__fcol_363` from (select t.invoice_id as \"ID\", t.vin as \"VIN\", --t.bill_type as \"单据类型\",\nt.bill_type_desc as \"单据类型\", --t.invoice_type as \"票据类型\",\nt.invoice_type_desc as \"票据类型\", --t.make_invoice_type as \"开票类型\",\nt.make_invoice_type_desc as \"开票类型\", t.invoice_code as \"发票代码\", t.invoice_no as \"发票号码\", t.invoice_date as \"开票日期\", t.sales_code as \"卖方代码\", t.sales_name as \"卖方名称\", --t.sales_tel as \"卖方电话\",\n--t.sales_address as \"卖方地址\",\n--t.sales_bank as \"卖方银行\",\n--t.sales_account as \"卖方账户\",\n--t.sales_tax_no as \"卖方纳税人识别号\",\nt.buyer_code as \"购方编码\", t.buyer_name as \"购方名称\", --t.buyer_tel as \"购方电话\",\n--t.buyer_address as \"购方地址\",\n--t.buyer_bank as \"购方银行\",\n--t.buyer_account as \"购方账户\",\n--t.buyer_tax_no as \"买方纳税人识别号\",\nt.detail_name as \"货物或应税劳务、服务名称\", t.vehicle_type as \"车辆类型\", t.factory_signal as \"厂牌型号\", t.producer as \"产地\", t.certificate_no as \"合格证号\", t.import_certificate_no as \"进口证明书号\", t.business_check_no as \"商检单号\", t.engine_no as \"发动机号码\", t.factory_name as \"生产企业名称\", t.tax_certificate_no as \"完税凭证号码\", t.tonnage as \"吨位\", t.people_number as \"限乘人数\", --t.total_amount as \"含税价\",\n--t.tax_rate as \"税率\",\n--t.exclude_tax_amount as \"不含税价\",\nt.drawer_name as \"开票人\", t.order_no as \"批售单号\", t.nums as \"开票数量\", t.material_code as \"物料码\", t.dealer_code as \"厅店代码\" from aito_sa_ads.ads_sa_fact_so_make_invoice t where exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.role_type ='1') or exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.dealer_code = t.dealer_code) ) as `T_6555D002334D490E9E45B` where `T_6555D002334D490E9E45B`.`单据类型` in ( '内部销售', '销售', '销售手工' ) ) as `t20` where `t20`.`__fcol_363` = 1 ) as `t21` on `t19`.`__fcol_323` = `t21`.`__fcol_364` where ( `t21`.`__fcol_365` is null or `t21`.`__fcol_365` in ( '内部销售', '销售', '销售手工' ) ) ) as `t23` left outer join ( select `t24`.`__fcol_444` as `__fcol_446`, `t24`.`__fcol_445` as `__fcol_447` from ( select `T_22D080D312E14811B7F44`.`Vmall定单编号` as `__fcol_444`, left(array_join(array_sort(array_distinct(array_agg(`T_22D080D312E14811B7F44`.`优惠名称`))),'/'), 1000) as `__fcol_445` from (select t.promotion_id as \"ID\", t.sales_outlets_code as \"销售厅店\", t.delivery_store_code as \"交付厅店\", t.delivery_car_no as \"交车单号\", t.external_order_no as \"Vmall定单编号\", t.large_order_no as \"大定单号(JK)\", t.promotion_type as \"优惠类型\", t.promotion_name as \"优惠名称\", t.discount_fee as \"优惠金额\", t.promotion_desc as \"优惠详情\", t.created_at as \"创建时间\", t.updated_at as \"更新时间\" from aito_sa_ads.ads_sa_fact_do_order_promotion t where exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.role_type ='1') or exists (select 1 from aito_sa_ads.ads_sa_dim_sale_power_detail u1 where u1.user_code = '40930' and u1.dealer_code = t.delivery_store_code) ) as `T_22D080D312E14811B7F44` group by 1 ) as `t24` ) as `t25` on `t23`.`__fcol_418` = `t25`.`__fcol_446` ) as `t27` left outer join `T_BI_OS_CC276CF11A374040946DCF6856DF7815` as `OS_CC276CF11A374040946D` on `t27`.`__fcol_495` = `OS_CC276CF11A374040946D`.`field1` ) as `t29` ) as `t30` ) as `t31` ) as `t32` ) as `t33` ) as `t34` ) as `t35` ) as `t36` limit 1000001;\n";

        connectContext.setDumpInfo(null);

        Pair<HttpResponseStatus, String> statusAndRes =
                QueryDumper.dumpQuery("default_catalog", "aito_sa_ads", sql, true);
        System.out.println(statusAndRes.second);

        // write to file



    }

    @Ignore
    @Test
    public void testPushdownSubfield() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/pushdown_subfield");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
        Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(
                "get_json_string(107: mock_031, '$.\"fY21_Territory_Score__c\"')\n" +
                "  |  \n" +
                "  9:OlapScanNode\n" +
                "     TABLE: tbl_mock_103"));
    }

    @Test
    public void testEliminateConstantCTEAndNestLoopJoin() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/eliminate_nestloop_join");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
        Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  29:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----28:EXCHANGE\n" +
                "  |    \n" +
                "  18:Project\n" +
                "  |  <slot 119> : 119: mock_189\n" +
                "  |  \n" +
                "  17:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 286: mock_278 = 67: mock_216\n" +
                "  |  \n" +
                "  |----16:EXCHANGE\n" +
                "  |    \n" +
                "  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 6\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 28\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  27:Project\n" +
                "  |  <slot 603> : array_contains(601: array_agg, 'asdfasdfasdf')\n" +
                "  |  \n" +
                "  26:AGGREGATE (merge finalize)\n" +
                "  |  output: array_agg(601: array_agg)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  25:EXCHANGE"));
    }

    @Test
    public void testUnionWithEmptyInput() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/union_with_empty_input");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
        Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("" +
                "RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET"));
    }
}
