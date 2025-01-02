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
package com.starrocks.sql.optimizer.rule.join;

import com.google.api.client.util.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;

import java.util.List;

// JoinReorderFactory is used to choose join reorder algorithm in RBO phase,
// it is used by ReorderJoinRule.rewrite method, at present JoinReorderFactory
// provides two factory implementation:
// 1. factory for creating JoinReorderDummyStatistics, which used by AutoMV to eliminate
//    cross join;
// 2. factory for creating JoinReorderCardinalityPreserving, which used by table pruning
//    feature.
public interface JoinReorderFactory {
    List<JoinOrder> create(OptimizerContext context, OptExpression innerJoinRoot, MultiJoinNode multiJoinNode);

    // used by AutoMV to eliminate cross join.
    static JoinReorderFactory createJoinReorderDummyStatisticsFactory() {
        return (context, innerJoinRoot, multiJoinNode) -> List.of(new JoinReorderDummyStatistics(context));
    }

    // used by table pruning feature.
    static JoinReorderFactory createJoinReorderCardinalityPreserving() {
        return (context, innerJoinRoot, multiJoinNode) -> List.of(new JoinReorderCardinalityPreserving(context));
    }

    static JoinReorderFactory createJoinReorderAdaptive() {
        return (context, innerJoinRoot, multiJoinNode) -> {
            List<JoinOrder> algorithms = Lists.newArrayList();
            algorithms.add(new JoinReorderLeftDeep(context));

            // If there is no statistical information, the DP and greedy reorder algorithm are disabled,
            // and the query plan degenerates to the left deep tree
            if (Utils.hasUnknownColumnsStats(innerJoinRoot) &&
                    (!FeConstants.runningUnitTest || FeConstants.isReplayFromQueryDump)) {
                return algorithms;
            }

            SessionVariable sv = context.getSessionVariable();
            if (multiJoinNode.getAtoms().size() <= sv.getCboMaxReorderNodeUseDP() && sv.isCboEnableDPJoinReorder2()) {
                algorithms.add(new JoinReorderDP(context));
            }

            if (sv.isCboEnableGreedyJoinReorder2() &&
                    multiJoinNode.getAtoms().size() <= context.getSessionVariable().getCboMaxReorderNodeUseGreedy()) {
                algorithms.add(new JoinReorderGreedy(context));
            }

            return algorithms;
        };
    }

    static JoinReorderFactory createJoinReorderLeftDeep() {
        return (context, innerJoinRoot, multiJoinNode) -> List.of(new JoinReorderLeftDeep(context));
    }
}
