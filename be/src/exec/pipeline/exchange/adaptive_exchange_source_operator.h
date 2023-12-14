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

#pragma once

#include "exec/pipeline/adaptive/adaptive_fwd.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"

namespace starrocks::pipeline {

class AdaptiveExchangeSourceOperator : public SourceOperator {
public:
    AdaptiveExchangeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                   std::vector<std::shared_ptr<Operator>> exchange_sources,
                                   CollectStatsContextPtr collect_stats_ctx);

    ~AdaptiveExchangeSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    const int32_t _inner_dop;

    const std::vector<std::shared_ptr<Operator>> _exchange_sources;
    std::shared_ptr<DataStreamRecvr> _stream_recvr = nullptr;

    CollectStatsContextPtr _collect_stats_ctx;

    bool _is_finishing = false;
};

class AdaptiveExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AdaptiveExchangeSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                          std::shared_ptr<ExchangeSourceOperatorFactory> exchange_source_factory,
                                          CollectStatsContextPtr collect_stats_ctx);
    ~AdaptiveExchangeSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool could_local_shuffle() const override;
    TPartitionType::type partition_type() const override;

    SourceOperatorFactory::AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }

private:
    std::shared_ptr<ExchangeSourceOperatorFactory> _exchange_source_factory;
    CollectStatsContextPtr _collect_stats_ctx;
};

} // namespace starrocks::pipeline