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

#include "exec/pipeline/exchange/adaptive_exchange_source_operator.h"

#include "exec/pipeline/adaptive/collect_stats_context.h"
#include "glog/logging.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// ------------------------------------------------------------------------------------
// AdaptiveExchangeSourceOperator
// ------------------------------------------------------------------------------------
AdaptiveExchangeSourceOperator::AdaptiveExchangeSourceOperator(OperatorFactory* factory, int32_t id,
                                                               int32_t plan_node_id, int32_t driver_sequence,
                                                               std::vector<std::shared_ptr<Operator>> exchange_sources,
                                                               CollectStatsContextPtr collect_stats_ctx)
        : SourceOperator(factory, id, "adaptive_exchange_source", plan_node_id, false, driver_sequence),
          _inner_dop(exchange_sources.size()),
          _exchange_sources(std::move(exchange_sources)),
          _collect_stats_ctx(std::move(collect_stats_ctx)) {}

Status AdaptiveExchangeSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));

    for (auto& exchange_source : _exchange_sources) {
        RETURN_IF_ERROR(exchange_source->prepare(state));
    }

    _collect_stats_ctx->ref();
    for (int i = 0; i < _inner_dop; i++) {
        _collect_stats_ctx->incr_sinker();
    }

    _stream_recvr = static_cast<ExchangeSourceOperator*>(_exchange_sources[0].get())->_stream_recvr;

    return Status::OK();
}

bool AdaptiveExchangeSourceOperator::has_output() const {
    if (is_finished()) {
        return false;
    }

    if (!_stream_recvr->has_output_for_pipeline()) {
        return false;
    }

    for (const auto& exchange_source : _exchange_sources) {
        if (exchange_source->has_output()) {
            return true;
        }
    }

    for (int i = 0; i < _inner_dop; i++) {
        if (_collect_stats_ctx->is_upstream_finished(i)) {
            return true;
        }
    }

    return false;
}

bool AdaptiveExchangeSourceOperator::is_finished() const {
    return _is_finishing || _exchange_sources[0]->is_finished();
}

Status AdaptiveExchangeSourceOperator::set_finishing(RuntimeState* state) {
    _is_finishing = true;

    for (auto& exchange_source : _exchange_sources) {
        // TODO(liuzihe): will this block if return error?
        RETURN_IF_ERROR(exchange_source->set_finishing(state));
    }

    for (int i = 0; i < _inner_dop; i++) {
        RETURN_IF_ERROR(_collect_stats_ctx->set_finishing(_driver_sequence));
    }

    return Status::OK();
}

StatusOr<ChunkPtr> AdaptiveExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    bool has_pulled_chunk = true;
    while (has_pulled_chunk) {
        has_pulled_chunk = false;
        for (int i = 0; i < _inner_dop; i++) {
            auto& exchange_source = _exchange_sources[i];

            if (_collect_stats_ctx->is_upstream_finished(i)) {
                RETURN_IF_ERROR(exchange_source->set_finishing(state));
                continue;
            }

            while (exchange_source->has_output()) {
                ASSIGN_OR_RETURN(auto chunk, exchange_source->pull_chunk(state));
                RETURN_IF_ERROR(_collect_stats_ctx->push_chunk(i, chunk));
                has_pulled_chunk = true;
            }
        }
    }

    return nullptr;
}

// ------------------------------------------------------------------------------------
// AdaptiveExchangeSourceOperatorFactory
// ------------------------------------------------------------------------------------
AdaptiveExchangeSourceOperatorFactory::AdaptiveExchangeSourceOperatorFactory(
        int32_t id, int32_t plan_node_id, std::shared_ptr<ExchangeSourceOperatorFactory> exchange_source_factory,
        CollectStatsContextPtr collect_stats_ctx)
        : SourceOperatorFactory(id, "adaptive_exchange_source", plan_node_id),
          _exchange_source_factory(std::move(exchange_source_factory)),
          _collect_stats_ctx(std::move(collect_stats_ctx)) {}

OperatorPtr AdaptiveExchangeSourceOperatorFactory::create(int32_t dop, int32_t driver_sequence) {
    const int inner_dop = _exchange_source_factory->degree_of_parallelism();

    std::vector<std::shared_ptr<Operator>> exchange_sources;
    exchange_sources.reserve(inner_dop);
    for (int i = 0; i < inner_dop; i++) {
        exchange_sources.emplace_back(_exchange_source_factory->create(inner_dop, i));
    }

    return std::make_shared<AdaptiveExchangeSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                            std::move(exchange_sources), _collect_stats_ctx);
}

bool AdaptiveExchangeSourceOperatorFactory::could_local_shuffle() const {
    return _exchange_source_factory->could_local_shuffle();
}
TPartitionType::type AdaptiveExchangeSourceOperatorFactory::partition_type() const {
    return _exchange_source_factory->partition_type();
}

} // namespace starrocks::pipeline