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

#include "gutil/strings/substitute.h"
#include "simd/simd.h"
#include "storage/predicate_tree/predicate_tree.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// PredicateTreeCompoundNode
// ------------------------------------------------------------------------------------

template <CompoundNodeType Type>
std::string PredicateTreeCompoundNode<Type>::debug_string() const {
    std::stringstream ss;
    if constexpr (Type == CompoundNodeType::AND) {
        ss << "{\"and\":[";
    } else {
        ss << "{\"or\":[";
    }
    size_t num_children = 0;
    for (const auto& child : _children) {
        ss << child.visit([](const auto& node) { return node.debug_string(); });
        if (++num_children < _children.size()) {
            ss << ",";
        }
    }
    ss << "]}";
    return ss.str();
}

template <CompoundNodeType Type>
void PredicateTreeCompoundNode<Type>::add_child(PredicateTreeNode&& child) {
    child.visit([&]<typename NodeType>(NodeType& node) {
        if constexpr (std::is_same_v<std::decay_t<NodeType>, PredicateTreeCompoundNode<Type>>) {
            _children.insert(_children.end(), std::make_move_iterator(node.children().begin()),
                             std::make_move_iterator(node.children().end()));
        } else {
            _children.emplace_back(std::move(child));
        }
    });
}

template <CompoundNodeType Type>
void PredicateTreeCompoundNode<Type>::add_child(const PredicateTreeNode& child) {
    add_child(PredicateTreeNode{child});
}

template <CompoundNodeType Type>
template <PredicateTreeNodeType Node>
void PredicateTreeCompoundNode<Type>::add_child(Node&& child) {
    add_child(PredicateTreeNode{std::forward<Node>(child)});
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate(CompoundNodeContexts& contexts,
                                                                         const Chunk* chunk, uint8_t* selection,
                                                                         uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate_and(CompoundNodeContexts& contexts,
                                                                             const Chunk* chunk, uint8_t* selection,
                                                                             uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate_or(CompoundNodeContexts& contexts,
                                                                            const Chunk* chunk, uint8_t* selection,
                                                                            uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate(CompoundNodeContexts& contexts,
                                                                        const Chunk* chunk, uint8_t* selection,
                                                                        uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate_and(CompoundNodeContexts& contexts,
                                                                            const Chunk* chunk, uint8_t* selection,
                                                                            uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate_or(CompoundNodeContexts& contexts,
                                                                           const Chunk* chunk, uint8_t* selection,
                                                                           uint16_t from, uint16_t to) const;

// ------------------------------------------------------------------------------------
// PredicateTreeAndNode
// ------------------------------------------------------------------------------------

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate(CompoundNodeContexts& contexts,
                                                                         const Chunk* chunk, uint8_t* selection,
                                                                         uint16_t from, uint16_t to) const {
    const auto num_rows = to - from;
    if (_children.empty()) {
        memset(selection + from, 1, num_rows);
        return Status::OK();
    }

    auto& node_ctx = contexts[id()];

    if (!node_ctx.and_context.has_value()) {
        auto& ctx = node_ctx.and_context.emplace();
        ctx.non_vec_children.reserve(_children.size());
        ctx.vec_children.reserve(_children.size());

        for (const auto& child : _children) {
            const auto* non_vec_child = child.visit(overloaded{
                    [&](const PredicateTreeColumnNode& child_node) -> const PredicateTreeColumnNode* {
                        return !child_node.col_pred()->can_vectorized() ? &child_node : nullptr;
                    },
                    [&]<CompoundNodeType ChildType>(const PredicateTreeCompoundNode<ChildType>& child_node)
                            -> const PredicateTreeColumnNode* { return nullptr; },
            });
            if (non_vec_child != nullptr) {
                ctx.non_vec_children.emplace_back(non_vec_child);
            } else {
                ctx.vec_children.emplace_back(&child);
            }
        }

        std::sort(ctx.non_vec_children.begin(), ctx.non_vec_children.end(),
                  [](const PredicateTreeColumnNode* lhs, const PredicateTreeColumnNode* rhs) {
                      return lhs->col_pred()->num_values() < rhs->col_pred()->num_values();
                  });
    }
    auto& ctx = node_ctx.and_context.value();

    // Evaluate vectorized predicates first.
    bool first = true;
    bool contains_true = true;
    for (const auto& child : ctx.vec_children) {
        if (first) {
            first = false;
            RETURN_IF_ERROR(child->visit(
                    [&](const auto& pred) { return pred.evaluate(contexts, chunk, selection, from, to); }));
        } else {
            RETURN_IF_ERROR(child->visit(
                    [&](const auto& pred) { return pred.evaluate_and(contexts, chunk, selection, from, to); }));
        }

        contains_true = SIMD::count_nonzero(selection + from, num_rows);
        if (!contains_true) {
            break;
        }
    }

    // Evaluate non-vectorized predicates using evaluate_branchless.
    if (contains_true && !ctx.non_vec_children.empty()) {
        auto& selected_idx_buffer = node_ctx.selected_idx_buffer;
        if (UNLIKELY(selected_idx_buffer.size() < to)) {
            selected_idx_buffer.resize(to);
        }
        auto* selected_idx = selected_idx_buffer.data();

        uint16_t selected_size = 0;
        if (first) {
            // When there is no any vectorized predicate, should initialize selected_idx in a vectorized way.
            selected_size = to - from;
            for (uint16_t i = from, j = 0; i < to; ++i, ++j) {
                selected_idx[j] = i;
            }
        } else {
            for (uint16_t i = from; i < to; ++i) {
                selected_idx[selected_size] = i;
                selected_size += selection[i];
            }
        }

        if (ctx.num_evaluate_non_vec_times++ % 32 == 0) {
            std::vector<size_t> filtered_rows(ctx.non_vec_children.size(), 0);
            size_t max_filtered_rows = 0;
            for (int i = 0; i < ctx.non_vec_children.size(); ++i) {
                const auto* col_pred = ctx.non_vec_children[i];
                auto prev_selected_size = selected_size;
                ASSIGN_OR_RETURN(selected_size, col_pred->evaluate_branchless(chunk, selected_idx, selected_size));
                filtered_rows[i] = prev_selected_size - selected_size;
                max_filtered_rows = std::max(max_filtered_rows, filtered_rows[i]);
                if (selected_size == 0) {
                    break;
                }
            }

            if (max_filtered_rows != filtered_rows[0]) {
                std::vector<size_t> idx(ctx.non_vec_children.size());
                std::iota(idx.begin(), idx.end(), 0);
                std::sort(idx.begin(), idx.end(),
                          [&](size_t lhs, size_t rhs) { return filtered_rows[lhs] > filtered_rows[rhs]; });
                std::vector<const PredicateTreeColumnNode*> sorted_non_vec_children(ctx.non_vec_children.size());
                for (int i = 0; i < ctx.non_vec_children.size(); ++i) {
                    sorted_non_vec_children[i] = ctx.non_vec_children[idx[i]];
                }
                ctx.non_vec_children = std::move(sorted_non_vec_children);
            }

        } else {
            for (const auto& col_pred : ctx.non_vec_children) {
                ASSIGN_OR_RETURN(selected_size, col_pred->evaluate_branchless(chunk, selected_idx, selected_size));
                if (selected_size == 0) {
                    break;
                }
            }
        }

        memset(&selection[from], 0, to - from);
        for (uint16_t i = 0; i < selected_size; ++i) {
            selection[selected_idx[i]] = 1;
        }
    }

    return Status::OK();
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate_and(CompoundNodeContexts& contexts,
                                                                             const Chunk* chunk, uint8_t* selection,
                                                                             uint16_t from, uint16_t to) const {
    for (auto& child : _children) {
        RETURN_IF_ERROR(
                child.visit([&](const auto& pred) { return pred.evaluate_and(contexts, chunk, selection, from, to); }));
    }
    return Status::OK();
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate_or(CompoundNodeContexts& contexts,
                                                                            const Chunk* chunk, uint8_t* selection,
                                                                            uint16_t from, uint16_t to) const {
    if (_children.empty()) {
        return Status::OK();
    }

    auto& node_ctx = contexts[id()];
    auto& selection_buffer = node_ctx.selection_buffer;

    if (UNLIKELY(selection_buffer.size() < to)) {
        selection_buffer.resize(to);
    }
    auto* or_selection = selection_buffer.data();

    RETURN_IF_ERROR(evaluate(contexts, chunk, or_selection, from, to));
    for (int i = from; i < to; i++) {
        selection[i] |= or_selection[i];
    }

    return Status::OK();
}

// ------------------------------------------------------------------------------------
// PredicateTreeOrNode
// ------------------------------------------------------------------------------------

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate(CompoundNodeContexts& contexts,
                                                                        const Chunk* chunk, uint8_t* selection,
                                                                        uint16_t from, uint16_t to) const {
    const auto num_rows = to - from;
    if (_children.empty()) {
        memset(selection + from, 1, num_rows);
        return Status::OK();
    }

    bool first = true;
    for (const auto& child : _children) {
        if (first) {
            first = false;
            RETURN_IF_ERROR(
                    child.visit([&](const auto& pred) { return pred.evaluate(contexts, chunk, selection, from, to); }));
        } else {
            RETURN_IF_ERROR(child.visit(
                    [&](const auto& pred) { return pred.evaluate_or(contexts, chunk, selection, from, to); }));
        }

        const auto num_falses = SIMD::count_zero(selection + from, num_rows);
        if (!num_falses) {
            break;
        }
    }

    return Status::OK();
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate_and(CompoundNodeContexts& contexts,
                                                                            const Chunk* chunk, uint8_t* selection,
                                                                            uint16_t from, uint16_t to) const {
    if (_children.empty()) {
        return Status::OK();
    }

    auto& node_ctx = contexts[id()];
    auto& selection_buffer = node_ctx.selection_buffer;

    if (UNLIKELY(selection_buffer.size() < to)) {
        selection_buffer.resize(to);
    }
    auto* and_selection = selection_buffer.data();

    RETURN_IF_ERROR(evaluate(contexts, chunk, and_selection, from, to));
    for (int i = from; i < to; i++) {
        selection[i] &= and_selection[i];
    }

    return Status::OK();
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate_or(CompoundNodeContexts& contexts,
                                                                           const Chunk* chunk, uint8_t* selection,
                                                                           uint16_t from, uint16_t to) const {
    for (auto& child : _children) {
        RETURN_IF_ERROR(
                child.visit([&](const auto& pred) { return pred.evaluate_or(contexts, chunk, selection, from, to); }));
    }
    return Status::OK();
}

// ------------------------------------------------------------------------------------
// PredicateTreeNode
// ------------------------------------------------------------------------------------

template <typename Vistor>
void PredicateTreeNode::partition_copy(Vistor&& cond, PredicateTreeNode* true_pred_tree,
                                       PredicateTreeNode* false_pred_tree) const {
    visit(overloaded{
            [&](const PredicateTreeColumnNode& node) {
                PredicateTreeNode new_node{node};
                if (cond(new_node)) {
                    *true_pred_tree = std::move(new_node);
                    *false_pred_tree = PredicateTreeNode{};
                } else {
                    *true_pred_tree = PredicateTreeNode{};
                    *false_pred_tree = std::move(new_node);
                }
            },
            [&]<CompoundNodeType Type>(const PredicateTreeCompoundNode<Type>& node) {
                *true_pred_tree = PredicateTreeNode{PredicateTreeCompoundNode<Type>{}};
                *false_pred_tree = PredicateTreeNode{PredicateTreeCompoundNode<Type>{}};
                for (const auto& child : node.children()) {
                    if (cond(child)) {
                        true_pred_tree->visit([&](auto& pred) { pred.add_child(child); });
                    } else {
                        false_pred_tree->visit([&](auto& pred) { pred.add_child(child); });
                    }
                }
            },
    });
}

template <typename Vistor>
void PredicateTreeNode::partition_move(Vistor&& cond, PredicateTreeNode* true_pred_tree,
                                       PredicateTreeNode* false_pred_tree) {
    visit(overloaded{
            [&](PredicateTreeColumnNode& node) {
                auto moved_node = PredicateTreeNode{std::move(node)};
                if (cond(moved_node)) {
                    *true_pred_tree = std::move(moved_node);
                    *false_pred_tree = PredicateTreeNode{};
                } else {
                    *true_pred_tree = PredicateTreeNode{};
                    *false_pred_tree = std::move(moved_node);
                }
            },
            [&]<CompoundNodeType Type>(PredicateTreeCompoundNode<Type>& node) {
                *true_pred_tree = PredicateTreeNode{PredicateTreeCompoundNode<Type>{}};
                *false_pred_tree = PredicateTreeNode{PredicateTreeCompoundNode<Type>{}};
                for (auto& child : node.children()) {
                    if (cond(child)) {
                        true_pred_tree->visit([&](auto& pred) { pred.add_child(std::move(child)); });
                    } else {
                        false_pred_tree->visit([&](auto& pred) { pred.add_child(std::move(child)); });
                    }
                }
            },
    });
}

// ------------------------------------------------------------------------------------
// CompoundNodeContext
// ------------------------------------------------------------------------------------

template <CompoundNodeType Type>
const ColumnPredicateMap& CompoundNodeContext::cid_to_column_preds(const PredicateTreeCompoundNode<Type>& node) const {
    if (_cached_cid_to_column_preds.has_value()) {
        return _cached_cid_to_column_preds.value();
    }

    auto& cid_to_column_preds = _cached_cid_to_column_preds.emplace();
    for (const auto& child : node.children()) {
        child.visit([&]<typename ChildType>(const ChildType& child_node) {
            if constexpr (std::is_same_v<std::decay_t<ChildType>, PredicateTreeColumnNode>) {
                cid_to_column_preds[child_node.col_pred()->column_id()].emplace_back(child_node.col_pred());
            }
        });
    }
    return cid_to_column_preds;
}

} // namespace starrocks
