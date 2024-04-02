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
#include "storage/predicate_tree.h"

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

template <CompoundNodeType Type>
const PredicateTreeCompoundNode<Type>::ColumnPredicateMap& PredicateTreeCompoundNode<Type>::cid_to_column_preds()
        const {
    if (_cached_cid_to_column_preds.has_value()) {
        return _cached_cid_to_column_preds.value();
    }

    auto& cid_to_column_preds = _cached_cid_to_column_preds.emplace();
    for (const auto& child : _children) {
        child.visit([&](const auto& node) {
            if constexpr (std::is_same_v<std::decay_t<decltype(node)>, PredicateTreeColumnNode>) {
                cid_to_column_preds[node.col_pred()->column_id()].emplace_back(node.col_pred());
            }
        });
    }
    return cid_to_column_preds;
}

struct ColumnPredsCollector {
    void operator()(const PredicateTreeColumnNode& node) const {
        const auto* col_pred = node.col_pred();
        const auto cid = col_pred->column_id();
        column_ids.emplace(cid);
    }

    template <CompoundNodeType Type>
    void operator()(const PredicateTreeCompoundNode<Type>& node) const {
        for (const auto& child : node.children()) {
            child.visit(*this);
        }
    }

    std::unordered_set<ColumnId>& column_ids;
};

template <CompoundNodeType Type>
const std::unordered_set<ColumnId>& PredicateTreeCompoundNode<Type>::column_ids() const {
    if (_cached_column_ids.has_value()) {
        return _cached_column_ids.value();
    }

    auto& all_column_ids = _cached_column_ids.emplace();
    ColumnPredsCollector collector{all_column_ids};
    for (const auto& child : _children) {
        child.visit(collector);
    }
    return all_column_ids;
}

template <CompoundNodeType Type>
bool PredicateTreeCompoundNode<Type>::contains_column(ColumnId cid) const {
    return column_ids().contains(cid);
}

template <CompoundNodeType Type>
size_t PredicateTreeCompoundNode<Type>::num_columns() const {
    return column_ids().size();
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate(const Chunk* chunk, uint8_t* selection,
                                                                         uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate_and(const Chunk* chunk, uint8_t* selection,
                                                                             uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate_or(const Chunk* chunk, uint8_t* selection,
                                                                            uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate(const Chunk* chunk, uint8_t* selection,
                                                                        uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate_and(const Chunk* chunk, uint8_t* selection,
                                                                            uint16_t from, uint16_t to) const;
template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate_or(const Chunk* chunk, uint8_t* selection,
                                                                           uint16_t from, uint16_t to) const;

// ------------------------------------------------------------------------------------
// PredicateTreeAndNode
// ------------------------------------------------------------------------------------

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate(const Chunk* chunk, uint8_t* selection,
                                                                         uint16_t from, uint16_t to) const {
    const auto num_rows = to - from;
    if (_children.empty()) {
        memset(selection + from, 1, num_rows);
        return Status::OK();
    }

    // Evaluate vectorized predicates first.
    bool first = true;
    std::vector<const PredicateTreeNode*> non_vec_children;
    non_vec_children.reserve(_children.size());
    for (const auto& child : _children) {
        const bool is_non_vec = child.visit(overloaded{
                [&](const PredicateTreeColumnNode& node) { return !node.col_pred()->can_vectorized(); },
                [&]<CompoundNodeType ChildType>(const PredicateTreeCompoundNode<ChildType>& node) { return false; },
        });
        if (is_non_vec) {
            non_vec_children.emplace_back(&child);
            continue;
        }

        if (first) {
            first = false;
            RETURN_IF_ERROR(child.visit([&](const auto& pred) { return pred.evaluate(chunk, selection, from, to); }));
        } else {
            RETURN_IF_ERROR(
                    child.visit([&](const auto& pred) { return pred.evaluate_and(chunk, selection, from, to); }));
        }
    }

    // Evaluate non-vectorized predicates using evaluate_branchless.
    if (!non_vec_children.empty()) {
        if (UNLIKELY(_selected_idx_buffer.size() < to)) {
            _selected_idx_buffer.resize(to);
        }
        auto* selected_idx = _selected_idx_buffer.data();

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

        for (const auto& child : non_vec_children) {
            const auto& col_pred = std::get<PredicateTreeColumnNode>(child->node);
            ASSIGN_OR_RETURN(selected_size, col_pred.evaluate_branchless(chunk, selected_idx, selected_size));
            if (selected_size == 0) {
                break;
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
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate_and(const Chunk* chunk, uint8_t* selection,
                                                                             uint16_t from, uint16_t to) const {
    for (auto& child : _children) {
        RETURN_IF_ERROR(child.visit([&](const auto& pred) { return pred.evaluate_and(chunk, selection, from, to); }));
    }
    return Status::OK();
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::AND>::evaluate_or(const Chunk* chunk, uint8_t* selection,
                                                                            uint16_t from, uint16_t to) const {
    if (_children.empty()) {
        return Status::OK();
    }

    if (UNLIKELY(_selection_buffer.size() < to)) {
        _selection_buffer.resize(to);
    }
    auto* or_selection = _selection_buffer.data();

    RETURN_IF_ERROR(evaluate(chunk, or_selection, from, to));
    for (int i = from; i < to; i++) {
        selection[i] |= or_selection[i];
    }

    return Status::OK();
}

// ------------------------------------------------------------------------------------
// PredicateTreeOrNode
// ------------------------------------------------------------------------------------

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate(const Chunk* chunk, uint8_t* selection,
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
            RETURN_IF_ERROR(child.visit([&](const auto& pred) { return pred.evaluate(chunk, selection, from, to); }));
        } else {
            RETURN_IF_ERROR(
                    child.visit([&](const auto& pred) { return pred.evaluate_or(chunk, selection, from, to); }));
        }

        const auto num_falses = SIMD::count_zero(selection + from, num_rows);
        if (!num_falses) {
            break;
        }
    }

    return Status::OK();
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate_and(const Chunk* chunk, uint8_t* selection,
                                                                            uint16_t from, uint16_t to) const {
    if (_children.empty()) {
        return Status::OK();
    }

    if (UNLIKELY(_selection_buffer.size() < to)) {
        _selection_buffer.resize(to);
    }
    auto* and_selection = _selection_buffer.data();

    RETURN_IF_ERROR(evaluate(chunk, and_selection, from, to));
    for (int i = from; i < to; i++) {
        selection[i] &= and_selection[i];
    }

    return Status::OK();
}

template <>
inline Status PredicateTreeCompoundNode<CompoundNodeType::OR>::evaluate_or(const Chunk* chunk, uint8_t* selection,
                                                                           uint16_t from, uint16_t to) const {
    for (auto& child : _children) {
        RETURN_IF_ERROR(child.visit([&](const auto& pred) { return pred.evaluate_or(chunk, selection, from, to); }));
    }
    return Status::OK();
}

// ------------------------------------------------------------------------------------
// PredicateTreeNode
// ------------------------------------------------------------------------------------

template <typename Vistor>
void PredicateTreeNode::shallow_partition_copy(Vistor&& cond, PredicateTreeNode* true_pred_tree,
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
void PredicateTreeNode::shallow_partition_move(Vistor&& cond, PredicateTreeNode* true_pred_tree,
                                               PredicateTreeNode* false_pred_tree) {
    visit(overloaded{
            [&](PredicateTreeColumnNode& node) {
                if (cond(node)) {
                    *true_pred_tree = PredicateTreeNode{std::move(node)};
                    *false_pred_tree = PredicateTreeNode{};
                } else {
                    *true_pred_tree = PredicateTreeNode{};
                    *false_pred_tree = PredicateTreeNode{std::move(node)};
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

} // namespace starrocks
