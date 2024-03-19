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

#include <memory>
#include <set>
#include <vector>

#include "storage/column_predicate.h"
#include "storage/predicate_tree_fwd.h"

namespace starrocks {

class ColumnPredicate;

// ------------------------------------------------------------------------------------
// Utils
// ------------------------------------------------------------------------------------

template <typename... Base>
struct overloaded : Base... {
    using Base::operator()...;
};
template <typename... T>
overloaded(T...) -> overloaded<T...>;

using PredicateTreeNodeVariant = std::variant<PredicateTreeColumnNode, PredicateTreeAndNode, PredicateTreeOrNode>;

template <typename T>
concept PredicateTreeNodeType = std::is_same_v<T, PredicateTreeColumnNode> || std::is_same_v<T, PredicateTreeAndNode> ||
        std::is_same_v<T, PredicateTreeOrNode>;

// ------------------------------------------------------------------------------------
// PredicateTree Nodes
// ------------------------------------------------------------------------------------

class PredicateTreeBaseNode {};

template <typename Derived>
class PredicateTreeBaseNodeHelper : public PredicateTreeBaseNode {
public:
    Status evaluate(const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate(chunk, selection, 0, chunk->num_rows());
    }
    Status evaluate_and(const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate_and(chunk, selection, 0, chunk->num_rows());
    }
    Status evaluate_or(const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate_or(chunk, selection, 0, chunk->num_rows());
    }

private:
    Derived* derived() { return down_cast<Derived*>(this); }
    const Derived* derived() const { return down_cast<const Derived*>(this); }
};

class PredicateTreeColumnNode final : public PredicateTreeBaseNodeHelper<PredicateTreeColumnNode> {
public:
    explicit PredicateTreeColumnNode(const ColumnPredicate* col_pred) : _col_pred(DCHECK_NOTNULL(col_pred)) {}

    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;
    Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;
    Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    StatusOr<uint16_t> evaluate_branchless(const Chunk* chunk, uint16_t* sel, uint16_t sel_size) const;

    const ColumnPredicate* col_pred() const { return _col_pred; }
    void set_col_pred(const ColumnPredicate* col_pred) { _col_pred = col_pred; }

    void add_child(PredicateTreeNode&& child) { // Do nothing.
    }
    void add_child(const PredicateTreeNode& child) { // Do nothing.
    }
    template <PredicateTreeNodeType Node>
    void add_child(Node&& child) { // Do nothing.
    }

    std::string debug_string() const;

    using PredicateTreeBaseNodeHelper::evaluate;

private:
    const ColumnPredicate* _col_pred;
};

template <CompoundNodeType Type>
class PredicateTreeCompoundNode final : public PredicateTreeBaseNodeHelper<PredicateTreeCompoundNode<Type>> {
public:
    using ColumnPredicates = std::vector<const ColumnPredicate*>;
    using ColumnPredicateMap = std::unordered_map<ColumnId, ColumnPredicates>;
    using PredicateTreeNodes = std::vector<PredicateTreeNode>;

    template <PredicateTreeNodeType Node>
    void add_child(Node&& child);
    void add_child(PredicateTreeNode&& child);
    void add_child(const PredicateTreeNode& child);

    const PredicateTreeNodes& children() const { return _children; }
    PredicateTreeNodes& children() { return _children; }
    const ColumnPredicateMap& cid_to_column_preds() const;

    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;
    Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;
    Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    std::string debug_string() const;

    using PredicateTreeBaseNodeHelper<PredicateTreeCompoundNode>::evaluate;

private:
    PredicateTreeNodes _children;

    mutable std::vector<uint8_t> _selection_buffer;
    mutable std::vector<uint16_t> _selected_idx_buffer;

    mutable std::optional<ColumnPredicateMap> _cached_cid_to_column_preds;
};

// ------------------------------------------------------------------------------------
// PredicateTreeNode
// ------------------------------------------------------------------------------------

struct PredicateTreeNode {
    PredicateTreeNode() : node(PredicateTreeAndNode{}) {}

    explicit PredicateTreeNode(PredicateTreeNodeVariant&& node) : node(std::move(node)) {}

    template <PredicateTreeNodeType Node>
    explicit PredicateTreeNode(Node&& node) : node(std::forward<Node>(node)) {}

    template <typename Vistor>
    auto visit(Vistor&& vistor) const {
        return std::visit(std::forward<Vistor>(vistor), node);
    }

    template <typename Vistor>
    auto visit(Vistor&& vistor) {
        return std::visit(std::forward<Vistor>(vistor), node);
    }

    size_t size() const;
    bool empty() const;

    template <typename Vistor>
    void shallow_partition_copy(Vistor&& cond, PredicateTreeNode* true_pred_tree,
                                PredicateTreeNode* false_pred_tree) const;

    const std::map<ColumnId, std::vector<const ColumnPredicate*>>& column_preds() const;
    bool contains_column(ColumnId cid) const;
    size_t num_columns() const;

    /// ColumnPredicates are placed before CompoundPredicates, and ColumnPredicates with the same column_id are placed together.
    void sort_children();

    PredicateTreeNodeVariant node;

    mutable std::optional<std::map<ColumnId, std::vector<const ColumnPredicate*>>> _cached_column_preds;
};

} // namespace starrocks
