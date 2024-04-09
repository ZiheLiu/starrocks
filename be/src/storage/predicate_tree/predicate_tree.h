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

#include "common/overloaded.h"
#include "storage/column_predicate.h"
#include "storage/predicate_tree/predicate_tree_fwd.h"

namespace starrocks {

class ColumnPredicate;

// ------------------------------------------------------------------------------------
// Utils
// ------------------------------------------------------------------------------------

using PredicateTreeNodeVariant = std::variant<PredicateTreeColumnNode, PredicateTreeAndNode, PredicateTreeOrNode>;

template <typename T>
concept PredicateTreeNodeType = std::is_same_v<T, PredicateTreeColumnNode> || std::is_same_v<T, PredicateTreeAndNode> ||
        std::is_same_v<T, PredicateTreeOrNode>;

// ------------------------------------------------------------------------------------
// CompoundNodeContext
// ------------------------------------------------------------------------------------

using PredicateTreeNodeId = uint32_t;
using ColumnPredicates = std::vector<const ColumnPredicate*>;
using ColumnPredicateMap = std::unordered_map<ColumnId, ColumnPredicates>;

struct CompoundNodeContext {
    template <CompoundNodeType Type>
    const ColumnPredicateMap& cid_to_column_preds(const PredicateTreeCompoundNode<Type>& node) const;

    std::vector<uint8_t> selection_buffer;
    std::vector<uint16_t> selected_idx_buffer;

    struct CompoundAndContext {
        std::vector<const PredicateTreeColumnNode*> non_vec_children;
        std::vector<const PredicateTreeNode*> vec_children;
        size_t num_evaluate_non_vec_times = 0;
    };
    std::optional<CompoundAndContext> and_context;

    mutable std::optional<ColumnPredicateMap> _cached_cid_to_column_preds;
};
using CompoundNodeContexts = std::vector<CompoundNodeContext>;

// ------------------------------------------------------------------------------------
// PredicateTreeNode Nodes
// ------------------------------------------------------------------------------------

class PredicateTreeBaseNode {
public:
    PredicateTreeNodeId id() const { return _id; }
    void set_id(PredicateTreeNodeId id) { _id = id; }

protected:
    /// The id of all the compound nodes is placed before the id of all the column nodes (leaf nodes).
    /// The id of each node is set after the PredicateTree is constructed.
    PredicateTreeNodeId _id = 0;
};

template <typename Derived>
class PredicateTreeBaseNodeHelper : public PredicateTreeBaseNode {
public:
    Status evaluate(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate(contexts, chunk, selection, 0, chunk->num_rows());
    }
    Status evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate_and(contexts, chunk, selection, 0, chunk->num_rows());
    }
    Status evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate_or(contexts, chunk, selection, 0, chunk->num_rows());
    }

private:
    Derived* derived() { return down_cast<Derived*>(this); }
    const Derived* derived() const { return down_cast<const Derived*>(this); }
};

class PredicateTreeColumnNode final : public PredicateTreeBaseNodeHelper<PredicateTreeColumnNode> {
public:
    explicit PredicateTreeColumnNode(const ColumnPredicate* col_pred) : _col_pred(DCHECK_NOTNULL(col_pred)) {}

    Status evaluate(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                    uint16_t to) const;
    Status evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                        uint16_t to) const;
    Status evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                       uint16_t to) const;

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
    using PredicateTreeNodes = std::vector<PredicateTreeNode>;

    template <PredicateTreeNodeType Node>
    void add_child(Node&& child);
    void add_child(PredicateTreeNode&& child);
    void add_child(const PredicateTreeNode& child);

    const PredicateTreeNodes& children() const { return _children; }
    PredicateTreeNodes& children() { return _children; }

    Status evaluate(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                    uint16_t to) const;
    Status evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                        uint16_t to) const;
    Status evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                       uint16_t to) const;

    std::string debug_string() const;

    using PredicateTreeBaseNodeHelper<PredicateTreeCompoundNode>::evaluate;

private:
    PredicateTreeNodes _children;
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

    template <typename Vistor>
    void preorder_traversal(Vistor&& vistor) const;
    template <typename Vistor>
    void preorder_traversal(Vistor&& vistor);

    template <typename Vistor>
    void partition_copy(Vistor&& cond, PredicateTreeNode* true_pred_tree, PredicateTreeNode* false_pred_tree) const;
    template <typename Vistor>
    void partition_move(Vistor&& cond, PredicateTreeNode* true_pred_tree, PredicateTreeNode* false_pred_tree);

    PredicateTreeNodeVariant node;
};

// ------------------------------------------------------------------------------------
// PredicateTree
// ------------------------------------------------------------------------------------

class PredicateTree {
public:
    PredicateTree() = default;
    static PredicateTree create(PredicateTreeNode&& root);

    Status evaluate(const Chunk* chunk, uint8_t* selection) const;
    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    const std::unordered_set<ColumnId>& column_ids() const;
    bool contains_column(ColumnId cid) const;
    size_t num_columns() const;

    size_t size() const;
    bool empty() const;

    const PredicateTreeNode& root() const { return _root; }
    PredicateTreeNode release_root();

    template <typename Vistor>
    auto visit(Vistor&& vistor) const {
        return std::visit(std::forward<Vistor>(vistor), _root.node);
    }
    template <typename Vistor>
    auto visit(Vistor&& vistor) {
        return std::visit(std::forward<Vistor>(vistor), _root.node);
    }

    template <typename Vistor>
    void preorder_traversal(Vistor&& vistor) const {
        _root.preorder_traversal(std::forward<Vistor>(vistor));
    }

    const CompoundNodeContext& compound_node_context(size_t idx) const { return _compound_node_contexts[idx]; }
    CompoundNodeContext& compound_node_context(size_t idx) { return _compound_node_contexts[idx]; }

private:
    explicit PredicateTree(PredicateTreeNode&& root, uint32_t num_compound_nodes);

private:
    PredicateTreeNode _root;

    mutable std::optional<std::unordered_set<ColumnId>> _cached_column_ids;

    CompoundNodeContexts _compound_node_contexts;
};

} // namespace starrocks
