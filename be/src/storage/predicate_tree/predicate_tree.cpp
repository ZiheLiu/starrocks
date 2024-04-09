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

#include "storage/predicate_tree/predicate_tree.hpp"

namespace starrocks {

// ------------------------------------------------------------------------------------
// PredicateTreeColumnNode
// ------------------------------------------------------------------------------------

Status PredicateTreeColumnNode::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection,
                                         uint16_t from, uint16_t to) const {
    return _col_pred->evaluate(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}
Status PredicateTreeColumnNode::evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection,
                                             uint16_t from, uint16_t to) const {
    return _col_pred->evaluate_and(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}
Status PredicateTreeColumnNode::evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection,
                                            uint16_t from, uint16_t to) const {
    return _col_pred->evaluate_or(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}

StatusOr<uint16_t> PredicateTreeColumnNode::evaluate_branchless(const Chunk* chunk, uint16_t* sel,
                                                                uint16_t sel_size) const {
    return _col_pred->evaluate_branchless(chunk->get_column_by_id(_col_pred->column_id()).get(), sel, sel_size);
}

std::string PredicateTreeColumnNode::debug_string() const {
    return strings::Substitute(R"({"pred":"$0"})", _col_pred->debug_string());
}

// ------------------------------------------------------------------------------------
// PredicateTreeNode
// ------------------------------------------------------------------------------------

template <typename Vistor>
struct PredicateTreePreroderTraversal {
    void operator()(PredicateTreeColumnNode& node) const { vistor(node); }

    template <CompoundNodeType Type>
    void operator()(PredicateTreeCompoundNode<Type>& node) {
        vistor(node);
        for (auto& child : node.children()) {
            child.visit(*this);
        }
    }

    Vistor vistor;
};

template <typename Vistor>
void PredicateTreeNode::preorder_traversal(Vistor&& vistor) const {
    visit(PredicateTreePreroderTraversal{std::forward<Vistor>(vistor)});
}

template <typename Vistor>
void PredicateTreeNode::preorder_traversal(Vistor&& vistor) {
    visit(PredicateTreePreroderTraversal{std::forward<Vistor>(vistor)});
}

// ------------------------------------------------------------------------------------
// PredicateTree
// ------------------------------------------------------------------------------------

PredicateTree PredicateTree::create(PredicateTreeNode&& root) {
    if (std::holds_alternative<PredicateTreeColumnNode>(root.node)) {
        auto new_root_node = PredicateTreeAndNode{};
        new_root_node.add_child(std::move(root));
        root = PredicateTreeNode{std::move(new_root_node)};
    }

    // The id of all the compound nodes is placed before the id of all the column nodes (leaf nodes).
    PredicateTreeNodeId next_id = 0;

    // ColumnPredicates are placed before CompoundPredicates, and ColumnPredicates with the same column_id are placed together.
    auto get_sort_oridinality = [](const PredicateTreeNode& node) {
        return node.visit(overloaded{
                [](const PredicateTreeColumnNode& child_node) { return child_node.col_pred()->column_id(); },
                [&]<CompoundNodeType ChildType>(const PredicateTreeCompoundNode<ChildType>&) {
                    return std::numeric_limits<ColumnId>::max();
                },
        });
    };
    root.preorder_traversal(overloaded{
            [](PredicateTreeColumnNode& node) {
                // Do nothing.
            },
            [&]<CompoundNodeType Type>(PredicateTreeCompoundNode<Type>& node) {
                node.set_id(next_id++);
                std::ranges::sort(node.children(), [&](const PredicateTreeNode& lhs, const PredicateTreeNode& rhs) {
                    return get_sort_oridinality(lhs) < get_sort_oridinality(rhs);
                });
            },
    });

    const auto num_compound_nodes = next_id;
    root.preorder_traversal(overloaded{
            [&](PredicateTreeColumnNode& node) { node.set_id(next_id++); },
            []<CompoundNodeType Type>(PredicateTreeCompoundNode<Type>& node) {
                // Do nothing.
            },
    });

    return PredicateTree(std::move(root), num_compound_nodes);
}

PredicateTree::PredicateTree(PredicateTreeNode&& root, uint32_t num_compound_nodes)
        : _root(std::move(root)), _compound_node_contexts(num_compound_nodes) {}

Status PredicateTree::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    return visit([&](const auto& node) { return node.evaluate(_compound_node_contexts, chunk, selection, from, to); });
}
Status PredicateTree::evaluate(const Chunk* chunk, uint8_t* selection) const {
    return evaluate(chunk, selection, 0, chunk->num_rows());
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

const std::unordered_set<ColumnId>& PredicateTree::column_ids() const {
    if (_cached_column_ids.has_value()) {
        return _cached_column_ids.value();
    }

    auto& all_column_ids = _cached_column_ids.emplace();
    visit(ColumnPredsCollector{all_column_ids});
    return all_column_ids;
}

bool PredicateTree::contains_column(ColumnId cid) const {
    return column_ids().contains(cid);
}
size_t PredicateTree::num_columns() const {
    return column_ids().size();
}

struct SizeVisitor {
    size_t operator()(const PredicateTreeColumnNode& node) const { return 1; }

    template <CompoundNodeType Type>
    size_t operator()(const PredicateTreeCompoundNode<Type>& node) const {
        return std::accumulate(node.children().begin(), node.children().end(), 0,
                               [this](size_t acc, const auto& child) { return acc + child.visit(*this); });
    }
};

size_t PredicateTree::size() const {
    return visit(SizeVisitor());
}

struct EmptyVisitor {
    bool operator()(const PredicateTreeColumnNode& node) const { return false; }

    template <CompoundNodeType Type>
    bool operator()(const PredicateTreeCompoundNode<Type>& node) const {
        return std::ranges::all_of(node.children(), [this](const auto& child) { return child.visit(*this); });
    }
};

bool PredicateTree::empty() const {
    return visit(EmptyVisitor());
}

PredicateTreeNode PredicateTree::release_root() {
    return std::move(_root);
}

} // namespace starrocks