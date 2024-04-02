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

#include "storage/predicate_tree.hpp"

namespace starrocks {

// ------------------------------------------------------------------------------------
// PredicateTreeColumnNode
// ------------------------------------------------------------------------------------

Status PredicateTreeColumnNode::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _col_pred->evaluate(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}
Status PredicateTreeColumnNode::evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _col_pred->evaluate_and(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}
Status PredicateTreeColumnNode::evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _col_pred->evaluate_or(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}

StatusOr<uint16_t> PredicateTreeColumnNode::evaluate_branchless(const Chunk* chunk, uint16_t* sel,
                                                                uint16_t sel_size) const {
    return _col_pred->evaluate_branchless(chunk->get_column_by_id(_col_pred->column_id()).get(), sel, sel_size);
}

std::string PredicateTreeColumnNode::debug_string() const {
    return strings::Substitute("{\"pred\":\"$0\"}", _col_pred->debug_string());
}

bool PredicateTreeColumnNode::contains_column(ColumnId cid) const {
    return _col_pred->column_id() == cid;
}
size_t PredicateTreeColumnNode::num_columns() const {
    return 1;
}

// ------------------------------------------------------------------------------------
// PredicateTreeNode
// ------------------------------------------------------------------------------------

struct SizeVisitor {
    size_t operator()(const PredicateTreeColumnNode& node) const { return 1; }

    template <CompoundNodeType Type>
    size_t operator()(const PredicateTreeCompoundNode<Type>& node) const {
        return std::accumulate(node.children().begin(), node.children().end(), 0,
                               [this](size_t acc, const auto& child) { return acc + child.visit(*this); });
    }
};

size_t PredicateTreeNode::size() const {
    return visit(SizeVisitor());
}

struct EmptyVisitor {
    bool operator()(const PredicateTreeColumnNode& node) const { return false; }

    template <CompoundNodeType Type>
    bool operator()(const PredicateTreeCompoundNode<Type>& node) const {
        return std::ranges::all_of(node.children(), [this](const auto& child) { return child.visit(*this); });
    }
};

bool PredicateTreeNode::empty() const {
    return visit(EmptyVisitor());
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

bool PredicateTreeNode::contains_column(ColumnId cid) const {
    return visit([&](const auto& node) { return node.contains_column(cid); });
}
size_t PredicateTreeNode::num_columns() const {
    return visit([](const auto& node) { return node.num_columns(); });
}

struct ChildrenSorter {
    void operator()(PredicateTreeColumnNode& node) const { // Do nothing.
    }

    template <CompoundNodeType Type>
    void operator()(PredicateTreeCompoundNode<Type>& node) const {
        auto get_identity = [](const PredicateTreeNode& node) {
            return node.visit(overloaded{
                    [](const PredicateTreeColumnNode& child_node) { return child_node.col_pred()->column_id(); },
                    [&]<CompoundNodeType ChildType>(const PredicateTreeCompoundNode<ChildType>&) {
                        return std::numeric_limits<ColumnId>::max();
                    },
            });
        };
        std::ranges::sort(node.children(), [&get_identity](const PredicateTreeNode& lhs, const PredicateTreeNode& rhs) {
            return get_identity(lhs) < get_identity(rhs);
        });

        for (auto& child : node.children()) {
            child.visit(*this);
        }
    }
};

void PredicateTreeNode::sort_children() {
    visit(ChildrenSorter{});
}

} // namespace starrocks