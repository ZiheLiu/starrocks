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
        if (auto it = column_preds.find(cid); it != column_preds.end()) {
            it->second.emplace_back(col_pred);
        } else {
            column_preds.emplace(cid, std::vector<const ColumnPredicate*>{col_pred});
        }
    }

    template <CompoundNodeType Type>
    void operator()(const PredicateTreeCompoundNode<Type>& node) const {
        for (const auto& child : node.children()) {
            child.visit(*this);
        }
    }

    std::map<ColumnId, std::vector<const ColumnPredicate*>>& column_preds;
};

const std::map<ColumnId, std::vector<const ColumnPredicate*>>& PredicateTreeNode::column_preds() const {
    if (_cached_column_preds.has_value()) {
        return _cached_column_preds.value();
    }

    auto& all_column_preds = _cached_column_preds.emplace();
    visit(ColumnPredsCollector{all_column_preds});
    return all_column_preds;
}

bool PredicateTreeNode::contains_column(ColumnId cid) const {
    return column_preds().contains(cid);
}
size_t PredicateTreeNode::num_columns() const {
    return column_preds().size();
}

struct ChildrenSorter {
    void operator()(PredicateTreeColumnNode& node) const { // Do nothing.
    }

    template <CompoundNodeType Type>
    void operator()(PredicateTreeCompoundNode<Type>& node) const {
        auto get_identity = [](const PredicateTreeNode& node) {
            return node.visit(PredicateTreeNodeVisitor{
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