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
package com.starrocks.sql.optimizer.operator;

import com.google.api.client.util.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class UKFKConstraints {
    // ColumnRefOperator::id -> UniqueConstraint
    private final Map<Integer, UniqueConstraintWrapper> uniqueKeys = Maps.newHashMap();
    // ColumnRefOperator::id -> UniqueConstraint
    private final Map<Integer, UniqueConstraintWrapper> relaxedUniqueKeys = Maps.newHashMap();
    // The unique key of the data table needs to be collected when eliminating aggregation
    private final List<UniqueConstraintWrapper> tableUniqueKeys = Lists.newArrayList();
    // ColumnRefOperator::id -> ForeignKeyConstraint
    private final Map<Integer, ForeignKeyConstraintWrapper> foreignKeys = Maps.newHashMap();
    private JoinProperty joinProperty;

    public void addUniqueKey(int id, UniqueConstraintWrapper uniqueKey) {
        uniqueKeys.put(id, uniqueKey);
        relaxedUniqueKeys.put(id, uniqueKey);
    }

    public void addTableUniqueKey(UniqueConstraintWrapper uniqueKey) {
        tableUniqueKeys.add(uniqueKey);
    }

    public void addForeignKey(int id, ForeignKeyConstraintWrapper foreignKey) {
        foreignKeys.put(id, foreignKey);
    }

    public UniqueConstraintWrapper getUniqueConstraint(Integer id) {
        return uniqueKeys.get(id);
    }

    public List<UniqueConstraintWrapper> getTableUniqueKeys() {
        return tableUniqueKeys;
    }

    public ForeignKeyConstraintWrapper getForeignKeyConstraint(Integer id) {
        return foreignKeys.get(id);
    }

    public UniqueConstraintWrapper getRelaxedUniqueConstraint(Integer id) {
        return relaxedUniqueKeys.get(id);
    }

    public JoinProperty getJoinProperty() {
        return joinProperty;
    }

    public void setJoinProperty(JoinProperty joinProperty) {
        this.joinProperty = joinProperty;
    }

    public static UKFKConstraints inheritFrom(UKFKConstraints from, ColumnRefSet toOutputColumns) {
        UKFKConstraints clone = new UKFKConstraints();
        from.uniqueKeys.entrySet().stream()
                .filter(entry -> toOutputColumns.contains(entry.getKey()))
                .forEach(entry -> clone.uniqueKeys.put(entry.getKey(), entry.getValue()));
        clone.inheritForeignKey(from, toOutputColumns);
        clone.inheritRelaxedUniqueKey(from, toOutputColumns);

        from.getTableUniqueKeys().stream()
                .filter(entry -> toOutputColumns.containsAll(entry.ukColumnRefs))
                .forEach(clone::addTableUniqueKey);

        return clone;
    }

    public void inheritForeignKey(UKFKConstraints other, ColumnRefSet outputColumns) {
        other.foreignKeys.entrySet().stream()
                .filter(entry -> outputColumns.contains(entry.getKey()))
                .forEach(entry -> foreignKeys.put(entry.getKey(), entry.getValue()));
    }

    public void inheritRelaxedUniqueKey(UKFKConstraints other, ColumnRefSet outputColumns) {
        Stream.concat(other.uniqueKeys.entrySet().stream(), other.relaxedUniqueKeys.entrySet().stream())
                .filter(entry -> outputColumns.contains(entry.getKey()))
                .forEach(entry -> relaxedUniqueKeys.put(entry.getKey(), entry.getValue()));
    }

    public static final class UniqueConstraintWrapper {
        public final UniqueConstraint constraint;
        public final ColumnRefSet nonUKColumnRefs;
        public final boolean isIntact;

        public final ColumnRefSet ukColumnRefs;
        public final ColumnRefSet scopedColumnRefs;

        public UniqueConstraintWrapper(UniqueConstraint constraint,
                                       ColumnRefSet nonUKColumnRefs, boolean isIntact) {
            this.constraint = constraint;
            this.nonUKColumnRefs = nonUKColumnRefs;
            this.isIntact = isIntact;
            this.ukColumnRefs = new ColumnRefSet();
            this.scopedColumnRefs = new ColumnRefSet();
        }

        public UniqueConstraintWrapper(UniqueConstraint constraint, ColumnRefSet nonUKColumnRefs, boolean isIntact,
                                       ColumnRefSet ukColumnRefs) {
            this.constraint = constraint;
            this.nonUKColumnRefs = nonUKColumnRefs;
            this.isIntact = isIntact;
            this.ukColumnRefs = ukColumnRefs;
            this.scopedColumnRefs = new ColumnRefSet();
        }

        public UniqueConstraintWrapper(UniqueConstraint constraint, ColumnRefSet nonUKColumnRefs, boolean isIntact,
                                       ColumnRefSet ukColumnRefs, ColumnRefSet scopedColumnRefs) {
            this.constraint = constraint;
            this.nonUKColumnRefs = nonUKColumnRefs;
            this.isIntact = isIntact;
            this.ukColumnRefs = ukColumnRefs;
            this.scopedColumnRefs = scopedColumnRefs;
        }
    }

    public static final class ForeignKeyConstraintWrapper {
        public final ForeignKeyConstraint constraint;
        public final boolean isOrderByFK;

        public ForeignKeyConstraintWrapper(ForeignKeyConstraint constraint, boolean isOrderByFK) {
            this.constraint = constraint;
            this.isOrderByFK = isOrderByFK;
        }
    }

    public static final class JoinProperty {
        public final BinaryPredicateOperator predicate;
        public final UniqueConstraintWrapper ukConstraint;
        public final ForeignKeyConstraintWrapper fkConstraint;
        public final ColumnRefOperator ukColumnRef;
        public final ColumnRefOperator fkColumnRef;
        public final boolean isLeftUK;
        public Expr ukColumn;
        public Expr fkColumn;

        public JoinProperty(BinaryPredicateOperator predicate,
                            UniqueConstraintWrapper ukConstraint,
                            ForeignKeyConstraintWrapper fkConstraint,
                            ColumnRefOperator ukColumnRef,
                            ColumnRefOperator fkColumnRef, boolean isLeftUK) {
            this.predicate = predicate;
            this.ukConstraint = ukConstraint;
            this.fkConstraint = fkConstraint;
            this.ukColumnRef = ukColumnRef;
            this.fkColumnRef = fkColumnRef;
            this.isLeftUK = isLeftUK;
        }

        public void buildExpr(ExecPlan context) {
            ukColumn = ScalarOperatorToExpr.buildExecExpression(ukColumnRef,
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));
            fkColumn = ScalarOperatorToExpr.buildExecExpression(fkColumnRef,
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));
        }
    }
}
