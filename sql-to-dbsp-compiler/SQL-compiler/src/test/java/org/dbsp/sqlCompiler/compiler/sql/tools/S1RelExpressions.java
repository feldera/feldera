package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/** Observe every local Rex-bearing field in the 17 lowering relation forms.
 * RelNode.accept(RexShuttle) omits Values, Aggregate and Window internals and
 * the ASOF match condition. Explicit field walking also avoids creating copied
 * RelNodes while observing SEARCH expansion. Relational inputs are walked by
 * the caller; unknown classes require a field audit before acceptance. */
final class S1RelExpressions {
    private S1RelExpressions() { }

    static void visit(RelNode node, RexShuttle visitor, Consumer<RelDataType> types) {
        forEachRoot(node, expression -> expression.accept(visitor), types);
    }

    /** Visit local expression roots without descending into their operands. */
    static void forEachRoot(RelNode node, Consumer<RexNode> expressions, Consumer<RelDataType> types) {
        Class<?> kind = node.getClass();
        if (kind == LogicalProject.class) {
            for (RexNode expression: ((LogicalProject) node).getProjects()) expressions.accept(expression);
        } else if (kind == LogicalFilter.class) {
            expressions.accept(((LogicalFilter) node).getCondition());
        } else if (kind == LogicalAsofJoin.class) {
            var join = (LogicalAsofJoin) node;
            expressions.accept(join.getCondition());
            expressions.accept(join.getMatchCondition());
        } else if (kind == LogicalJoin.class) {
            expressions.accept(((LogicalJoin) node).getCondition());
        } else if (kind == LogicalValues.class) {
            for (var tuple: ((LogicalValues) node).getTuples())
                for (var literal: tuple) expressions.accept(literal);
        } else if (kind == LogicalAggregate.class) {
            for (var call: ((LogicalAggregate) node).getAggCallList()) {
                types.accept(call.getType());
                for (RexNode expression: call.rexList) expressions.accept(expression);
            }
        } else if (kind == LogicalWindow.class) {
            var window = (LogicalWindow) node;
            for (var literal: window.constants) expressions.accept(literal);
            for (var group: window.groups) {
                visitNullable(group.lowerBound.getOffset(), expressions);
                visitNullable(group.upperBound.getOffset(), expressions);
                for (var call: group.aggCalls) expressions.accept(call);
            }
        } else if (kind == LogicalSort.class) {
            var sort = (LogicalSort) node;
            visitNullable(sort.offset, expressions);
            visitNullable(sort.fetch, expressions);
        } else if (kind == LogicalTableFunctionScan.class) {
            expressions.accept(((LogicalTableFunctionScan) node).getCall());
        } else if (kind == LogicalTableScan.class || kind == JdbcTableScan.class
                || kind == LogicalUnion.class || kind == LogicalIntersect.class
                || kind == LogicalMinus.class || kind == LogicalCorrelate.class
                || kind == Collect.class || kind == Uncollect.class) {
            // These forms have no local Rex fields. Types, traits, keys, hints
            // and correlation declarations belong to other S1 field inventories.
        } else {
            throw new IllegalArgumentException("Unmodeled S1 relational expression fields: " + kind.getName());
        }
    }

    private static void visitNullable(@Nullable RexNode node, Consumer<RexNode> expressions) {
        if (node != null) expressions.accept(node);
    }
}
