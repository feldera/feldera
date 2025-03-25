package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;

import java.util.ArrayList;
import java.util.List;

/** Optimizer using the Calcite program rewrite rules */
public class CalciteOptimizer implements IWritesLogs {
    final List<CalciteOptimizerStep> steps;
    final int level;
    final RelBuilder builder;

    public interface CalciteOptimizerStep {
        /** Name of the optimizer step */
        String getName();
        /** Optimize the program for the specified optimization level */
        RelNode optimize(RelNode rel, int level);
    }

    /** Base class for optimizations that use a Hep optimizer */
    public static abstract class HepOptimizerStep implements CalciteOptimizerStep {
        /** The program that performs the optimization for the specified optimization level */
        abstract HepProgram getProgram(RelNode node, int level);

        @Override
        public RelNode optimize(RelNode rel, int level) {
            HepProgram program = this.getProgram(rel, level);
            HepPlanner planner = new HepPlanner(program);
            planner.setRoot(rel);
            return planner.findBestExp();
        }
    }

    public class BaseOptimizerStep extends HepOptimizerStep {
        final int level;
        final String name;
        final HepProgramBuilder builder;

        public BaseOptimizerStep(String name, int level) {
            this.level = level;
            this.name = name;
            this.builder = new HepProgramBuilder();
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        HepProgram getProgram(RelNode node, int level) {
            if (level < this.level) {
                // Return an empty program
                return new HepProgramBuilder().build();
            }
            return this.builder.build();
        }

        void addRules(int level, RelOptRule... rules) {
            if (this.level > level) return;
            for (RelOptRule rule: rules) {
                Logger.INSTANCE.belowLevel(CalciteOptimizer.this, 3)
                        .appendSupplier(this::getName)
                        .append(" adding rule: ")
                        .appendSupplier(rule::toString)
                        .newline();
                this.builder.addRuleInstance(rule);
            }
        }
    }

    public class SimpleOptimizerStep extends BaseOptimizerStep {
        SimpleOptimizerStep(String name, int level, RelOptRule... rules) {
            super(name, level);
            this.addRules(level, rules);
        }
    }

    public CalciteOptimizer(int level, RelBuilder builder) {
        this.builder = builder;
        this.steps = new ArrayList<>();
        this.level = level;
        this.createOptimizer();
    }

    RelNode apply(RelNode rel) {
        for (CalciteOptimizerStep step: this.steps) {
            RelNode optimized = step.optimize(rel, this.level);
            if (rel != optimized) {
                Logger.INSTANCE.belowLevel(CalciteOptimizer.this, 1)
                        .append("After ")
                        .appendSupplier(step::getName)
                        .increase()
                        .appendSupplier(() -> SqlToRelCompiler.getPlan(optimized))
                        .decrease()
                        .newline();
            }
            rel = optimized;
        }
        return rel;
    }

    /** Helper class to discover whether a query contains outer joins */
    static class OuterJoinFinder extends RelVisitor {
        public int outerJoinCount = 0;
        public int joinCount = 0;
        @Override public void visit(
                RelNode node, int ordinal,
                @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
            if (node instanceof Join join) {
                ++joinCount;
                if (join.getJoinType().isOuterJoin())
                    ++outerJoinCount;
            }
            super.visit(node, ordinal, parent);
        }

        void run(RelNode node) {
            this.go(node);
        }
    }

    void createOptimizer() {
        this.addStep(new SimpleOptimizerStep("Constant fold", 2,
                CoreRules.COERCE_INPUTS,
                CoreRules.FILTER_REDUCE_EXPRESSIONS,
                CoreRules.PROJECT_REDUCE_EXPRESSIONS,
                CoreRules.JOIN_REDUCE_EXPRESSIONS,
                CoreRules.WINDOW_REDUCE_EXPRESSIONS,
                CoreRules.CALC_REDUCE_EXPRESSIONS,
                CoreRules.CALC_REDUCE_DECIMALS,
                CoreRules.FILTER_VALUES_MERGE,
                CoreRules.PROJECT_FILTER_VALUES_MERGE,
                // Rule is buggy; disabled due to
                // https://github.com/feldera/feldera/issues/217
                // CoreRules.PROJECT_VALUES_MERGE
                CoreRules.AGGREGATE_VALUES));
        this.addStep(new SimpleOptimizerStep("Remove empty relations", 0,
                PruneEmptyRules.UNION_INSTANCE,
                PruneEmptyRules.INTERSECT_INSTANCE,
                PruneEmptyRules.MINUS_INSTANCE,
                PruneEmptyRules.PROJECT_INSTANCE,
                PruneEmptyRules.FILTER_INSTANCE,
                PruneEmptyRules.SORT_INSTANCE,
                PruneEmptyRules.AGGREGATE_INSTANCE,
                PruneEmptyRules.JOIN_LEFT_INSTANCE,
                PruneEmptyRules.JOIN_RIGHT_INSTANCE,
                PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE
        ));

        this.addStep(new CalciteOptimizerStep() {
            @Override
            public String getName() {
                return "Decorrelate";
            }

            @Override
            public RelNode optimize(RelNode rel, int level) {
                return RelDecorrelator.decorrelateQuery(rel, CalciteOptimizer.this.builder);
            }
        });

        this.addStep(new SimpleOptimizerStep("Expand windows", 0,
                CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE,
                // I suspect that in the absence of the above rule
                // there is a bug in Calcite in RexOVer which causes
                // the following rule to crash the ComplexQueriesTest.calcite6020issueTest().
                // See discussion in https://issues.apache.org/jira/browse/CALCITE-6020
                CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW
        ));
        this.addStep(new SimpleOptimizerStep("Isolate DISTINCT aggregates", 0,
                CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
                // Rule is unsound https://issues.apache.org/jira/browse/CALCITE-6403
                CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES));
        this.addStep(new BaseOptimizerStep("Join order", 2) {
            @Override
            HepProgram getProgram(RelNode node, int level) {
                this.addRules(level,
                        CoreRules.JOIN_CONDITION_PUSH,
                        CoreRules.JOIN_PUSH_EXPRESSIONS,
                        // Below rule crashes with test NaiveIncrementalTests.inTest
                        // CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
                        // https://issues.apache.org/jira/browse/CALCITE-5387
                        // TODO: Rule is unsound
                        // https://github.com/feldera/feldera/issues/1702
                        CoreRules.FILTER_INTO_JOIN
                );
                OuterJoinFinder finder = new OuterJoinFinder();
                finder.run(node);
                // Bushy join optimization fails when the query contains outer joins.
                boolean hasOuterJoins = (finder.outerJoinCount > 0) || (finder.joinCount < 3);
                if (!hasOuterJoins) {
                    this.addRules(level,
                            CoreRules.JOIN_TO_MULTI_JOIN,
                            CoreRules.PROJECT_MULTI_JOIN_MERGE,
                            CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY
                            //CoreRules.FILTER_MULTI_JOIN_MERGE,
                            //CoreRules.MULTI_JOIN_BOTH_PROJECT,
                            //CoreRules.MULTI_JOIN_LEFT_PROJECT,
                            //CoreRules.MULTI_JOIN_RIGHT_PROJECT,
                            //CoreRules.MULTI_JOIN_OPTIMIZE
                    );
                }
                this.builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
                return this.builder.build();
            }
        });

        SimpleOptimizerStep merge = new SimpleOptimizerStep(
                "Merge identical operations", 0,
                CoreRules.PROJECT_MERGE,
                CoreRules.MINUS_MERGE,
                CoreRules.UNION_MERGE,
                CoreRules.AGGREGATE_MERGE,
                CoreRules.INTERSECT_MERGE);
        // this.addStep(merge); -- messes up the shape of uncollect

        this.addStep(new SimpleOptimizerStep("Move projections", 0,
                // Rule is unsound: https://issues.apache.org/jira/browse/CALCITE-6681
                // CoreRules.PROJECT_CORRELATE_TRANSPOSE,
                CoreRules.PROJECT_WINDOW_TRANSPOSE,
                CoreRules.PROJECT_SET_OP_TRANSPOSE,
                CoreRules.FILTER_PROJECT_TRANSPOSE
                // Rule is unsound, replaced with UnusedFields done later.
                //CoreRules.PROJECT_JOIN_TRANSPOSE
        ));

        this.addStep(merge);
        this.addStep(new SimpleOptimizerStep("Remove dead code", 0,
                CoreRules.AGGREGATE_REMOVE,
                CoreRules.UNION_REMOVE,
                CoreRules.PROJECT_REMOVE,
                CoreRules.PROJECT_JOIN_JOIN_REMOVE,
                CoreRules.PROJECT_JOIN_REMOVE
        ));
        /*
        return Linq.list(
            CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
            CoreRules.AGGREGATE_UNION_AGGREGATE,
        );
        */
    }

    public void addStep(CalciteOptimizerStep step) {
        this.steps.add(step);
    }
}
