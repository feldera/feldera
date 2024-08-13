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
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;

import java.util.ArrayList;
import java.util.List;

/** Optimizer using the Calcite program rewrite rules */
public class CalciteOptimizer implements IWritesLogs {
    public abstract class CalciteOptimizerStep {
        /** Name of the optimizer step */
        abstract String getName();
        /** The program that performs the optimization for the specified optimization level */
        abstract HepProgram getProgram(RelNode node, int level);

        RelNode optimize(RelNode rel, int level) {
            HepProgram program = this.getProgram(rel, level);
            HepPlanner planner = new HepPlanner(program);
            planner.setRoot(rel);
            RelNode result = planner.findBestExp();
            if (rel != result) {
                Logger.INSTANCE.belowLevel(CalciteOptimizer.this, 1)
                        .append("After ")
                        .append(this.getName())
                        .increase()
                        .append(CalciteCompiler.getPlan(result))
                        .decrease()
                        .newline();
            }
            return result;
        }
    }

    public class BaseOptimizerStep extends CalciteOptimizerStep {
        final int level;
        final String name;
        final HepProgramBuilder builder;

        public BaseOptimizerStep(String name, int level) {
            this.level = level;
            this.name = name;
            this.builder = new HepProgramBuilder();
        }

        @Override
        String getName() {
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
                Logger.INSTANCE.belowLevel(CalciteOptimizer.this, 2)
                        .append(this.getName())
                        .append(" adding rule: ")
                        .append(rule.toString())
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

    final List<CalciteOptimizerStep> steps;
    final int level;

    public CalciteOptimizer(int level) {
        this.steps = new ArrayList<>();
        this.level = level;
        this.createOptimizer(level);
    }

    RelNode apply(RelNode rel) {
        for (CalciteOptimizerStep step: this.steps) {
            rel = step.optimize(rel, this.level);
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

    void createOptimizer(int level) {
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
        this.addStep(new SimpleOptimizerStep("Expand windows", 0,
                CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW
        ));
        this.addStep(new SimpleOptimizerStep("Isolate DISTINCT aggregates", 0,
                CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
                // Rule is unsound https://issues.apache.org/jira/browse/CALCITE-6403
                CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES));
        this.addStep(new SimpleOptimizerStep("Not distinct", 0,
            CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM));
        this.addStep(new BaseOptimizerStep("Join order", level) {
            @Override
            HepProgram getProgram(RelNode node, int level) {
                this.addRules(level,
                        CoreRules.JOIN_CONDITION_PUSH,
                        CoreRules.JOIN_PUSH_EXPRESSIONS,
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
        this.addStep(new SimpleOptimizerStep(
                "Move projections", 2,
                CoreRules.PROJECT_CORRELATE_TRANSPOSE,
                CoreRules.PROJECT_WINDOW_TRANSPOSE,
                CoreRules.PROJECT_SET_OP_TRANSPOSE,
                CoreRules.FILTER_PROJECT_TRANSPOSE
                // Rule is unsound
                // CoreRules.PROJECT_JOIN_TRANSPOSE
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
