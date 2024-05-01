package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
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
        /** The program that performs the optimization */
        abstract HepProgram getProgram(RelNode node);

        RelNode optimize(RelNode rel) {
            HepProgram program = this.getProgram(rel);
            HepPlanner planner = new HepPlanner(program);
            planner.setRoot(rel);
            RelNode result = planner.findBestExp();
            if (rel != result) {
                Logger.INSTANCE.belowLevel(CalciteOptimizer.this, 3)
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
        final String name;
        final HepProgramBuilder builder;

        public BaseOptimizerStep(String name) {
            this.name = name;
            this.builder = new HepProgramBuilder();
        }

        @Override
        String getName() {
            return this.name;
        }

        @Override
        HepProgram getProgram(RelNode node) {
            return this.builder.build();
        }

        void addRules(RelOptRule... rules) {
            for (RelOptRule rule: rules)
                this.builder.addRuleInstance(rule);
        }
    }

    public class SimpleOptimizerStep extends BaseOptimizerStep {
        SimpleOptimizerStep(String name, RelOptRule... rules) {
            super(name);
            for (RelOptRule r: rules)
                this.builder.addRuleInstance(r);
        }
    }

    final List<CalciteOptimizerStep> steps;

    public CalciteOptimizer(int level) {
        this.steps = new ArrayList<>();
        if (level < 1)
            // For optimization levels below 1 we don't even apply Calcite optimizations.
            // Note that this may cause compilation to fail, since our compiler does not
            // handle all possible RelNode programs.
            return;
        this.createOptimizer();
    }

    RelNode apply(RelNode rel) {
        for (CalciteOptimizerStep step: this.steps) {
            rel = step.optimize(rel);
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
            if (node instanceof Join) {
                Join join = (Join)node;
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

    /** Helper class to discover whether a query contains aggregations with group sets. */
    /* Should be removed when https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6317
     * is fixed. */
    static class AggregationGroupSets extends RelVisitor {
        public boolean hasGroupSets = false;

        @Override public void visit(
                RelNode node, int ordinal,
                @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
            if (node instanceof Aggregate) {
                Aggregate aggregate = (Aggregate)node;
                if (!aggregate.groupSets.isEmpty())
                    this.hasGroupSets = true;
            }
            super.visit(node, ordinal, parent);
        }

        void run(RelNode node) {
            this.go(node);
        }
    }

    void createOptimizer() {
        this.addStep(new BaseOptimizerStep("Constant fold") {
            @Override
            HepProgram getProgram(RelNode node) {
                // Check if program contains Aggregates with groupSets.
                AggregationGroupSets ags = new AggregationGroupSets();
                ags.run(node);

                this.addRules(
                        CoreRules.COERCE_INPUTS,
                        CoreRules.FILTER_REDUCE_EXPRESSIONS);
                // Rule is buggy: https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6317
                if (!ags.hasGroupSets)
                    this.addRules(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
                this.addRules(
                        CoreRules.JOIN_REDUCE_EXPRESSIONS,
                        CoreRules.WINDOW_REDUCE_EXPRESSIONS,
                        CoreRules.CALC_REDUCE_EXPRESSIONS,
                        CoreRules.CALC_REDUCE_DECIMALS,
                        CoreRules.FILTER_VALUES_MERGE,
                        CoreRules.PROJECT_FILTER_VALUES_MERGE,
                        // Rule is buggy; disabled due to
                        // https://github.com/feldera/feldera/issues/217
                        // CoreRules.PROJECT_VALUES_MERGE
                        CoreRules.AGGREGATE_VALUES);
                return this.builder.build();
            }
        });
        this.addStep(new SimpleOptimizerStep("Remove empty relations",
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
        this.addStep(new SimpleOptimizerStep("Expand windows",
                CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW
        ));
        this.addStep(new BaseOptimizerStep("Isolate DISTINCT aggregates") {
            @Override
            HepProgram getProgram(RelNode node) {
                AggregationGroupSets finder = new AggregationGroupSets();
                finder.run(node);
                if (!finder.hasGroupSets) {
                    // Convert DISTINCT aggregates into separate computations and join the results.
                    // The following rule is unsound if aggregates contain groupSets
                    // https://issues.apache.org/jira/browse/CALCITE-6332
                    this.addRules(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN);
                } else {
                    // TODO: This sometimes triggers a bug in our compiler
                    this.addRules(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
                }
                return this.builder.build();
            }
        });

        this.addStep(new BaseOptimizerStep("Join order") {
            @Override
            HepProgram getProgram(RelNode node) {
                this.addRules(
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
                    this.addRules(
                            CoreRules.JOIN_TO_MULTI_JOIN,
                            CoreRules.PROJECT_MULTI_JOIN_MERGE,
                            CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY
                    );
                }
                this.builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
                return this.builder.build();
            }
        });

        this.addStep(new SimpleOptimizerStep(
                "Move projections",
                CoreRules.PROJECT_CORRELATE_TRANSPOSE,
                CoreRules.PROJECT_SET_OP_TRANSPOSE,
                CoreRules.FILTER_PROJECT_TRANSPOSE
                // Rule is unsound
                // CoreRules.PROJECT_JOIN_TRANSPOSE
        ));
        this.addStep(new SimpleOptimizerStep(
                "Merge identical operations",
                CoreRules.PROJECT_MERGE,
                CoreRules.MINUS_MERGE,
                CoreRules.UNION_MERGE,
                CoreRules.AGGREGATE_MERGE,
                CoreRules.INTERSECT_MERGE));
        this.addStep(new SimpleOptimizerStep("Remove dead code",
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
