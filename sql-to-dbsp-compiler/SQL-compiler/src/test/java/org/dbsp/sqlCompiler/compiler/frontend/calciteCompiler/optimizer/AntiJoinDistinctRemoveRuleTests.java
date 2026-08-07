package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link AntiJoinDistinctRemoveRule}, applied directly
 * to relational plans built with a {@link RelBuilder}.
 * Each plan reads two single-column collections built from VALUES:
 * t with column x, and s with column y; the SQL comments use these names. */
public class AntiJoinDistinctRemoveRuleTests {
    static RelBuilder createBuilder() {
        // Not RelBuilder.create(FrameworkConfig): that route needs a JDBC connection
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RelOptCluster cluster = RelOptCluster.create(
                new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
        return RelFactories.LOGICAL_BUILDER.create(cluster, null)
                // The builder must not simplify the plans the tests specify:
                // it can remove a distinct() over provably-unique VALUES and
                // fold IS NULL of a non-nullable column to FALSE
                .transform(config -> config
                        .withSimplify(false)
                        .withSimplifyValues(false)
                        .withAggregateUnique(true)
                        .withPruneInputOfAggregate(false));
    }

    static RelNode optimize(RelNode node) {
        HepProgram program = new HepProgramBuilder()
                .addRuleInstance(new AntiJoinDistinctRemoveRule())
                .build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(node);
        return planner.findBestExp();
    }

    static int countAggregates(RelNode node) {
        int count = node instanceof Aggregate ? 1 : 0;
        for (RelNode input : node.getInputs())
            count += countAggregates(input);
        return count;
    }

    /** Optimize the plan, which must contain exactly one Aggregate.
     * @return True if the optimization removed the Aggregate. */
    static boolean distinctRemoved(RelNode plan) {
        Assert.assertEquals(RelOptUtil.toString(plan), 1, countAggregates(plan));
        RelNode optimized = optimize(plan);
        int count = countAggregates(optimized);
        // The rule can only remove the Aggregate, never add one
        Assert.assertTrue(RelOptUtil.toString(optimized), count <= 1);
        return count == 0;
    }

    /** Filter(IS NULL(right col)) over LeftJoin(left, Distinct(right)),
     * with the right column non-nullable: the distinct must be removed. */
    @Test
    public void removesDistinct() {
        // SELECT * FROM t
        // LEFT JOIN (SELECT DISTINCT y FROM s) d ON t.x = d.y
        // WHERE d.y IS NULL
        RelBuilder builder = createBuilder();
        RelNode plan = builder
                .values(new String[]{"x"}, 1, 2)
                .values(new String[]{"y"}, 1, 1)
                .distinct()
                .join(JoinRelType.LEFT,
                        builder.equals(builder.field(2, 0, "x"), builder.field(2, 1, "y")))
                .filter(builder.isNull(builder.field("y")))
                .build();
        Assert.assertTrue(distinctRemoved(plan));
    }

    /** Extra conjuncts over other columns do not prevent the rewrite */
    @Test
    public void removesDistinctExtraConjunct() {
        // SELECT * FROM t
        // LEFT JOIN (SELECT DISTINCT y FROM s) d ON t.x = d.y
        // WHERE d.y IS NULL AND t.x > 0
        RelBuilder builder = createBuilder();
        RelNode plan = builder
                .values(new String[]{"x"}, 1, 2)
                .values(new String[]{"y"}, 1, 1)
                .distinct()
                .join(JoinRelType.LEFT,
                        builder.equals(builder.field(2, 0, "x"), builder.field(2, 1, "y")))
                .filter(
                        builder.isNull(builder.field("y")),
                        builder.greaterThan(builder.field("x"), builder.literal(0)))
                .build();
        Assert.assertTrue(distinctRemoved(plan));
    }

    /** A nullable right column can be NULL in a matched row,
     * so IS NULL does not prove the absence of a match. */
    @Test
    public void keepsNullableColumn() {
        // SELECT * FROM t
        // LEFT JOIN (SELECT DISTINCT y FROM s) d ON t.x = d.y
        // WHERE d.y IS NULL
        // where s.y is nullable
        RelBuilder builder = createBuilder();
        RelNode plan = builder
                .values(new String[]{"x"}, 1, 2)
                .values(new String[]{"y"}, 1, null)
                .distinct()
                .join(JoinRelType.LEFT,
                        builder.equals(builder.field(2, 0, "x"), builder.field(2, 1, "y")))
                .filter(builder.isNull(builder.field("y")))
                .build();
        Assert.assertFalse(distinctRemoved(plan));
    }

    /** An inner join propagates right multiplicities to the output */
    @Test
    public void keepsInnerJoin() {
        // SELECT * FROM t
        // JOIN (SELECT DISTINCT y FROM s) d ON t.x = d.y
        // WHERE d.y IS NULL
        RelBuilder builder = createBuilder();
        RelNode join = builder
                .values(new String[]{"x"}, 1, 2)
                .values(new String[]{"y"}, 1, 1)
                .distinct()
                .join(JoinRelType.INNER,
                        builder.equals(builder.field(2, 0, "x"), builder.field(2, 1, "y")))
                .build();
        // Build the filter directly to avoid optimization by the builder
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        RelNode plan = LogicalFilter.create(join,
                rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
                        rexBuilder.makeInputRef(join, 1)));
        Assert.assertFalse(distinctRemoved(plan));
    }

    /** An Aggregate that computes something is not a plain DISTINCT */
    @Test
    public void keepsRealAggregate() {
        // SELECT * FROM t
        // LEFT JOIN (SELECT y, COUNT(*) c FROM s GROUP BY y) d ON t.x = d.y
        // WHERE d.y IS NULL
        RelBuilder builder = createBuilder();
        RelNode plan = builder
                .values(new String[]{"x"}, 1, 2)
                .values(new String[]{"y"}, 1, 1)
                .aggregate(builder.groupKey(0), builder.count(false, "c"))
                .join(JoinRelType.LEFT,
                        builder.equals(builder.field(2, 0, "x"), builder.field(2, 1, "y")))
                .filter(builder.isNull(builder.field("y")))
                .build();
        Assert.assertFalse(distinctRemoved(plan));
    }

    /** An Aggregate grouping by a subset of its columns changes the schema,
     * so it cannot be replaced by its input. */
    @Test
    public void keepsPartialGroupBy() {
        // SELECT * FROM t
        // LEFT JOIN (SELECT y FROM s2 GROUP BY y) d ON t.x = d.y
        // WHERE d.y IS NULL
        // where s2 has columns (y, z) and the Aggregate reads both
        RelBuilder builder = createBuilder();
        RelNode plan = builder
                .values(new String[]{"x"}, 1, 2)
                .values(new String[]{"y", "z"}, 1, 10, 2, 20)
                .aggregate(builder.groupKey(0))
                .join(JoinRelType.LEFT,
                        builder.equals(builder.field(2, 0, "x"), builder.field(2, 1, "y")))
                .filter(builder.isNull(builder.field("y")))
                .build();
        Assert.assertFalse(distinctRemoved(plan));
    }

    /** IS NOT NULL selects the matched rows, whose multiplicity the
     * distinct bounds; only IS NULL is rewritten. */
    @Test
    public void keepsIsNotNull() {
        // SELECT * FROM t
        // LEFT JOIN (SELECT DISTINCT y FROM s) d ON t.x = d.y
        // WHERE NOT(d.y IS NULL)
        RelBuilder builder = createBuilder();
        RexNode isNull = builder
                .values(new String[]{"x"}, 1, 2)
                .values(new String[]{"y"}, 1, 1)
                .distinct()
                .join(JoinRelType.LEFT,
                        builder.equals(builder.field(2, 0, "x"), builder.field(2, 1, "y")))
                .isNull(builder.field("y"));
        RelNode plan = builder
                .filter(builder.not(isNull))
                .build();
        Assert.assertFalse(distinctRemoved(plan));
    }
}
