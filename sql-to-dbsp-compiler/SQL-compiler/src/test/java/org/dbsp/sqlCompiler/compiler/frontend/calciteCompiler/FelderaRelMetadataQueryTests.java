package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Tests both the production metadata-query wiring and its recursive calls. */
public class FelderaRelMetadataQueryTests {
    private static RelOptCluster createCluster() {
        return new SqlToRelCompiler(new CompilerOptions(), new StderrErrorReporter()).cluster;
    }

    private static RelNode scan(RelOptCluster cluster) {
        RelDataType rowType = cluster.getTypeFactory().builder()
                .add("entity_id", SqlTypeName.INTEGER)
                .add("category", SqlTypeName.INTEGER)
                .add("batch_id", SqlTypeName.INTEGER)
                .add("updated_at", SqlTypeName.INTEGER)
                .build();
        var table = RelOptTableImpl.create(null, rowType, List.of("events"), Expressions.constant(null));
        return LogicalTableScan.create(cluster, table, List.of());
    }

    private static RelNode union(RelOptCluster cluster, int inputs) {
        List<RelNode> branches = new ArrayList<>();
        for (int i = 0; i < inputs; i++)
            branches.add(scan(cluster));
        return LogicalUnion.create(branches, true);
    }

    private static RexNode field(RelNode rel, int index) {
        return RexInputRef.of(index, rel.getRowType().getFieldList());
    }

    private static RexNode rowNumber(RelNode input) {
        RexBuilder builder = input.getCluster().getRexBuilder();
        return builder.makeOver(
                input.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                SqlStdOperatorTable.ROW_NUMBER, List.of(),
                List.of(field(input, 0), field(input, 1), field(input, 2)),
                ImmutableList.of(new RexFieldCollation(field(input, 3), ImmutableSet.of())),
                RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.CURRENT_ROW,
                true, true, false, false, false);
    }

    @Test
    public void recreatesProtectedQueryAfterInvalidation() {
        RelOptCluster cluster = createCluster();
        RelMetadataQuery first = cluster.getMetadataQuery();
        Assert.assertTrue(first instanceof FelderaRelMetadataQuery);
        cluster.invalidateMetadataQuery();
        RelMetadataQuery second = cluster.getMetadataQuery();
        Assert.assertTrue(second instanceof FelderaRelMetadataQuery);
        Assert.assertNotSame(first, second);
    }

    @Test
    public void preservesOrdinaryColumnAndScalarLineage() {
        RelNode input = scan(createCluster());
        RelMetadataQuery query = input.getCluster().getMetadataQuery();
        Set<RexNode> column = query.getExpressionLineage(input, field(input, 0));
        Assert.assertNotNull(column);
        Assert.assertEquals(1, column.size());
        RexTableInputRef reference = (RexTableInputRef) column.iterator().next();
        Assert.assertEquals(0, reference.getIndex());
        Assert.assertEquals(List.of("events"), reference.getQualifiedName());

        RexNode sum = input.getCluster().getRexBuilder().makeCall(
                SqlStdOperatorTable.PLUS, field(input, 0), field(input, 1));
        Set<RexNode> scalar = query.getExpressionLineage(input, sum);
        Assert.assertNotNull(scalar);
        Assert.assertEquals(1, scalar.size());
        RexCall resolved = (RexCall) scalar.iterator().next();
        Assert.assertEquals(SqlKind.PLUS, resolved.getKind());
        Assert.assertTrue(resolved.getOperands().stream().allMatch(RexTableInputRef.class::isInstance));
    }

    @Test
    public void preservesLiteralAndUnknownLineage() {
        RelOptCluster cluster = createCluster();
        RelNode input = scan(cluster);
        RexNode literal = cluster.getRexBuilder().makeExactLiteral(BigDecimal.ONE);
        Assert.assertEquals(Set.of(literal), cluster.getMetadataQuery().getExpressionLineage(input, literal));

        RelBuilder builder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
        RelNode values = builder.values(new String[]{"x"}, 1, 2).build();
        Assert.assertNull(cluster.getMetadataQuery().getExpressionLineage(values, field(values, 0)));
    }

    @Test
    public void preservesUnionColumnOrigins() {
        RelNode input = union(createCluster(), 8);
        Set<RexNode> lineage = input.getCluster().getMetadataQuery().getExpressionLineage(input, field(input, 0));
        Assert.assertNotNull(lineage);
        Assert.assertEquals(8, lineage.size());
        Assert.assertEquals(8, lineage.stream().map(node -> ((RexTableInputRef) node).getIdentifier())
                .collect(Collectors.toSet()).size());
    }

    @Test(timeout = 10000)
    public void refusesWindowLineageBeforeEnumeratingUnionCombinations() {
        // Keep the unfixed case finite (8^4 combinations) so a regression fails
        // an assertion instead of exhausting the test runner's heap.
        RelNode input = union(createCluster(), 8);
        RelMetadataQuery query = input.getCluster().getMetadataQuery();
        RexNode over = rowNumber(input);
        String before = RelOptUtil.toString(input);
        Assert.assertNull(query.getExpressionLineage(input, over));
        RexNode wrapped = input.getCluster().getRexBuilder().makeCall(
                SqlStdOperatorTable.PLUS, over,
                input.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.ONE));
        Assert.assertNull(query.getExpressionLineage(input, wrapped));
        Assert.assertEquals(before, RelOptUtil.toString(input));
    }

    @Test
    public void recursivelyRejectsProjectedWindowButKeepsOtherColumns() {
        RelNode input = union(createCluster(), 4);
        RelNode project = LogicalProject.create(input, List.of(),
                List.of(field(input, 0), rowNumber(input)), List.of("entity_id", "rn"), Set.of());
        RelMetadataQuery query = project.getCluster().getMetadataQuery();
        Assert.assertNull(query.getExpressionLineage(project, field(project, 1)));
        Set<RexNode> ordinaryColumn = query.getExpressionLineage(project, field(project, 0));
        Assert.assertNotNull(ordinaryColumn);
        Assert.assertEquals(4, ordinaryColumn.size());
    }

    @Test
    public void recursivelyRejectsCalcWindowButKeepsOtherColumns() {
        RelNode input = union(createCluster(), 4);
        RexProgram program = RexProgram.create(input.getRowType(),
                List.of(field(input, 0), rowNumber(input)), null, List.of("entity_id", "rn"),
                input.getCluster().getRexBuilder());
        RelNode calc = LogicalCalc.create(input, program);
        RelMetadataQuery query = calc.getCluster().getMetadataQuery();
        Assert.assertNull(query.getExpressionLineage(calc, field(calc, 1)));
        Set<RexNode> ordinaryColumn = query.getExpressionLineage(calc, field(calc, 0));
        Assert.assertNotNull(ordinaryColumn);
        Assert.assertEquals(4, ordinaryColumn.size());
    }

    @Test
    public void windowWithoutColumnReferencesStillHasUnknownLineage() {
        RelNode input = scan(createCluster());
        RexNode over = input.getCluster().getRexBuilder().makeOver(
                input.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                SqlStdOperatorTable.ROW_NUMBER, List.of(), List.of(), ImmutableList.of(),
                RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING,
                false, true, false, false, false);
        Assert.assertNull(input.getCluster().getMetadataQuery().getExpressionLineage(input, over));
    }

    @Test(timeout = 30000)
    public void compilesRankFilterOverUnion() {
        String branches = IntStream.range(0, 16)
                .mapToObj(i -> "SELECT entity_id, category, batch_id, updated_at + " + i + " AS updated_at FROM events")
                .collect(Collectors.joining(" UNION ALL "));
        String sql = """
                CREATE TABLE events(entity_id INT, category INT, batch_id INT, updated_at INT);
                CREATE LOCAL VIEW combined AS
                """ + branches + ";\n" + """
                CREATE VIEW latest AS
                SELECT entity_id, category, batch_id, updated_at FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY entity_id, category, batch_id ORDER BY updated_at DESC
                    ) AS rn FROM combined
                ) WHERE rn = 1;
                """;
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation(sql);
        Assert.assertNotNull(compiler.getFinalCircuit(false));
        Assert.assertEquals(compiler.messages.toString(), 0, compiler.messages.errorCount());
    }
}
