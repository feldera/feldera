package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/** Run positive D1d fixtures with the Java planner and with the independent
 * Rust set constructors imported through the S1 relation decoder. */
public class DataFusionSetOperationTests extends BaseSQLTests {
    private static RelNode construct(JsonNode value, RelOptCluster cluster, String column, int depth)
            throws IOException {
        if (depth > 24 || !value.path("nullable").isBoolean())
            throw new IOException("Malformed or overdeep set relation");
        boolean nullable = value.path("nullable").booleanValue();
        String kind = value.path("kind").asText();
        if (kind.equals("integer_values")) {
            var rows = value.path("rows");
            if (!rows.isArray() || rows.size() > 1000)
                throw new IOException("Invalid INTEGER rows");
            var types = cluster.getTypeFactory();
            var columnType = types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), nullable);
            var rowType = types.builder().add(column, columnType).build();
            var tuples = com.google.common.collect.ImmutableList
                    .<com.google.common.collect.ImmutableList<RexLiteral>>builder();
            for (var row: rows) {
                if (row.isNull() && !nullable || !row.isNull() && !row.isInt())
                    throw new IOException("Invalid INTEGER cell");
                var literal = row.isNull() ? cluster.getRexBuilder().makeNullLiteral(columnType)
                        : cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(row.intValue()), columnType);
                tuples.add(com.google.common.collect.ImmutableList.of(literal));
            }
            return LogicalValues.create(cluster, rowType, tuples.build());
        }
        if (!value.path("all").isBoolean())
            throw new IOException("Missing set operation quantifier");
        var left = construct(value.path("left"), cluster, column, depth + 1);
        var right = construct(value.path("right"), cluster, column, depth + 1);
        boolean all = value.path("all").booleanValue();
        return switch (kind) {
            case "intersect" -> LogicalIntersect.create(List.of(left, right), all);
            case "minus" -> LogicalMinus.create(List.of(left, right), all);
            default -> throw new IOException("Unexpected set operator " + kind);
        };
    }

    private static String expected(JsonNode bag) {
        var table = new StringBuilder("k | weight\n---+-------");
        for (var row: bag) {
            var cell = row.path("row");
            table.append('\n').append(cell.isNull() ? "NULL" : cell.intValue())
                    .append(" | ").append(row.path("weight").intValue());
        }
        return table.toString();
    }

    @Test public void preservesSetMultiplicityThroughS1() throws IOException {
        String manifest = System.getProperty("feldera.dataFusionSets.manifest");
        Assume.assumeNotNull(manifest);
        var mapper = Utilities.deterministicObjectMapper();
        JsonNode fixtures = mapper.readTree(Files.readString(Path.of(manifest)));
        var directory = Path.of(System.getProperty("feldera.dataFusionSets.s1"));
        Files.createDirectory(directory);
        Assert.assertTrue(fixtures.isArray());
        for (var fixture: fixtures) {
            String id = fixture.path("id").asText();
            Assert.assertTrue(id.matches("[a-z][a-z0-9_-]*"));
            Assert.assertEquals(id, fixture.path("expected"), fixture.path("rust").path("rows"));
            for (boolean importPlan: List.of(false, true)) {
                var compiler = this.testCompiler();
                int[] imported = {0};
                if (importPlan) {
                    compiler.getSqlToRelCompiler().optimizerInputTransform = (stage, original) -> {
                        if (!stage.equals("Remove sub-queries") || original.getRowType().getFieldCount() != 1)
                            return original;
                        try {
                            var rowType = original.getRowType();
                            var field = rowType.getFieldList().get(0);
                            var recipe = fixture.path("rust").path("relation");
                            if (field.getType().getSqlTypeName() != SqlTypeName.INTEGER
                                    || field.getType().isNullable() != recipe.path("nullable").booleanValue())
                                throw new IOException("Rust root has a different SQL type");
                            var graph = construct(recipe, original.getCluster(), field.getName(), 0);
                            var registry = new S1BuiltinOperatorRegistry();
                            var wire = CorrelationPlanProbe.encode(id, graph, compiler, registry);
                            Files.writeString(directory.resolve(id + ".json"), wire.toString(),
                                    StandardOpenOption.CREATE_NEW);
                            var decoded = S1RelNodes.decodeCatalog(wire, registry);
                            if (decoded.roots().size() != 1 || !rowType.equals(decoded.roots().get(0).getRowType()))
                                throw new IOException("S1 import changed set root contract");
                            imported[0]++;
                            return decoded.roots().get(0);
                        } catch (IOException error) {
                            throw new UncheckedIOException(error);
                        }
                    };
                }
                compiler.submitStatementsForCompilation("CREATE VIEW v AS " + fixture.path("sql").asText() + ";");
                var circuit = this.getCCS(compiler);
                Assert.assertEquals(id, importPlan ? 1 : 0, imported[0]);
                circuit.step("", expected(fixture.path("expected")));
            }
        }
    }
}
