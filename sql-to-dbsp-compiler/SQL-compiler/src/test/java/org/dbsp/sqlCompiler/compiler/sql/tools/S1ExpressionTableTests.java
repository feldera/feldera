package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLambda;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Consumer;

public class S1ExpressionTableTests {
    @Test public void inputReferencesWidenNullabilityWithoutMergingArenaObjects() throws IOException {
        ObjectNode wire = S1ExpressionProbe.fixtureTable();
        ((ObjectNode) wire.at("/expressions/0")).put("ty", 2);
        ((ArrayNode) wire.at("/operators/0/parameters")).set(0, wire.numberNode(2));
        var registry = new S1BuiltinOperatorRegistry();
        var equal = S1ExpressionTable.decode(wire, registry);
        Assert.assertNotSame(equal.types().get(0), equal.expressions().get(0).getType());
        ((ObjectNode) wire.at("/type_table/types/2")).put("nullable", true);
        ((ObjectNode) wire.at("/operators/0")).put("result", 2);
        ((ObjectNode) wire.at("/expressions/4")).put("ty", 2);
        Assert.assertTrue(S1ExpressionTable.decode(wire, registry).expressions().get(0).getType().isNullable());
        ((ObjectNode) wire.at("/type_table/types/0")).put("nullable", true);
        ((ObjectNode) wire.at("/type_table/types/2")).put("nullable", false);
        Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(wire, registry));
    }

    private static S1ExpressionTable.Decoded decode(ObjectNode table) throws IOException {
        return S1ExpressionTable.decode(table, new S1BuiltinOperatorRegistry());
    }

    @Test public void contextualExporterPreservesAllSampleGraphs() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        for (var sample: S1ExpressionTransportProbe.samples(registry)) {
            ObjectNode table = S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry, sample.source());
            var imported = S1ExpressionTable.decode(table, registry);
            Assert.assertEquals(sample.roots().size(), imported.roots().size());
        }
    }

    @Test public void characterInputReferencesRetainTheExactArenaType() throws IOException {
        var factory = new S1TypeTable.Factory();
        var character = factory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR, 20);
        var row = new org.apache.calcite.rel.type.RelRecordType(org.apache.calcite.rel.type.StructKind.FULLY_QUALIFIED,
                List.of(new org.apache.calcite.rel.type.RelDataTypeFieldImpl("X", 0, character)), false);
        var input = new org.apache.calcite.rex.RexInputRef(0, character);
        var registry = new S1BuiltinOperatorRegistry();
        ObjectNode wire = S1ExpressionEncoder.encode(List.of(input, input), row, registry);
        var decoded = S1ExpressionTable.decode(wire, registry);
        Assert.assertSame(decoded.types().get(wire.at("/expressions/0/ty").intValue()), decoded.roots().get(0).getType());
        Assert.assertSame(decoded.roots().get(0), decoded.roots().get(1));
    }

    @Test public void sourceBearingCallsRetainExactUtf16CoordinatesAndText() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).stream()
                .filter(item -> item.name().equals("source-bearing-call")).findFirst().orElseThrow();
        ObjectNode wire = S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry, sample.source());
        Assert.assertEquals(sample.source().text(), wire.at("/type_table/sources/0/text").textValue());
        var imported = (RexCall) S1ExpressionTable.decode(wire, registry).roots().get(0);
        Assert.assertEquals(((RexCall) sample.roots().get(0)).getParserPosition(), imported.getParserPosition());
        Assert.assertEquals(4, imported.getParserPosition().getColumnNum());
        Assert.assertEquals(5, imported.getParserPosition().getEndColumnNum());
    }

    @Test public void malformedSourceRecordsAndOutOfTextRangesFail() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).stream()
                .filter(item -> item.name().equals("source-bearing-call")).findFirst().orElseThrow();
        List<Consumer<ObjectNode>> mutations = List.of(
                wire -> ((ObjectNode) wire.at("/type_table/sources/0")).put("extra", true),
                wire -> ((ObjectNode) wire.at("/type_table/sources/0")).remove("text"),
                wire -> ((ObjectNode) wire.at("/expressions/0/origin")).put("source", 99),
                wire -> ((ObjectNode) wire.at("/expressions/0/origin/start")).put("line", 0),
                wire -> ((ObjectNode) wire.at("/expressions/0/origin/start")).put("column", 0),
                wire -> ((ObjectNode) wire.at("/expressions/0/origin/end")).put("line", 99),
                wire -> ((ObjectNode) wire.at("/expressions/0/origin/end")).put("column", 99),
                wire -> ((ObjectNode) wire.at("/expressions/0/origin/start")).put("line", 2).put("column", 6),
                wire -> ((ObjectNode) wire.at("/expressions/0/origin/start")).put("extra", 1),
                wire -> ((ArrayNode) wire.at("/type_table/sources")).removeAll());
        for (var mutation: mutations) {
            ObjectNode wire = S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry, sample.source());
            mutation.accept(wire);
            Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(wire, registry));
        }
    }

    @Test public void onePastLineEndIsAnExplicitEofAnchor() throws IOException {
        var sources = S1ExpressionSources.of(new S1ExpressionSources.Source("query.sql", "😀"));
        var eof = new org.apache.calcite.sql.parser.SqlParserPos(1, 3, 1, 3);
        Assert.assertEquals(eof, sources.position(sources.origin(eof)));
        var outside = new org.apache.calcite.sql.parser.SqlParserPos(1, 4);
        Assert.assertThrows(IOException.class, () -> sources.origin(outside));
        var quoted = new org.apache.calcite.sql.parser.SqlParserPos(1, 1).withQuoting(true);
        Assert.assertThrows(IOException.class, () -> sources.origin(quoted));
        Assert.assertThrows(IOException.class,
                () -> sources.origin(org.apache.calcite.sql.parser.SqlParserPos.ZERO.withQuoting(true)));
    }

    @Test public void actualOptimizedProjectExpressionsRoundTripWithTheirInputContext() throws IOException {
        assertOptimizedProjectRoundTrip("CREATE TABLE T(X INTEGER NOT NULL);\n"
                + "CREATE VIEW V AS SELECT X + 1 AS Y, X * 2 AS Z FROM T;");
        assertOptimizedProjectRoundTrip("CREATE TABLE T(r ROW(a INT, b INT NULL), s ROW(a INT, b INT NULL));\n"
                + "CREATE VIEW V AS SELECT r <=> s AS eq FROM T;");
        assertOptimizedProjectRoundTrip("CREATE TABLE T(g VARCHAR, x INT);\n"
                + "CREATE VIEW V AS SELECT g, MAX(CASE WHEN x > 2 THEN 1 ELSE 0 END) FROM T GROUP BY g;");
    }

    @Test public void internalDatetimeSubtractionSurvivesTransport() throws IOException {
        assertOptimizedProjectRoundTrip("CREATE TABLE T(ts TIMESTAMP);"
                + "CREATE VIEW V AS SELECT CAST(ts AS DATE) - INTERVAL '1' DAY FROM T;");
    }

    @Test public void identityFunctionBodiesRetainTheirParameterType() throws IOException {
        assertOptimizedProjectRoundTrip("CREATE FUNCTION sum_args(a INT, b INT) RETURNS INT AS a + b;"
                + "CREATE TABLE T(x INT);CREATE VIEW V AS SELECT sum_args(x, 1) FROM T;");
        assertOptimizedProjectRoundTrip("CREATE FUNCTION F(A INT ARRAY) RETURNS INT ARRAY AS A;"
                + "CREATE VIEW V AS SELECT ARRAY_REVERSE(F(ARRAY[2, 3, 4]));");
        assertOptimizedProjectRoundTrip("CREATE TABLE t(bin BINARY);"
                + "CREATE FUNCTION nbin2nbin(i BINARY NOT NULL) RETURNS BINARY NOT NULL AS i;"
                + "CREATE VIEW v AS SELECT nbin2nbin(bin) FROM t;");
    }

    private static void assertOptimizedProjectRoundTrip(String sql) throws IOException {
        var options = new org.dbsp.sqlCompiler.compiler.CompilerOptions();
        options.ioOptions.inputCircuit = true;
        var compiler = new org.dbsp.sqlCompiler.compiler.DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        Assert.assertNotNull(compiler.getFinalCircuit(false));
        compiler.throwIfErrorsOccurred();
        var registry = new S1BuiltinOperatorRegistry();
        var plans = compiler.getPlans(new org.dbsp.util.IndentStream(new StringBuilder()));
        var queue = new java.util.ArrayDeque<>(plans.keySet());
        var visited = new java.util.IdentityHashMap<org.apache.calcite.rel.RelNode, Boolean>();
        int projects = 0;
        while (!queue.isEmpty()) {
            var relation = queue.removeFirst();
            if (visited.put(relation, true) != null) continue;
            if (relation instanceof org.apache.calcite.rel.logical.LogicalProject project) {
                ObjectNode wire = S1ExpressionEncoder.encode(project.getProjects(), project.getInput().getRowType(),
                        registry, new S1ExpressionSources.Source("query.sql", sql));
                var imported = S1ExpressionTable.decode(wire, registry);
                Assert.assertEquals(project.getProjects(), imported.roots());
                var reconstructed = project.copy(project.getTraitSet(), project.getInput(), imported.roots(), project.getRowType());
                Assert.assertTrue(project.deepEquals(reconstructed));
                projects++;
            }
            queue.addAll(relation.getInputs());
        }
        Assert.assertTrue("The SQL must exercise an actual optimized project", projects > 0);
    }

    @Test public void generatedRangesRetainUnknownComponentsInsteadOfInventingPositions() throws IOException {
        var sources = S1ExpressionSources.of(new S1ExpressionSources.Source("query.sql", "first\nsecond"));
        var position = new org.apache.calcite.sql.parser.SqlParserPos(0, 0, 2, 4);
        ObjectNode origin = sources.origin(position);
        Assert.assertEquals("generated", origin.path("kind").textValue());
        Assert.assertEquals(position, sources.position(origin));
        ((ObjectNode) origin.path("start")).put("line", 1).put("column", 1);
        Assert.assertThrows(IOException.class, () -> sources.position(origin));
        ((ObjectNode) origin.path("start")).put("line", 0).put("column", 0);
        ((ObjectNode) origin.path("end")).put("line", 0).put("column", 0);
        Assert.assertThrows(IOException.class, () -> sources.position(origin));
    }

    @Test public void exporterRejectsUnmodeledSubclassesAndSourcePositions() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).get(0);
        var type = sample.input().getFieldList().get(0).getType();
        var dynamic = new org.apache.calcite.rex.RexDynamicParam(type, 0);
        Assert.assertThrows(IOException.class, () -> S1ExpressionEncoder.encode(List.of(dynamic), sample.input(), registry));
        var builder = new org.apache.calcite.rex.RexBuilder(new S1TypeTable.Factory());
        var input = builder.makeInputRef(type, 0);
        var sourceCall = builder.makeCall(new org.apache.calcite.sql.parser.SqlParserPos(2, 3), type,
                org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS, List.of(input, input));
        Assert.assertThrows(IOException.class, () -> S1ExpressionEncoder.encode(List.of(sourceCall), sample.input(), registry));
    }

    @Test public void arbitraryArenaOrderRetainsForwardReferencesAndRepeatedRoots() throws IOException {
        ObjectNode table = S1ExpressionProbe.fixtureTable();
        ArrayNode nodes = (ArrayNode) table.path("expressions");
        // Swap call 4 and input 0. Both the call and field access now have forward edges.
        var input = nodes.get(0); nodes.set(0, nodes.get(4)); nodes.set(4, input);
        ((ArrayNode) nodes.get(0).path("kind").path("arguments")).set(0, table.numberNode(4));
        ((ArrayNode) table.path("roots")).set(0, table.numberNode(0));
        ((ArrayNode) table.path("roots")).set(2, table.numberNode(0));
        var decoded = decode(table);
        var call = (RexCall) decoded.expressions().get(0);
        Assert.assertSame(decoded.expressions().get(4), call.getOperands().get(0));
        Assert.assertSame(decoded.roots().get(0), decoded.roots().get(2));
        Assert.assertNotSame(decoded.types().get(0), decoded.types().get(2));
        Assert.assertEquals(decoded.types().get(0), decoded.types().get(2));
    }

    @Test public void declarationAndBodyRetainSeparateEqualObjects() throws IOException {
        ObjectNode table = S1ExpressionProbe.fixtureTable();
        ArrayNode nodes = (ArrayNode) table.path("expressions");
        nodes.add(nodes.get(5).deepCopy());
        ((ObjectNode) nodes.get(6).path("kind").path("parameters").get(0)).put("declaration", 7);
        var decoded = decode(table);
        var lambda = (RexLambda) decoded.expressions().get(6);
        Assert.assertSame(decoded.expressions().get(7), lambda.getParameters().get(0));
        Assert.assertSame(decoded.expressions().get(5), lambda.getExpression());
        Assert.assertEquals(lambda.getExpression(), lambda.getParameters().get(0));
        Assert.assertNotSame(lambda.getExpression(), lambda.getParameters().get(0));
    }

    @Test public void bindingFreeValuesCanBeSharedBetweenEvaluationScopes() throws IOException {
        ObjectNode table = S1ExpressionProbe.fixtureTable();
        ((ObjectNode) table.path("expressions").get(6).path("kind")).put("body", 3);
        var decoded = decode(table);
        Assert.assertSame(decoded.expressions().get(3), ((RexLambda) decoded.expressions().get(6)).getExpression());
    }

    @Test public void disconnectedExpressionsAreChecked() throws IOException {
        ObjectNode table = S1ExpressionProbe.fixtureTable();
        ((ArrayNode) table.path("roots")).removeAll();
        ((ObjectNode) table.path("expressions").get(4).path("kind")).put("operator", 999);
        Assert.assertThrows(IOException.class, () -> decode(table));
    }

    @Test public void registeredSearchOperatorsStillRequireExpansion() throws IOException {
        ObjectNode table = S1ExpressionProbe.fixtureTable();
        String search = new S1OperatorRegistry().key(org.apache.calcite.sql.fun.SqlStdOperatorTable.SEARCH);
        ((ObjectNode) table.path("operator_objects").get(0)).put("key", search);
        var error = Assert.assertThrows(IOException.class, () -> decode(table));
        Assert.assertTrue(error.getMessage().contains("expanded"));
    }

    @Test public void malformedReferencesSignaturesAndScopesFail() throws IOException {
        List<Consumer<ObjectNode>> mutations = List.of(
                table -> table.put("schema_version", "expressions-v0"),
                table -> table.put("unexpected", true),
                table -> ((ObjectNode) table.path("scopes").get(0)).remove("parent"),
                table -> ((ObjectNode) table.path("scopes").get(0)).put("parent", 1),
                table -> ((ArrayNode) table.path("scopes").get(1).path("bindings")).add(0),
                table -> ((ObjectNode) table.path("bindings").get(0)).put("kind", "lambda_parameter"),
                table -> ((ObjectNode) table.path("expressions").get(0).path("kind")).put("field", 3),
                table -> ((ObjectNode) table.path("expressions").get(0).path("kind")).put("field", -1),
                table -> ((ObjectNode) table.path("expressions").get(4).path("kind")).put("operator", 3),
                table -> ((ObjectNode) table.path("operator_objects").get(0)).put("registry_version", "future"),
                table -> ((ObjectNode) table.path("operator_objects").get(0)).put("key", "java.lang.Runtime#exec"),
                table -> ((ObjectNode) table.path("operators").get(0)).put("result", 2),
                table -> ((ArrayNode) table.path("operators").get(0).path("parameters")).remove(0),
                table -> ((ArrayNode) table.path("expressions").get(4).path("kind").path("arguments")).set(0, table.numberNode(4)),
                table -> ((ObjectNode) table.path("expressions").get(6).path("kind")).put("body", 0),
                table -> ((ObjectNode) table.path("expressions").get(6).path("kind").path("parameters").get(0)).put("declaration", 3),
                table -> ((ObjectNode) table.path("expressions").get(6).path("kind").path("parameters").get(0)).remove("declaration"),
                table -> ((ObjectNode) table.path("expressions").get(5)).put("scope", 0),
                table -> ((ObjectNode) table.path("expressions").get(3).path("kind").path("literal")).put("ty", 2),
                table -> ((ObjectNode) table.path("expressions").get(2).path("kind")).put("kind", "sub_query"));
        for (int index = 0; index < mutations.size(); index++) {
            ObjectNode table = S1ExpressionProbe.fixtureTable();
            mutations.get(index).accept(table);
            int mutation = index;
            Assert.assertThrows("mutation " + mutation, IOException.class, () -> decode(table));
        }
    }

    @Test public void sharedSubtreesStillEnforceMaximumDepth() throws IOException {
        ObjectNode table = S1ExpressionProbe.fixtureTable();
        ArrayNode nodes = (ArrayNode) table.path("expressions");
        int child = 4;
        for (int index = 0; index < 256; index++) {
            ObjectNode call = nodes.get(4).deepCopy();
            ((ArrayNode) call.path("kind").path("arguments")).set(0, table.numberNode(child));
            child = nodes.size(); nodes.add(call);
        }
        Assert.assertThrows(IOException.class, () -> decode(table));
    }

    @Test public void fileReaderRejectsDuplicateFieldsAndTrailingDocuments() throws IOException {
        var input = Files.createTempFile("s1-expression-reader", ".json");
        try {
            String valid = S1ExpressionProbe.fixtureTable().toString();
            Files.writeString(input, valid + " {}");
            Assert.assertThrows(IOException.class, () -> S1ExpressionTable.read(input, new S1BuiltinOperatorRegistry()));
            Files.writeString(input, valid.replace("\"schema_version\":\"expressions-v4\"",
                    "\"schema_version\":\"expressions-v4\",\"schema_version\":\"expressions-v4\""));
            Assert.assertThrows(IOException.class, () -> S1ExpressionTable.read(input, new S1BuiltinOperatorRegistry()));
            Files.writeString(input, valid);
            Assert.assertEquals(7, S1ExpressionTable.read(input, new S1BuiltinOperatorRegistry()).expressions().size());
        } finally {
            Files.delete(input);
        }
    }
}
