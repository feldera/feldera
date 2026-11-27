package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.frontend.statements.*;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;

public class S1ProgramTests {
    private static ObjectNode capture(String sql) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var input = mapper.createObjectNode().put("id", "statement-program");
        input.set("options", mapper.valueToTree(new CompilerOptions()));
        input.putArray("submissions").addObject().put("batch", 0).put("many", true).put("sql", sql);
        return S1Program.capture(input, new S1BuiltinOperatorRegistry());
    }

    @Test public void tableScanOrderSurvivesImportAndRejectsInvalidPermutations() throws IOException {
        var wire = capture("CREATE TABLE a(x INT); CREATE TABLE b(x INT); CREATE VIEW v AS SELECT a.x FROM a JOIN b ON a.x = b.x;");
        var registry = new S1BuiltinOperatorRegistry();
        var decoded = S1Program.decode(wire, registry);
        int previous = -1;
        for (var identity: wire.path("table_scan_order")) {
            int actual = decoded.relations().nodes().get(identity.intValue()).getId();
            Assert.assertTrue(actual > previous); previous = actual;
        }
        var duplicate = wire.deepCopy();
        duplicate.withArray("table_scan_order").set(1, duplicate.path("table_scan_order").get(0));
        Assert.assertThrows(IOException.class, () -> S1Program.decode(duplicate, registry));
        var missing = wire.deepCopy(); missing.remove("table_scan_order");
        Assert.assertThrows(IOException.class, () -> S1Program.decode(missing, registry));
        var reversed = wire.deepCopy();
        var order = reversed.withArray("table_scan_order");
        for (int left = 0, right = order.size() - 1; left < right; left++, right--) {
            var saved = order.get(left); order.set(left, order.get(right)); order.set(right, saved);
        }
        var reverseDecoded = S1Program.decode(reversed, registry);
        Assert.assertTrue(reverseDecoded.relations().nodes().get(order.get(0).intValue()).getId()
                < reverseDecoded.relations().nodes().get(order.get(1).intValue()).getId());
    }

    @Test public void snapshotsPredatingTheRustOptimizerUseJavaPasses() throws IOException {
        var wire = capture("CREATE VIEW v AS SELECT 1 AS x;");
        var io = wire.withObject("/context/options/ioOptions");
        io.remove("rustOptimizerPasses");
        var registry = new S1BuiltinOperatorRegistry();
        Assert.assertFalse(S1Program.decode(wire, registry).context().options().ioOptions.rustOptimizerPasses);
        io.putNull("rustOptimizerPasses");
        Assert.assertThrows(IOException.class, () -> S1Program.decode(wire, registry));
    }

    @Test public void orderedStatementsAndDmlRoundTripWithoutParsingSql() throws IOException {
        var wire = capture("""
                SET ENFORCE_POSITIVE_INPUTS = TRUE;
                CREATE TYPE record_t AS ("Field" INT);
                CREATE FUNCTION plus_one(x INT) RETURNS INT AS x + 1;
                CREATE TABLE "T"("A" INT NOT NULL PRIMARY KEY, b INT DEFAULT 7);
                INSERT INTO "T"(b, "A") VALUES (20, 10);
                REMOVE FROM "T"(b, "A") VALUES (20, 10);
                CREATE VIEW v AS SELECT "A", plus_one(b) AS b FROM "T";
                CREATE INDEX ix ON v(b, "A");
                """);
        var decoded = S1Program.decode(wire, new S1BuiltinOperatorRegistry());
        var kinds = decoded.statements().stream().filter(RelStatement::isVisible).map(s -> s.getClass().getSimpleName()).toList();
        Assert.assertTrue(kinds.containsAll(List.of("SetOptionStatement", "CreateTypeStatement", "CreateFunctionStatement",
                "CreateTableStatement", "TableModifyStatement", "CreateViewStatement", "CreateIndexStatement")));
        Assert.assertEquals(2, decoded.statements().stream().filter(s -> s instanceof TableModifyStatement).count());
        Assert.assertTrue(decoded.statements().stream().anyMatch(s -> !s.isVisible()));
        var types = decoded.relations().expressions().types(); var typeIds = new IdentityHashMap<org.apache.calcite.rel.type.RelDataType, Integer>();
        for (int id = 0; id < types.size(); id++) typeIds.put(types.get(id), id);
        var typeTable = new S1TypeTable.Encoded((ObjectNode) wire.at("/table/type_table"), typeIds);
        var relationIds = new IdentityHashMap<org.apache.calcite.rel.RelNode, Integer>();
        for (int id = 0; id < decoded.relations().nodes().size(); id++) relationIds.put(decoded.relations().nodes().get(id), id);
        var sources = S1ExpressionSources.decode(wire.at("/table/type_table/sources"));
        for (int id = 0; id < decoded.statements().size(); id++) {
            var statement = decoded.statements().get(id); var expected = wire.path("statements").get(id);
            var restored = S1Statements.encode(S1Statements.capture(statement, sources), relationIds, typeTable,
                    expected.path("schema").asInt(-1), expected.path("values").asInt(-1),
                    expected.has("function") ? expected.path("function").intValue() : null);
            Assert.assertEquals(expected, restored);
            Assert.assertTrue(statement.parsedStatement.statement() instanceof org.apache.calcite.sql.FelderaS1StatementSource);
        }
        for (var statement: decoded.statements()) {
            if (!(statement instanceof CreateTableStatement table)) continue;
            var schemas = decoded.relations().catalog().schemas().stream().filter(s -> s.name().equals(table.getName())).toList();
            Assert.assertTrue(schemas.stream().anyMatch(s -> s.columns() == table.columns));
        }
    }

    @Test public void aggregateDeclarationsAndCallsShareOneOperator() throws IOException {
        var wire = capture("""
                CREATE LINEAR AGGREGATE "Total"(value INT) RETURNS INT;
                CREATE LINEAR AGGREGATE unused(value INT) RETURNS INT;
                CREATE TABLE t(x INT);
                CREATE VIEW v AS SELECT "Total"(x) AS total FROM t;
                """);
        var decoded = S1Program.decode(wire, new S1BuiltinOperatorRegistry());
        var declarations = decoded.statements().stream().filter(s -> s instanceof CreateAggregateStatement)
                .map(s -> (CreateAggregateStatement) s).toList();
        Assert.assertEquals(2, declarations.size());
        Assert.assertTrue(declarations.get(0).getFunction().isLinear());
        Assert.assertTrue(declarations.get(1).getFunction().isLinear());
        Assert.assertEquals("Total", declarations.get(0).getFunction().description.name.getSimple());
        var calls = decoded.relations().nodes().stream().filter(n -> n instanceof org.apache.calcite.rel.core.Aggregate)
                .flatMap(n -> ((org.apache.calcite.rel.core.Aggregate) n).getAggCallList().stream()).toList();
        Assert.assertTrue(calls.stream().anyMatch(call -> call.getAggregation() == declarations.get(0).function));
        Assert.assertFalse(calls.stream().anyMatch(call -> call.getAggregation() == declarations.get(1).function));
        var invalid = wire.deepCopy();
        invalid.withObject("/properties/operators").put("kind", "owned");
        Assert.assertThrows(IOException.class, () -> S1Program.decode(invalid, new S1BuiltinOperatorRegistry()));
    }

    @Test public void generatedSqlKeepsItsOwnSourceWhenUserTextIsShort() throws IOException {
        var wire = capture("CREATE VIEW v AS SELECT 1 AS x;");
        var sources = S1ExpressionSources.decode(wire.at("/table/type_table/sources"));
        Assert.assertEquals("CREATE VIEW v AS SELECT 1 AS x;", sources.text(0));
        var decoded = S1Program.decode(wire, new S1BuiltinOperatorRegistry());
        for (int id = 0; id < decoded.statements().size(); id++) {
            var statement = decoded.statements().get(id);
            int source = wire.path("statements").get(id).at("/source/source").intValue();
            if (statement.isVisible()) Assert.assertEquals(0, source);
            else {
                Assert.assertTrue(source > 0);
                Assert.assertEquals(sources.text(source), statement.parsedStatement.generatedSource());
                Assert.assertTrue(sources.text(source).contains("CREATE TABLE FELDERA_ERROR_TABLE"));
            }
        }
        var invalid = wire.deepCopy();
        for (var statement: invalid.path("statements"))
            if (!statement.at("/source/visible").booleanValue()) {
                ((ObjectNode) statement.path("source")).put("source", 0);
                break;
            }
        Assert.assertThrows(IOException.class, () -> S1Program.decode(invalid, new S1BuiltinOperatorRegistry()));
    }

    @Test public void foreignKeyPositionsAndQuotedNamesSurvive() throws IOException {
        var wire = capture("""
                CREATE TABLE "Parent"("Id" INT NOT NULL PRIMARY KEY);
                CREATE TABLE child(id INT, FOREIGN KEY (id) REFERENCES "Parent"("Id"));
                CREATE VIEW v AS SELECT * FROM child;
                """);
        var decoded = S1Program.decode(wire, new S1BuiltinOperatorRegistry());
        var table = decoded.statements().stream().filter(s -> s instanceof CreateTableStatement t && t.getName().name().equals("child"))
                .map(s -> (CreateTableStatement) s).findFirst().orElseThrow();
        Assert.assertEquals(1, table.foreignKeys.size());
        var key = table.foreignKeys.get(0);
        Assert.assertTrue(key.otherTable.tableName.toIdentifier().isQuoted());
        Assert.assertTrue(key.otherTable.columnNames.get(0).toIdentifier().isQuoted());
        Assert.assertEquals(2, key.otherTable.listPos.start.line);
        Assert.assertEquals(2, key.thisTable.listPos.start.line);
    }
}
