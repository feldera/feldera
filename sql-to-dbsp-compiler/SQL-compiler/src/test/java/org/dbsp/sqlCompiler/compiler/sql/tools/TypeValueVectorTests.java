package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Executes diagnosed type probe shapes with signed transactions and compares
 * Java-generated runtime output against bags evaluated by the Rust probe. */
public class TypeValueVectorTests extends BaseSQLTests {
    private static final String MAX = "CAST('9223372036854775807' AS BIGINT)";
    private static final String MIN = "CAST('-9223372036854775808' AS BIGINT)";
    private static final String VALUE = "CAST(a AS BIGINT)";

    private static String query(String operation) {
        return switch (operation) {
            case "equal" -> "SELECT rid, a = " + MAX + " AS result FROM t";
            case "less" -> "SELECT rid, a < CAST(0 AS BIGINT) AS result FROM t";
            case "in" -> "SELECT rid, " + VALUE + " IN (" + MIN + ", CAST(0 AS BIGINT), "
                            + MAX + ", CAST(NULL AS BIGINT)) AS result FROM t";
            case "between" -> "SELECT rid, " + VALUE + " BETWEEN CAST(-1 AS BIGINT) "
                            + "AND CAST(1 AS BIGINT) AS result FROM t";
            case "case" -> "SELECT rid, CASE WHEN a < CAST(0 AS BIGINT) "
                            + "THEN CAST(-1 AS BIGINT) ELSE " + VALUE + " END AS result FROM t";
            case "join" -> "SELECT t.rid AS l, u.rid AS r FROM t JOIN u ON CAST(t.a AS BIGINT) = u.b";
            case "union_all" -> setOperation("UNION ALL");
            case "intersect_all" -> setOperation("INTERSECT ALL");
            case "except_all" -> setOperation("EXCEPT ALL");
            default -> throw new IllegalArgumentException("Unknown value variant " + operation);
        };
    }

    private static String setOperation(String operation) {
        return "SELECT " + VALUE + " AS result FROM t " + operation + " SELECT b AS result FROM u";
    }

    private static void changes(StringBuilder sql, JsonNode entries, String table, String column, boolean string) {
        Assert.assertTrue(entries.isArray());
        for (var entry: entries) {
            var row = entry.path("row");
            var id = row.path("id");
            var value = row.path(column);
            var weight = entry.path("weight");
            Assert.assertTrue(id.isInt() && (value.isNull() || (string ? value.isTextual() : value.isIntegralNumber())));
            Assert.assertTrue(weight.isInt() && weight.intValue() != 0 && Math.abs(weight.intValue()) <= 32);
            if (value.isTextual()) Assert.assertTrue(value.asText().matches("[+-]?[0-9]{1,19}"));
            if (value.isIntegralNumber()) Assert.assertTrue(value.canConvertToLong());
            String sqlValue = value.isNull() ? "NULL"
                    : "'" + value.asText() + "'";
            if (!string && !value.isNull())
                sqlValue = "CAST(" + sqlValue + " AS BIGINT)";
            for (int i = 0; i < Math.abs(weight.intValue()); i++) {
                sql.append(weight.intValue() > 0 ? "INSERT INTO " : "REMOVE FROM ")
                        .append(table).append(" VALUES (").append(id.intValue()).append(", ")
                        .append(sqlValue).append(");\n");
            }
        }
    }

    private static String zset(JsonNode delta, boolean set) {
        var table = new StringBuilder(set ? "result | weight\n---+-------"
                : "a | b | weight\n--+--+-------");
        Assert.assertTrue(delta.isArray());
        for (var change: delta) {
            var row = change.path("row");
            Assert.assertEquals(set ? 1 : 2, row.size());
            table.append('\n');
            for (var cell: row) {
                Assert.assertTrue(cell.isNull() || cell.isBoolean()
                        || cell.isIntegralNumber() && cell.canConvertToLong());
                table.append(cell.isNull() ? "NULL" : cell.asText()).append(" | ");
            }
            table.append(change.path("weight").longValue());
        }
        return table.toString();
    }

    @Test public void typeValueVectorsOnTheGeneratedRustRuntime() throws IOException {
        String manifest = System.getProperty("feldera.typeValues.manifest");
        Assume.assumeNotNull(manifest);
        var fixtures = Utilities.deterministicObjectMapper().readTree(Files.readString(Path.of(manifest)));
        Assert.assertEquals(9, fixtures.size());
        for (var fixture: fixtures) {
            String operation = fixture.path("operation").asText();
            boolean set = operation.endsWith("_all");
            var compiler = this.testCompiler();
            compiler.options.languageOptions.incrementalize = true;
            compiler.submitStatementsForCompilation(
                    "CREATE TABLE t(rid INT NOT NULL, a VARCHAR(32)); "
                    + "CREATE TABLE u(rid INT NOT NULL, b BIGINT); "
                    + "CREATE VIEW v AS " + query(operation) + ";");
            var circuit = this.getCCS(compiler);
            var transactions = fixture.path("transactions");
            var deltas = fixture.path("deltas");
            Assert.assertEquals(transactions.size(), deltas.size());
            for (int i = 0; i < transactions.size(); i++) {
                var sql = new StringBuilder();
                changes(sql, transactions.get(i).path("text"), "t", "a", true);
                changes(sql, transactions.get(i).path("integers"), "u", "b", false);
                circuit.step(sql.toString(), zset(deltas.get(i), set));
            }
        }
    }
}
