package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;

public class ParserProbeTests {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void viewKindsAreExportedFromTypedAstFields() throws IOException {
        for (String kind: new String[]{"STANDARD", "LOCAL", "MATERIALIZED"}) {
            for (boolean replace: new boolean[]{false, true}) {
                String sql = "CREATE " + (replace ? "OR REPLACE " : "")
                        + (kind.equals("STANDARD") ? "" : kind + " ") + "VIEW v AS SELECT 1";
                ObjectNode parsed = ParserProbe.parse(new DBSPCompiler(new CompilerOptions()), submission(sql));
                Assert.assertEquals("accepted", parsed.get("verdict").asText());
                Assert.assertEquals(kind, parsed.at("/statements/0/view_kind").asText());
                Assert.assertEquals(replace, parsed.at("/statements/0/replace").asBoolean());
            }
        }
    }

    @Test
    public void tableHintRenderingFailureDoesNotBecomeASyntaxRejection() throws IOException {
        ObjectNode source = Utilities.deterministicObjectMapper().createObjectNode();
        source.put("id", "hint-rendering-failure");
        source.putArray("submissions").add(submission("SELECT * FROM t /*+ h('a' 'b') */ AS x"));
        var input = this.temporary.newFile("hints-input.jsonl").toPath();
        var output = this.temporary.getRoot().toPath().resolve("hints-output");
        Files.writeString(input, source + "\n");
        ParserProbe.main(new String[]{input.toString(), output.toString()});
        var mapper = Utilities.deterministicObjectMapper();
        var result = mapper.readTree(Files.readString(output.resolve("cases.jsonl")));
        Assert.assertTrue(result.get("accepted").asBoolean());
        var statement = result.at("/submissions/0/statements/0");
        Assert.assertFalse(statement.has("sql"));
        Assert.assertEquals("java.lang.ClassCastException", statement.at("/unparse_failure/type").asText());
        Assert.assertFalse(statement.at("/unparse_failure/stack").isEmpty());
        var complete = mapper.readTree(Files.readString(output.resolve("complete.json")));
        Assert.assertEquals(1, complete.get("unparse_failures").asInt());
        Assert.assertFalse(complete.get("rendering_complete").asBoolean());
    }

    @Test
    public void unrelatedUnparseClassCastFailuresRemainFatal() {
        Assert.assertFalse(ParserProbe.knownHintUnparseFailure(new ClassCastException("unrelated")));
        Assert.assertFalse(ParserProbe.knownHintUnparseFailure(new ClassCastException(
                "org.apache.calcite.sql.SqlBasicCall cannot be cast to org.apache.calcite.sql.SqlLiteral")));
    }

    @Test
    public void internalCompilerExceptionsCannotBecomeSyntaxGoldens() {
        for (Throwable cause: new Throwable[]{
                new InternalCompilerError("broken parser invariant"),
                new IllegalStateException("broken parser state"),
                new AssertionError("broken parser assertion"),
                new org.apache.calcite.runtime.CalciteException("unexpected resource error", null),
                new org.apache.calcite.sql.validate.SqlValidatorException("unexpected validator error", null)}) {
            SqlParseException error = new SqlParseException("parser failed",
                    new SqlParserPos(1, 1), null, null, cause);
            IOException failure = Assert.assertThrows(IOException.class,
                    () -> ParserProbe.requireSyntaxRejection(error));
            Assert.assertSame(error, failure.getCause());
        }
    }

    @Test
    public void integerPrecisionOverflowIsAParserRejection() throws IOException {
        for (String type: new String[]{"VARCHAR(2147483648)", "DECIMAL(10,-2147483648)",
                "TIMESTAMP(2147483648)"}) {
            ObjectNode parsed = ParserProbe.parse(new DBSPCompiler(new CompilerOptions()),
                    submission("CREATE TABLE t(x " + type + ")"));
            Assert.assertEquals("rejected", parsed.path("verdict").asText());
            Assert.assertTrue(parsed.path("error").asText().contains("2147483648"));
            Assert.assertTrue(parsed.path("column").asInt() > 0);
        }
        var unrelated = org.apache.calcite.util.Static.RESOURCE
                .invalidLiteral("2147483648", Integer.class.getCanonicalName()).ex();
        var wrapper = new SqlParseException(unrelated.getMessage(), new SqlParserPos(1, 1),
                null, null, unrelated);
        Assert.assertThrows(IOException.class, () -> ParserProbe.requireSyntaxRejection(wrapper));
    }

    @Test
    public void unsupportedCharacterSetIsAParserRejection() throws IOException {
        Assert.assertEquals("rejected", ParserProbe.parse(new DBSPCompiler(new CompilerOptions()),
                submission("SELECT _binary'abc'")).path("verdict").asText());
        Assert.assertEquals("accepted", ParserProbe.parse(new DBSPCompiler(new CompilerOptions()),
                submission("SELECT _utf8'abc'")).path("verdict").asText());
        var unrelated = org.apache.calcite.util.Static.RESOURCE.unknownCharacterSet("binary").ex();
        var wrapper = new SqlParseException(unrelated.getMessage(), new SqlParserPos(1, 1),
                null, null, unrelated);
        Assert.assertThrows(IOException.class, () -> ParserProbe.requireSyntaxRejection(wrapper));
    }

    @Test
    public void exportPreservesUnpairedSurrogateEvidence() throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode source = mapper.createObjectNode();
        source.put("id", "unicode/lone-surrogate");
        source.putArray("submissions").add(submission("SELECT U&\"\\D800\""));
        var input = this.temporary.newFile("input.jsonl").toPath();
        var output = this.temporary.getRoot().toPath().resolve("output");
        Files.writeString(input, source + "\n");
        ParserProbe.main(new String[]{input.toString(), output.toString()});
        String exported = Files.readString(output.resolve("cases.jsonl"));
        Assert.assertTrue(exported.chars().allMatch(c -> c < 128));
        var result = mapper.readTree(exported);
        Assert.assertTrue(result.get("accepted").asBoolean());
        Assert.assertTrue(result.at("/submissions/0/statements/0/sql").asText()
                .indexOf((char) 0xD800) >= 0);
        Assert.assertTrue(mapper.readTree(Files.readString(output.resolve("complete.json")))
                .get("complete").asBoolean());
    }

    private static ObjectNode submission(String sql) {
        ObjectNode result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("sql", sql);
        result.put("many", true);
        return result;
    }

    @Test
    public void jsonConstructorGrammarErrorsHaveSourcePositions() throws IOException {
        for (String sql : java.util.List.of(
                "SELECT JSON_OBJECT(KEY x, y)",
                "SELECT JSON_OBJECTAGG(KEY x: y)",
                "SELECT JSON_ARRAYAGG(x ORDER BY y) WITHIN GROUP (ORDER BY z)")) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            var result = ParserProbe.parse(compiler, submission(sql));
            Assert.assertEquals("rejected", result.get("verdict").asText());
            Assert.assertTrue(result.get("column").asInt() > 0);
        }
    }

    @Test
    public void emptySubmissionUsesCompilerLineAccounting() throws IOException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        ObjectNode empty = ParserProbe.parse(compiler, submission(""));
        Assert.assertEquals("accepted", empty.get("verdict").asText());
        Assert.assertTrue(empty.get("statements").isEmpty());
        ObjectNode next = ParserProbe.parse(compiler, submission("SELECT 1"));
        Assert.assertEquals(2, next.at("/statements/0/position/line").asInt());
    }

    @Test
    public void preservesStatementBoundariesAndRejectsInvalidSuffix() throws IOException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        ObjectNode valid = ParserProbe.parse(compiler, submission("SELECT ';'; SELECT 2"));
        Assert.assertEquals(2, valid.get("statements").size());
        ObjectNode invalid = ParserProbe.parse(compiler, submission("SELECT 1; SELECT ("));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertFalse(invalid.has("statements"));
        Assert.assertTrue(invalid.get("line").asInt() > 0);
    }

    @Test
    public void unterminatedStringIsARejection() throws IOException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        ObjectNode invalid = ParserProbe.parse(compiler, submission("SELECT 'unterminated"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertFalse(invalid.get("error").asText().isBlank());
        Assert.assertEquals(8, invalid.get("column").asInt());
    }

    @Test
    public void invalidHexadecimalDigitsAreAParserRejection() throws IOException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        ObjectNode invalid = ParserProbe.parse(compiler, submission("SELECT X'gg'"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertEquals("org.apache.calcite.sql.validate.SqlValidatorException", invalid.get("cause_type").asText());
        Assert.assertTrue(invalid.get("column").asInt() > 0);
    }

    @Test
    public void malformedUnicodeEscapeIsAParserRejection() throws IOException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        ObjectNode invalid = ParserProbe.parse(compiler, submission("SELECT U&'text\\123'"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertEquals("org.apache.calcite.sql.validate.SqlValidatorException", invalid.get("cause_type").asText());
        Assert.assertTrue(invalid.get("column").asInt() > 0);
    }

    @Test
    public void rejectionPositionsIncludePreviousSubmissions() throws IOException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        ParserProbe.parse(compiler, submission("SELECT\n1"));
        ObjectNode invalid = ParserProbe.parse(compiler, submission("SELECT )"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertEquals(3, invalid.get("line").asInt());
    }

    @Test
    public void invalidAsofConditionsAreSyntaxRejections() throws IOException {
        for (String sql: new String[]{
                "SELECT * FROM a LEFT ASOF JOIN b ON TRUE",
                "SELECT * FROM a ASOF JOIN b ON TRUE",
                "SELECT * FROM a JOIN b MATCH_CONDITION TRUE ON TRUE"}) {
            DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
            ObjectNode invalid = ParserProbe.parse(compiler, submission(sql));
            Assert.assertEquals("rejected", invalid.get("verdict").asText());
            Assert.assertEquals("org.apache.calcite.runtime.CalciteException",
                    invalid.get("cause_type").asText());
            Assert.assertTrue(invalid.get("line").asInt() > 0);
        }
    }

    @Test
    public void unsupportedStatementIsASyntaxRejection() throws IOException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        ObjectNode invalid = ParserProbe.parse(compiler,
                submission("ALTER TABLE t ADD COLUMN n INT LATENESS 1"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertEquals("org.apache.calcite.runtime.CalciteException",
                invalid.get("cause_type").asText());
        Assert.assertTrue(invalid.get("line").asInt() > 0);
    }

    @Test
    public void betweenWithoutAndIsASyntaxRejection() throws IOException {
        ObjectNode invalid = ParserProbe.parse(new DBSPCompiler(new CompilerOptions()),
                submission("SELECT 1 BETWEEN 2"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertEquals("org.apache.calcite.sql.validate.SqlValidatorException",
                invalid.get("cause_type").asText());
    }

    @Test
    public void nonQueryTableFactorIsASyntaxRejection() throws IOException {
        ObjectNode invalid = ParserProbe.parse(new DBSPCompiler(new CompilerOptions()),
                submission("SELECT DISTINCT SAFE_CAST(x AS TINYINT) AS k FROM (SE)ECT DISTINCT x FROM t)"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertEquals("org.apache.calcite.runtime.CalciteException",
                invalid.get("cause_type").asText());
    }

    @Test
    public void misplacedFromQueryWhereIsASyntaxRejection() throws IOException {
        ObjectNode invalid = ParserProbe.parse(new DBSPCompiler(new CompilerOptions()),
                submission("CREATE VIEW v AS FROM t WHERE TRUE"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertEquals("org.apache.calcite.runtime.CalciteException",
                invalid.get("cause_type").asText());
    }

    @Test
    public void emptyIntervalLiteralsAreSyntaxRejections() throws IOException {
        for (String qualifier: new String[]{"YEAR", "YEAR TO MONTH", "MONTH", "DAY",
                "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND", "HOUR",
                "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE", "MINUTE TO SECOND", "SECOND"}) {
            ObjectNode invalid = ParserProbe.parse(new DBSPCompiler(new CompilerOptions()),
                    submission("SELECT INTERVAL '' " + qualifier));
            Assert.assertEquals("rejected", invalid.get("verdict").asText());
            Assert.assertEquals("org.apache.calcite.runtime.CalciteException",
                    invalid.get("cause_type").asText());
        }
    }

    @Test
    public void nonQueryViewBodyIsASyntaxRejection() throws IOException {
        for (String sql: new String[]{"CREATE VIEW v AS 1", "CREATE VIEW v AS SELE)T 1"}) {
            DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
            ObjectNode invalid = ParserProbe.parse(compiler, submission(sql));
            Assert.assertEquals("rejected", invalid.get("verdict").asText());
            Assert.assertEquals("org.apache.calcite.runtime.CalciteException",
                    invalid.get("cause_type").asText());
            Assert.assertTrue(invalid.get("line").asInt() > 0);
        }
    }

    @Test
    public void queryInLatenessIsASyntaxRejection() throws IOException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        ObjectNode invalid = ParserProbe.parse(compiler,
                submission("CREATE TABLE t (n INT LATENESS (SELECT 1))"));
        Assert.assertEquals("rejected", invalid.get("verdict").asText());
        Assert.assertEquals("org.apache.calcite.runtime.CalciteException",
                invalid.get("cause_type").asText());
        Assert.assertTrue(invalid.get("line").asInt() > 0);
    }

    @Test
    public void invalidQualifiedDatetimeExpressionIsASyntaxRejection() throws IOException {
        for (String expression: new String[]{"(a)", "(a + b)"}) {
            DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
            ObjectNode invalid = ParserProbe.parse(compiler,
                    submission("SELECT " + expression + " YEAR FROM t"));
            Assert.assertEquals("rejected", invalid.get("verdict").asText());
            Assert.assertEquals("org.apache.calcite.runtime.CalciteException",
                    invalid.get("cause_type").asText());
            Assert.assertTrue(invalid.get("line").asInt() > 0);
        }
    }
}
