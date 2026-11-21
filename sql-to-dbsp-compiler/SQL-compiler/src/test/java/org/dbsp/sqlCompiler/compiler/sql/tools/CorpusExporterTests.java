package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CorpusExporterTests {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static class CollectCase extends BaseSQLTests {
        TestCase registered;
        @Override void runOneTestCase(TestCase test) { this.registered = test; }
    }

    private static CompilerCircuitStream stream(CollectCase test) {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.incrementalize = true;
        options.languageOptions.throwOnError = true;
        options.ioOptions.emitHandles = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t (amount DECIMAL(38, 10));
                CREATE VIEW v AS SELECT amount FROM t;
                """);
        return new CompilerCircuitStream(compiler, test);
    }

    private static CorpusExporter exporter(Path root) {
        return new CorpusExporter(root, "compiler-revision", "calcite-revision", "runtime-revision");
    }

    private static JsonNode read(Path file) throws IOException {
        return Utilities.deterministicObjectMapper().readTree(Files.readString(file));
    }

    @Test
    public void snapshotsProgramThenExportsLateTypedSteps() throws IOException {
        Path root = this.temporary.newFolder().toPath();
        CorpusExporter exporter = exporter(root);
        CollectCase test = new CollectCase();
        CompilerCircuitStream stream = stream(test);
        exporter.register(test.registered);
        String decimal = "1234567890123456789012345678.1234567890";
        stream.stepWeightOne("INSERT INTO t VALUES (" + decimal + ");", """
                 amount
                --------
                """ + decimal);
        stream.blockForCompaction();
        Assert.assertTrue(stream.compiler.sources.getWholeProgram().contains("INSERT"));
        // Later test/compiler mutations must not change the recorded compile options.
        stream.compiler.options.languageOptions.incrementalize = false;
        stream.compiler.options.ioOptions.gen2 = true;
        exporter.finish("CorpusExporterTests#lateSteps", null);
        Path directory = root.resolve(CorpusExporter.directoryName("CorpusExporterTests#lateSteps"));
        Path file = directory.resolve("case-0000.json");
        String text = Files.readString(file);
        Assert.assertTrue("Decimal precision must survive JSON serialization", text.contains(decimal));
        JsonNode record = read(file);
        Assert.assertFalse(record.get("program").asText().contains("INSERT"));
        Assert.assertEquals(1, record.get("submissions").size());
        Assert.assertFalse(record.at("/submissions/0/sql").asText().contains("INSERT"));
        Assert.assertEquals("classic", record.get("engine_mode").asText());
        Assert.assertEquals("delta", record.get("step_semantics").asText());
        Assert.assertEquals("exported", record.get("status").asText());
        Assert.assertFalse(record.get("runtime_verified").asBoolean());
        Assert.assertEquals(2, record.get("commands").size());
        Assert.assertEquals("compact", record.at("/commands/1/kind").asText());
        Assert.assertEquals("DBSPZSetExpression", record.at("/commands/0/outputs/0/zset_ir/class").asText());
        Assert.assertEquals(1, read(directory.resolve("complete.json")).get("exported").asInt());

        // Replay must use the detached circuit and typed changes even if the SQL is unavailable.
        ObjectNode detached = (ObjectNode) record;
        detached.remove(List.of("program", "submissions", "compilation_options"));
        Path frozen = root.resolve("frozen.json");
        Utilities.deterministicObjectMapper().writeValue(frozen.toFile(), detached);
        TestCase replay = CorpusRuntimeReplay.load(frozen);
        Assert.assertTrue(replay.ccs.circuit.incremental);
        Assert.assertTrue(replay.ccs.stream.commands.get(1) instanceof BlockForCompaction);
        Path generated = root.resolve("lib.rs");
        CorpusRuntimeReplay.main(new String[] { generated.toString(), frozen.toString() });
        String rust = Files.readString(generated);
        Assert.assertTrue(rust.contains("wait_for_compaction"));
        BigDecimal exact = new BigDecimal(decimal).stripTrailingZeros();
        Assert.assertTrue(rust.contains(exact.unscaledValue() + "i128, " + exact.scale()));
    }

    @Test
    public void decodesFrozenTimeWithAnEmptyFraction() throws IOException {
        JsonNode encoded = Utilities.deterministicObjectMapper().readTree("""
                {"class":"DBSPTimeLiteral","id":0,"value":"13:00:00.",
                 "type":{"class":"DBSPTypeTime","id":1,"code":"TIME","mayBeNull":false}}
                """);
        DBSPTimeLiteral time = new JsonDecoder().decodeInner(encoded, DBSPTimeLiteral.class);
        Assert.assertEquals("TIME '13:00:00'", time.toSqlString());
    }

    @Test
    public void preservesCompilationModeWhenTheRunnerRestoresTestOptions() throws IOException {
        Path root = this.temporary.newFolder().toPath();
        CorpusExporter exporter = exporter(root);
        CollectCase test = new CollectCase();
        CompilerCircuitStream stream = stream(test);
        Assert.assertTrue(stream.circuit.incremental);
        stream.compiler.options.languageOptions.incrementalize = false;
        exporter.register(test.registered);
        exporter.finish("CorpusExporterTests#restoredOptions", null);
        JsonNode record = read(root.resolve(CorpusExporter.directoryName(
                "CorpusExporterTests#restoredOptions")).resolve("case-0000.json"));
        Assert.assertEquals("whole_relation", record.get("step_semantics").asText());
        Assert.assertFalse(record.at("/options/languageOptions/incrementalize").asBoolean());
        Assert.assertTrue(record.at("/compilation_options/languageOptions/incrementalize").asBoolean());
        Assert.assertTrue(record.at("/circuit/incremental").asBoolean());
    }

    @Test
    public void preservesExpectedRuntimeErrorsAndCaseOrder() throws IOException {
        Path root = this.temporary.newFolder().toPath();
        CorpusExporter exporter = exporter(root);
        CollectCase test = new CollectCase();
        CompilerCircuitStream stream = stream(test);
        exporter.register(test.registered);
        exporter.register(new TestCase("failure", "test", stream, "division by zero"));
        exporter.finish("Tests#runtimeError", null);
        Path directory = root.resolve(CorpusExporter.directoryName("Tests#runtimeError"));
        Assert.assertTrue(read(directory.resolve("case-0000.json")).get("expected_runtime_error").isNull());
        JsonNode failure = read(directory.resolve("case-0001.json"));
        Assert.assertEquals("division by zero", failure.get("expected_runtime_error").asText());
        Assert.assertEquals(1, failure.get("case_index").asInt());
        Assert.assertEquals(2, read(directory.resolve("complete.json")).get("case_count").asInt());
    }

    @Test
    public void failedBodiesProduceExplicitSkipRecords() throws IOException {
        Path root = this.temporary.newFolder().toPath();
        CorpusExporter exporter = exporter(root);
        CollectCase test = new CollectCase();
        stream(test);
        exporter.register(test.registered);
        exporter.finish("Tests#failure", new AssertionError("expected table was invalid"));
        Path directory = root.resolve(CorpusExporter.directoryName("Tests#failure"));
        JsonNode record = read(directory.resolve("case-0000.json"));
        Assert.assertEquals("skipped", record.get("status").asText());
        Assert.assertTrue(record.get("reason").asText().contains("expected table was invalid"));
        JsonNode manifest = read(directory.resolve("complete.json"));
        Assert.assertEquals("failed", manifest.get("test_body_status").asText());
        Assert.assertEquals(0, manifest.get("exported").asInt());
        Assert.assertEquals(1, manifest.get("skipped").asInt());
    }

    @Test
    public void rejectsOverwritingAndKeepsTestNamesOutOfPaths() throws IOException {
        Path root = this.temporary.newFolder().toPath();
        String name = "Tests#../../escape";
        Assert.assertTrue(CorpusExporter.directoryName(name).matches("[0-9a-f]{64}"));
        exporter(root).finish(name, null);
        Assert.assertThrows(UncheckedIOException.class, () -> exporter(root).finish(name, null));
        Assert.assertEquals(name, read(root.resolve(CorpusExporter.directoryName(name))
                .resolve("complete.json")).get("source_test").asText());
    }

    @Test
    public void preservesSubmissionModesAndBoundariesWithoutAddingSemicolons() throws IOException {
        Path root = this.temporary.newFolder().toPath();
        CollectCase test = new CollectCase();
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.throwOnError = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementForCompilation("CREATE TABLE t(i INT)");
        compiler.submitStatementsForCompilation("CREATE VIEW v AS SELECT i FROM t");
        new CompilerCircuitStream(compiler, test);
        CorpusExporter exporter = exporter(root);
        exporter.register(test.registered);
        exporter.finish("Tests#boundaries", null);
        JsonNode record = read(root.resolve(CorpusExporter.directoryName("Tests#boundaries"))
                .resolve("case-0000.json"));
        Assert.assertEquals(2, record.get("submissions").size());
        Assert.assertFalse(record.at("/submissions/0/many").asBoolean());
        Assert.assertTrue(record.at("/submissions/1/many").asBoolean());
        var replay = new DBSPCompiler(options);
        for (JsonNode submission: record.get("submissions")) {
            Assert.assertEquals(0, submission.get("batch").asInt());
            if (submission.get("many").asBoolean())
                replay.submitStatementsForCompilation(submission.get("sql").asText());
            else
                replay.submitStatementForCompilation(submission.get("sql").asText());
        }
        Assert.assertNotNull(replay.getFinalCircuit(false));
        replay.throwIfErrorsOccurred();
        Assert.assertEquals(record.get("schema"), replay.metadata.asJson());
    }

    @Test
    public void runtimeFailureSnapshotRetainsCompileWarnings() throws IOException {
        Path root = this.temporary.newFolder().toPath();
        CorpusExporter exporter = exporter(root);
        CollectCase test = new CollectCase() {
            @Override void runOneTestCase(TestCase testCase) {
                super.runOneTestCase(testCase);
                exporter.register(testCase);
            }
        };
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.throwOnError = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation("CREATE VIEW v AS SELECT 1 / 0");
        new CompilerCircuitStream(compiler, test, "division by zero");
        Assert.assertTrue("The test helper clears messages after registering", compiler.messages.isEmpty());
        exporter.finish("Tests#runtimeWarning", null);
        JsonNode record = read(root.resolve(CorpusExporter.directoryName("Tests#runtimeWarning"))
                .resolve("case-0000.json"));
        Assert.assertFalse(record.get("diagnostics").isEmpty());
        Assert.assertEquals("Division by zero", record.at("/diagnostics/0/error_type").asText());
        Assert.assertTrue(record.at("/diagnostics/0/warning").asBoolean());
    }
}
