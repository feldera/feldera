package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** One JVM serves the complete generated C9 matrix; each probe owns a fresh compiler. */
public class TypeProbeTests {
    private static ObjectNode compile(String program) {
        // Each probe is independent and its circuit is serialized before the next
        // probe starts. Reproduce fresh-JVM local names without renaming user fields
        // or weakening the structural comparator. This runner is single-threaded.
        DBSPVariablePath.reset();
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.quiet = true;
        options.ioOptions.inputCircuit = true;
        options.languageOptions.optimizationLevel = 0;
        options.languageOptions.throwOnError = false;
        options.languageOptions.generateInputForEveryTable = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(program);
        var circuit = compiler.getFinalCircuit(false);
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        result.put("accepted", circuit != null && !compiler.hasErrors());
        result.put("compiler_bug", compiler.messages.messages.stream().anyMatch(message ->
                message.errorType.equals("Compiler error")
                        || message.errorType.startsWith("This is a bug in the compiler")));
        result.set("diagnostics", compiler.messages.toJson(compiler.sources));
        result.set("schema", compiler.metadata.asJson());
        result.set("options", mapper.valueToTree(options));
        if (circuit != null && !compiler.hasErrors()) {
            var stream = new IndentStream(new StringBuilder()).setIndentAmount(0);
            var visitor = new ToJsonOuterVisitor(compiler, 1,
                    new ToJsonInnerVisitor(compiler, new JsonStream(stream), 1));
            visitor.apply(circuit);
            result.putRawValue("s2", new RawValue(visitor.getJsonString()));
        }
        return result;
    }

    @Test
    public void generateGoldenTable() throws IOException {
        String input = System.getProperty("feldera.probes.input", "");
        String output = System.getProperty("feldera.probes.output", "");
        Assume.assumeFalse("Set feldera.probes.input and feldera.probes.output to run the generated matrix",
                input.isBlank() && output.isBlank());
        Assert.assertFalse("Missing probe input", input.isBlank());
        Assert.assertFalse("Missing probe output directory", output.isBlank());
        long expected = Long.getLong("feldera.probes.expectedCount", 0);
        Assert.assertTrue("Set feldera.probes.expectedCount to the generator's reported count", expected > 0);
        var mapper = Utilities.deterministicObjectMapper()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        ObjectNode manifest = mapper.createObjectNode();
        manifest.put("schema_version", 1);
        for (String identity: new String[] {"producer", "calcite", "runtime"}) {
            String value = System.getProperty("feldera.probes." + identity, "");
            Assert.assertFalse("Missing probe build identity: " + identity, value.isBlank());
            manifest.put(identity + "_commit", value);
        }
        Path directory = Path.of(output);
        Files.createDirectory(directory);
        long count = 0;
        long accepted = 0;
        long bugs = 0;
        try (var reader = Files.newBufferedReader(Path.of(input));
             var writer = Files.newBufferedWriter(directory.resolve("golden.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode probe = mapper.readTree(line);
                Assert.assertEquals("Probe schema version", 1, probe.path("schema_version").asInt());
                Assert.assertEquals("Sequential probe identity", count, probe.path("id").asLong(-1));
                Assert.assertTrue("Probe requires SQL", probe.path("program").isTextual());
                ObjectNode result = compile(probe.get("program").asText());
                result.put("schema_version", 1);
                result.set("probe", probe);
                if (result.get("accepted").asBoolean())
                    accepted++;
                if (result.get("compiler_bug").asBoolean())
                    bugs++;
                writer.write(mapper.writeValueAsString(result));
                writer.newLine();
                count++;
            }
        }
        Assert.assertTrue("No probes were supplied", count > 0);
        Assert.assertEquals("Probe input was truncated or has extra records", expected, count);
        manifest.put("expected_probes", expected);
        manifest.put("probes", count);
        manifest.put("accepted", accepted);
        manifest.put("rejected", count - accepted);
        manifest.put("compiler_bugs", bugs);
        manifest.put("oracle_ready", bugs == 0);
        manifest.put("runtime_verified", false);
        Files.writeString(directory.resolve("complete.json"), mapper.writeValueAsString(manifest),
                StandardOpenOption.CREATE_NEW);
        Assert.assertEquals("Compiler bugs are not valid rejection goldens", 0, bugs);
    }

    @Test
    public void goldenRecordsIncludeRejectionsAndUnoptimizedCasts() throws IOException {
        var accepted = compile("""
                CREATE TABLE t(a DECIMAL(38,10));
                CREATE VIEW v AS SELECT a + CAST(NULL AS DECIMAL(10,2)) AS result FROM t;
                """);
        Assert.assertTrue(accepted.get("accepted").asBoolean());
        Assert.assertNotNull(accepted.get("s2"));
        String json = Utilities.deterministicObjectMapper().writeValueAsString(accepted);
        Assert.assertFalse("A golden record must occupy one physical line", json.contains("\n"));
        Assert.assertTrue(accepted.path("schema").path("outputs").isArray());
        var rejected = compile("CREATE TABLE t(a INT); CREATE VIEW v AS SELECT missing FROM t;");
        Assert.assertFalse(rejected.get("accepted").asBoolean());
        Assert.assertTrue(rejected.get("diagnostics").size() > 0);
        Assert.assertNull(rejected.get("s2"));
    }

    @Test
    public void probeNamesAreIndependentOfPreviousCompilations() throws IOException {
        String program = "CREATE TABLE t(a INT); CREATE VIEW v AS SELECT ABS(a) AS result FROM t;";
        var mapper = Utilities.deterministicObjectMapper();
        var before = mapper.readTree(compile(program).toString()).get("s2");
        compile("CREATE TABLE t(b BIGINT); CREATE VIEW v AS SELECT b + 2 AS result FROM t;");
        var after = mapper.readTree(compile(program).toString()).get("s2");
        Assert.assertNotNull(before);
        Assert.assertNotNull(after);
        Assert.assertEquals(before.findValues("name"), after.findValues("name"));
        Assert.assertEquals(before.findValues("variable"), after.findValues("variable"));
    }
}
