package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.CircuitDumps;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelReference;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/** Tests the capture protocol without generating or running Rust. */
public class CircuitDumpTests {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void asofGcOrderDoesNotDependOnObjectIdentity() {
        List<String> expected = null;
        for (int attempt = 0; attempt < 12; attempt++) {
            CompilerOptions options = new CompilerOptions();
            options.languageOptions.incrementalize = true;
            options.ioOptions.writeS3 = "capture.json";
            options.ioOptions.dumpProducerCommit = "test-compiler-revision";
            options.ioOptions.dumpRuntimeAbi = "test-runtime-build";
            DBSPCompiler compiler = new DBSPCompiler(options);
            int unrelatedObjects = attempt;
            if (attempt != 0) {
                // Observation must not affect scheduling. Vary identity hashes to expose
                // iteration over maps keyed by operators, without changing the circuit.
                compiler.circuitObservationHook = json -> {
                    for (int i = 0; i < unrelatedObjects; i++)
                        System.identityHashCode(new Object());
                };
            }
            compiler.submitStatementsForCompilation("""
                    CREATE TABLE L (k BIGINT NOT NULL, ts BIGINT NOT NULL LATENESS 0);
                    CREATE TABLE R (k BIGINT NOT NULL, ts BIGINT NOT NULL LATENESS 0);
                    CREATE LOCAL VIEW V AS
                    SELECT L.k, L.ts AS lts, R.ts AS rts
                    FROM L LEFT ASOF JOIN R
                    MATCH_CONDITION(L.ts >= R.ts) ON L.k = R.k;
                    CREATE VIEW C AS SELECT rts, COUNT(*) AS c FROM V GROUP BY rts;
                    """);
            DBSPCircuit circuit = compiler.getFinalCircuit(false);
            compiler.throwIfErrorsOccurred();
            Assert.assertNotNull(circuit);
            List<String> order = new ArrayList<>();
            for (var operator: circuit.getAllOperators()) {
                var hash = OperatorHash.getHash(operator, true);
                Assert.assertNotNull(hash);
                order.add(hash.toString());
            }
            if (expected == null)
                expected = order;
            else
                Assert.assertEquals("GC scheduling changed on attempt " + attempt, expected, order);
        }
    }

    private CompilerOptions options(Path directory, boolean gen2) {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.incrementalize = true;
        options.languageOptions.throwOnError = true;
        options.ioOptions.gen2 = gen2;
        options.ioOptions.dumpCircuits = directory.toString();
        options.ioOptions.dumpProducerCommit = "test-compiler-revision";
        options.ioOptions.dumpRuntimeAbi = "test-runtime-build";
        return options;
    }

    private static DBSPCircuit compile(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t (id INT NOT NULL, value DECIMAL(10, 2));
                CREATE VIEW v AS SELECT id, value * 2 AS doubled FROM t WHERE value > 1;
                """);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        Assert.assertNotNull(circuit);
        return circuit;
    }

    private static JsonNode read(Path path) throws IOException {
        return Utilities.deterministicObjectMapper().readTree(Files.readString(path));
    }

    @Test
    public void captureBothModesAndNestedPasses() throws IOException {
        for (boolean gen2: List.of(false, true)) {
            Path directory = this.temporary.newFolder().toPath();
            DBSPCompiler compiler = new DBSPCompiler(this.options(directory, gen2));
            CircuitTransform identity = new CircuitTransform() {
                @Override public String getName() { return "DumpTestIdentity"; }
                @Override public DBSPCircuit apply(DBSPCircuit circuit) { return circuit; }
            };
            compiler.optimizerHook = optimizer -> optimizer.add(new Passes(
                    "DumpTestNested", compiler, new Repeat(compiler, identity)));
            compile(compiler);
            Path compilation = directory.resolve("compilation-0000");
            JsonNode manifest = read(compilation.resolve("complete.json"));
            Assert.assertEquals("S3", manifest.get("final_stage").asText());
            int count = manifest.get("dump_count").asInt();
            Assert.assertTrue(count > 3);
            List<Path> dumps;
            try (var files = Files.list(compilation)) {
                dumps = files.filter(p -> !p.getFileName().toString().equals("complete.json"))
                        .sorted().toList();
            }
            Assert.assertEquals(count, dumps.size());
            boolean sawNestedRepeat = false;
            for (int index = 0; index < dumps.size(); index++) {
                JsonNode dump = read(dumps.get(index));
                Assert.assertEquals(1, dump.get("schema_version").asInt());
                Assert.assertEquals(gen2 ? "gen2" : "classic", dump.get("engine_mode").asText());
                Assert.assertFalse(dump.get("replayable").asBoolean());
                Assert.assertEquals("DBSPCircuit", dump.at("/circuit/class").asText());
                Assert.assertEquals(index, dump.at("/observation/sequence").asInt());
                Assert.assertEquals(index == 0 ? "S2" : index == count - 1 ? "S3" : "S4",
                        dump.at("/observation/stage").asText());
                JsonNode path = dump.at("/observation/path");
                if (path.size() >= 3 && path.get(path.size() - 1).get("container").asText().equals("Repeat"))
                    sawNestedRepeat = true;
                Assert.assertFalse(dump.at("/options/ioOptions").has("dumpCircuits"));
                Assert.assertTrue(dump.at("/options/languageOptions/incrementalize").asBoolean());
            }
            Assert.assertTrue("Nested Repeat must have its enclosing pass path", sawNestedRepeat);
        }
    }

    @Test
    public void unchangedRelationsKeepTheirRenderedSqlAcrossTailSnapshots() throws IOException {
        var options = new CompilerOptions();
        options.ioOptions.writeS3 = "observe";
        var compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation("""
                CREATE TYPE item AS (id BIGINT NOT NULL);
                CREATE TABLE t (items item ARRAY);
                CREATE VIEW v AS SELECT * FROM t, UNNEST(t.items);
                """);
        var circuit = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();

        var renderings = new HashMap<RelReference, String>();
        var sqlByRelation = new HashMap<Integer, String>();
        boolean observedCorrelation = false;
        int repeated = 0;
        for (int observation = 0; observation < 2; observation++) {
            var writer = ToJsonOuterVisitor.createForReplay(compiler, renderings);
            writer.apply(circuit);
            var snapshot = Utilities.deterministicObjectMapper().readTree(writer.getJsonString());
            for (var rel : snapshot.findParents("relId")) {
                String sql = rel.required("sql").asText();
                String prior = sqlByRelation.putIfAbsent(rel.required("relId").asInt(), sql);
                if (prior != null) {
                    repeated++;
                    Assert.assertEquals(prior, sql);
                }
                observedCorrelation |= sql.contains("$cor");
            }
        }
        Assert.assertTrue("expected a rendered correlation", observedCorrelation);
        Assert.assertTrue("expected multiple observations of the same relation", repeated > 0);
    }

    @Test
    public void inputCircuitProducesOnlyS2() throws IOException {
        Path directory = this.temporary.newFolder().toPath();
        CompilerOptions options = this.options(directory, false);
        options.ioOptions.inputCircuit = true;
        compile(new DBSPCompiler(options));
        JsonNode manifest = read(directory.resolve("compilation-0000/complete.json"));
        Assert.assertEquals(1, manifest.get("dump_count").asInt());
        Assert.assertEquals("S2", manifest.get("final_stage").asText());
    }

    @Test
    public void compilersUseSeparateDumpDirectories() throws IOException {
        Path directory = this.temporary.newFolder().toPath();
        CompilerOptions options = this.options(directory, false);
        options.ioOptions.inputCircuit = true;
        compile(new DBSPCompiler(options));
        compile(new DBSPCompiler(options));
        Assert.assertTrue(Files.exists(directory.resolve("compilation-0000/complete.json")));
        Assert.assertTrue(Files.exists(directory.resolve("compilation-0001/complete.json")));
    }

    @Test
    public void variantDumpHasOneTypeField() throws IOException {
        var mapper = Utilities.deterministicObjectMapper()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        for (boolean streaming: List.of(false, true)) {
            Path directory = this.temporary.newFolder().toPath();
            CompilerOptions options = this.options(directory, true);
            options.ioOptions.inputCircuit = true;
            List<String> observations = new ArrayList<>();
            if (streaming)
                options.ioOptions.dumpCircuits = "";
            DBSPCompiler compiler = new DBSPCompiler(options);
            if (streaming)
                compiler.circuitObservationHook = observations::add;
            compiler.submitStatementsForCompilation("""
                    CREATE VIEW v AS SELECT TO_JSON(VARIANT_DEEP_FILTER(
                        CAST(NULL AS VARIANT), (p, v) -> TRUE));
                    """);
            compiler.getFinalCircuit(false);
            compiler.throwIfErrorsOccurred();
            String dump = streaming ? observations.get(0)
                    : Files.readString(directory.resolve("compilation-0000/000000-S2.json"));
            Assert.assertNotNull(mapper.readTree(dump));
        }
    }

    @Test
    public void failedPassLeavesNoCompletionMarker() throws IOException {
        Path directory = this.temporary.newFolder().toPath();
        DBSPCompiler compiler = new DBSPCompiler(this.options(directory, false));
        compiler.optimizerHook = optimizer -> optimizer.add(new CircuitTransform() {
            @Override public String getName() { return "FailDumpTest"; }
            @Override public DBSPCircuit apply(DBSPCircuit circuit) {
                throw new IllegalStateException("intentional test failure");
            }
        });
        Assert.assertThrows(RuntimeException.class, () -> compile(compiler));
        Path compilation = directory.resolve("compilation-0000");
        Assert.assertTrue(Files.exists(compilation.resolve("000000-S2.json")));
        Assert.assertFalse(Files.exists(compilation.resolve("complete.json")));
    }

    @Test
    public void requiresExplicitBuildIdentity() throws IOException {
        CompilerOptions options = this.options(this.temporary.newFolder().toPath(), false);
        options.ioOptions.dumpRuntimeAbi = "";
        DBSPCompiler compiler = new DBSPCompiler(options);
        Assert.assertThrows(IllegalArgumentException.class, () -> new CircuitDumps(compiler, 0));
    }

    private static List<JsonNode> observations(Path compilation) throws IOException {
        List<Path> files;
        try (var entries = Files.list(compilation)) {
            files = entries.filter(p -> !p.getFileName().toString().equals("complete.json"))
                    .sorted().toList();
        }
        List<JsonNode> result = new ArrayList<>();
        for (Path file: files)
            result.add(read(file));
        return result;
    }

    @Test
    public void capturesMonotonicitySubstepsInBothModes() throws IOException {
        for (boolean gen2: List.of(false, true)) {
            for (boolean incremental: List.of(false, true)) {
                Path directory = this.temporary.newFolder().toPath();
                CompilerOptions options = this.options(directory, gen2);
                options.languageOptions.incrementalize = incremental;
                compile(new DBSPCompiler(options));
                List<String> steps = new ArrayList<>();
                for (JsonNode dump: observations(directory.resolve("compilation-0000"))) {
                    JsonNode path = dump.at("/observation/path");
                    if (path.isEmpty())
                        continue;
                    JsonNode frame = path.get(path.size() - 1);
                    if (!frame.get("container").asText().equals("MonotoneAnalyzer"))
                        continue;
                    Assert.assertTrue(path.size() >= 2);
                    steps.add(frame.get("index").asInt() + ":" + frame.get("pass").asText());
                    Assert.assertFalse(dump.get("replayable").asBoolean());
                }
                List<String> expected = new ArrayList<>(List.of("0:EnsureExpressionTrees", "1:Graph"));
                if (incremental)
                    expected.addAll(List.of("2:SeparateIntegrators", "3:Graph"));
                expected.addAll(List.of("4:OrderErrorConsumers", "5:DeltaExpandOperators",
                        "6:Monotonicity", "7:InsertLimiters", "8:MergeGC", "9:Graph", "10:CheckRetain"));
                Assert.assertEquals(expected, steps);
            }
        }
    }

    @Test
    public void observesOnlyExecutedConditionalBodies() throws IOException {
        Path directory = this.temporary.newFolder().toPath();
        DBSPCompiler compiler = new DBSPCompiler(this.options(directory, false));
        compiler.optimizerHook = optimizer -> {
            for (boolean enabled: List.of(false, true)) {
                CircuitTransform identity = new CircuitTransform() {
                    @Override public String getName() { return "DumpConditional" + enabled; }
                    @Override public DBSPCircuit apply(DBSPCircuit circuit) { return circuit; }
                };
                optimizer.add(new Conditional(compiler, identity, () -> enabled));
            }
        };
        compile(compiler);
        List<String> executed = new ArrayList<>();
        for (JsonNode dump: observations(directory.resolve("compilation-0000"))) {
            JsonNode path = dump.at("/observation/path");
            if (path.isEmpty())
                continue;
            JsonNode frame = path.get(path.size() - 1);
            if (frame.get("container").asText().equals("Conditional") &&
                    frame.get("pass").asText().startsWith("DumpConditional"))
                executed.add(frame.get("pass").asText());
        }
        Assert.assertEquals(List.of("DumpConditionaltrue"), executed);
    }

    @Test
    public void postCompilationErrorsLeaveNoCompletionMarker() throws IOException {
        for (boolean throwOnError: List.of(false, true)) {
            Path directory = this.temporary.newFolder().toPath();
            CompilerOptions options = this.options(directory, false);
            options.languageOptions.throwOnError = throwOnError;
            DBSPCompiler compiler = new DBSPCompiler(options);
            compiler.submitStatementsForCompilation("""
                    SET FELDERA_WARNINGS_ARE_ERRORS = ON;
                    CREATE TABLE t (id INT);
                    CREATE VIEW v AS SELECT id FROM t;
                    LATENESS missing.id 1;
                    """);
            if (throwOnError)
                Assert.assertThrows(RuntimeException.class, () -> compiler.getFinalCircuit(false));
            else
                compiler.getFinalCircuit(false);
            Assert.assertTrue(compiler.hasErrors());
            Path compilation = directory.resolve("compilation-0000");
            // Prove that the error occurs after the circuit was captured, not in parsing.
            Assert.assertTrue(Files.exists(compilation.resolve("000000-S2.json")));
            Assert.assertFalse(Files.exists(compilation.resolve("complete.json")));
        }
    }

    @Test
    public void streamingCaptureIncludesHashesAndPropagatesConsumerFailures() throws IOException {
        for (boolean gen2: List.of(false, true)) {
            CompilerOptions options = this.options(this.temporary.newFolder().toPath(), gen2);
            options.ioOptions.dumpCircuits = "";
            var compiler = new DBSPCompiler(options);
            List<JsonNode> dumps = new ArrayList<>();
            List<String> hashKinds = new ArrayList<>();
            compiler.circuitObservationHook = json -> {
                Assert.assertFalse("Stream records must occupy one physical line", json.contains("\n"));
                try {
                    dumps.add(Utilities.deterministicObjectMapper().readTree(json));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
            compiler.hashInputHook = (kind, value) -> {
                Assert.assertFalse(value.isEmpty());
                hashKinds.add(kind);
            };
            compile(compiler);
            Assert.assertEquals("S2", dumps.get(0).at("/observation/stage").asText());
            Assert.assertEquals("S3", dumps.get(dumps.size() - 1).at("/observation/stage").asText());
            for (int i = 0; i < dumps.size(); i++)
                Assert.assertEquals(i, dumps.get(i).at("/observation/sequence").asInt());
            Assert.assertTrue(hashKinds.containsAll(List.of("inner-rust", "local-operator", "global-operator")));

            var failing = new DBSPCompiler(options);
            failing.circuitObservationHook = json -> { throw new IllegalStateException("consumer failed"); };
            Assert.assertThrows(RuntimeException.class, () -> compile(failing));
            Assert.assertTrue(failing.hasErrors());
        }
    }
}
