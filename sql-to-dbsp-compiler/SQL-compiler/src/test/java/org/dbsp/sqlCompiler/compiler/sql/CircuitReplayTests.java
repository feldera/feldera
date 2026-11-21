package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.CircuitReplay;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.Shuffle;
import org.dbsp.util.Utilities;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.LastRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.ProvenanceCodec;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.PositionOnlyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelAnd;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelReference;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelSequence;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Exercise the actual file/CLI boundary, including a fresh compiler context. */
public class CircuitReplayTests {
    @Test
    public void retainedRelationSurvivesRenumberingWithinOneOptimizerCall() {
        var source = new RelReference.Snapshot(64, 7, false, "digest", "plan", "SELECT 1", null, null);
        var origin = Utilities.deterministicObjectMapper().createObjectNode().put("id", 0).put("kind", "LastRel");
        origin.set("position", SourcePositionRange.INVALID.asJson());
        origin.putArray("positions");
        var relation = origin.putObject("relation").put("id", 4).put("retained", 0).put("planId", 7);
        var decoder = new ProvenanceCodec.Decoder(List.of(source));
        Assert.assertSame(source, decoder.decode(origin).to(LastRel.class).relNode);
        var second = origin.deepCopy().put("id", 1);
        second.putObject("relation").put("ref", 4);
        Assert.assertSame(source, decoder.decode(second).to(LastRel.class).relNode);
        Assert.assertThrows(IllegalArgumentException.class, () -> new ProvenanceCodec.Decoder().decode(origin));
        relation.put("planId", 8);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ProvenanceCodec.Decoder(List.of(source)).decode(origin));
    }

    @Test
    public void trimmedReadersShareTheirProducersOriginAfterRenumbering() {
        var source = new PositionOnlyRel(SourcePositionRange.INVALID);
        var other = new PositionOnlyRel(SourcePositionRange.INVALID);
        var encoder = new ProvenanceCodec.Encoder(new DBSPCompiler(new CompilerOptions()),
                java.util.Map.of(), new java.util.HashMap<>(), new ArrayList<>());
        encoder.retainOriginIdentities();
        var first = encoder.encode(source).put("id", 12);
        var second = encoder.encode(other).put("id", 24);
        var decoder = encoder.decoder();
        Assert.assertSame(source, decoder.decode(first));
        Assert.assertSame(other, decoder.decode(second));
        Assert.assertSame(source, decoder.decode(first.objectNode().put("ref", 12)));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ProvenanceCodec.Decoder().decode(first));
    }

    @Test
    public void liveFlatmapRetainsOnlyTheMatchingShuffle() {
        var mapper = Utilities.deterministicObjectMapper();
        var saved = mapper.createObjectNode().put("class", "ExplicitShuffle").put("inputLength", 2);
        saved.putArray("indexes").add(1).add(0);
        var live = new ExplicitShuffle(2, java.util.List.of(1, 0));
        Assert.assertSame(live, Shuffle.fromJson(saved, live));
        saved.putArray("indexes").add(0).add(1);
        var changed = Shuffle.fromJson(saved, live);
        Assert.assertNotSame(live, changed);
        Assert.assertEquals(java.util.List.of(1, 2), changed.shuffle(java.util.List.of(1, 2)));
        saved.put("class", "IdShuffle");
        Assert.assertNotSame(live, Shuffle.fromJson(saved, live));
    }

    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static CompilerMessages run(Path input, String... extra) throws Exception {
        List<String> args = new ArrayList<>(List.of("-i", "--dump-producer-commit", "replay-test",
                "--dump-runtime-abi", "runtime-test"));
        args.addAll(List.of(extra));
        args.add(input.toString());
        return CompilerMain.execute(args.toArray(String[]::new));
    }

    private Path source(String sql) throws Exception {
        Path path = this.temporary.newFile().toPath();
        Files.writeString(path, sql);
        return path;
    }

    private Path export(Path sql, String... extra) throws Exception {
        Path path = this.temporary.newFolder().toPath().resolve("s2.json");
        List<String> args = new ArrayList<>(List.of("--input_circuit", "--jit", "--noRust", "--write-s2", path.toString()));
        args.addAll(List.of(extra));
        CompilerMessages messages = run(sql, args.toArray(String[]::new));
        Assert.assertEquals(messages.toString(), 0, messages.exitCode);
        return path;
    }

    @Test
    public void replayContextRejectsAccidentalFrontEndUse() {
        var compiler = DBSPCompiler.forCircuitReplay(new CompilerOptions());
        var error = Assert.assertThrows(IllegalStateException.class, compiler::getSqlToRelCompiler);
        Assert.assertTrue(error.getMessage().contains("circuit replay"));
    }

    @Test
    public void classicWorkspaceSchemaAndMessagesSurviveFileBoundary() throws Exception {
        for (String sql: List.of(
                "CREATE VIEW v AS SELECT 1 AS result;",
                "CREATE VIEW v AS SELECT CAST(CAST(ROW(1, 'x') AS ROW(\"Named\" INT, value VARCHAR)) AS VARIANT) AS result;",
                "CREATE TYPE nested AS (\"Field\" INT); CREATE TABLE t(x nested); CREATE VIEW v AS SELECT x FROM t;",
                "CREATE TABLE t(id INT NOT NULL, value INT DEFAULT 7); CREATE VIEW v AS SELECT id FROM t WHERE id > 0;",
                "SET FELDERA_AVOID_STAR_JOINS = true; CREATE TABLE t(x VARCHAR); CREATE VIEW v AS SELECT x || '!' AS x FROM t;")) {
            Path source = this.source(sql);
            Path baseline = this.temporary.newFolder().toPath();
            Path replayed = this.temporary.newFolder().toPath();
            CompilerMessages direct = run(source, "-o", baseline.resolve("program.rs").toString(),
                    "--js", baseline.resolve("schema.json").toString());
            Assert.assertEquals(direct.toString(), 0, direct.exitCode);
            Path file = this.export(source);
            CompilerMessages resumed = run(file, "--read-s2", "-o", replayed.resolve("program.rs").toString(),
                    "--js", replayed.resolve("schema.json").toString());
            Assert.assertEquals(resumed.toString(), 0, resumed.exitCode);
            Assert.assertEquals(direct.toString(), resumed.toString());
            for (String artifact: List.of("schema.json", "program.rs", "stubs.rs"))
                Assert.assertEquals(artifact + " for " + sql, Files.readString(baseline.resolve(artifact)),
                        Files.readString(replayed.resolve(artifact)));
        }
    }

    @Test
    public void rejectsMismatchedAndIncompleteInputs() throws Exception {
        Path file = this.export(this.source("CREATE TABLE t(x INT); CREATE VIEW v AS SELECT x FROM t;"));
        Assert.assertTrue(run(file, "--read-s2", "--streaming", "--noRust").toString().contains("options mismatch"));
        Assert.assertTrue(run(file, "--read-s2", "-O", "0", "--noRust").toString().contains("options mismatch"));
        JsonNode original = Utilities.deterministicObjectMapper().readTree(Files.readString(file));
        ObjectNode oldVersion = original.deepCopy();
        oldVersion.put("schema_version", 3);
        Files.writeString(file, oldVersion.toString());
        Assert.assertTrue(run(file, "--read-s2", "--noRust").toString().contains("version-4"));
        ObjectNode changed = original.deepCopy();
        changed.put("producer_commit", "wrong");
        Files.writeString(file, changed.toString());
        Assert.assertTrue(run(file, "--read-s2", "--noRust").toString().contains("identity mismatch"));
        changed = original.deepCopy();
        changed.remove("unresolved_view_lateness");
        Files.writeString(file, changed.toString());
        Assert.assertNotEquals(0, run(file, "--read-s2", "--noRust").exitCode);
        changed = original.deepCopy();
        changed.put("unresolved_view_lateness", "malformed");
        Files.writeString(file, changed.toString());
        Assert.assertTrue(run(file, "--read-s2", "--noRust").toString().contains("pending LATENESS"));
        changed = original.deepCopy();
        ((ObjectNode) changed.path("circuit").path("allOperators").get(0)).remove("provenance");
        Files.writeString(file, changed.toString());
        Assert.assertTrue(run(file, "--read-s2", "--noRust").toString().contains("requires provenance"));
        changed = original.deepCopy();
        ((ObjectNode) changed.path("circuit").path("allOperators").get(0)).putObject("provenance").put("ref", -1);
        Files.writeString(file, changed.toString());
        Assert.assertNotEquals(0, run(file, "--read-s2", "--noRust").exitCode);
        changed = original.deepCopy();
        changed.put("stage", "S4");
        Files.writeString(file, changed.toString());
        Assert.assertTrue(run(file, "--read-s2", "--noRust").toString().contains("not an observation"));
        Files.writeString(file, original + " {}");
        Assert.assertNotEquals(0, run(file, "--read-s2", "--noRust").exitCode);
        Files.writeString(file, "{\"schema_version\":1,\"schema_version\":1}");
        Assert.assertNotEquals(0, run(file, "--read-s2", "--noRust").exitCode);
    }

    @Test
    public void relationalOperatorsAndFunctionsSurviveS2() throws Exception {
        for (String sql: List.of(
                "CREATE FUNCTION rf(r REAL) RETURNS REAL AS ROUND(r, 6); CREATE TABLE t(x REAL); CREATE VIEW v AS SELECT rf(x) FROM t;",
                "CREATE TABLE t(k INT, v INT); CREATE MATERIALIZED VIEW v AS SELECT * FROM t; CREATE INDEX idx ON v(k);",
                "CREATE TABLE t(k INT, v INT); CREATE VIEW v AS SELECT k, SUM(v), COUNT(*), MIN(v) FROM t GROUP BY k;",
                "CREATE TABLE t(k INT, v INT); CREATE TABLE u(k INT, v INT); CREATE VIEW v AS SELECT t.k, t.v, u.v AS w FROM t JOIN u ON t.k = u.k;",
                "CREATE TABLE t(k INT, v INT); CREATE TABLE u(k INT, v INT); CREATE VIEW v AS SELECT t.k, t.v, u.v AS w FROM t FULL JOIN u ON t.k = u.k;",
                "CREATE TABLE t(k INT); CREATE TABLE u(k INT); CREATE VIEW v AS SELECT k FROM t EXCEPT ALL SELECT k FROM u;",
                "CREATE TABLE t(k INT); CREATE TABLE u(k INT); CREATE VIEW v AS SELECT k FROM t INTERSECT SELECT k FROM u;",
                "CREATE TABLE t(a INT ARRAY); CREATE VIEW v AS SELECT x FROM t, UNNEST(a) AS u(x);",
                "CREATE TABLE t(k INT NOT NULL, v INT); CREATE VIEW v AS SELECT k, SUM(v) OVER (ORDER BY k RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS s, LAG(v) OVER (ORDER BY k) AS p FROM t;",
                "CREATE TABLE t(k INT, ts TIMESTAMP NOT NULL); CREATE VIEW v AS SELECT * FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' HOUR));",
                "CREATE FUNCTION f(x INT) RETURNS INT AS x + 1; CREATE TABLE t(k INT); CREATE VIEW v AS SELECT f(k) AS x FROM t;")) {
            Path source = this.source(sql);
            Path baseline = this.temporary.newFolder().toPath();
            Path replayed = this.temporary.newFolder().toPath();
            CompilerMessages direct = run(source, "-o", baseline.resolve("program.rs").toString(),
                    "--js", baseline.resolve("schema.json").toString());
            Assert.assertEquals(sql + "\n" + direct, 0, direct.exitCode);
            CompilerMessages resumed = run(this.export(source), "--read-s2", "-o", replayed.resolve("program.rs").toString(),
                    "--js", replayed.resolve("schema.json").toString());
            Assert.assertEquals(sql + "\n" + resumed, 0, resumed.exitCode);
            Assert.assertEquals(sql, direct.toString(), resumed.toString());
            for (String artifact: List.of("schema.json", "program.rs", "stubs.rs"))
                Assert.assertEquals(artifact + " for " + sql, Files.readString(baseline.resolve(artifact)),
                        Files.readString(replayed.resolve(artifact)));
        }
    }

    @Test
    public void dataflowAndStreamingDiagnosticsSurviveFileBoundary() throws Exception {
        Path source = this.source("""
                CREATE TABLE unused_table(x INT);
                CREATE TABLE t(id INT NOT NULL, value INT);
                CREATE VIEW a AS SELECT id, value + 1 AS value FROM t WHERE value > 0;
                CREATE VIEW b AS SELECT id FROM a WHERE value < 10;
                CREATE VIEW c AS SELECT DISTINCT id FROM t;
                CREATE VIEW d AS SELECT id FROM b UNION ALL SELECT id FROM c;
                """);
        for (boolean gen2: List.of(false, true)) {
            List<String> flags = new ArrayList<>(List.of("--streaming", "--correlatedColumns", "--alltables"));
            if (gen2)
                flags.add("--gen2");
            Path file = this.export(source, flags.toArray(String[]::new));
            Path baseline = this.temporary.newFolder().toPath();
            Path replay = this.temporary.newFolder().toPath();
            List<CompilerMessages> messages = new ArrayList<>();
            for (Path dir: List.of(baseline, replay)) {
                List<String> args = new ArrayList<>(flags);
                args.addAll(List.of("--noRust", "--dataflow", dir.resolve("dataflow.json").toString(),
                        "--plan", dir.resolve("plan.json").toString()));
                if (dir.equals(replay))
                    args.add("--read-s2");
                CompilerMessages result = run(dir.equals(replay) ? file : source, args.toArray(String[]::new));
                Assert.assertEquals(result.toString(), 0, result.exitCode);
                messages.add(result);
            }
            Assert.assertEquals(messages.get(0).toString(), messages.get(1).toString());
            Assert.assertTrue(messages.get(0).toString().contains("DISTINCT"));
            for (String artifact: List.of("dataflow.json", "plan.json"))
                Assert.assertEquals(artifact, Utilities.deterministicObjectMapper().readTree(
                                Files.readString(baseline.resolve(artifact))),
                        Utilities.deterministicObjectMapper().readTree(Files.readString(replay.resolve(artifact))));
        }
    }

    @Test
    public void emitFinalAnnotationPositionsSurviveS2() throws Exception {
        Path source = this.source("""
                CREATE TABLE t(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                CREATE VIEW v
                WITH ('emit_final' = 'ts')
                AS SELECT ts FROM t;
                """);
        for (boolean gen2: List.of(false, true)) {
            List<String> flags = new ArrayList<>(List.of("--streaming"));
            if (gen2)
                flags.add("--gen2");
            Path seam = this.export(source, flags.toArray(String[]::new));
            List<JsonNode> dataflows = new ArrayList<>();
            for (boolean replay: List.of(false, true)) {
                Path output = this.temporary.newFolder().toPath().resolve("dataflow.json");
                List<String> args = new ArrayList<>(flags);
                args.addAll(List.of("--noRust", "--dataflow", output.toString()));
                if (replay)
                    args.add("--read-s2");
                CompilerMessages result = run(replay ? seam : source, args.toArray(String[]::new));
                Assert.assertEquals(result.toString(), 0, result.exitCode);
                dataflows.add(Utilities.deterministicObjectMapper().readTree(Files.readString(output)));
            }
            Assert.assertEquals("emit_final provenance, gen2=" + gen2, dataflows.get(0), dataflows.get(1));
        }
    }

    @Test
    public void optimizerErrorsKeepTheirOriginalDiagnostics() throws Exception {
        Path source = this.source("""
                SET FELDERA_WARNINGS_ARE_ERRORS = true;
                CREATE TABLE t(id INT NOT NULL, unused INT);
                CREATE VIEW v AS SELECT id FROM t;
                """);
        CompilerMessages direct = run(source, "--noRust");
        Assert.assertNotEquals(0, direct.exitCode);
        Assert.assertEquals(1, direct.errorCount());
        Assert.assertEquals("Unused column", direct.getMessage(0).errorType);
        Path dumps = this.temporary.newFolder().toPath();
        CompilerMessages resumed = run(this.export(source), "--read-s2", "--noRust",
                "--dump-circuits", dumps.toString());
        Assert.assertEquals(direct.exitCode, resumed.exitCode);
        Assert.assertEquals(direct.toString(), resumed.toString());
        Assert.assertFalse(Files.exists(dumps.resolve("compilation-0000/complete.json")));
    }

    @Test
    public void preservesMultiCrateWorkspace() throws Exception {
        Path source = this.source("""
                CREATE TABLE t(id INT NOT NULL PRIMARY KEY, value DECIMAL(12, 2) DEFAULT 7.25);
                CREATE VIEW v AS SELECT id, value * 2 AS doubled FROM t WHERE value > 0;
                """);
        Path baseline = this.temporary.newFolder().toPath();
        Path resumed = this.temporary.newFolder().toPath();
        CompilerMessages direct = run(source, "--crates", "circuit", "-o", baseline.toString());
        Assert.assertEquals(direct.toString(), 0, direct.exitCode);
        CompilerMessages replay = run(this.export(source, "--crates", "circuit"),
                "--read-s2", "--crates", "circuit", "-o", resumed.toString());
        Assert.assertEquals(replay.toString(), 0, replay.exitCode);
        try (var files = Files.walk(baseline); var replayFiles = Files.walk(resumed)) {
            var expected = files.filter(Files::isRegularFile).map(baseline::relativize).sorted().toList();
            Assert.assertEquals(expected,
                    replayFiles.filter(Files::isRegularFile).map(resumed::relativize).sorted().toList());
            Assert.assertTrue(expected.size() > 3);
            for (Path file: expected)
                Assert.assertArrayEquals(file.toString(), Files.readAllBytes(baseline.resolve(file)),
                        Files.readAllBytes(resumed.resolve(file)));
        }
    }

    @Test
    public void fallbackProvenanceTextDoesNotExposeAllocationIds() {
        List<String> text = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            DBSPCompiler compiler = new DBSPCompiler(options(false, false));
            compiler.submitStatementsForCompilation("CREATE TABLE t(x INT); CREATE VIEW v AS SELECT x FROM t;");
            compiler.getFinalCircuit(false);
            compiler.throwIfErrorsOccurred();
            var plans = compiler.getPlans(new org.dbsp.util.IndentStream(new StringBuilder()));
            var scan = plans.keySet().stream()
                    .filter(node -> node instanceof org.apache.calcite.rel.core.TableScan)
                    .filter(node -> node.getTable().getQualifiedName().contains("t"))
                    .findFirst().orElseThrow();
            LastRel last = new LastRel(scan, SourcePositionRange.INVALID);
            RelAnd reads = new RelAnd();
            reads.add(last);
            text.add(new RelReference.Live(scan).text() + "\n" + reads.toInternalString()
                    + "\n" + new RelSequence(List.of(last)).toInternalString());
        }
        Assert.assertEquals(text.get(0), text.get(1));
        Assert.assertFalse(text.get(0).contains("rel#"));
    }

    @Test
    public void s3TailPreservesSingleAndMultiCrateWorkspaces() throws Exception {
        for (String sql: List.of(
                "CREATE FUNCTION rf(r REAL) RETURNS REAL AS ROUND(r, 6); CREATE TABLE t(x REAL); CREATE VIEW v AS SELECT rf(x) FROM t;",
                "CREATE TABLE t(k INT, v INT); CREATE MATERIALIZED VIEW v AS SELECT * FROM t; CREATE INDEX idx ON v(k);",
                "CREATE TABLE edges(src INT NOT NULL, dst INT NOT NULL); "
                        + "DECLARE RECURSIVE VIEW reach(src INT NOT NULL, dst INT NOT NULL); "
                        + "CREATE VIEW reach AS SELECT src, dst FROM edges UNION "
                        + "SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src;",
                "CREATE VIEW v AS SELECT 'a nontrivial string' AS result;",
                "CREATE TABLE t(k INT, v INT); CREATE VIEW v AS SELECT k, SUM(v), MIN(v) FROM t GROUP BY k;",
                "CREATE TABLE t(k INT, v INT); CREATE TABLE u(k INT, v INT); CREATE VIEW v AS SELECT t.k, t.v, u.v AS w FROM t FULL JOIN u ON t.k = u.k;",
                "CREATE TABLE t(k INT NOT NULL, v INT); CREATE VIEW v AS SELECT k, LAG(v) OVER (ORDER BY k) AS p FROM t;",
                "CREATE FUNCTION f(x INT) RETURNS INT AS x + 1; CREATE TABLE t(k INT); CREATE VIEW v AS SELECT f(k) AS x FROM t;",
                "CREATE TABLE t(id INT NOT NULL PRIMARY KEY, value DECIMAL(12,2)); "
                        + "CREATE VIEW v AS SELECT id, value * 2 AS doubled FROM t WHERE value > 0;")) {
            Path source = this.source(sql);
            for (boolean multiCrate: List.of(false, true)) {
                List<String> flags = multiCrate ? List.of("--crates", "circuit") : List.of();
                Path baseline = this.temporary.newFolder().toPath();
                Path replay = this.temporary.newFolder().toPath();
                Path file = this.temporary.newFolder().toPath().resolve("s3.json");
                List<String> exportArgs = new ArrayList<>(flags);
                exportArgs.addAll(List.of("--write-s3", file.toString(), "-o",
                        multiCrate ? baseline.toString() : baseline.resolve("program.rs").toString()));
                CompilerMessages direct = run(source, exportArgs.toArray(String[]::new));
                Assert.assertEquals(direct.toString(), 0, direct.exitCode);
                List<String> importArgs = new ArrayList<>(flags);
                importArgs.addAll(List.of("--read-s3", "-o",
                        multiCrate ? replay.toString() : replay.resolve("program.rs").toString()));
                CompilerMessages resumed = run(file, importArgs.toArray(String[]::new));
                Assert.assertEquals(resumed.toString(), 0, resumed.exitCode);
                Assert.assertEquals(direct.toString(), resumed.toString());
                try (var files = Files.walk(baseline); var replayFiles = Files.walk(replay)) {
                    var expected = files.filter(Files::isRegularFile).map(baseline::relativize).sorted().toList();
                    Assert.assertEquals(expected,
                            replayFiles.filter(Files::isRegularFile).map(replay::relativize).sorted().toList());
                    for (Path path: expected)
                        Assert.assertArrayEquals(path.toString(), Files.readAllBytes(baseline.resolve(path)),
                                Files.readAllBytes(replay.resolve(path)));
                }
                Assert.assertNotEquals(0, run(file, "--read-s2", "--noRust").exitCode);
            }
        }
    }

    @Test
    public void s3RecomputesHashInputsInsteadOfTrustingAnnotations() throws Exception {
        for (boolean gen2: List.of(false, true)) {
            DBSPCompiler direct = new DBSPCompiler(options(gen2, false));
            List<String> expected = new ArrayList<>();
            direct.hashInputHook = (kind, text) -> expected.add(kind + "\n" + text);
            direct.submitStatementsForCompilation("CREATE TABLE t(id INT NOT NULL, value INT); "
                    + "CREATE VIEW v AS SELECT id + 1 AS id FROM t WHERE value > 0;");
            var circuit = direct.getFinalCircuit(false);
            direct.throwIfErrorsOccurred();
            Path file = this.temporary.newFolder().toPath().resolve("s3.json");
            CircuitReplay.write(direct, circuit, file, CircuitReplay.Stage.S3);
            JsonNode json = Utilities.deterministicObjectMapper().readTree(Files.readString(file));
            int corrupted = 0;
            for (JsonNode operator: json.path("circuit").path("allOperators")) {
                for (JsonNode annotation: operator.path("annotations")) {
                    if (annotation.path("class").asText().equals("OperatorHash")) {
                        ((ObjectNode) annotation).put("hash", "0".repeat(64));
                        corrupted++;
                    }
                }
            }
            Assert.assertTrue(corrupted > 0);
            Files.writeString(file, json.toString());
            DBSPCompiler resumed = new DBSPCompiler(options(gen2, false));
            List<String> actual = new ArrayList<>();
            resumed.hashInputHook = (kind, text) -> actual.add(kind + "\n" + text);
            var decoded = CircuitReplay.read(resumed, file, CircuitReplay.Stage.S3);
            resumed.throwIfErrorsOccurred();
            Assert.assertFalse(expected.isEmpty());
            Assert.assertEquals(expected, actual);
            Assert.assertNotNull(decoded);
            for (int i = 0; i < circuit.allOperators.size(); i++) {
                for (boolean global: List.of(false, true))
                    Assert.assertEquals(
                            org.dbsp.sqlCompiler.circuit.annotation.OperatorHash.getHash(circuit.allOperators.get(i), global),
                            org.dbsp.sqlCompiler.circuit.annotation.OperatorHash.getHash(decoded.allOperators.get(i), global));
            }
        }
    }

    private static CompilerOptions options(boolean gen2, boolean s2) {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.incrementalize = true;
        options.ioOptions.gen2 = gen2;
        options.ioOptions.inputCircuit = s2;
        options.ioOptions.dumpProducerCommit = "replay-test";
        options.ioOptions.dumpRuntimeAbi = "runtime-test";
        return options;
    }

    @Test
    public void internalClockKeepsSharedOutputType() throws Exception {
        for (boolean gen2: List.of(false, true)) {
            CompilerOptions options = options(gen2, false);
            options.ioOptions.nowStream = false;
            DBSPCompiler direct = new DBSPCompiler(options);
            List<JsonNode> expected = new ArrayList<>();
            direct.circuitObservationHook = json -> {
                try {
                    JsonNode value = observationWithNodeCounts(json);
                    if (value.path("observation").path("stage").asText().equals("S3"))
                        expected.add(value.get("nodes"));
                } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            };
            direct.submitStatementsForCompilation("CREATE TABLE t(d DATE); "
                    + "CREATE VIEW v AS SELECT d FROM t WHERE d >= CAST(NOW() AS DATE);");
            var circuit = direct.getFinalCircuit(false);
            direct.throwIfErrorsOccurred();
            Path file = this.temporary.newFolder().toPath().resolve("s3.json");
            CircuitReplay.write(direct, circuit, file, CircuitReplay.Stage.S3);
            DBSPCompiler resumed = new DBSPCompiler(options);
            List<JsonNode> actual = new ArrayList<>();
            resumed.circuitObservationHook = json -> {
                try { actual.add(observationWithNodeCounts(json).get("nodes")); }
                catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            };
            CircuitReplay.read(resumed, file, CircuitReplay.Stage.S3);
            resumed.throwIfErrorsOccurred();
            Assert.assertEquals(1, expected.size());
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void hashInputsAndPassSequenceMatchInBothModes() throws Exception {
        String sql = """
                SET FELDERA_WINDOW_SHARING_THRESHOLD = 3;
                SET custom_setting = 'retained warning';
                CREATE TABLE t (id INT NOT NULL PRIMARY KEY, unused INT DEFAULT 7);
                CREATE VIEW v AS SELECT id + 2 AS id FROM t WHERE id > 0;
                CREATE VIEW a AS SELECT ARRAY_AGG(id) FROM t;
                CREATE TABLE u(id INT NOT NULL) WITH ('append_only' = 'true');
                CREATE VIEW b AS SELECT MIN(id), COUNT(*), SUM(id) FROM u;
                """;
        for (boolean gen2: List.of(false, true)) {
            DBSPCompiler direct = new DBSPCompiler(options(gen2, false));
            List<String> expectedHashes = new ArrayList<>();
            direct.hashInputHook = (kind, text) -> expectedHashes.add(kind + "\n" + text);
            List<JsonNode> expectedPasses = new ArrayList<>();
            direct.circuitObservationHook = json -> {
                try {
                    expectedPasses.add(observationWithNodeCounts(json));
                } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            };
            direct.submitStatementsForCompilation(sql);
            direct.getFinalCircuit(false);
            direct.throwIfErrorsOccurred();
            DBSPCompiler exporter = new DBSPCompiler(options(gen2, true));
            exporter.submitStatementsForCompilation(sql);
            var s2 = exporter.getFinalCircuit(false);
            exporter.throwIfErrorsOccurred();
            Path file = this.temporary.newFolder().toPath().resolve("s2.json");
            CircuitReplay.write(exporter, s2, file);
            DBSPCompiler resumed = new DBSPCompiler(options(gen2, false));
            List<String> actualHashes = new ArrayList<>();
            resumed.hashInputHook = (kind, text) -> actualHashes.add(kind + "\n" + text);
            List<JsonNode> actualPasses = new ArrayList<>();
            resumed.circuitObservationHook = json -> {
                try {
                    actualPasses.add(observationWithNodeCounts(json));
                } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            };
            var optimized = CircuitReplay.read(resumed, file);
            Assert.assertSame(resumed.metadata, optimized.metadata);
            Assert.assertEquals(direct.metadata.asJson(), resumed.metadata.asJson());
            Assert.assertEquals(direct.messages.toJson(direct.sources), resumed.messages.toJson(resumed.sources));
            Assert.assertFalse(expectedHashes.isEmpty());
            Assert.assertEquals(expectedHashes, actualHashes);
            Assert.assertEquals(expectedPasses.size(), actualPasses.size());
            for (int i = 0; i < expectedPasses.size(); i++)
                Assert.assertEquals("Observation " + i, expectedPasses.get(i), actualPasses.get(i));
        }
    }

    /** Catch sharing changes during aggregate expansion, even when a later
     * optimization removes them. The corpus harness compares full graphs too. */
    private static JsonNode observationWithNodeCounts(String json) throws java.io.IOException {
        var mapper = Utilities.deterministicObjectMapper();
        JsonNode envelope = mapper.readTree(json);
        ObjectNode result = mapper.createObjectNode();
        result.set("observation", envelope.get("observation"));
        var counts = new java.util.TreeMap<String, Integer>();
        for (JsonNode kind: envelope.get("circuit").findValues("class"))
            counts.merge(kind.asText(), 1, Integer::sum);
        result.set("nodes", mapper.valueToTree(counts));
        return result;
    }

    @Test
    public void postCompilationWarningsKeepTheirOrderAndPolicyAcrossS2() throws Exception {
        for (boolean gen2: List.of(false, true)) {
            for (boolean promote: List.of(false, true)) {
                Path source = this.source((promote ? "SET FELDERA_WARNINGS_ARE_ERRORS = true;\n" : "") + """
                        CREATE TABLE t(x INT, unused INT);
                        LATENESS missing.x 1;
                        CREATE VIEW v AS SELECT x FROM t;
                        """);
                List<String> flags = new ArrayList<>(List.of("--noRust"));
                if (gen2)
                    flags.add("--gen2");
                CompilerMessages direct = run(source, flags.toArray(String[]::new));
                Path file = this.export(source, gen2 ? new String[]{"--gen2"} : new String[]{});
                flags.add("--read-s2");
                CompilerMessages resumed = run(file, flags.toArray(String[]::new));
                Assert.assertEquals(direct.exitCode, resumed.exitCode);
                Assert.assertEquals(direct.toString(), resumed.toString());
                Assert.assertTrue(direct.toString().contains("LATENESS statement not found"));
                Assert.assertTrue(direct.toString().indexOf("Unused column") <
                        direct.toString().indexOf("No such view"));
            }
        }
    }
}
