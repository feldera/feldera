package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.codec.digest.DigestUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.rust.FindStatics;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitPostfix;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/** Execute frozen inputs and assertions on an exported circuit or a fresh hybrid compilation. */
public final class CorpusRuntimeReplay {
    private static final ObjectMapper MAPPER = Utilities.deterministicObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private CorpusRuntimeReplay() {}

    private static JsonNode array(JsonNode node, String field) {
        JsonNode value = node.required(field);
        if (!value.isArray())
            throw new IllegalArgumentException(field + " must be an array");
        return value;
    }

    private static ProgramIdentifier identifier(JsonNode node) {
        return new ProgramIdentifier(Utilities.getStringProperty(node, "text"),
                Utilities.getBooleanProperty(node, "quoted"));
    }

    private static Change change(JsonNode command, String field) {
        var sets = array(command, field);
        var tables = new TableData[sets.size()];
        for (int i = 0; i < sets.size(); i++) {
            JsonNode table = sets.get(i);
            List<Integer> keys = Linq.list(Linq.map(array(table, "primary_keys").elements(), key -> {
                if (!key.isIntegralNumber() || !key.canConvertToInt() || key.intValue() < 0)
                    throw new IllegalArgumentException("Expected a nonnegative primary key column index");
                return key.intValue();
            }));
            // The exporter starts a fresh ID scope for each bag.
            var bag = new JsonDecoder().decodeInner(table.required("zset_ir"), DBSPZSetExpression.class);
            tables[i] = new TableData(identifier(table.required("name")), bag, keys);
        }
        return new Change(tables);
    }

    private static List<String> order(JsonNode node, String field) {
        return Linq.list(Linq.map(array(node, field).elements(), name -> {
            if (!name.isTextual())
                throw new IllegalArgumentException("Expected a stream name in " + field);
            return name.textValue();
        }));
    }

    static TestCase load(Path path) throws IOException {
        JsonNode record = MAPPER.readTree(path.toFile());
        if (Utilities.getIntProperty(record, "schema_version") != 1
                || !Utilities.getStringProperty(record, "status").equals("exported"))
            throw new IllegalArgumentException("Expected an exported schema-version-1 case");
        var options = MAPPER.treeToValue(record.required("options"), CompilerOptions.class);
        String engine = options.ioOptions.gen2 ? "gen2" : "classic";
        if (!Utilities.getStringProperty(record, "engine_mode").equals(engine))
            throw new IllegalArgumentException("Engine mode disagrees with test options");
        String semantics = options.languageOptions.incrementalize ? "delta" : "whole_relation";
        if (!Utilities.getStringProperty(record, "step_semantics").equals(semantics))
            throw new IllegalArgumentException("Step semantics disagree with test options");
        DBSPCompiler compiler;
        DBSPCircuit circuit;
        String worker = System.getProperty("feldera.runtime.s3Worker");
        if (worker == null) {
            compiler = DBSPCompiler.forCircuitReplay(options);
            circuit = new JsonDecoder(compiler.metadata)
                    .decodeOuter(record.required("circuit"), DBSPCircuit.class);
        } else {
            Path scratch = Files.createTempDirectory("p2-runtime-");
            try {
                var restored = P2Artifacts.classicRuntime(record, scratch, worker);
                compiler = restored.compiler();
                circuit = restored.circuit();
            } finally {
                try (var files = Files.walk(scratch)) {
                    for (Path file : files.sorted(java.util.Comparator.reverseOrder()).toList())
                        Files.delete(file);
                }
            }
        }
        // Older version-1 exports derive the actual mode from the circuit itself.
        JsonNode effective = record.get("compilation_options");
        if (effective != null && circuit.incremental != Utilities.getBooleanProperty(
                effective.required("languageOptions"), "incrementalize"))
            throw new IllegalArgumentException("Circuit stream kind disagrees with compilation options");

        var stream = new InputOutputChangeStream(order(record, "input_order"), order(record, "output_order"));
        for (JsonNode command : array(record, "commands")) {
            switch (Utilities.getStringProperty(command, "kind")) {
                case "step" -> stream.addPair(change(command, "inputs"), change(command, "outputs"));
                case "compact" -> stream.addBlockForCompaction();
                default -> throw new IllegalArgumentException("Unsupported replay command " + command.get("kind"));
            }
        }
        String identity = Utilities.getStringProperty(record, "source_test")
                + "#" + Utilities.getIntProperty(record, "case_index");
        JsonNode panic = record.required("expected_runtime_error");
        if (!panic.isNull() && !panic.isTextual())
            throw new IllegalArgumentException("Expected a runtime error string or null");
        var ccs = new CompilerCircuitStream(compiler, circuit, stream);
        return new TestCase(Utilities.getStringProperty(record, "name"), identity, ccs, panic.textValue());
    }

    private static void useSingleFileWriter(TestCase test) {
        var compiler = test.ccs.compiler;
        if (!compiler.options.ioOptions.multiCrates())
            return;
        // The multi-crate writer declares statics within each generated operator.
        // Give them a shared scope in the replay's single-file circuit constructor.
        var circuit = test.ccs.circuit;
        var statics = new FindStatics(compiler);
        statics.getCircuitVisitor(true).apply(circuit);
        var declared = new HashSet<String>();
        for (var declaration : circuit.declarations)
            declared.add(declaration.getName());
        for (var expression : statics.expressions())
            if (declared.add(expression.getName()))
                circuit.addDeclaration(new DBSPDeclaration(new DBSPStaticItem(expression)));
        compiler.options.ioOptions.crates = "";
    }

    private static void writeClassic(Path output, List<TestCase> cases) throws IOException {
        DBSPCompiler compiler = cases.getFirst().ccs.compiler;
        for (TestCase test : cases) {
            if (test.ccs.compiler.options.ioOptions.gen2 || !compiler.options.same(test.ccs.compiler.options))
                throw new IllegalArgumentException("Classic replay requires matching classic options: " + test.javaTestName);
            useSingleFileWriter(test);
        }
        for (String file : List.of(DBSPCompiler.STUBS_FILE_NAME, DBSPCompiler.UDF_FILE_NAME)) {
            Path support = output.resolveSibling(file);
            if (!Files.exists(support))
                Files.createFile(support);
        }
        try (var destination = Files.newBufferedWriter(output, StandardOpenOption.CREATE_NEW)) {
            var writer = new RustFileWriter(new CircuitPostfix(compiler));
            writer.setOutputBuilder(new IndentStream(destination));
            var changes = new HashMap<Long, DBSPFunction>();
            for (int index = 0; index < cases.size(); index++) {
                TestCase test = cases.get(index);
                test.ccs.circuit.setName("circuit" + index);
                writer.add(test.ccs.circuit);
                if (!test.hasData() && test.message == null) {
                    System.out.println("compile-only\t" + test.javaTestName);
                    continue;
                }
                String name = "test" + index;
                if (Boolean.getBoolean("feldera.runtime.caseNames"))
                    name += "_" + DigestUtils.sha256Hex(test.javaTestName);
                for (var tester : test.createTesterCode(index, name, null, ".", changes))
                    writer.addTest(tester);
                System.out.println(name + "\t" + test.javaTestName);
            }
            writer.write(compiler);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2)
            throw new IllegalArgumentException("Usage: CorpusRuntimeReplay <lib.rs | --gen2 | --decode-only> CASE.json...");
        boolean classic = !args[0].equals("--decode-only") && !args[0].equals("--gen2");
        var cases = new ArrayList<TestCase>();
        int failures = 0;
        for (int index = 1; index < args.length; index++) {
            Path path = Path.of(args[index]);
            try {
                var test = load(path);
                switch (args[0]) {
                    case "--decode-only" -> System.out.println("decoded\t" + test.javaTestName);
                    case "--gen2" -> {
                        Gen2Runner.run(test);
                        System.out.println((test.hasData() ? "executed" : "loaded") + "\t" + test.javaTestName);
                    }
                    default -> cases.add(test);
                }
            } catch (IOException | RuntimeException | AssertionError error) {
                var failure = new IllegalArgumentException("Cannot replay " + path, error);
                if (classic)
                    throw failure;
                failure.printStackTrace(System.err);
                failures++;
            }
        }
        if (failures > 0)
            throw new IllegalStateException(failures + " of " + (args.length - 1) + " replays failed");
        if (classic)
            writeClassic(Path.of(args[0]), cases);
    }
}
