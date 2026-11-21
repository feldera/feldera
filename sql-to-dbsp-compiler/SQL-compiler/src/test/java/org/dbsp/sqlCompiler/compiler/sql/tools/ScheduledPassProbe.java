package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.EnsureDistinctOutputs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.MergeSums;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Utilities;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Observe the original passes in their production order, with no graph changes. */
public final class ScheduledPassProbe {
    private static final ObjectMapper MAPPER = Utilities.deterministicObjectMapper();

    private ScheduledPassProbe() { }

    private static final class Capture implements DBSPCompiler.StateObserver {
        final DBSPCompiler compiler;
        final String id;
        final boolean activity;
        final String selectedContainer;
        final Set<String> selectedPasses;
        final Map<String, ObjectNode> records = new LinkedHashMap<>();

        Capture(DBSPCompiler compiler, String id, boolean activity, String selectedContainer,
                Set<String> selectedPasses) {
            this.compiler = compiler;
            this.id = id;
            this.activity = activity;
            this.selectedContainer = selectedContainer;
            this.selectedPasses = selectedPasses;
        }

        private JsonNode snapshot(DBSPCircuit circuit) {
            try {
                return MultisetProbe.circuitJson(this.compiler, circuit);
            } catch (Exception e) {
                throw new IllegalStateException("Cannot snapshot scheduled pass", e);
            }
        }

        private boolean observed(String container, CircuitTransform pass) {
            if (this.selectedContainer != null)
                return this.selectedContainer.equals(container)
                        && (this.selectedPasses.isEmpty() || this.selectedPasses.contains(pass.getName()));
            if (this.activity)
                return container.equals("Optimizer");
            return pass instanceof MergeSums || pass instanceof EnsureDistinctOutputs;
        }

        private void describeState(ObjectNode record, String label, DBSPCircuit circuit) {
            ObjectNode state = record.putObject(label);
            state.put("graph_sha256", DigestUtils.sha256Hex(this.snapshot(circuit).toString()));
            state.put("diagnostics_sha256", DigestUtils.sha256Hex(
                    this.compiler.messages.toJson(this.compiler.sources).toString()));
            state.put("next_generated_name", DBSPVariablePath.nextGeneratedName());
            state.put("operators", circuit.allOperators.size());
        }

        @Override
        public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {
            if (!this.observed(container, pass))
                return;
            var record = MAPPER.createObjectNode();
            String key = this.activity || this.selectedContainer != null
                    ? index + "/" + pass.getName() : pass.getName();
            record.put("id", this.id + "/" + key);
            record.put("pass", pass.getName());
            record.put("container", container);
            record.put("pass_index", index);
            record.put("iteration", iteration);
            if (this.activity) {
                record.put("implementation", pass.getClass().getSimpleName());
                this.describeState(record, "before", circuit);
            } else {
                record.set("circuit", this.snapshot(circuit));
                if (this.selectedContainer != null)
                    record.put("next_generated_name_before", DBSPVariablePath.nextGeneratedName());
            }
            if (this.records.putIfAbsent(key, record) != null)
                throw new IllegalStateException("Repeated observed pass " + pass.getName());
        }

        @Override
        public void after(String container, int index, int iteration, CircuitTransform pass,
                          DBSPCircuit circuit, boolean completed) {
            if (!this.observed(container, pass))
                return;
            if (!completed || circuit == null)
                throw new IllegalStateException("Pass failed: " + pass.getName());
            if (this.activity)
                this.describeState(this.records.get(index + "/" + pass.getName()), "after", circuit);
            else {
                var record = this.records.get(this.selectedContainer == null
                        ? pass.getName() : index + "/" + pass.getName());
                record.set("expected", this.snapshot(circuit));
                if (this.selectedContainer != null)
                    record.put("next_generated_name_after", DBSPVariablePath.nextGeneratedName());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        boolean activity = args.length == 5 && args[4].equals("--activity");
        String container = (args.length == 6 || args.length == 7) && args[4].equals("--container")
                ? args[5] : null;
        Set<String> selectedPasses = args.length == 7 ? Set.of(args[6].split(",", -1)) : Set.of();
        if ((args.length != 4 && !activity && container == null)
                || !("classic".equals(args[2]) || "gen2".equals(args[2]))
                || !("sets".equals(args[3]) || "bags".equals(args[3])))
            throw new IllegalArgumentException(
                    "Usage: ScheduledPassProbe INPUT OUTPUT classic|gen2 sets|bags [--activity|--container NAME [PASS,...]]");
        try (var source = Files.newBufferedReader(Path.of(args[0]));
             var output = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = source.readLine()) != null) {
                JsonNode test = MAPPER.readTree(line);
                var options = MultisetProbe.options("S3", args[2]);
                options.languageOptions.outputsAreSets = "sets".equals(args[3]);
                DBSPCompiler compiler = new DBSPCompiler(options);
                String id = args[2] + "/" + args[3] + "/" + test.get("id").asText();
                Capture capture = new Capture(compiler, id, activity, container, selectedPasses);
                compiler.stateObserver = capture;
                compiler.submitStatementsForCompilation(test.get("sql").asText());
                var circuit = compiler.getFinalCircuit(false);
                compiler.throwIfErrorsOccurred();
                boolean complete = activity || container != null ? !capture.records.isEmpty()
                        : capture.records.size() == (options.languageOptions.outputsAreSets ? 2 : 1);
                if (circuit == null || !complete)
                    throw new IllegalStateException("Incomplete pass capture for " + id);
                for (var record : capture.records.values()) {
                    if (!record.has(activity ? "after" : "expected"))
                        throw new IllegalStateException("Pass output missing at " + id);
                    output.write(MAPPER.writeValueAsString(record));
                    output.newLine();
                }
            }
        }
    }
}
