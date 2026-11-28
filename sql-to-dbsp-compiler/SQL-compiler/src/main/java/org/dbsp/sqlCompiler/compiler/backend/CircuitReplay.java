package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ValidateStreamKinds;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/** Spike-1 file boundary for a deliberately restricted operator subset. This is
 * separate from observation dumps and public Gen-2 IR. Relational provenance is
 * restored as detached references; the executable Calcite plans are carried as JSON. */
public final class CircuitReplay {
    public enum Stage {
        S2, S3;

        String wireName() { return this.name() + "-subset"; }
    }

    public static final class ResourceLimit extends BaseCompilerException {
        public ResourceLimit(String message) {
            super(message + ". Compile the original SQL directly with the Java optimizer,"
                            + " without checkpoint flags and without --rust-optimizer-passes.",
                    SourcePositionRange.INVALID);
        }

        @Override
        public String getErrorKind() {
            return "Optimizer resource limit";
        }
    }

    private static final int DEFAULT_MAX_BYTES = 64 * 1024 * 1024;
    // Tests can lower the checkpoint limit for one compiler process.
    private static final int MAX_BYTES = Math.max(1, Math.min(DEFAULT_MAX_BYTES,
            Integer.getInteger("feldera.test.checkpointMaxBytes", DEFAULT_MAX_BYTES)));

    private static void checkSize(Stage stage, String operation, long size) {
        if (size > MAX_BYTES)
            throw new ResourceLimit(stage + " " + operation + " is " + size +
                    " bytes. The transport limit is " + MAX_BYTES + " bytes");
    }

    private static final Set<String> OPERATORS = Set.of(
            "DBSPSourceMultisetOperator", "DBSPSourceMapOperator", "DBSPViewOperator",
            "DBSPSinkOperator", "DBSPMapOperator", "DBSPFilterOperator", "DBSPConstantOperator",
            "DBSPNowOperator", "DBSPMapIndexOperator", "DBSPDeindexOperator", "DBSPSumOperator",
            "DBSPDistinctOperator", "DBSPStreamDistinctOperator",
            "DBSPStreamAggregateOperator", "DBSPDifferentiateOperator", "DBSPIntegrateOperator",
            "DBSPStreamJoinOperator", "DBSPAggregateZeroOperator", "DBSPLeftJoinOperator",
            "DBSPNoopOperator", "DBSPPartitionedRollingAggregateOperator", "DBSPIndexedTopKOperator",
            "DBSPNegateOperator", "DBSPFlatMapOperator", "DBSPStreamAntiJoinOperator",
            "DBSPLagOperator", "DBSPPositiveOperator", "DBSPRowNumberOperator",
            "DBSPViewDeclarationOperator", "DBSPAsofJoinOperator", "DBSPRankOperator",
            "DBSPSubtractOperator", "DBSPHopOperator");

    private static final Set<String> S3_OPERATORS = Set.of(
            "DBSPSourceMultisetOperator", "DBSPSourceMapOperator", "DBSPSinkOperator",
            "DBSPMapOperator", "DBSPFilterOperator", "DBSPFlatMapOperator", "DBSPSumOperator",
            "DBSPIncrementalConstantOperator", "DBSPConstantOperator", "DBSPDistinctOperator",
            "DBSPStreamDistinctOperator", "DBSPDelayOperator", "DBSPControlledKeyFilterOperator",
            "DBSPWaterlineOperator", "DBSPMapIndexOperator", "DBSPDeindexOperator",
            "DBSPDifferentiateOperator", "DBSPIntegrateOperator", "DBSPApplyOperator",
            "DBSPAggregateLinearPostprocessOperator", "DBSPFlatMapIndexOperator", "DBSPNegateOperator",
            "DBSPStreamJoinOperator", "DBSPIntegrateTraceRetainKeysOperator", "DBSPStreamAggregateOperator",
            "DBSPAtomicSumOperator", "DBSPAggregateOperator", "DBSPJoinIndexOperator",
            "DBSPPartitionedRollingAggregateOperator", "DBSPJoinOperator", "DBSPIndexedTopKOperator",
            "DBSPLeftJoinOperator", "DBSPAggregateLinearPostprocessRetainKeysOperator",
            "DBSPChainAggregateOperator", "DBSPLagOperator", "DBSPAntiJoinOperator",
            "DBSPIntegrateTraceRetainNValuesOperator", "DBSPWindowOperator", "DBSPApplyNOperator",
            "DBSPPositiveOperator", "DBSPApply2Operator", "DBSPJoinFilterMapOperator",
            "DBSPStarJoinOperator", "DBSPLeftJoinIndexOperator", "DBSPLeftJoinFilterMapOperator",
            "DBSPIntegrateTraceRetainValuesOperator", "DBSPRowNumberOperator", "DBSPSubtractOperator",
            "DBSPRankOperator", "DBSPInputMapWithWaterlineOperator", "DBSPNestedOperator",
            "DBSPStarJoinIndexOperator", "DBSPConcreteAsofJoinOperator",
            "DBSPPartitionedRollingAggregateWithWaterlineOperator", "DBSPWeightValidatorOperator",
            "DBSPNowOperator", "DBSPSourceLazyMapOperator", "DBSPStarJoinFilterMapOperator");

    private CircuitReplay() { }

    private static ObjectMapper mapper() {
        return Utilities.deterministicObjectMapper()
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    }

    private static ObjectNode options(CompilerOptions options) {
        ObjectNode result = CircuitDumps.effectiveOptions(options);
        // These select an output stage or encoding; they do not change the optimizer.
        // multiCrates is retained because it changes static-expression lowering.
        ((ObjectNode) result.get("ioOptions")).remove(List.of(
                "inputCircuit", "noRust", "interpreterJson"));
        return result;
    }

    private static void identities(CompilerOptions options) {
        if (options.ioOptions.dumpProducerCommit.isBlank() || options.ioOptions.dumpRuntimeAbi.isBlank())
            throw new IllegalArgumentException("Circuit replay requires compiler and runtime identities");
    }

    public static void write(DBSPCompiler compiler, DBSPCircuit circuit, Path path) throws IOException {
        write(compiler, circuit, path, Stage.S2);
    }

    public static void write(DBSPCompiler compiler, DBSPCircuit circuit, Path path, Stage stage) throws IOException {
        identities(compiler.options);
        if (compiler.options.ioOptions.inputCircuit != (stage == Stage.S2) || compiler.hasErrors())
            throw new IllegalArgumentException(stage + " export requires a successful compilation at that stage");
        ObjectNode result = mapper().createObjectNode();
        result.put("schema_version", 4);
        result.put("stage", stage.wireName());
        result.put("producer_commit", compiler.options.ioOptions.dumpProducerCommit);
        result.put("runtime_abi", compiler.options.ioOptions.dumpRuntimeAbi);
        result.set("options", options(compiler.options));
        result.put("next_generated_name", DBSPVariablePath.nextGeneratedName());
        ObjectNode weight = result.putObject("compiler_weight_variable");
        weight.put("variable", compiler.weightVar.variable);
        result.set("sources", compiler.sources.replaySnapshot());
        result.set("diagnostics", compiler.messages.replaySnapshot());
        var pending = result.putArray("unresolved_view_lateness");
        if (stage == Stage.S2) {
            for (var late: compiler.unresolvedViewLateness()) {
                ObjectNode entry = pending.addObject();
                entry.put("view", late.quotedView());
                entry.set("range", late.range().asJson());
            }
        }
        JsonStream settings = JsonStream.compactCircuit();
        compiler.metadata.writeSettings(new ToJsonInnerVisitor(compiler, settings, 1));
        result.putRawValue("settings", new RawValue(settings.toString()));
        StringBuilder plans = new StringBuilder();
        var planIds = compiler.getPlans(new IndentStream(plans));
        result.putRawValue("calcite_plan", new RawValue(plans.toString()));
        ToJsonOuterVisitor writer = ToJsonOuterVisitor.createForCheckpoint(compiler, planIds);
        writer.apply(circuit);
        if (writer.innerVisitor.serialized.contains(compiler.weightVar.getId()))
            weight.put("node", compiler.weightVar.getId());
        result.putRawValue("circuit", new RawValue(writer.getJsonString()));
        byte[] bytes = mapper().writeValueAsBytes(result);
        checkSize(stage, "export", bytes.length);
        Files.write(path, bytes, StandardOpenOption.CREATE_NEW);
    }

    /** Decode and run the complete optimizer in one new compiler context in an
     * isolated process. Generated names and VARIANT representation remain JVM-global
     * in Java. S4 observations cannot be used as inputs to this method. */
    @Nullable public static DBSPCircuit read(DBSPCompiler compiler, Path path) throws IOException {
        return read(compiler, path, Stage.S2);
    }

    @Nullable public static DBSPCircuit read(DBSPCompiler compiler, Path path, Stage stage) throws IOException {
        identities(compiler.options);

        byte[] bytes;
        try (var file = FileChannel.open(path)) {
            checkSize(stage, "replay", file.size());
            // The checkpoint can grow after the size check.
            bytes = Channels.newInputStream(file).readNBytes(MAX_BYTES + 1);
            checkSize(stage, "replay", bytes.length);
        }
        JsonNode saved = mapper().readTree(bytes);
        if (!saved.path("schema_version").isIntegralNumber() || !saved.path("schema_version").canConvertToInt()
                || saved.path("schema_version").asInt() != 4
                || !saved.path("stage").asText().equals(stage.wireName()))
            throw new IllegalArgumentException("Expected a version-4 " + stage + " subset file, not an observation dump");
        if (!compiler.options.ioOptions.dumpProducerCommit.equals(saved.path("producer_commit").asText())
                || !compiler.options.ioOptions.dumpRuntimeAbi.equals(saved.path("runtime_abi").asText()))
            throw new IllegalArgumentException(stage + " compiler/runtime identity mismatch");
        if (!options(compiler.options).equals(saved.get("options")))
            throw new IllegalArgumentException(stage + " effective compiler options mismatch");
        JsonNode circuit = Utilities.getProperty(saved, "circuit");
        if (!circuit.path("allOperators").isArray())
            throw new IllegalArgumentException(stage + " circuit requires an operator array");
        for (JsonNode operator: circuit.get("allOperators")) {
            if (!operator.path("provenance").isObject())
                throw new IllegalArgumentException(stage + " operator requires provenance");
            String kind = operator.path("class").asText();
            if (!(stage == Stage.S2 ? OPERATORS : S3_OPERATORS).contains(kind))
                throw new IllegalArgumentException(stage + " subset does not support operator " + kind);
        }
        if (!circuit.path("declarations").isArray())
            throw new IllegalArgumentException(stage + " circuit requires a declarations array");
        for (JsonNode declaration: circuit.get("declarations")) {
            String kind = declaration.path("item").path("class").asText();
            if (!declaration.path("class").asText().equals("DBSPDeclaration")
                    || !(kind.equals("DBSPStructItem") || kind.equals("DBSPFunctionItem")
                    || (stage == Stage.S3 && (kind.equals("DBSPStaticItem") || kind.equals("DBSPComparatorItem")))))
                throw new IllegalArgumentException(stage + " subset does not support declaration " + kind);
        }
        JsonNode plans = Utilities.getProperty(saved, "calcite_plan");
        if (!plans.isObject())
            throw new IllegalArgumentException(stage + " requires a Calcite plan object");
        compiler.restorePlans(plans.toPrettyString());
        compiler.sources.restoreSnapshot(Utilities.getProperty(saved, "sources"));
        compiler.messages.restoreSnapshot(Utilities.getProperty(saved, "diagnostics"));
        JsonNode pending = Utilities.getProperty(saved, "unresolved_view_lateness");
        if (!pending.isArray() || (stage == Stage.S3 && !pending.isEmpty()))
            throw new IllegalArgumentException(stage + " requires a valid pending LATENESS check array");
        List<DBSPCompiler.UnresolvedViewLateness> checks = new ArrayList<>();
        for (JsonNode entry: pending)
            checks.add(new DBSPCompiler.UnresolvedViewLateness(
                    Utilities.getStringProperty(entry, "view"),
                    SourcePositionRange.fromJson(
                            Utilities.getProperty(entry, "range"))));
        compiler.restoreUnresolvedViewLateness(checks);
        // Settings have their own inner-node identity space. Restore them before
        // circuit types, because FELDERA_FLAT_VARIANT selects their representation.
        compiler.metadata.readSettings(Utilities.getProperty(saved, "settings"), new JsonDecoder());
        JsonDecoder decoder = new JsonDecoder(compiler.metadata);
        DBSPCircuit decoded = decoder.decodeOuter(circuit, DBSPCircuit.class);
        compiler.weightVar = decoder.decodeWeightVariable(Utilities.getProperty(saved, "compiler_weight_variable"));
        new ValidateStreamKinds(compiler).apply(decoded);
        JsonNode nextName = Utilities.getProperty(saved, "next_generated_name");
        if (!nextName.isIntegralNumber() || !nextName.canConvertToLong())
            throw new IllegalArgumentException(stage + " requires a generated-name counter");
        DBSPVariablePath.restoreGeneratedNames(nextName.longValue());
        if (stage == Stage.S3) {
            if (decoded.incremental != compiler.options.languageOptions.incrementalize)
                throw new IllegalArgumentException("S3 incrementality does not match compiler options");
            return compiler.finishFromS3(decoded);
        }
        DBSPCircuit optimized = compiler.optimizeFromS2(decoded);
        if (compiler.hasErrors())
            return null;
        if (optimized == null)
            throw new IllegalArgumentException("S2 optimizer returned no circuit");
        return optimized;
    }
}
