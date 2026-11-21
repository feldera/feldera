package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.GlobalAggregate;
import org.dbsp.sqlCompiler.circuit.annotation.JoinStrategy;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.IContainsIntegrator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.backend.CircuitReplay;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.PositionOnlyRel;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.MultisetAnalysis;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.TagRegions;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ValidateStreamKinds;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.ShareWindowIntegrals;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.WindowGroupsReplay;
import org.dbsp.sqlCompiler.compiler.visitors.outer.StrayGC;
import org.dbsp.sqlCompiler.compiler.visitors.outer.PushDifferentialsUp;
import org.dbsp.sqlCompiler.compiler.visitors.outer.BalancedJoins;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CloneOperatorsWithFanout;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LinearPostprocessRetainKeys;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ChainVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.EnsureDistinctOutputs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ImplementJoins;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeDistinctVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeIncrementalVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MonotoneAnalyzer;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DeltaExpandOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveTable;
import org.dbsp.sqlCompiler.compiler.visitors.outer.MergeSums;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.PropagateConstants;
import org.dbsp.sqlCompiler.compiler.visitors.outer.PropagateEmptySources;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveConstantFilters;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveDeindexOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveIAfterD;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveNoops;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveIdentityOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.compiler.visitors.outer.InsertWeightValidation;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveViewOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.dbsp.sqlCompiler.compiler.visitors.outer.UnboundedStateReplay;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Independent Java facts for the disposable P0 Rust multiset-analysis slice.
 * Each record contains the unmodified circuit and facts in operator/port order.
 * S3 records also carry a state-complete subset file for a Rust rewrite followed
 * by the Java tail. This does not execute the generated circuit. */
public final class MultisetProbe {
    private static final ObjectMapper MAPPER = Utilities.deterministicObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private MultisetProbe() { }

    static CompilerOptions options(String stage, String mode) {
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.inputCircuit = stage.equals("S2");
        options.ioOptions.gen2 = mode.equals("gen2");
        options.ioOptions.nowStream = !Boolean.getBoolean("feldera.probe.systemClock");
        options.languageOptions.incrementalize = !Boolean.getBoolean("feldera.probe.snapshot");
        options.languageOptions.throwOnError = !Boolean.getBoolean("feldera.probe.collectErrors");
        options.ioOptions.dumpProducerCommit = System.getProperty("feldera.probe.producer", "");
        options.ioOptions.dumpRuntimeAbi = System.getProperty("feldera.probe.runtime", "");
        return options;
    }

    private static CompilerOptions options(String stage, String[] args) {
        CompilerOptions options = options(stage, args[3]);
        if (args.length == 5) {
            options.languageOptions.outputsAreSets = true;
        }
        return options;
    }

    private static ObjectNode hashInputs(DBSPCompiler compiler) {
        ObjectNode hashes = MAPPER.createObjectNode();
        compiler.hashInputHook = (kind, value) -> hashes.withArray(kind).add(value);
        return hashes;
    }

    static JsonNode circuitJson(DBSPCompiler compiler, DBSPCircuit circuit) throws Exception {
        ToJsonOuterVisitor writer = ToJsonOuterVisitor.create(compiler, 1);
        writer.apply(circuit);
        return MAPPER.readTree(writer.getJsonString());
    }

    private static void describe(ObjectNode result, DBSPCompiler compiler, DBSPCircuit circuit,
                                 ObjectNode hashes, Path artifacts) throws Exception {
        result.set("circuit", circuitJson(compiler, circuit));
        result.set("schema", compiler.metadata.asJson());
        result.set("diagnostics", compiler.messages.toJson(compiler.sources));
        result.set("hash_inputs", hashes);
        if (compiler.options.ioOptions.inputCircuit)
            return;
        if (compiler.options.ioOptions.gen2) {
            circuit.setName("circuit");
            var bytes = new ByteArrayOutputStream();
            try (var stream = new PrintStream(bytes, false, StandardCharsets.UTF_8)) {
                compiler.getDataflow(stream, circuit);
            }
            JsonNode dataflow = MAPPER.readTree(bytes.toByteArray());
            result.put("dataflow_sha256", DigestUtils.sha256Hex(dataflow.toString()));
        } else {
            result.set("rust", CorpusDeterminism.rust(compiler, circuit, artifacts));
        }
    }

    private static void writeReplayCircuit(DBSPCompiler compiler, DBSPCircuit circuit, Path path)
            throws IOException {
        Files.writeString(path, replayCircuitText(compiler, circuit), StandardOpenOption.CREATE_NEW);
    }

    private static String replayCircuitText(DBSPCompiler compiler, DBSPCircuit circuit) {
        var writer = ToJsonOuterVisitor.createForReplay(compiler);
        writer.apply(circuit);
        return writer.getJsonString();
    }

    private static ObjectNode nameAwareReplayCircuit(DBSPCompiler compiler, DBSPCircuit circuit) throws IOException {
        ObjectNode wire = (ObjectNode) MAPPER.readTree(replayCircuitText(compiler, circuit));
        return wire.put("next_generated_name", DBSPVariablePath.nextGeneratedName());
    }

    /** The Java control runs the same codec with Java's scheduled transform. */
    private record ScheduledReplay(String runner, Set<String> passNames, RustWorker worker)
            implements AutoCloseable {
        private static final Set<String> REPEATED_PASSES =
                Set.of("PropagateEmptySources", "RemoveOperators", "RemoveViewOperators",
                        "ValidateRecursiveOperators", "OptimizeIncrementalVisitor");
        private static final Set<String> MONOTONE_PASSES =
                Set.of("EnsureExpressionTrees", "SeparateIntegrators", "OrderErrorConsumers",
                        "DeltaExpandOperators", "Monotonicity", "InsertLimiters", "MergeGC", "CheckRetain");
        private static final Set<String> INACTIVE_REPLAY_PASSES =
                Set.of("RecursiveComponents", "LeftJoinChains", "RemoveUselessLeftJoins",
                        "RemoveStarJoins", "ExpandHop", "LowerAsof", "ExpandAggregates",
                        "UDFInliner", "Lineage", "LowerCircuitVisitor", "ShareIndexes",
                        "ShareOutputIntegrators", "ComparatorDeclarations",
                        "OptimizeProjectionVisitor", "DecomposeExpensiveFilters", "FuseExpensiveMaps",
                        "SimplifyConditionals", "SimplifyWaterline");
        private static final Set<String> NUMBERED_GUARDED_PASSES =
                Set.of("FilterJoinVisitor", "CSE", "FuseIntoArrayAgg");
        private static final Set<String> DIAGNOSTIC_PASSES =
                Set.of("ConstantViews", "DeterministicFunctions", "CheckHints");
        private static final List<String> EARLY_SET_PASSES = List.of(
                "EnsureDistinctOutputs", "PropagateConstants", "PropagateEmptySources", "MergeSums");
        private static final Set<String> STREAM_GROUP_REQUESTS = Set.of(
                "RemoveOperators#3", "PropagateEmptySources#2", "OptimizeDistinctVisitor",
                "OptimizeIncrementalVisitor#1", "RemoveOperators#4", "IncrementalizeVisitor",
                "OptimizeIncrementalVisitor#2", "RemoveIAfterD", "RemoveOperators#5");

        ScheduledReplay(String runner, Set<String> passNames) {
            this(runner, passNames, !runner.equals("java") && Boolean.getBoolean("feldera.probe.reuseRustWorker")
                    ? new RustWorker(runner) : null);
        }

        @Override
        public void close() throws IOException {
            if (this.worker != null)
                this.worker.close();
        }

        static ScheduledReplay configured(String stage) {
            String runner = System.getProperty("feldera.probe.scheduledRunner");
            String passes = System.getProperty("feldera.probe.scheduledPasses");
            if (runner == null && passes == null)
                return null;
            if (!(stage.equals("S3") || stage.equals("resume-s2"))
                    || runner == null || runner.isBlank() || passes == null)
                throw new IllegalArgumentException("Scheduled replay needs an optimizer run, a runner, and pass names");
            Set<String> requested = Set.of(passes.split(",", -1));
            if (!Set.of("EnsureDistinctOutputs", "MergeSums", "PropagateEmptySources#1",
                    "PropagateEmptySources#2", "PropagateEmptySources#3",
                    "RemoveOperators#1", "RemoveOperators#2", "RemoveOperators#3",
                    "RemoveOperators#4", "RemoveOperators#5",
                    "RemoveViewOperators#1", "RemoveViewOperators#2", "RemoveIAfterD",
                    "RemoveConstantFilters", "PropagateConstants", "RemoveDeindexOperators",
                    "RemoveNoops#1", "RemoveNoops#2", "RemoveTable", "ConstantViews",
                    "CompactNames", "NoIntegralVisitor", "InsertWeightValidation",
                    "RemoveIdentityOperators", "ValidateRecursiveOperators#1",
                    "ValidateRecursiveOperators#2", "NoConnectorMetadata", "DeterministicFunctions", "CheckHints",
                    "TagRegions", "StrayGC", "PushDifferentialsUp", "BalancedJoins",
                    "LinearPostprocessRetainKeys", "CloneOperatorsWithFanout", "StopOnError", "ImplementJoins",
                    "RemoveNowTable", "IncrementalizeVisitor", "OptimizeIncrementalVisitor#1",
                    "OptimizeIncrementalVisitor#2", "OrderErrorConsumers", "SeparateIntegrators",
                    "EnsureExpressionTrees", "DeltaExpandOperators", "Monotonicity", "InsertLimiters", "MergeGC", "CheckRetain",
                    "OptimizeDistinctVisitor", "EarlySetOptimization", "ExpandAggregateZeros",
                    "StreamOptimization", "ShareWindowIntegrals", "ChainVisitor#1", "ChainVisitor#2",
                    "AdjustSqlIndex", "CanonicalForm", "ImplementChains", "ExpandCasts", "CreateRuntimeErrorWrappers", "PullFilterVisitor", "ExpandIndexedInputs", "ImplementNow", "InnerCSE",
                            "OptimizeProjections#1", "OptimizeProjections#2", "OptimizeProjections#3",
                            "Simplify#1", "Simplify#2", "Simplify#3", "Simplify#4",
                            "Gen2Passes", "ImplementStatics", "CreateStarJoins", "UnusedFields", "FindUnboundedState",
                            "DeadCode#1", "DeadCode#2", "DeadCode#3", "DeadCode#4", "DeadCode#5",
                            "RecursiveComponents", "LeftJoinChains", "RemoveUselessLeftJoins",
                            "RemoveStarJoins", "ExpandHop", "LowerAsof", "ExpandAggregates",
                            "UDFInliner", "Lineage", "LowerCircuitVisitor", "FilterJoinVisitor#1",
                            "FilterJoinVisitor#2", "CSE#1", "CSE#2", "CSE#3", "CSE#4",
                            "FuseIntoArrayAgg#1", "FuseIntoArrayAgg#2", "ShareIndexes",
                            "ShareOutputIntegrators", "ComparatorDeclarations", "OptimizeProjectionVisitor",
                            "DecomposeExpensiveFilters", "FuseExpensiveMaps", "SimplifyConditionals",
                            "SimplifyWaterline")
                    .containsAll(requested))
                throw new IllegalArgumentException("Unknown scheduled replay pass: " + passes);
            if (requested.contains("EarlySetOptimization") && requested.stream().anyMatch(
                    pass -> EARLY_SET_PASSES.contains(pass) || pass.equals("PropagateEmptySources#1")))
                throw new IllegalArgumentException("EarlySetOptimization already includes its four constituent passes");
            if (requested.contains("ImplementNow") && requested.contains("RemoveNowTable"))
                throw new IllegalArgumentException("ImplementNow already includes RemoveNowTable");
            if (requested.contains("Monotonicity") && !requested.contains("InsertLimiters"))
                throw new IllegalArgumentException("Monotonicity replay requires the Rust limiter consumer");
            if (requested.contains("RecursiveComponents") && requested.contains("ValidateRecursiveOperators#1"))
                throw new IllegalArgumentException("RecursiveComponents already includes the first recursion check");
            if (requested.contains("RemoveUselessLeftJoins")
                    && requested.stream().anyMatch(pass -> pass.startsWith("RemoveOperators#")))
                throw new IllegalArgumentException("RemoveUselessLeftJoins overlaps nested dead-code removal");
            if (requested.stream().anyMatch(pass -> pass.startsWith("DeadCode#"))
                    && (requested.stream().anyMatch(pass -> pass.startsWith("RemoveOperators#"))
                    || requested.contains("StreamOptimization")))
                throw new IllegalArgumentException("DeadCode overlaps nested removal and stream optimization");
            if (requested.contains("StreamOptimization") && requested.stream().anyMatch(STREAM_GROUP_REQUESTS::contains))
                throw new IllegalArgumentException("StreamOptimization already includes the requested stream passes");
            return new ScheduledReplay(runner, requested);
        }

        void install(DBSPCompiler compiler, Path directory) {
            if (this.passNames.contains("RemoveOperators#1")
                    && compiler.options.languageOptions.generateInputForEveryTable)
                throw new IllegalArgumentException("The first RemoveOperators replay prunes unused inputs");
            if ((this.passNames.contains("RemoveTable") || this.passNames.contains("OrderErrorConsumers"))
                    && !DBSPCompiler.ERROR_TABLE_NAME.name().equals("feldera_error_table"))
                throw new IllegalArgumentException("Error-table replay needs default identifier casing");
            if ((this.passNames.contains("RemoveNowTable") || this.passNames.contains("ImplementNow"))
                    && !DBSPCompiler.NOW_TABLE_NAME.name().equals("now"))
                throw new IllegalArgumentException("Clock-table replay needs default identifier casing");
            if (this.passNames.contains("ConstantViews") && !DBSPCompiler.ERROR_VIEW_NAME.name()
                    .equals("error_view"))
                throw new IllegalArgumentException("Constant-view replay needs default identifier casing");
            compiler.optimizerHook = optimizer -> {
                Set<String> remaining = new HashSet<>(this.passNames);
                this.replace(optimizer, compiler, directory, remaining, new HashMap<>());
                if (!remaining.isEmpty())
                    throw new IllegalStateException("Scheduled replay passes not found: " + remaining);
            };
        }

        private void replace(Passes group, DBSPCompiler compiler, Path directory, Set<String> remaining,
                             Map<String, Integer> occurrences) {
            for (int i = 0; i < group.passes.size(); i++) {
                CircuitTransform original = group.passes.get(i);
                if (original.getName().equals(EARLY_SET_PASSES.getFirst())
                        && remaining.remove("EarlySetOptimization")) {
                    this.replaceConsecutive(group, i, compiler, directory, "EarlySetOptimization", EARLY_SET_PASSES);
                    occurrences.merge("PropagateEmptySources", 1, Integer::sum);
                    continue;
                }
                if (group.getName().equals("Optimizer") && original instanceof DeadCode
                        && i + 1 < group.passes.size()
                        && group.passes.get(i + 1) instanceof PropagateEmptySources
                        && remaining.remove("StreamOptimization")) {
                    this.replaceStreamPasses(group, i, compiler, directory);
                    occurrences.merge("RemoveOperators", 3, Integer::sum);
                    occurrences.merge("PropagateEmptySources", 1, Integer::sum);
                    occurrences.merge("OptimizeIncrementalVisitor", 2, Integer::sum);
                    continue;
                }
                if (original instanceof MonotoneAnalyzer) {
                    Set<String> enabled = new HashSet<>(MONOTONE_PASSES);
                    enabled.retainAll(remaining);
                    if (!enabled.isEmpty()) {
                        remaining.removeAll(enabled);
                        group.passes.set(i, this.replayMonotoneAnalyzer(compiler, directory, enabled));
                        continue;
                    }
                }
                if (group.getName().equals("Optimizer") && original instanceof DeadCode) {
                    String request = "DeadCode#" + occurrences.merge("DeadCode", 1, Integer::sum);
                    if (remaining.remove(request)) {
                        group.passes.set(i, this.replacement(
                                compiler, original, directory.resolve(request), request, false));
                        continue;
                    }
                }
                if (original instanceof ShareWindowIntegrals && remaining.remove("ShareWindowIntegrals")) {
                    String name = "ShareWindowIntegrals";
                    group.passes.set(i, this.replacement(compiler, original, directory.resolve(name), name, false));
                    continue;
                }
                String transformName = original.getName();
                if (NUMBERED_GUARDED_PASSES.contains(transformName)) {
                    String name = transformName + "#" + occurrences.merge(transformName, 1, Integer::sum);
                    if (remaining.remove(name)) {
                        group.passes.set(i, this.replacement(
                                compiler, original, directory.resolve(name), name, false));
                        continue;
                    }
                }
                if (INACTIVE_REPLAY_PASSES.contains(transformName) && remaining.remove(transformName)) {
                    if (transformName.equals("RecursiveComponents"))
                        occurrences.merge("ValidateRecursiveOperators", 1, Integer::sum);
                    group.passes.set(i, this.replacement(
                            compiler, original, directory.resolve(transformName), transformName, false));
                    continue;
                }
                if (Set.of("OptimizeProjections", "Simplify").contains(transformName)) {
                    String request = transformName + "#" + occurrences.merge(transformName, 1, Integer::sum);
                    if (remaining.remove(request))
                        group.passes.set(i, this.replacement(
                                compiler, original, directory.resolve(request), request, false));
                    continue;
                }
                if (Set.of("AdjustSqlIndex", "CanonicalForm", "ExpandCasts", "CreateRuntimeErrorWrappers",
                        "PullFilterVisitor", "ExpandIndexedInputs", "ImplementNow", "InnerCSE",
                        "Gen2Passes", "ImplementStatics", "CreateStarJoins", "UnusedFields", "FindUnboundedState")
                        .contains(transformName)
                        && remaining.remove(transformName)) {
                    group.passes.set(i, this.replacement(
                            compiler, original, directory.resolve(transformName), transformName, false));
                    continue;
                }
                if (original instanceof RemoveIdentityOperators || original instanceof TagRegions) {
                    String name = original.getClass().getSimpleName();
                    if (remaining.remove(name))
                        group.passes.set(i, this.replacement(compiler, original, directory.resolve(name), name, false));
                    continue;
                }
                if (original instanceof Conditional conditional
                        && conditional.transform instanceof InsertWeightValidation
                        && remaining.remove("InsertWeightValidation")) {
                    String name = "InsertWeightValidation";
                    group.passes.set(i, new Conditional(compiler, this.replacement(compiler,
                            conditional.transform, directory.resolve(name), name, false),
                            compiler.metadata::enforcePositiveInputs));
                    continue;
                }
                if (group.getName().equals("ImplementNow")
                        && original instanceof Conditional conditional
                        && conditional.transform instanceof RemoveTable
                        && remaining.remove("RemoveNowTable")) {
                    String name = "RemoveNowTable";
                    group.passes.set(i, conditional.withTransform(this.replacement(compiler,
                            conditional.transform, directory.resolve(name), name, false)));
                    continue;
                }
                if (original instanceof Repeat repeat && repeat.transform instanceof Passes body) {
                    if (repeat.getName().equals("RemoveNoops")) {
                        String request = "RemoveNoops#" + occurrences.merge("RemoveNoops", 1, Integer::sum);
                        if (remaining.remove(request))
                            this.replaceRepeated(body, compiler, directory.resolve(request), RemoveNoops.class);
                        continue;
                    }
                    if (repeat.getName().equals("ChainVisitor")) {
                        String request = "ChainVisitor#" + occurrences.merge("ChainVisitor", 1, Integer::sum);
                        if (remaining.remove(request))
                            this.replaceRepeated(body, compiler, directory.resolve(request), ChainVisitor.class);
                        continue;
                    }
                    Optional<Class<? extends CircuitTransform>> checker = switch (repeat.getName()) {
                        case "StrayGC" -> Optional.of(StrayGC.class);
                        case "PushDifferentialsUp" -> Optional.of(PushDifferentialsUp.class);
                        case "BalancedJoins" -> Optional.of(BalancedJoins.class);
                        case "CloneOperatorsWithFanout" -> Optional.of(CloneOperatorsWithFanout.class);
                        default -> Optional.empty();
                    };
                    if (checker.isPresent()) {
                        String name = repeat.getName();
                        if (remaining.remove(name))
                            this.replaceRepeated(body, compiler, directory.resolve(name), checker.get());
                        continue;
                    }
                }
                if (original instanceof Passes nested) {
                    this.replace(nested, compiler, directory, remaining, occurrences);
                    continue;
                }
                String name = "NoConnectorMetadata".equals(original.getName())
                        ? original.getName() : original.getClass().getSimpleName();
                if (name.equals("RemoveOperators") && !(group instanceof DeadCode))
                    continue;
                String request = REPEATED_PASSES.contains(name)
                        ? name + "#" + occurrences.merge(name, 1, Integer::sum)
                        : name;
                if (!this.passNames.contains(request))
                    continue;
                if (!remaining.remove(request))
                    throw new IllegalStateException("More than one scheduled " + request + " pass");
                String rustPass = switch (request) {
                    case "RemoveTable" -> "RemoveErrorTable";
                    case "RemoveViewOperators#2" -> "RemoveAllViewOperators";
                    case "RemoveOperators#2", "RemoveOperators#3", "RemoveOperators#4", "RemoveOperators#5" ->
                            "RemoveOperatorsKeepSources";
                    default -> name;
                };
                group.passes.set(i, this.replacement(
                        compiler, original, directory.resolve(request), rustPass, false));
            }
        }

        private void replaceStreamPasses(Passes group, int start, DBSPCompiler compiler, Path directory) {
            var passes = new ArrayList<>(List.of("DeadCode", "PropagateEmptySources", "OptimizeDistinctVisitor",
                    "OptimizeIncrementalVisitor", "DeadCode"));
            boolean incremental = compiler.options.languageOptions.incrementalize;
            if (incremental)
                passes.add("IncrementalizeVisitor");
            passes.addAll(List.of("OptimizeIncrementalVisitor", "RemoveIAfterD", "DeadCode"));
            String name = incremental ? "IncrementalStreamOptimization" : "SnapshotStreamOptimization";
            this.replaceConsecutive(group, start, compiler, directory, name, passes);
        }

        private void replaceConsecutive(Passes group, int start, DBSPCompiler compiler, Path directory,
                                        String name, List<String> passes) {
            if (start + passes.size() > group.passes.size())
                throw new IllegalStateException(name + " is missing a constituent pass");
            var original = group.passes.subList(start, start + passes.size());
            var actual = original.stream().map(CircuitTransform::getName).toList();
            if (!actual.equals(passes))
                throw new IllegalStateException(name + " expects " + passes + " consecutively, found " + actual);
            var control = new Passes(name, compiler, List.copyOf(original)) {
                @Override
                protected void afterPass(DBSPCircuit circuit) {
                    new ValidateStreamKinds(compiler).apply(circuit);
                }
            };
            original.clear();
            group.passes.add(start, this.replacement(compiler, control, directory.resolve(name), name, false));
        }

        private MonotoneAnalyzer replayMonotoneAnalyzer(DBSPCompiler compiler, Path directory, Set<String> enabled) {
            return new MonotoneAnalyzer(compiler) {
                private CircuitTransform replayIfSelected(String name, CircuitTransform original) {
                    return enabled.contains(name)
                            ? replacement(compiler, original, directory.resolve(name), name, false)
                            : original;
                }

                @Override
                protected CircuitTransform ensureExpressionTrees() {
                    return replayIfSelected("EnsureExpressionTrees", super.ensureExpressionTrees());
                }

                @Override
                protected CircuitTransform separateIntegrators(CircuitGraphs graphs) {
                    return replayIfSelected("SeparateIntegrators", super.separateIntegrators(graphs));
                }

                @Override
                protected CircuitTransform checkRetain(CircuitGraphs graphs) {
                    return replayIfSelected("CheckRetain", super.checkRetain(graphs));
                }

                @Override
                protected CircuitTransform mergeGC() {
                    return replayIfSelected("MergeGC", super.mergeGC());
                }

                @Override
                protected CircuitTransform insertLimiters(CircuitTransform original) {
                    return replayIfSelected("InsertLimiters", original);
                }

                @Override
                protected CircuitTransform analyzeMonotonicity(CircuitTransform original, DBSPCircuit originalCircuit) {
                    if (!enabled.contains("Monotonicity"))
                        return original;
                    return new CircuitTransform() {
                        @Override
                        public String getName() {
                            return original.getName();
                        }

                        @Override
                        public DBSPCircuit apply(DBSPCircuit expanded) {
                            String name = "Monotonicity";
                            Path artifacts = directory.resolve(name);
                            try {
                                Files.createDirectories(artifacts);
                                Path input = artifacts.resolve("original.json");
                                ObjectNode before = nameAwareReplayCircuit(compiler, originalCircuit);
                                Files.writeString(input, before.toString(), StandardOpenOption.CREATE_NEW);
                                Files.writeString(artifacts.resolve("before.json"),
                                        nameAwareReplayCircuit(compiler, expanded).toString(), StandardOpenOption.CREATE_NEW);
                                DBSPCircuit result = expanded;
                                if (runner.equals("java")) {
                                    result = original.apply(expanded);
                                } else {
                                    Path report = artifacts.resolve("limiter-analysis.json");
                                    runRust(name, input, report, artifacts.resolve("rust.log"));
                                    JsonNode facts = readCircuit(report);
                                    if (facts.isNull()) {
                                        result = original.apply(expanded);
                                    } else {
                                        JsonNode next = facts.required("next_generated_name");
                                        if (!next.isIntegralNumber() || !next.canConvertToLong()
                                                || next.longValue() < before.required("next_generated_name").longValue())
                                            throw new IllegalArgumentException("Invalid monotonicity name counter");
                                        DBSPVariablePath.restoreGeneratedNames(next.longValue());
                                    }
                                }
                                Files.writeString(artifacts.resolve("after.json"),
                                        nameAwareReplayCircuit(compiler, result).toString(), StandardOpenOption.CREATE_NEW);
                                return result;
                            } catch (IOException error) {
                                throw new UncheckedIOException("Monotonicity replay", error);
                            }
                        }
                    };
                }

                @Override
                protected DeltaExpandOperators deltaExpander() {
                    if (!enabled.contains("DeltaExpandOperators"))
                        return super.deltaExpander();
                    return new DeltaExpandOperators(compiler) {
                        @Override
                        public DBSPCircuit apply(DBSPCircuit circuit) {
                            try {
                                String name = "DeltaExpandOperators";
                                Path artifacts = directory.resolve(name);
                                Files.createDirectories(artifacts);
                                Path input = artifacts.resolve("before.json");
                                Path output = artifacts.resolve("after.json");
                                ObjectNode before = nameAwareReplayCircuit(compiler, circuit);
                                Files.writeString(input, before.toString(), StandardOpenOption.CREATE_NEW);
                                if (runner.equals("java")) {
                                    DBSPCircuit expanded = super.apply(circuit);
                                    Files.writeString(output, nameAwareReplayCircuit(compiler, expanded).toString(),
                                            StandardOpenOption.CREATE_NEW);
                                    return expanded;
                                }
                                Path resultPath = artifacts.resolve("delta-expansion.json");
                                runRust(name, input, resultPath, artifacts.resolve("rust.log"));
                                JsonNode result = readCircuit(resultPath);
                                DBSPCircuit expanded = DeltaExpansionReplay.restore(circuit, before, result, this.expansion);
                                JsonNode after = result.required("circuit");
                                JsonNode next = after.required("next_generated_name");
                                if (!next.isIntegralNumber() || !next.canConvertToLong()
                                        || next.longValue() < before.required("next_generated_name").longValue())
                                    throw new IllegalArgumentException("Invalid delta expansion name counter");
                                DBSPVariablePath.restoreGeneratedNames(next.longValue());
                                Files.writeString(output, after.toString(), StandardOpenOption.CREATE_NEW);
                                return expanded;
                            } catch (IOException error) {
                                throw new UncheckedIOException("Scheduled delta expansion", error);
                            }
                        }
                    };
                }

                @Override
                protected Set<DBSPOperator> reachableFromError(DBSPCircuit circuit, CircuitGraph graph) {
                    if (!enabled.contains("OrderErrorConsumers"))
                        return super.reachableFromError(circuit, graph);
                    try {
                        Path artifacts = directory.resolve("OrderErrorConsumers");
                        Files.createDirectories(artifacts);
                        Path input = artifacts.resolve("before.json");
                        Path selection = artifacts.resolve("selection.json");
                        writeReplayCircuit(compiler, circuit, input);
                        if (runner.equals("java")) {
                            Set<DBSPOperator> operators = super.reachableFromError(circuit, graph);
                            var ids = circuit.allOperators.stream().filter(operators::contains)
                                    .map(DBSPOperator::getId).toList();
                            Files.writeString(selection, MAPPER.writeValueAsString(ids), StandardOpenOption.CREATE_NEW);
                            return operators;
                        }

                        runRust("OrderErrorConsumers", input, selection, artifacts.resolve("rust.log"));
                        JsonNode report = readCircuit(selection);
                        if (!report.isArray())
                            throw new IllegalArgumentException("Expected error-consumer operator IDs");
                        Set<Long> ids = new HashSet<>();
                        for (JsonNode id : report)
                            if (!ids.add(operatorId(id)))
                                throw new IllegalArgumentException("Duplicate error-consumer operator ID");
                        Set<DBSPOperator> operators = new HashSet<>();
                        for (DBSPOperator operator : circuit.allOperators)
                            if (ids.remove(operator.getId()))
                                operators.add(operator);
                        if (!ids.isEmpty())
                            throw new IllegalArgumentException("Unknown error-consumer operators " + ids);
                        return operators;
                    } catch (IOException error) {
                        throw new UncheckedIOException("Scheduled error-consumer selection", error);
                    }
                }
            };
        }

        private void replaceRepeated(Passes body, DBSPCompiler compiler, Path directory,
                                     Class<? extends CircuitTransform> pass) {
            for (int i = 0; i < body.passes.size(); i++) {
                CircuitTransform original = body.passes.get(i);
                if (pass.isInstance(original)) {
                    body.passes.set(i, this.replacement(compiler, original, directory, pass.getSimpleName(), true));
                    return;
                }
            }
            throw new IllegalStateException("Repeat lost its " + pass.getSimpleName() + " transform");
        }

        private CircuitTransform replacement(DBSPCompiler compiler, CircuitTransform original, Path directory,
                                             String rustPass, boolean repeated) {
            return new CircuitTransform() {
                private int iteration;

                @Override
                public String getName() {
                    return original.getName();
                }

                @Override
                public DBSPCircuit apply(DBSPCircuit circuit) {
                    try {
                        Path artifacts = repeated ? directory.resolve(Integer.toString(this.iteration++)) : directory;
                        return replay(compiler, circuit, original, artifacts, rustPass, repeated);
                    } catch (IOException error) {
                        throw new UncheckedIOException("Scheduled " + rustPass + " replay", error);
                    }
                }
            };
        }

        private DBSPCircuit replay(DBSPCompiler compiler, DBSPCircuit circuit,
                                   CircuitTransform original, Path directory, String name, boolean repeated)
                throws IOException {
            Files.createDirectories(directory);
            Path input = directory.resolve("before.json");
            Path output = directory.resolve("after.json");
            if (name.equals("CheckRetain")) {
                writeReplayCircuit(compiler, circuit, input);
                if (this.runner.equals("java")) {
                    original.apply(circuit);
                } else {
                    Path report = directory.resolve("duplicate-retention.json");
                    this.runRust(name, input, report, directory.resolve("rust.log"));
                    JsonNode duplicate = readCircuit(report);
                    if (!duplicate.isNull()) {
                        var first = findReportedOperator(circuit, duplicate.required("operator"));
                        var second = findReportedOperator(circuit, duplicate.required("other"));
                        throw new InternalCompilerError("Operator " + first.inputs.get(0)
                                + " has two " + duplicate.required("dimension").asText()
                                + " retention policies: " + first + " and " + second);
                    }
                }
                writeReplayCircuit(compiler, circuit, output);
                return circuit;
            }
            if (name.equals("FindUnboundedState")) {
                writeReplayCircuit(compiler, circuit, input);
                Files.writeString(directory.resolve("diagnostics-before.json"),
                        compiler.messages.toJson(compiler.sources).toString(), StandardOpenOption.CREATE_NEW);
                DBSPCircuit result;
                if (this.runner.equals("java")) {
                    result = original.apply(circuit);
                } else {
                    Path findings = directory.resolve("unbounded-state-findings.json");
                    this.runRust(name, input, findings, directory.resolve("rust.log"));
                    result = new UnboundedStateReplay(compiler).report(circuit, readCircuit(findings));
                }
                Files.writeString(directory.resolve("diagnostics.json"),
                        compiler.messages.toJson(compiler.sources).toString(), StandardOpenOption.CREATE_NEW);
                writeReplayCircuit(compiler, result, output);
                return result;
            }
            int passNumber = name.indexOf('#');
            String numberedPass = passNumber >= 0 ? name.substring(0, passNumber) : "";
            boolean nameAware = name.equals("RemoveIdentityOperators") || name.equals("RemoveNoops")
                    || name.equals("PushDifferentialsUp") || name.equals("ImplementJoins")
                    || name.equals("PropagateEmptySources") || name.equals("ChainVisitor")
                    || name.equals("CanonicalForm") || name.equals("ImplementChains") || name.equals("ExpandCasts")
                    || name.startsWith("OptimizeProjections#") || name.startsWith("Simplify#")
                    || Set.of("Gen2Passes", "ImplementStatics", "CreateStarJoins", "UnusedFields").contains(name)
                    || INACTIVE_REPLAY_PASSES.contains(name)
                    || NUMBERED_GUARDED_PASSES.contains(numberedPass)
                    || name.equals("CreateRuntimeErrorWrappers") || name.equals("PullFilterVisitor")
                    || name.equals("ExpandIndexedInputs") || name.equals("ImplementNow")
                    || name.equals("IncrementalizeVisitor") || name.equals("OptimizeIncrementalVisitor")
                    || name.equals("InsertLimiters")
                    || name.equals("SeparateIntegrators") || name.equals("OptimizeDistinctVisitor")
                    || name.equals("EarlySetOptimization") || name.equals("ExpandAggregateZeros")
                    || name.equals("IncrementalStreamOptimization") || name.equals("SnapshotStreamOptimization");
            if (nameAware)
                Files.writeString(input, nameAwareReplayCircuit(compiler, circuit).toString(),
                        StandardOpenOption.CREATE_NEW);
            else
                writeReplayCircuit(compiler, circuit, input);
            if (name.equals("ShareWindowIntegrals")) {
                CircuitTransform sharing = original;
                if (!this.runner.equals("java")) {
                    sharing = new ShareWindowIntegrals(compiler, new WindowGroupsReplay(compiler, request -> {
                        Path candidates = directory.resolve("window-candidates.json");
                        Path groups = directory.resolve("window-groups.json");
                        try {
                            Files.writeString(candidates, request.toString(), StandardOpenOption.CREATE_NEW);
                            this.runRust("GroupWindows", candidates, groups, directory.resolve("rust.log"));
                            return readCircuit(groups);
                        } catch (IOException error) {
                            throw new UncheckedIOException("Window grouping replay", error);
                        }
                    }));
                }
                DBSPCircuit transformed = sharing.apply(circuit);
                writeReplayCircuit(compiler, transformed, output);
                return transformed;
            }
            if (name.equals("StopOnError")) {
                Path report = directory.resolve("stop-on-error.json");
                if (this.runner.equals("java")) {
                    try {
                        original.apply(circuit);
                    } catch (CompilationError error) {
                        Files.writeString(report, "true", StandardOpenOption.CREATE_NEW);
                        throw error;
                    }
                    Files.writeString(report, "false", StandardOpenOption.CREATE_NEW);
                } else {
                    Path status = directory.resolve("compiler-status.json");
                    var request = MAPPER.createObjectNode().put("exit_code", compiler.messages.exitCode);
                    Files.writeString(status, request.toString(), StandardOpenOption.CREATE_NEW);
                    this.runRust(name, status, report, directory.resolve("rust.log"));
                    JsonNode stop = readCircuit(report);
                    if (!stop.isBoolean())
                        throw new IllegalArgumentException("Invalid Rust optimizer stop decision");
                    if (stop.booleanValue())
                        throw new CompilationError("Stopping compilation");
                }
                writeReplayCircuit(compiler, circuit, output);
                return circuit;
            }
            if (name.equals("StrayGC")) {
                Path request = directory.resolve("trace-retention-input.json");
                var wire = MAPPER.createObjectNode();
                wire.set("circuit", readCircuit(input));
                collectTraceCapabilities(circuit.allOperators, wire.putArray("operator_capabilities"));
                Files.writeString(request, wire.toString(), StandardOpenOption.CREATE_NEW);
                if (this.runner.equals("java")) {
                    original.apply(circuit);
                } else {
                    Path report = directory.resolve("trace-retention-violation.json");
                    this.runRust(name, request, report, directory.resolve("rust.log"));
                    rejectTraceViolation(circuit, readCircuit(report));
                }
                writeReplayCircuit(compiler, circuit, output);
                return circuit;
            }
            if (name.equals("BalancedJoins") || name.equals("CloneOperatorsWithFanout")) {
                DBSPCircuit transformed;
                if (this.runner.equals("java")) {
                    transformed = original.apply(circuit);
                } else {
                    Path selected = directory.resolve(name.equals("BalancedJoins")
                            ? "balanced-joins.json" : "fanout-clones.json");
                    this.runRust(name, input, selected, directory.resolve("rust.log"));
                    transformed = applySelectedRewrite(compiler, circuit, readCircuit(selected), name);
                }
                writeReplayCircuit(compiler, transformed, output);
                return transformed;
            }
            if (name.equals("LinearPostprocessRetainKeys")) {
                Path request = directory.resolve("linear-retention-input.json");
                var wire = MAPPER.createObjectNode();
                wire.set("circuit", readCircuit(input));
                ArrayNode inputs = wire.putArray("input_order");
                circuit.sourceOperators.values().forEach(source -> inputs.add(source.asOperator().getId()));
                ArrayNode outputs = wire.putArray("output_order");
                circuit.sinkOperators.values().forEach(sink -> outputs.add(sink.getId()));
                Files.writeString(request, wire.toString(), StandardOpenOption.CREATE_NEW);
                DBSPCircuit transformed;
                if (this.runner.equals("java")) {
                    transformed = original.apply(circuit);
                } else {
                    Path report = directory.resolve("linear-retention-plan.json");
                    this.runRust(name, request, report, directory.resolve("rust.log"));
                    transformed = new LinearRetentionReplay(compiler, readCircuit(report)).apply(circuit);
                }
                writeReplayCircuit(compiler, transformed, output);
                return transformed;
            }
            boolean reportsDiagnostics = DIAGNOSTIC_PASSES.contains(name);
            if (reportsDiagnostics || name.equals("ValidateRecursiveOperators")
                    || name.equals("NoConnectorMetadata") || name.equals("TagRegions")
                    || name.equals("NoIntegralVisitor")) {
                if (this.runner.equals("java")) {
                    original.apply(circuit);
                } else {
                    Path report = directory.resolve(switch (name) {
                        case "ValidateRecursiveOperators" -> "recursive-violation.json";
                        case "NoConnectorMetadata" -> "connector-metadata-violation.json";
                        case "DeterministicFunctions" -> "nondeterministic-initializers.json";
                        case "CheckHints" -> "join-hint-conflicts.json";
                        case "TagRegions" -> "conflicting-regions.json";
                        case "NoIntegralVisitor" -> "integration-operator.json";
                        case "ConstantViews" -> "constant-sinks.json";
                        default -> throw new IllegalArgumentException("Unsupported analysis " + name);
                    });
                    String command = name.equals("NoIntegralVisitor") ? "ValidateNoIntegrals" : name;
                    this.runRust(command, input, report, directory.resolve("rust.log"));
                    JsonNode findings = readCircuit(report);
                    switch (name) {
                        case "ValidateRecursiveOperators" -> rejectRecursiveViolation(circuit, findings);
                        case "NoConnectorMetadata" -> rejectMetadataViolation(findings);
                        case "DeterministicFunctions" -> reportNondeterminism(compiler, circuit, findings);
                        case "CheckHints" -> reportJoinHintConflicts(compiler, circuit, findings);
                        case "TagRegions" -> applyRegionReport(circuit, findings);
                        case "NoIntegralVisitor" -> {
                            if (!findings.isNull())
                                throw new InternalCompilerError("Circuit contains an integration operator",
                                        findReportedOperator(circuit, findings));
                        }
                        case "ConstantViews" -> reportConstantViews(compiler, circuit, findings);
                        default -> throw new IllegalArgumentException("Unsupported analysis " + name);
                    }
                }
                writeReplayCircuit(compiler, circuit, output);
                if (reportsDiagnostics) {
                    Files.writeString(directory.resolve("diagnostics.json"),
                            compiler.messages.toJson(compiler.sources).toString(), StandardOpenOption.CREATE_NEW);
                }
                return circuit;
            }
            if (name.equals("ImplementNow")) {
                var request = MAPPER.createObjectNode()
                        .put("now_stream", compiler.options.ioOptions.nowStream)
                        .put("weight_variable", compiler.weightVar.variable);
                request.set("circuit", readCircuit(input));
                Path clockInput = directory.resolve("clock-input.json");
                Files.writeString(clockInput, request.toString(), StandardOpenOption.CREATE_NEW);
                if (!this.runner.equals("java")) {
                    Path clockOutput = directory.resolve("clock-output.json");
                    this.runRust(name, clockInput, clockOutput, directory.resolve("rust.log"));
                    var result = readCircuit(clockOutput);
                    var warnings = result.required("warnings");
                    if (!warnings.isArray())
                        throw new IllegalArgumentException("Invalid clock warnings");
                    for (JsonNode position: warnings) {
                        compiler.reportWarning(SourcePositionRange.fromJson(position), "Inefficient pattern",
                                "NOW() expression is used in a pattern that could require expensive computations\n"
                                        + org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.RewriteNow.NOW_DOCUMENTATION.citation());
                    }
                    Files.writeString(output, result.required("circuit").toString(), StandardOpenOption.CREATE_NEW);
                }
            }
            boolean deadCode = name.startsWith("DeadCode#");
            if (deadCode) {
                boolean keepSources = !name.equals("DeadCode#1")
                        || compiler.options.languageOptions.generateInputForEveryTable;
                var request = MAPPER.createObjectNode().put("keep_sources", keepSources);
                request.set("circuit", readCircuit(input));
                ArrayNode inputOrder = request.putArray("input_order");
                circuit.sourceOperators.values().forEach(source -> inputOrder.add(source.asOperator().getId()));
                Path deadCodeInput = directory.resolve("dead-code-input.json");
                Files.writeString(deadCodeInput, request.toString(), StandardOpenOption.CREATE_NEW);
                if (!this.runner.equals("java")) {
                    Path deadCodeOutput = directory.resolve("dead-code-output.json");
                    this.runRust("DeadCode", deadCodeInput, deadCodeOutput, directory.resolve("rust.log"));
                    JsonNode result = readCircuit(deadCodeOutput);
                    JsonNode ids = result.required("unused_inputs");
                    if (!ids.isArray())
                        throw new IllegalArgumentException("Invalid unused input report");
                    for (JsonNode id : ids) {
                        var source = findReportedOperator(circuit, id)
                                .to(org.dbsp.sqlCompiler.circuit.operator.IInputOperator.class);
                        compiler.reportWarning(source.getSourcePosition(), "Unused",
                                "Table " + source.getTableName().singleQuote() + " is not used");
                    }
                    Files.writeString(output, result.required("circuit").toString(), StandardOpenOption.CREATE_NEW);
                }
            }
            if (this.runner.equals("java")) {
                DBSPCircuit transformed = original.apply(circuit);
                if (nameAware)
                    Files.writeString(output, nameAwareReplayCircuit(compiler, transformed).toString(),
                            StandardOpenOption.CREATE_NEW);
                else
                    writeReplayCircuit(compiler, transformed, output);
            } else if (!name.equals("ImplementNow") && !deadCode) {
                String command = name;
                if (name.equals("ExpandIndexedInputs")) {
                    if (compiler.metadata.lazyUpsert())
                        throw new IllegalArgumentException("Indexed-input replay requires eager input maps");
                    command = compiler.options.ioOptions.gen2
                            ? "ExpandIndexedInputsGen2" : "ExpandIndexedInputsClassic";
                } else if (name.equals("Gen2Passes")) {
                    command = compiler.options.ioOptions.gen2
                            ? "Gen2PassesEnabled" : "Gen2PassesDisabled";
                } else if (name.equals("CreateStarJoins")) {
                    command = compiler.metadata.noStarJoins()
                            ? "CreateStarJoinsDisabled" : "CreateStarJoinsEnabled";
                } else if (name.equals("ImplementStatics")) {
                    if (!compiler.options.ioOptions.gen2 && compiler.options.ioOptions.multiCrates())
                        throw new IllegalArgumentException("Static declaration replay requires one output crate");
                    command = compiler.options.ioOptions.gen2
                            ? "ImplementStaticsDisabled" : "ImplementStaticsEnabled";
                } else if (name.equals("UnusedFields") && compiler.options.ioOptions.trimInputs) {
                    throw new IllegalArgumentException("Unused-field replay requires full input rows");
                }
                if (NUMBERED_GUARDED_PASSES.contains(numberedPass)) {
                    command = numberedPass;
                } else if (name.equals("LowerCircuitVisitor")) {
                    command = compiler.options.ioOptions.gen2 ? "LowerCircuitGen2" : "LowerCircuitClassic";
                } else if (name.equals("Lineage")) {
                    if (compiler.options.ioOptions.correlatedColumns)
                        throw new IllegalArgumentException("Lineage replay requires correlation tracking disabled");
                    command = "LineageDisabled";
                }
                if (name.equals("InsertLimiters")) {
                    Path report = directory.resolve("limiter-rewrite.json");
                    this.runRust(command, input, report, directory.resolve("rust.log"));
                    JsonNode rewritten = readCircuit(report);
                    if (rewritten.isNull()) {
                        DBSPCircuit transformed = original.apply(circuit);
                        Files.writeString(output, nameAwareReplayCircuit(compiler, transformed).toString(),
                                StandardOpenOption.CREATE_NEW);
                        return transformed;
                    }
                    Files.writeString(output, rewritten.toString(), StandardOpenOption.CREATE_NEW);
                } else {
                    this.runRust(command, input, output, directory.resolve("rust.log"));
                }
            }
            if (deadCode) {
                Files.writeString(directory.resolve("diagnostics.json"),
                        compiler.messages.toJson(compiler.sources).toString(), StandardOpenOption.CREATE_NEW);
            }
            JsonNode previous = readCircuit(input);
            JsonNode rewritten = readCircuit(output);
            // Repeat checks Java node identity for convergence. Preserve it for
            // a structural no-op instead of rebuilding identical objects.
            if (repeated && previous.equals(rewritten))
                return circuit;
            ObjectNode schema = previous.required("metadata").deepCopy();
            ProgramIdentifier removedTable = switch (name) {
                case "RemoveErrorTable" -> DBSPCompiler.ERROR_TABLE_NAME;
                case "RemoveNowTable" -> DBSPCompiler.NOW_TABLE_NAME;
                case "ImplementNow" -> hasMultisetSource(rewritten, DBSPCompiler.NOW_TABLE_NAME)
                        ? null : DBSPCompiler.NOW_TABLE_NAME;
                default -> null;
            };
            boolean removeTable = removedTable != null && hasMultisetSource(previous, removedTable);
            if (removeTable) {
                if (hasMultisetSource(rewritten, removedTable))
                    throw new IllegalArgumentException("Scheduled replay retained system input " + removedTable);
                ArrayNode inputs = (ArrayNode) schema.required("inputs");
                for (int index = inputs.size() - 1; index >= 0; index--) {
                    if (inputs.get(index).path("name").asText().equals(removedTable.name()))
                        inputs.remove(index);
                }
            }
            var decoder = new JsonDecoder() {
                @Override
                public ProgramMetadata decodeProgramMetadata(JsonNode saved) {
                    if (!saved.equals(schema))
                        throw new IllegalArgumentException("Scheduled " + name + " changed metadata");
                    return circuit.getMetadata();
                }
            };
            DBSPCircuit restored = decoder.decodeOuter(rewritten, DBSPCircuit.class);
            long weightId = compiler.weightVar.getId();
            if (rewritten.findParents("class").stream().anyMatch(node ->
                    node.path("class").asText().equals("DBSPVariablePath")
                            && node.path("id").asLong(-1) == weightId)) {
                var binding = MAPPER.createObjectNode().put("node", weightId)
                        .put("variable", compiler.weightVar.variable);
                compiler.weightVar = decoder.decodeWeightVariable(binding);
            }
            if (removeTable && !this.runner.equals("java"))
                compiler.removeTable(removedTable);
            if (nameAware) {
                JsonNode next = rewritten.required("next_generated_name");
                if (!next.isIntegralNumber() || !next.canConvertToLong()
                        || next.longValue() < previous.required("next_generated_name").longValue())
                    throw new IllegalArgumentException("Invalid scheduled generated-name counter");
                DBSPVariablePath.restoreGeneratedNames(next.longValue());
            }
            return restored;
        }

        private static boolean hasMultisetSource(JsonNode circuit, ProgramIdentifier table) {
            for (JsonNode operator : circuit.required("allOperators")) {
                if (operator.path("class").asText().equals("DBSPSourceMultisetOperator")
                        && operator.path("tableName").path("name").asText().equals(table.name()))
                    return true;
            }
            return false;
        }

        private static void reportConstantViews(DBSPCompiler compiler, DBSPCircuit circuit, JsonNode selected) {
            if (!selected.isArray())
                throw new IllegalArgumentException("ConstantViews must return a list of sink ids");
            var ids = new HashSet<Long>();
            for (JsonNode value : selected) {
                if (!value.isIntegralNumber() || !value.canConvertToLong() || !ids.add(value.longValue()))
                    throw new IllegalArgumentException("Invalid constant sink id");
            }
            var sinks = new ArrayList<DBSPSinkOperator>();
            for (var operator : circuit.allOperators) {
                if (operator instanceof DBSPSinkOperator sink && ids.remove(sink.getId())) {
                    if (sink.viewName.equals(DBSPCompiler.ERROR_VIEW_NAME))
                        throw new IllegalArgumentException("ConstantViews must skip the system error view");
                    sinks.add(sink);
                }
            }
            if (!ids.isEmpty())
                throw new IllegalArgumentException("Unrecognized constant sink ids");
            for (var sink : sinks)
                compiler.reportWarning(sink.getSourcePosition(), "View is constant",
                        "View " + sink.viewName.singleQuote() + " does not depend on any input data");
        }

        private static JsonNode readCircuit(Path path) throws IOException {
            if (Files.size(path) > 64L * 1024 * 1024)
                throw new IOException("Scheduled replay circuit exceeds 64 MiB");
            return MAPPER.readTree(Files.readAllBytes(path));
        }

        private static void collectTraceCapabilities(Iterable<DBSPOperator> operators, ArrayNode capabilities) {
            for (DBSPOperator operator : operators) {
                capabilities.addObject().put("operator", operator.getId())
                        .put("simple", operator instanceof DBSPSimpleOperator)
                        .put("contains_integrator", operator instanceof IContainsIntegrator);
                if (operator instanceof DBSPNestedOperator nested)
                    collectTraceCapabilities(nested.getAllOperators(), capabilities);
            }
        }

        private static final class LinearRetentionReplay extends LinearPostprocessRetainKeys {
            private final DBSPCompiler compiler;
            private final JsonNode plan;

            LinearRetentionReplay(DBSPCompiler compiler, JsonNode plan) {
                super(compiler);
                this.compiler = compiler;
                this.plan = plan;
            }

            @Override
            public DBSPCircuit apply(DBSPCircuit circuit) {
                if (this.plan.isNull())
                    return circuit;
                var available = new HashMap<Long, DBSPOperator>();
                circuit.allOperators.forEach(operator -> available.put(operator.getId(), operator));
                JsonNode order = this.plan.required("order");
                JsonNode replacements = this.plan.required("replacements");
                if (!order.isArray() || !replacements.isArray() || replacements.isEmpty())
                    throw new IllegalArgumentException("Invalid linear retention plan");
                var retainers = new HashMap<Long, DBSPIntegrateTraceRetainKeysOperator>();
                for (JsonNode entry : replacements) {
                    long aggregate = operatorId(entry.required("aggregate"));
                    var source = available.get(operatorId(entry.required("retainer")));
                    if (!(available.get(aggregate) instanceof DBSPAggregateLinearPostprocessOperator)
                            || !(source instanceof DBSPIntegrateTraceRetainKeysOperator retainer)
                            || retainers.putIfAbsent(aggregate, retainer) != null)
                        throw new IllegalArgumentException("Invalid linear retention replacement");
                }
                var sorted = new ArrayList<DBSPOperator>();
                for (JsonNode entry : order) {
                    var operator = available.remove(operatorId(entry));
                    if (operator == null)
                        throw new IllegalArgumentException("Unexpected linear retention operator " + entry);
                    sorted.add(operator);
                }
                if (!available.isEmpty())
                    throw new IllegalArgumentException("Incomplete linear retention order");
                circuit.allOperators.clear();
                circuit.allOperators.addAll(sorted);
                var builder = new ReplaceLinear(this.compiler, new CircuitGraphs()) {
                    @Override
                    protected Optional<DBSPIntegrateTraceRetainKeysOperator> selectRetainer(
                            DBSPAggregateLinearPostprocessOperator operator) {
                        return Optional.ofNullable(retainers.remove(operator.getId()));
                    }
                };
                DBSPCircuit transformed = builder.apply(circuit);
                if (!retainers.isEmpty())
                    throw new IllegalArgumentException("Unused linear retention replacements");
                return transformed;
            }
        }

        private static long operatorId(JsonNode id) {
            if (!id.isIntegralNumber() || !id.canConvertToLong())
                throw new IllegalArgumentException("Invalid reported operator id");
            return id.longValue();
        }

        private static DBSPCircuit applySelectedRewrite(DBSPCompiler compiler, DBSPCircuit circuit,
                                                        JsonNode selected, String pass) {
            if (!selected.isArray())
                throw new IllegalArgumentException("Expected an array of " + pass + " selections");
            var remaining = new HashSet<Long>();
            for (JsonNode id : selected) {
                if (!remaining.add(operatorId(id)))
                    throw new IllegalArgumentException("Duplicate " + pass + " selection");
            }
            if (remaining.isEmpty())
                return circuit;
            if (pass.equals("CloneOperatorsWithFanout"))
                return new CloneOperatorsWithFanout(compiler, new CircuitGraphs()).cloneSelected(circuit, remaining);
            CircuitTransform builder = switch (pass) {
                case "BalancedJoins" -> new BalancedJoins(compiler, new CircuitGraphs()) {
                    @Override
                    protected boolean canBalance(DBSPJoinBaseOperator join) {
                        return remaining.remove(join.getId());
                    }
                };
                default -> throw new IllegalArgumentException("Unknown selected rewrite " + pass);
            };
            DBSPCircuit transformed = builder.apply(circuit);
            if (!remaining.isEmpty())
                throw new IllegalArgumentException("Unexpected " + pass + " selections " + remaining);
            return transformed;
        }

        private static void rejectTraceViolation(DBSPCircuit circuit, JsonNode report) {
            if (report.isNull())
                return;
            DBSPOperator operator = findReportedOperator(circuit, report.required("operator"));
            String message = switch (report.required("reason").asText()) {
                case "input_accumulate" -> "Operator " + operator + " garbage-collects an input table "
                        + "and must use the non-accumulate variant";
                case "trace_nonaccumulate" -> "Operator " + operator
                        + " garbage-collects a trace inside the accumulate framework and must use the accumulate_ variant";
                case "missing_sibling" -> "Operator " + operator + " does not have a sibling to GC for";
                case "duplicate" -> {
                    var previous = findReportedOperator(circuit, report.required("previous"));
                    String dimension = operator instanceof DBSPIntegrateTraceRetainKeysOperator ? "keys" : "values";
                    yield "Operators " + previous + " and " + operator
                            + " both garbage-collect the " + dimension + " of " + operator.inputs.get(0).operator
                            + "; the runtime honors only one " + dimension + " retention condition per trace";
                }
                default -> throw new IllegalArgumentException("Unknown trace-retention violation");
            };
            throw new InternalCompilerError(message);
        }

        private static DBSPOperator findReportedOperator(DBSPCircuit circuit, JsonNode id) {
            if (!id.isIntegralNumber() || !id.canConvertToLong())
                throw new IllegalArgumentException("Invalid reported operator id");
            return findOperator(circuit.allOperators, id.longValue()).orElseThrow(
                    () -> new IllegalArgumentException("Unknown reported operator " + id));
        }

        private static void applyRegionReport(DBSPCircuit circuit, JsonNode findings) {
            if (!findings.isArray())
                throw new IllegalArgumentException("Conflicting regions must be an array");
            var ids = new HashSet<Integer>();
            for (JsonNode region : findings) {
                if (!region.isIntegralNumber() || !region.canConvertToInt() || !ids.add(region.intValue()))
                    throw new IllegalArgumentException("Invalid conflicting region id");
            }
            if (!ids.isEmpty())
                removeRegionAnnotations(circuit.allOperators, ids);
        }

        private static void removeRegionAnnotations(Iterable<DBSPOperator> operators, Set<Integer> ids) {
            for (var operator : operators) {
                if (operator instanceof DBSPNestedOperator nested)
                    removeRegionAnnotations(nested.getAllOperators(), ids);
                operator.annotations.annotations.removeIf(annotation ->
                        annotation instanceof GlobalAggregate region && ids.contains(region.id));
            }
        }

        private static void reportJoinHintConflicts(DBSPCompiler compiler, DBSPCircuit circuit, JsonNode conflicts) {
            if (!conflicts.isArray())
                throw new IllegalArgumentException("Join hint conflicts must be an array");
            for (JsonNode pair : conflicts) {
                var current = getJoinHint(circuit, pair.required("current"));
                var previous = getJoinHint(circuit, pair.required("previous"));
                compiler.reportError(current.getPosition(), "Incompatible hints",
                        "Found two conflicting hints for the same collection: " + current);
                compiler.reportError(previous.getPosition(), "Incompatible hints",
                        "Previous hint: " + previous, true);
            }
        }

        private static JoinStrategy getJoinHint(DBSPCircuit circuit, JsonNode address) {
            if (!address.path("join").isIntegralNumber() || !address.path("join").canConvertToLong()
                    || !address.path("annotation").isIntegralNumber() || !address.path("annotation").canConvertToInt())
                throw new IllegalArgumentException("Invalid join hint address");
            long id = address.required("join").longValue();
            int index = address.required("annotation").intValue();
            var operator = findOperator(circuit.allOperators, id).orElseThrow(
                    () -> new IllegalArgumentException("Unknown join operator " + id));
            if (!(operator instanceof DBSPJoinBaseOperator join)
                    || index < 0 || index >= join.annotations.annotations.size()
                    || !(join.annotations.annotations.get(index) instanceof JoinStrategy hint))
                throw new IllegalArgumentException("Invalid join hint address for operator " + id);
            return hint;
        }

        private static void reportNondeterminism(DBSPCompiler compiler, DBSPCircuit circuit, JsonNode findings) {
            if (!findings.isArray())
                throw new IllegalArgumentException("Nondeterminism findings must be an array");
            for (JsonNode finding : findings) {
                if (finding.has("declaration")) {
                    if (finding.size() != 1 || !finding.get("declaration").isIntegralNumber()
                            || !finding.get("declaration").canConvertToLong())
                        throw new IllegalArgumentException("Invalid nondeterministic declaration finding");
                    long id = finding.required("declaration").longValue();
                    var declaration = circuit.declarations.stream().filter(d -> d.getId() == id)
                            .findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown declaration " + id));
                    if (!(declaration.item instanceof DBSPFunctionItem item) || item.function.body == null)
                        throw new IllegalArgumentException("Expected a SQL function body for declaration " + id);
                    compiler.reportError(declaration.getSourcePosition(), "Non-deterministic UDF",
                            "Non-deterministic user-defined functions are not supported: "
                                    + Utilities.singleQuote(declaration.getName()));
                    continue;
                }
                if (finding.size() != 2 || !finding.path("source").isIntegralNumber()
                        || !finding.path("source").canConvertToLong() || !finding.path("column").isIntegralNumber()
                        || !finding.path("column").canConvertToInt())
                    throw new IllegalArgumentException("Invalid nondeterministic default finding");
                long id = finding.required("source").longValue();
                int index = finding.required("column").intValue();
                var operator = findOperator(circuit.allOperators, id).orElseThrow(
                        () -> new IllegalArgumentException("Unknown source operator " + id));
                if (!(operator instanceof DBSPSourceMultisetOperator source)
                        || index < 0 || index >= source.metadata.getColumnCount())
                    throw new IllegalArgumentException("Invalid default column for source " + id);
                var column = source.metadata.getColumnMetadata(index);
                if (column.defaultValue == null || column.defaultValuePosition == null)
                    throw new IllegalArgumentException("Missing default for " + column.name);
                compiler.reportError(column.defaultValuePosition, "Non-deterministic initializer",
                        "Default value for column " + column.name.singleQuote()
                                + " must be a compile-time constant.\n"
                                + "See https://github.com/feldera/feldera/issues/2051");
            }
        }

        private static void rejectRecursiveViolation(DBSPCircuit circuit, JsonNode report) {
            if (report.isNull())
                return;
            if (!report.path("operator").isIntegralNumber() || !report.path("operator").canConvertToLong()
                    || !report.path("operation").isTextual() || !report.path("internal").isBoolean())
                throw new IllegalArgumentException("Invalid recursive operator report");
            long id = report.required("operator").longValue();
            for (DBSPOperator root : circuit.allOperators) {
                if (!(root instanceof DBSPNestedOperator nested))
                    continue;
                var found = findOperator(nested.getAllOperators(), id);
                if (found.isPresent()) {
                    DBSPOperator operator = found.get();
                    String message = "Unsupported operation "
                            + Utilities.singleQuote(report.required("operation").textValue()) + " in recursive code";
                    if (report.required("internal").booleanValue())
                        throw new InternalCompilerError(message, operator.getNode());
                    throw new CompilationError(message, operator.getNode());
                }
            }
            throw new IllegalArgumentException("Unknown recursive operator id " + id);
        }

        private static Optional<DBSPOperator> findOperator(Iterable<DBSPOperator> operators, long id) {
            for (var operator : operators) {
                if (operator.getId() == id)
                    return Optional.of(operator);
                if (operator instanceof DBSPNestedOperator nested) {
                    var found = findOperator(nested.getAllOperators(), id);
                    if (found.isPresent())
                        return found;
                }
            }
            return Optional.empty();
        }

        private static void rejectMetadataViolation(JsonNode report) {
            if (report.isNull())
                return;
            JsonNode name = report.required("function");
            if (!name.isTextual() || !name.textValue().equalsIgnoreCase("connector_metadata"))
                throw new IllegalArgumentException("Invalid connector metadata report");
            JsonNode position = report.required("position");
            SourcePositionRange range = position.isNull() ? SourcePositionRange.INVALID
                    : SourcePositionRange.fromJson(position);
            throw new CompilationError("Calls to function " + Utilities.singleQuote(name.textValue())
                    + " are only allowed in DEFAULT column initializers", range);
        }

        private void runRust(String name, Path input, Path output, Path log) throws IOException {
            if (this.worker != null) {
                this.worker.rewrite(name, input, output, log);
                return;
            }
            var command = switch (name) {
                case "ConstantViews" -> List.of(this.runner, "--constant-views", input.toString(), output.toString());
                case "ValidateRecursiveOperators" ->
                        List.of(this.runner, "--validate-recursion", input.toString(), output.toString());
                case "NoConnectorMetadata" ->
                        List.of(this.runner, "--validate-connector-metadata", input.toString(), output.toString());
                default -> List.of(this.runner, "--rewrite-pass", name, input.toString(), output.toString());
            };
            var process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                if (!process.waitFor(30, TimeUnit.SECONDS))
                    throw new IOException("Rust " + name + " timed out; see " + log);
                if (process.exitValue() != 0)
                    throw new IOException("Rust " + name + " failed; see " + log);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IOException("Rust " + name + " interrupted", error);
            } finally {
                if (process.isAlive())
                    process.destroyForcibly();
            }
        }
    }

    /** Owns one serial Rust subprocess for an entire replay run. */
    private static final class RustWorker implements AutoCloseable {
        private final String runner;
        private final ExecutorService io = Executors.newSingleThreadExecutor();
        private Process process;
        private Path log;
        private boolean closed;

        RustWorker(String runner) {
            this.runner = runner;
        }

        void rewrite(String name, Path input, Path output, Path firstLog) throws IOException {
            if (this.closed)
                throw new IOException("Rust replay worker is closed");
            if (this.process == null) {
                this.log = firstLog;
                this.process = new ProcessBuilder(this.runner, "--serve-rewrites")
                        .redirectError(this.log.toFile()).start();
            }
            var request = MAPPER.createObjectNode().put("pass", name)
                    .put("input", input.toString()).put("output", output.toString());
            byte[] message = (request + "\n").getBytes(StandardCharsets.UTF_8);
            var exchange = this.io.submit(() -> {
                var stdin = this.process.getOutputStream();
                stdin.write(message);
                stdin.flush();
                return this.process.getInputStream().read();
            });
            try {
                if (exchange.get(30, TimeUnit.SECONDS) != '\n')
                    throw new IOException("Rust " + name + " worker returned no acknowledgement; see " + this.log);
            } catch (InterruptedException error) {
                this.abort();
                Thread.currentThread().interrupt();
                throw new IOException("Rust " + name + " worker interrupted", error);
            } catch (ExecutionException | TimeoutException error) {
                this.abort();
                throw new IOException("Rust " + name + " worker exchange failed; see " + this.log, error);
            } catch (IOException error) {
                this.abort();
                throw error;
            }
        }

        private void abort() {
            this.closed = true;
            if (this.process != null)
                this.process.destroyForcibly();
            this.io.shutdownNow();
        }

        @Override
        public void close() throws IOException {
            if (this.closed)
                return;
            try {
                if (this.process != null) {
                    this.process.getOutputStream().close();
                    if (!this.process.waitFor(2, TimeUnit.SECONDS) || this.process.exitValue() != 0)
                        throw new IOException("Rust replay worker did not exit cleanly; see " + this.log);
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IOException("Rust replay worker shutdown interrupted", error);
            } finally {
                this.abort();
            }
        }
    }

    private static void resume(String[] args, ScheduledReplay scheduledReplay) throws Exception {
        CircuitReplay.Stage stage = args[2].equals("resume-s2") ? CircuitReplay.Stage.S2 : CircuitReplay.Stage.S3;
        String prefix = args[3] + "/" + stage + "/";
        Path files = Path.of(args[1] + ".files");
        Files.createDirectory(files);
        Set<String> ids = new HashSet<>();
        try (var input = Files.newBufferedReader(Path.of(args[0]));
             var output = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonNode test = MAPPER.readTree(line);
                String id = test.path("id").asText();
                if (!id.startsWith(prefix) || !ids.add(id) || !test.path("seam").isObject())
                    throw new IllegalArgumentException("Resume needs a unique id matching the stage/mode and a subset file");
                Path seam = files.resolve(ids.size() + "." + stage.name().toLowerCase() + ".json");
                Files.write(seam, MAPPER.writeValueAsBytes(test.get("seam")), StandardOpenOption.CREATE_NEW);
                DBSPCompiler compiler = new DBSPCompiler(options("S3", args));
                if (scheduledReplay != null)
                    scheduledReplay.install(compiler, files.resolve(ids.size() + "-scheduled"));
                if (test.has("arithmetic_witness")) {
                    if (stage != CircuitReplay.Stage.S2 || !test.get("arithmetic_witness").isTextual())
                        throw new IllegalArgumentException("An arithmetic witness must resume from S2");
                    installArithmeticWitness(compiler, test.get("arithmetic_witness").asText());
                }
                ObjectNode hashes = hashInputs(compiler);
                DBSPCircuit circuit = CircuitReplay.read(compiler, seam, stage);
                compiler.throwIfErrorsOccurred();
                if (circuit == null)
                    throw new IllegalStateException("Resume returned no circuit");
                ObjectNode result = MAPPER.createObjectNode();
                result.put("id", args[3] + "/S3/" + id.substring(prefix.length()));
                result.put("mode", args[3]);
                result.put("stage", "S3");
                if (scheduledReplay != null) {
                    recordFacts(compiler, circuit, result);
                    recordPruning(compiler, circuit, result);
                    Path s3 = files.resolve(ids.size() + ".optimized-s3.json");
                    CircuitReplay.write(compiler, circuit, s3, CircuitReplay.Stage.S3);
                    result.set("seam", MAPPER.readTree(Files.readAllBytes(s3)));
                }
                describe(result, compiler, circuit, hashes, files.resolve(ids.size() + "-rust"));
                output.write(MAPPER.writeValueAsString(result));
                output.newLine();
                output.flush();
            }
        }
    }

    private static OutputPort add(DBSPCircuit circuit, DBSPOperator operator) {
        circuit.addOperator(operator);
        return operator.getOutput(0);
    }

    private static MultisetAnalysis recordFacts(DBSPCompiler compiler, DBSPCircuit circuit, ObjectNode result) {
        MultisetAnalysis analysis = new MultisetAnalysis(compiler);
        analysis.apply(circuit);
        ArrayNode facts = result.putArray("multisets");
        for (DBSPOperator operator : circuit.allOperators) {
            ArrayNode ports = facts.addArray();
            for (int port = 0; port < operator.outputCount(); port++) {
                if (operator.hasOutput(port))
                    ports.add(analysis.isMultiset(operator.getOutput(port)));
                else
                    ports.addNull();
            }
        }
        return analysis;
    }

    private static void recordPruning(DBSPCompiler compiler, DBSPCircuit circuit, ObjectNode result)
            throws Exception {
        DBSPCircuit pruned = new DeadCode(compiler, true).apply(circuit);
        result.set("dead_code", circuitJson(compiler, pruned));
        result.put("removed_operators", circuit.allOperators.size() - pruned.allOperators.size());
    }

    private static void cleanupFixtures(Path path) throws IOException {
        try (var output = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW)) {
            for (boolean delta : List.of(false, true)) {
                DBSPCompiler compiler = new DBSPCompiler(options("S2", "classic"));
                DBSPCircuit circuit = new DBSPCircuit(compiler.metadata, delta);
                var source = CalciteEmptyRel.INSTANCE;
                var item = new DBSPTupleExpression(new DBSPI32Literal(1));
                var value = new DBSPZSetExpression(item);
                OutputPort snapshot = add(circuit, new DBSPConstantOperator(source, value));
                OutputPort changes = add(circuit, new DBSPIncrementalConstantOperator(source, value));
                OutputPort differentiated = add(circuit, new DBSPDifferentiateOperator(source, snapshot));
                add(circuit, new DBSPIntegrateOperator(source, differentiated));
                add(circuit, new DBSPIntegrateOperator(source, changes));
                var empty = add(circuit, new DBSPConstantOperator(source,
                        new DBSPZSetExpression(item.getType())));
                var emptyChanges = add(circuit, new DBSPDifferentiateOperator(source, empty));
                add(circuit, new DBSPIntegrateOperator(source, emptyChanges));
                for (boolean predicate : List.of(false, true)) {
                    var closure = new DBSPBoolLiteral(predicate).closure(item.getType().ref().var());
                    add(circuit, new DBSPFilterOperator(source, closure, delta ? changes : snapshot));
                }
                var index = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, item.getType(), item.getType());
                var indexed = add(circuit, new DBSPConstantOperator(source,
                        new DBSPIndexedZSetExpression(CalciteObject.EMPTY, index)));
                add(circuit, new DBSPDeindexOperator(source, CalciteObject.EMPTY, indexed));
                for (var pass : List.of(new PropagateEmptySources(compiler),
                        new RemoveConstantFilters(compiler), new RemoveIAfterD(compiler),
                        new RemoveDeindexOperators(compiler))) {
                    var result = MAPPER.createObjectNode()
                            .put("id", (delta ? "delta/" : "snapshot/") + pass.getName())
                            .put("pass", pass.getName());
                    result.set("circuit", MAPPER.readTree(replayCircuitText(compiler, circuit)));
                    result.set("expected", MAPPER.readTree(replayCircuitText(compiler, pass.apply(circuit))));
                    output.write(MAPPER.writeValueAsString(result));
                    output.newLine();
                }
            }
            output.write(MAPPER.writeValueAsString(noopsFixture()));
            output.newLine();
            for (var fixture : identityFixtures()) {
                output.write(MAPPER.writeValueAsString(fixture));
                output.newLine();
            }
        }
    }

    private static ObjectNode identityResult(DBSPCompiler compiler, DBSPCircuit circuit, String id)
            throws IOException {
        var pass = new RemoveIdentityOperators(compiler);
        var result = MAPPER.createObjectNode().put("id", id).put("pass", pass.getName());
        result.set("circuit", nameAwareReplayCircuit(compiler, circuit));
        result.set("expected", nameAwareReplayCircuit(compiler, pass.apply(circuit)));
        return result;
    }

    private static List<ObjectNode> identityFixtures() throws IOException {
        var compiler = new DBSPCompiler(options("S3", "classic"));
        var source = CalciteEmptyRel.INSTANCE;
        var circuit = new DBSPCircuit(compiler.metadata, false);
        var item = new DBSPTupleExpression(new DBSPI32Literal(1), new DBSPI32Literal(2));
        var input = add(circuit, new DBSPConstantOperator(source, new DBSPZSetExpression(item)));
        var p = item.getType().ref().var();
        var fields = DBSPTypeTupleBase.flatten(p.deref());
        var identity = new DBSPTupleExpression(fields, false).closure(p);
        var swap = new DBSPTupleExpression(fields.get(1), fields.get(0)).closure(p);
        var map = add(circuit, new DBSPMapOperator(source, identity, input));
        add(circuit, new DBSPMapOperator(source, identity, map));
        add(circuit, new DBSPMapOperator(source, swap, map));

        var index = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, item.getType(), item.getType());
        var indexed = add(circuit, new DBSPConstantOperator(source,
                new DBSPIndexedZSetExpression(CalciteObject.EMPTY, index)));
        var pair = new DBSPRawTupleExpression(p, p).getType().var();
        var key = new DBSPTupleExpression(DBSPTypeTupleBase.flatten(pair.field(0).deref()), false);
        var value = new DBSPTupleExpression(DBSPTypeTupleBase.flatten(pair.field(1).deref()), false);
        var identityIndex = new DBSPRawTupleExpression(key, value).closure(pair);
        var swapIndex = new DBSPRawTupleExpression(value, key).closure(pair);
        add(circuit, new DBSPMapIndexOperator(source, identityIndex, indexed));
        add(circuit, new DBSPMapIndexOperator(source, swapIndex, indexed));
        var shapes = identityResult(compiler, circuit, "identity-maps");

        var lateness = latenessFixtureCompiler();
        var compiled = lateness.getFinalCircuit(false);
        lateness.throwIfErrorsOccurred();
        requireTraceRetention(compiled);
        var siblings = identityRetentionFixture(lateness, compiled, false);
        var indexedSiblings = identityRetentionFixture(lateness, compiled, true);
        return List.of(shapes, identityResult(lateness, siblings, "identity-retention-conflict"),
                identityResult(lateness, indexedSiblings, "indexed-identity-retention-conflict"));
    }

    private static DBSPCircuit identityRetentionFixture(DBSPCompiler compiler, DBSPCircuit compiled,
                                                       boolean indexed) {
        return new CircuitCloneVisitor(compiler, false) {
            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                var data = this.mapped(operator.left());
                var waterline = this.mapped(operator.right());
                var parameter = data.outputType().to(DBSPTypeZSet.class).elementType.ref().var();
                var row = new DBSPTupleExpression(DBSPTypeTupleBase.flatten(parameter.deref()), false);
                DBSPSimpleOperator first;
                DBSPSimpleOperator second;
                if (indexed) {
                    var index = new DBSPMapIndexOperator(operator.getRelNode(),
                            new DBSPRawTupleExpression(row, row).closure(parameter), data);
                    this.addOperator(index);
                    var pair = index.getOutputIndexedZSetType().getKVRefType().var();
                    var key = new DBSPTupleExpression(DBSPTypeTupleBase.flatten(pair.field(0).deref()), false);
                    var value = new DBSPTupleExpression(DBSPTypeTupleBase.flatten(pair.field(1).deref()), false);
                    var function = new DBSPRawTupleExpression(key, value).closure(pair);
                    first = new DBSPMapIndexOperator(operator.getRelNode(), function, index.outputPort());
                    second = new DBSPMapIndexOperator(operator.getRelNode(), function, index.outputPort());
                } else {
                    var function = row.closure(parameter);
                    first = new DBSPMapOperator(operator.getRelNode(), function, data);
                    second = new DBSPMapOperator(operator.getRelNode(), function, data);
                }
                this.addOperator(first);
                this.addOperator(second);
                this.addOperator(operator.withInputs(List.of(first.outputPort(), waterline), false)
                        .to(DBSPSimpleOperator.class));
                var retained = operator.withInputs(List.of(second.outputPort(), waterline), false)
                        .to(DBSPSimpleOperator.class);
                if (indexed) {
                    this.addOperator(retained);
                    super.postorder(operator);
                } else {
                    this.map(operator, retained);
                }
            }
        }.apply(compiled);
    }

    private static DBSPCompiler latenessFixtureCompiler() {
        var options = options("S3", "classic");
        options.languageOptions.outputsAreSets = true;
        var compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(
                "CREATE TABLE t(i INT, ts TIMESTAMP LATENESS INTERVAL 1 SECOND); "
                        + "CREATE VIEW v AS SELECT * FROM t;");
        return compiler;
    }

    private static void requireTraceRetention(DBSPCircuit circuit) {
        if (circuit == null || circuit.allOperators.stream()
                .noneMatch(DBSPIntegrateTraceRetainKeysOperator.class::isInstance))
            throw new IllegalStateException("Lateness fixture has no key retention operator");
    }

    private static ObjectNode noopsFixture() throws IOException {
        var compiler = latenessFixtureCompiler();
        DBSPCircuit compiled = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        requireTraceRetention(compiled);
        DBSPCircuit circuit = new CircuitCloneVisitor(compiler, false) {
            private void addNoops(DBSPSimpleOperator operator) {
                var inputs = new ArrayList<>(operator.inputs.stream().map(this::mapped).toList());
                var first = new DBSPNoopOperator(operator.getRelNode(), inputs.get(0));
                this.addOperator(first);
                var second = new DBSPNoopOperator(operator.getRelNode(), first.outputPort());
                this.addOperator(second);
                inputs.set(0, second.outputPort());
                this.map(operator, operator.withInputs(inputs, false).to(DBSPSimpleOperator.class));
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.addNoops(operator);
            }

            @Override
            public void postorder(DBSPSinkOperator operator) {
                this.addNoops(operator);
            }
        }.apply(compiled);
        var graph = new Graph(compiler);
        graph.apply(circuit);
        var result = MAPPER.createObjectNode().put("id", "gc-and-sink-noops").put("pass", "RemoveNoops");
        result.set("circuit", nameAwareReplayCircuit(compiler, circuit));
        result.set("expected", nameAwareReplayCircuit(compiler,
                new RemoveNoops(compiler, graph.getGraphs()).apply(circuit)));
        return result;
    }

    private static void constantFixtures(Path path) throws IOException {
        try (var output = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW)) {
            for (String fixture : List.of("negate", "indexed", "delta", "filter", "empty-filter",
                    "identity-map", "literal-map", "map-cancellation", "empty-map", "distinct", "chain",
                    "projected-filter", "projected-map", "projection-cancellation", "comparison-filter",
                    "arithmetic-map", "arithmetic-filter", "overflow-map", "overflow-filter",
                    "arithmetic-chain", "nested-overflow-map", "small-arithmetic-map",
                    "small-arithmetic-filter", "small-overflow-map",
                    "nullable-null-test", "nullable-comparison", "wrapped-overflow-filter")) {
                DBSPCompiler compiler = new DBSPCompiler(options("S2", "classic"));
                DBSPCircuit circuit = new DBSPCircuit(compiler.metadata, false);
                var source = CalciteEmptyRel.INSTANCE;
                var expressionPosition = new SourcePositionRange(new SourcePosition(2, 1), new SourcePosition(2, 5));
                var origin = new PositionOnlyRel(expressionPosition, List.of(expressionPosition), false);
                boolean projection = fixture.startsWith("project");
                boolean small = fixture.startsWith("small-");
                boolean cancellation = fixture.endsWith("-cancellation");
                boolean nullable = fixture.startsWith("nullable-") || fixture.startsWith("wrapped-");
                var one = projection
                        ? new DBSPTupleExpression(new DBSPI32Literal(1), new DBSPBoolLiteral(false))
                        : new DBSPTupleExpression(small ? new DBSPI8Literal((byte) 1)
                                : new DBSPI32Literal(fixture.startsWith("nullable-") ? null : 1, nullable));
                int second = fixture.contains("overflow")
                        ? (small ? Byte.MAX_VALUE : Integer.MAX_VALUE) : 2;
                var two = projection
                        ? new DBSPTupleExpression(new DBSPI32Literal(1), new DBSPBoolLiteral(true))
                        : new DBSPTupleExpression(small ? new DBSPI8Literal((byte) second)
                                : new DBSPI32Literal(second, nullable));
                var row = one.getType();
                var value = new DBSPZSetExpression(row);
                value.append(one, cancellation ? 2 : 1);
                value.append(two, cancellation ? -2 : 3);
                if (projection)
                    value.append(new DBSPTupleExpression(new DBSPI32Literal(2), new DBSPBoolLiteral(true)), 4);
                if (fixture.startsWith("nullable-"))
                    value.append(new DBSPTupleExpression(new DBSPI32Literal(5, true)), -4);
                boolean empty = fixture.startsWith("empty-");
                var input = fixture.equals("indexed")
                        ? add(circuit, new DBSPConstantOperator(source, new DBSPIndexedZSetExpression(
                                CalciteObject.EMPTY, new DBSPTypeIndexedZSet(CalciteObject.EMPTY, row, row))))
                        : fixture.equals("delta")
                                ? add(circuit, new DBSPIncrementalConstantOperator(source, value))
                                : add(circuit, new DBSPConstantOperator(source,
                                        empty ? new DBSPZSetExpression(row) : value));
                switch (fixture) {
                    case "nullable-null-test", "nullable-comparison", "wrapped-overflow-filter" -> {
                        var parameter = row.ref().var();
                        var column = parameter.deref().field(0);
                        DBSPExpression predicate;
                        if (fixture.equals("nullable-null-test")) {
                            predicate = column.is_null();
                        } else {
                            var operand = fixture.equals("wrapped-overflow-filter")
                                    ? new DBSPBinaryExpression(origin, column.getType(), DBSPOpcode.ADD,
                                            column, new DBSPI32Literal(1, true))
                                    : column;
                            var boolType = new DBSPBoolLiteral(true).getType().withMayBeNull(true);
                            predicate = new DBSPBinaryExpression(origin, boolType, DBSPOpcode.GT,
                                    operand, new DBSPI32Literal(2, true));
                        }
                        for (var expression : List.of(predicate, predicate.not()))
                            add(circuit, new DBSPFilterOperator(origin,
                                    expression.wrapBoolIfNeeded().closure(parameter), input));
                    }
                    case "negate", "indexed", "delta" -> add(circuit, new DBSPNegateOperator(origin, input));
                    case "filter", "empty-filter" -> {
                        for (boolean result : List.of(false, true)) {
                            var predicate = new DBSPBoolLiteral(result).closure(row.ref().var());
                            add(circuit, new DBSPFilterOperator(origin, predicate, input));
                        }
                    }
                    case "identity-map" -> {
                        var parameter = row.ref().var();
                        add(circuit, new DBSPMapOperator(origin, parameter.deref().closure(parameter), input));
                    }
                    case "projected-filter", "projected-map", "projection-cancellation" -> {
                        var parameter = row.ref().var();
                        if (fixture.equals("projected-filter"))
                            add(circuit, new DBSPFilterOperator(origin,
                                    parameter.deref().field(1).closure(parameter), input));
                        else
                            add(circuit, new DBSPMapOperator(origin,
                                    parameter.deref().field(0).closure(parameter), input));
                    }
                    case "literal-map", "map-cancellation", "empty-map" -> {
                        var literal = new DBSPTupleExpression(new DBSPI32Literal(9));
                        add(circuit, new DBSPMapOperator(origin, literal.closure(row.ref().var()), input));
                    }
                    case "comparison-filter" -> {
                        var bool = new DBSPBoolLiteral(true).getType();
                        for (var opcode : List.of(DBSPOpcode.EQ, DBSPOpcode.NEQ, DBSPOpcode.LT,
                                DBSPOpcode.LTE, DBSPOpcode.GT, DBSPOpcode.GTE)) {
                            var parameter = row.ref().var();
                            var field = parameter.deref().field(0);
                            var literal = new DBSPI32Literal(1);
                            var predicate = new DBSPBinaryExpression(CalciteObject.EMPTY, bool, opcode,
                                    field, literal).closure(parameter);
                            add(circuit, new DBSPFilterOperator(origin, predicate, input));
                        }
                    }
                    case "arithmetic-map", "arithmetic-filter", "overflow-map", "overflow-filter",
                         "small-arithmetic-map", "small-arithmetic-filter", "small-overflow-map" -> {
                        boolean overflow = fixture.contains("overflow");
                        var operations = overflow ? List.of(DBSPOpcode.ADD)
                                : List.of(DBSPOpcode.ADD, DBSPOpcode.SUB, DBSPOpcode.MUL);
                        for (var opcode : operations) {
                            var parameter = row.ref().var();
                            var field = parameter.deref().field(0);
                            int operand = overflow ? 1 : 3;
                            var right = small ? new DBSPI8Literal((byte) operand) : new DBSPI32Literal(operand);
                            var scalar = new DBSPBinaryExpression(origin, field.getType(), opcode,
                                    field, right);
                            if (fixture.endsWith("filter")) {
                                var threshold = small ? new DBSPI8Literal((byte) 3) : new DBSPI32Literal(3);
                                var predicate = new DBSPBinaryExpression(origin, new DBSPBoolLiteral(true).getType(),
                                        DBSPOpcode.GT, scalar, threshold).closure(parameter);
                                add(circuit, new DBSPFilterOperator(origin, predicate, input));
                            } else {
                                add(circuit, new DBSPMapOperator(origin, scalar.closure(parameter), input));
                            }
                        }
                    }
                    case "arithmetic-chain", "nested-overflow-map" -> {
                        var parameter = row.ref().var();
                        var field = parameter.deref().field(0);
                        var sum = new DBSPBinaryExpression(origin, field.getType(), DBSPOpcode.ADD,
                                field, new DBSPI32Literal(1));
                        if (fixture.equals("nested-overflow-map")) {
                            var product = new DBSPBinaryExpression(origin, field.getType(), DBSPOpcode.MUL,
                                    new DBSPI32Literal(0), sum);
                            add(circuit, new DBSPMapOperator(origin, product.closure(parameter), input));
                        } else {
                            var mapped = add(circuit, new DBSPMapOperator(origin, sum.closure(parameter), input));
                            var scalar = field.getType().ref().var();
                            var product = new DBSPBinaryExpression(origin, field.getType(), DBSPOpcode.MUL,
                                    scalar.deref(), new DBSPI32Literal(3));
                            add(circuit, new DBSPMapOperator(origin, product.closure(scalar), mapped));
                        }
                    }
                    case "distinct" -> {
                        var constant = add(circuit, new DBSPConstantOperator(origin, new DBSPZSetExpression(one)));
                        var setDelta = add(circuit, new DBSPDifferentiateOperator(origin, constant));
                        add(circuit, new DBSPDistinctOperator(origin, setDelta));
                        var bagDelta = add(circuit, new DBSPDifferentiateOperator(origin, input));
                        add(circuit, new DBSPDistinctOperator(origin, bagDelta));
                    }
                    case "chain" -> {
                        var negated = add(circuit, new DBSPNegateOperator(origin, input));
                        var parameter = row.ref().var();
                        var mapped = add(circuit, new DBSPMapOperator(origin,
                                parameter.deref().closure(parameter), negated));
                        add(circuit, new DBSPFilterOperator(origin,
                                new DBSPBoolLiteral(false).closure(row.ref().var()), mapped));
                    }
                    default -> throw new IllegalStateException(fixture);
                }
                var facts = new MultisetAnalysis(compiler);
                facts.apply(circuit);
                var pass = new PropagateConstants(compiler, facts);
                var record = MAPPER.createObjectNode().put("id", fixture).put("pass", pass.getName());
                record.set("circuit", MAPPER.readTree(replayCircuitText(compiler, circuit)));
                record.set("expected", MAPPER.readTree(replayCircuitText(compiler, pass.apply(circuit))));
                output.write(MAPPER.writeValueAsString(record));
                output.newLine();
            }
        }
    }

    /** Put a directed predicate on a live output just before constant analysis. */
    private static void installArithmeticWitness(DBSPCompiler compiler, String shape) {
        if (!Set.of("fold", "overflow", "nested", "nullable-comparison", "nullable-null-test").contains(shape))
            throw new IllegalArgumentException("Unknown arithmetic witness " + shape);
        var existingHook = compiler.optimizerHook;
        compiler.optimizerHook = optimizer -> {
            if (existingHook != null)
                existingHook.accept(optimizer);
            for (var step : optimizer.passes) {
                if (step instanceof Passes group && step.getName().equals("PropagateConstants")) {
                    group.passes.add(0, new CircuitCloneVisitor(compiler, false) {
                        @Override
                        public void postorder(DBSPSinkOperator sink) {
                            if (!sink.viewName.name().equals("v")) {
                                super.postorder(sink);
                                return;
                            }
                            var origin = sink.getRelNode();
                            boolean nullable = shape.startsWith("nullable-");
                            var one = new DBSPTupleExpression(new DBSPI32Literal(nullable ? null : 2, nullable));
                            int value = shape.equals("fold") || nullable ? 5 : Integer.MAX_VALUE;
                            var two = new DBSPTupleExpression(new DBSPI32Literal(value, nullable));
                            var input = new DBSPConstantOperator(origin, new DBSPZSetExpression(one, two));
                            this.addOperator(input);
                            var parameter = one.getType().ref().var();
                            var field = parameter.deref().field(0);
                            DBSPExpression predicate;
                            if (shape.equals("nullable-null-test")) {
                                predicate = field.is_null();
                            } else {
                                var sum = new DBSPBinaryExpression(origin, field.getType(), DBSPOpcode.ADD,
                                        field, new DBSPI32Literal(1, nullable));
                                var scalar = shape.equals("nested")
                                        ? new DBSPBinaryExpression(origin, field.getType(), DBSPOpcode.MUL,
                                                new DBSPI32Literal(0), sum)
                                        : sum;
                                var boolType = new DBSPBoolLiteral(true).getType().withMayBeNull(nullable);
                                predicate = new DBSPBinaryExpression(origin, boolType, DBSPOpcode.GT,
                                        scalar, new DBSPI32Literal(3, nullable));
                            }
                            var filter = new DBSPFilterOperator(origin,
                                    predicate.wrapBoolIfNeeded().closure(parameter), input.outputPort());
                            this.addOperator(filter);
                            this.map(sink, sink.withInputs(List.of(filter.outputPort()), false)
                                    .to(DBSPSimpleOperator.class));
                        }
                    });
                    return;
                }
            }
            throw new IllegalStateException("Arithmetic witness needs the constant propagation group");
        };
    }

    private static void sumFixtures(Path path) throws Exception {
        try (var writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW)) {
            for (boolean indexed : List.of(false, true)) {
                for (String shape : List.of("cancel-all", "cancel-first-copy", "unmatched-negation-order",
                        "double-negation", "one-input", "nested-duplicate", "cancel-before-flatten",
                        "rewired-negate-type", "rewired-sum-type", "rewired-distinct-type")) {
                    DBSPCompiler compiler = new DBSPCompiler(options("S2", "classic"));
                    DBSPCircuit circuit = new DBSPCircuit(compiler.metadata, false);
                    var row = new DBSPTypeTuple(new DBSPI32Literal(0).getType());
                    var index = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, row, row);
                    var source = CalciteEmptyRel.INSTANCE;
                    // Distinct source type identities expose changes in the first
                    // input of a rebuilt sum or negate.
                    var a = add(circuit, new DBSPConstantOperator(source, indexed
                            ? new DBSPIndexedZSetExpression(CalciteObject.EMPTY, index)
                            : new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI32Literal(1)))));
                    var b = add(circuit, new DBSPConstantOperator(source, indexed
                            ? new DBSPIndexedZSetExpression(CalciteObject.EMPTY,
                                    new DBSPTypeIndexedZSet(CalciteObject.EMPTY, row, row))
                            : new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI32Literal(2)))));
                    var negA = add(circuit, new DBSPNegateOperator(source, a));
                    var negB = add(circuit, new DBSPNegateOperator(source, b));
                    switch (shape) {
                        case "cancel-all" -> add(circuit, new DBSPSumOperator(source, negA, a, b, negB));
                        case "cancel-first-copy" -> add(circuit, new DBSPSumOperator(source, a, b, a, negA));
                        case "unmatched-negation-order" ->
                                add(circuit, new DBSPSumOperator(source, negB, negA, a, negB, a));
                        case "double-negation" -> {
                            var doubleNeg = add(circuit, new DBSPNegateOperator(source, negA));
                            add(circuit, new DBSPSumOperator(source, b, doubleNeg, negB));
                        }
                        case "one-input" -> add(circuit, new DBSPSumOperator(source, a));
                        case "nested-duplicate" -> {
                            var ab = add(circuit, new DBSPSumOperator(source, a, b));
                            add(circuit, new DBSPSumOperator(source, ab, a, ab));
                        }
                        case "cancel-before-flatten" -> {
                            var ab = add(circuit, new DBSPSumOperator(source, a, b));
                            add(circuit, new DBSPSumOperator(source, ab, negB));
                        }
                        case "rewired-negate-type" -> {
                            var alias = add(circuit, new DBSPSumOperator(source, a, b, negA));
                            add(circuit, new DBSPNegateOperator(source, alias));
                        }
                        case "rewired-sum-type" -> {
                            var empty = add(circuit, new DBSPSumOperator(source, a, negA));
                            add(circuit, new DBSPSumOperator(source, empty, b));
                        }
                        case "rewired-distinct-type" -> {
                            var empty = add(circuit, new DBSPSumOperator(source, a, negA));
                            var distinct = add(circuit, new DBSPStreamDistinctOperator(source, empty));
                            add(circuit, new DBSPSumOperator(source, distinct, b));
                        }
                        default -> throw new IllegalStateException(shape);
                    }
                    var result = MAPPER.createObjectNode();
                    result.put("id", (indexed ? "indexed/" : "unindexed/") + shape);
                    result.set("circuit", circuitJson(compiler, circuit));
                    recordFacts(compiler, circuit, result);
                    DBSPCircuit pruned = new DeadCode(compiler, true).apply(circuit);
                    result.set("dead_code", circuitJson(compiler, pruned));
                    result.put("removed_operators", circuit.allOperators.size() - pruned.allOperators.size());
                    result.set("merge_sums", circuitJson(compiler, new MergeSums(compiler).apply(circuit)));
                    writer.write(MAPPER.writeValueAsString(result));
                    writer.newLine();
                }
            }
        }
    }

    private static void joinFixtures(Path path) throws Exception {
        try (var writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW)) {
            for (boolean chain : List.of(false, true)) {
                var compiler = new DBSPCompiler(options("S2", "classic"));
                var circuit = new DBSPCircuit(compiler.metadata, false);
                var source = CalciteEmptyRel.INSTANCE;
                var row = new DBSPTypeTuple(new DBSPI32Literal(0).getType());
                var leftType = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, row, row);
                var rightType = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, row, row);
                var left = add(circuit, new DBSPConstantOperator(source,
                        new DBSPIndexedZSetExpression(CalciteObject.EMPTY, leftType)));
                var right = add(circuit, new DBSPConstantOperator(source,
                        new DBSPIndexedZSetExpression(CalciteObject.EMPTY, rightType)));
                var output = add(circuit, new DBSPStreamAntiJoinOperator(source, left, right));
                if (chain) {
                    var key = row.ref().var();
                    var leftValue = row.ref().var();
                    var rightValue = row.ref().var();
                    var function = new DBSPRawTupleExpression(key.deref(), leftValue.deref())
                            .closure(key, leftValue, rightValue);
                    output = add(circuit, new DBSPStreamJoinIndexOperator(
                            source, leftType, function, output, right, false));
                }
                add(circuit, new DBSPNoopOperator(source, output));
                var result = MAPPER.createObjectNode();
                result.put("id", chain ? "antijoin-and-indexed-join" : "antijoin");
                result.put("pass", "ImplementJoins");
                result.set("circuit", nameAwareReplayCircuit(compiler, circuit));
                result.set("expected", nameAwareReplayCircuit(compiler,
                        new ImplementJoins(compiler).apply(circuit)));
                writer.write(MAPPER.writeValueAsString(result));
                writer.newLine();
            }
        }
    }

    private static void integralFixtures(Path path) throws Exception {
        try (var writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW)) {
            for (boolean indexed : List.of(false, true)) {
                var compiler = new DBSPCompiler(options("S2", "classic"));
                var circuit = new DBSPCircuit(compiler.metadata, false);
                var origin = CalciteEmptyRel.INSTANCE;
                var row = new DBSPTypeTuple(new DBSPI32Literal(0).getType());
                var value = indexed
                        ? new DBSPIndexedZSetExpression(CalciteObject.EMPTY,
                                new DBSPTypeIndexedZSet(CalciteObject.EMPTY, row, row))
                        : new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI32Literal(1)));
                var snapshot = add(circuit, new DBSPConstantOperator(origin, value));
                var delta = add(circuit, new DBSPDifferentiateOperator(origin, snapshot));
                var integral = add(circuit, new DBSPIntegrateOperator(origin, delta));
                var noop = add(circuit, new DBSPNoopOperator(origin, integral));
                var cancel = add(circuit, new DBSPDifferentiateOperator(origin, noop));
                add(circuit, new DBSPNegateOperator(origin, cancel));
                add(circuit, new DBSPNegateOperator(origin, integral));
                var result = MAPPER.createObjectNode();
                result.put("id", indexed ? "indexed-noop" : "unindexed-noop");
                result.put("pass", "OptimizeIncrementalVisitor");
                result.set("circuit", nameAwareReplayCircuit(compiler, circuit));
                result.set("expected", nameAwareReplayCircuit(compiler,
                        new OptimizeIncrementalVisitor(compiler).apply(circuit)));
                writer.write(MAPPER.writeValueAsString(result));
                writer.newLine();
            }
        }
    }

    private static void distinctFixtures(Path path) throws Exception {
        try (var writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW)) {
            for (String fixture : List.of("constant-set", "filter", "sum", "join", "delta-positive")) {
                var compiler = new DBSPCompiler(options("S2", "classic"));
                var circuit = new DBSPCircuit(compiler.metadata, fixture.equals("delta-positive"));
                var origin = CalciteEmptyRel.INSTANCE;
                var value = new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI32Literal(1)));
                var constant = add(circuit, new DBSPConstantOperator(origin, value));
                var row = constant.getOutputZSetType().elementType;
                if (fixture.equals("join")) {
                    var indexed = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, row, row);
                    var input = add(circuit, new DBSPConstantOperator(origin,
                            new DBSPIndexedZSetExpression(CalciteObject.EMPTY, indexed)));
                    var bag = add(circuit, new DBSPSumOperator(origin, input, input));
                    var left = add(circuit, new DBSPStreamDistinctOperator(origin, bag));
                    var right = add(circuit, new DBSPStreamDistinctOperator(origin, bag));
                    var key = row.ref().var();
                    var leftValue = row.ref().var();
                    var rightValue = row.ref().var();
                    var closure = leftValue.deref().closure(key, leftValue, rightValue);
                    add(circuit, new DBSPStreamJoinOperator(
                            origin, constant.getOutputZSetType(), closure, left, right, false));
                } else if (fixture.equals("delta-positive")) {
                    var delta = add(circuit, new DBSPDifferentiateOperator(origin, constant));
                    var bag = add(circuit, new DBSPSumOperator(origin, delta, delta));
                    var first = add(circuit, new DBSPDistinctOperator(origin, bag));
                    var second = add(circuit, new DBSPDistinctOperator(origin, first));
                    var positive = add(circuit, new DBSPPositiveOperator(origin, second));
                    add(circuit, new DBSPNoopOperator(origin, positive));
                } else {
                    var bag = add(circuit, new DBSPSumOperator(origin, constant, constant));
                    var distinct = add(circuit, new DBSPStreamDistinctOperator(
                            origin, fixture.equals("constant-set") ? constant : bag));
                    if (fixture.equals("filter"))
                        add(circuit, new DBSPFilterOperator(origin, new DBSPBoolLiteral(true).closure(row.ref().var()), distinct));
                    if (fixture.equals("sum")) {
                        var sum = add(circuit, new DBSPSumOperator(origin, distinct, distinct));
                        add(circuit, new DBSPStreamDistinctOperator(origin, sum));
                    }
                }
                var result = MAPPER.createObjectNode();
                result.put("id", fixture);
                result.put("pass", "OptimizeDistinctVisitor");
                result.set("circuit", nameAwareReplayCircuit(compiler, circuit));
                var multisets = new MultisetAnalysis(compiler);
                multisets.apply(circuit);
                result.set("expected", nameAwareReplayCircuit(compiler,
                        new OptimizeDistinctVisitor(compiler, multisets).apply(circuit)));
                writer.write(MAPPER.writeValueAsString(result));
                writer.newLine();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--distinct-fixtures")) {
            distinctFixtures(Path.of(args[1]));
            return;
        }
        if (args.length == 2 && args[0].equals("--integral-fixtures")) {
            integralFixtures(Path.of(args[1]));
            return;
        }
        if (args.length == 2 && args[0].equals("--join-fixtures")) {
            joinFixtures(Path.of(args[1]));
            return;
        }
        if (args.length == 2 && args[0].equals("--sum-fixtures")) {
            sumFixtures(Path.of(args[1]));
            return;
        }
        if (args.length == 2 && args[0].equals("--cleanup-fixtures")) {
            cleanupFixtures(Path.of(args[1]));
            return;
        }
        if (args.length == 2 && args[0].equals("--constant-fixtures")) {
            constantFixtures(Path.of(args[1]));
            return;
        }
        if ((args.length != 4 && !(args.length == 5 && args[4].equals("sets")))
                || !(args[2].equals("S2") || args[2].equals("S3")
                || args[2].equals("resume-s2") || args[2].equals("resume-s3"))
                || !(args[3].equals("classic") || args[3].equals("gen2")))
            throw new IllegalArgumentException("Usage: MultisetProbe INPUT.jsonl OUTPUT.jsonl S2|S3|resume-s2|resume-s3 classic|gen2 [sets]");
        try (var scheduledReplay = ScheduledReplay.configured(args[2])) {
            runFiles(args, scheduledReplay);
        }
    }

    private static void runFiles(String[] args, ScheduledReplay scheduledReplay) throws Exception {
        if (args[2].startsWith("resume-")) {
            resume(args, scheduledReplay);
            return;
        }
        Path files = Path.of(args[1] + ".files");
        Files.createDirectory(files);
        Set<String> ids = new HashSet<>();
        try (var input = Files.newBufferedReader(Path.of(args[0]));
             var output = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonNode test = MAPPER.readTree(line);
                if (!test.path("id").isTextual() || !test.path("sql").isTextual()
                        || !ids.add(test.path("id").asText()))
                    throw new IllegalArgumentException("Each probe needs unique id and SQL text");
                DBSPCompiler compiler = new DBSPCompiler(options(args[2], args));
                if (scheduledReplay != null)
                    scheduledReplay.install(compiler, files.resolve(ids.size() + "-scheduled"));
                if (test.has("arithmetic_witness")) {
                    if (!test.get("arithmetic_witness").isTextual())
                        throw new IllegalArgumentException("The arithmetic witness must name a directed fixture");
                    if (args[2].equals("S3"))
                        installArithmeticWitness(compiler, test.get("arithmetic_witness").asText());
                    else if (!args[2].equals("S2"))
                        throw new IllegalArgumentException("An arithmetic witness needs an S2 or S3 capture");
                }
                ObjectNode hashes = hashInputs(compiler);
                compiler.submitStatementsForCompilation(test.path("sql").asText());
                DBSPCircuit circuit = compiler.getFinalCircuit(false);
                compiler.throwIfErrorsOccurred();
                if (circuit == null)
                    throw new IllegalStateException("Compilation returned no circuit");
                if (test.has("merge_sums_replay") && !test.get("merge_sums_replay").isBoolean())
                    throw new IllegalArgumentException("merge_sums_replay must be a boolean");
                ObjectNode result = MAPPER.createObjectNode();
                result.put("id", args[3] + "/" + args[2] + "/" + test.path("id").asText());
                result.put("mode", args[3]);
                result.put("stage", args[2]);
                if (args[2].equals("S2") && test.has("arithmetic_witness"))
                    result.set("arithmetic_witness", test.get("arithmetic_witness"));
                MultisetAnalysis analysis = recordFacts(compiler, circuit, result);
                recordPruning(compiler, circuit, result);
                DBSPCircuit withoutViews = new RemoveViewOperators(compiler, false).apply(circuit);
                result.set("remove_views", circuitJson(compiler, withoutViews));
                result.put("removed_views", circuit.allOperators.size() - withoutViews.allOperators.size());
                DBSPCircuit merged = new MergeSums(compiler).apply(circuit);
                result.set("merge_sums", circuitJson(compiler, merged));
                if (args[2].equals("S2") && test.path("merge_sums_replay").asBoolean()) {
                    Path mergedSeam = files.resolve(ids.size() + ".merged-s2.json");
                    CircuitReplay.write(compiler, merged, mergedSeam, CircuitReplay.Stage.S2);
                    result.set("sum_seam", MAPPER.readTree(Files.readAllBytes(mergedSeam)));
                }
                if (args[2].equals("S2")) {
                    DBSPCircuit distinct = new EnsureDistinctOutputs(compiler, analysis).apply(circuit);
                    result.set("ensure_distinct_outputs", circuitJson(compiler, distinct));
                    result.put("inserted_output_distincts", distinct.allOperators.size() - circuit.allOperators.size());
                    if (compiler.options.languageOptions.outputsAreSets) {
                        Path distinctSeam = files.resolve(ids.size() + ".distinct-s2.json");
                        CircuitReplay.write(compiler, distinct, distinctSeam, CircuitReplay.Stage.S2);
                        result.set("distinct_seam", MAPPER.readTree(Files.readAllBytes(distinctSeam)));
                    }
                }
                Path seam = files.resolve(ids.size() + "." + args[2].toLowerCase() + ".json");
                CircuitReplay.write(compiler, circuit, seam, CircuitReplay.Stage.valueOf(args[2]));
                result.set("seam", MAPPER.readTree(Files.readAllBytes(seam)));
                describe(result, compiler, circuit, hashes, files.resolve(ids.size() + "-rust"));
                output.write(MAPPER.writeValueAsString(result));
                output.newLine();
                output.flush();
            }
        }
    }
}
