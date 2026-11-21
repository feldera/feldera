package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.CheckPersistentIds;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveUselessLeftJoinsVisitor;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CSE;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CreateStarJoins;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FuseExpensiveMaps;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.FindMapChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.CollapseSharedChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitStatistics;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ExpandAggregateZero;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ExpandAggregates.ExpandStreamAggregates;
import org.dbsp.sqlCompiler.compiler.visitors.outer.EnsureDistinctOutputs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ConstantViews;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeterministicFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindUnboundedState;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.HashPrefixPass;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.SetOperatorHashes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveTable;
import org.dbsp.sqlCompiler.compiler.visitors.outer.StrayGC;
import org.dbsp.sqlCompiler.compiler.visitors.outer.AppendOnly;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerAsof;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields.FindUnusedInputFields;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.DeferCarriedColumns;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.TrimFilters;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields.TrimInputs;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindCommonProjections;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ReplaceCommonProjections;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.MultisetAnalysis;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.CircuitContainsNow;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.CheckRetain;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.OrderErrorConsumers;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MergeGC;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveViewOperators;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.util.Shuffle;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.FindWindowGroups;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.ImplementWindowGroups;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.NativeWindowGroups;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/** A Rust pass at its original position in a live optimizer. Java owns the compiler
 * metadata and the name allocator shared by the passes on both sides. */
public final class RustCircuitPass implements CircuitTransform {
    private static final ObjectMapper MAPPER = Utilities.deterministicObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    static {
        System.loadLibrary("feldera_sql_opt_jni");
    }

    private final DBSPCompiler compiler;
    private final String name;
    private final String nativeName;
    private final OptimizerSnapshots snapshots;
    @Nullable private final FindMapChains mapChains;
    @Nullable private final FindUnboundedState.DetectStreamingOperations streaming;
    @Nullable private final FindUnboundedState.FindGCedStreams garbageCollected;
    @Nullable private final FindUnboundedState.FindBounded bounded;
    @Nullable private final FindUnboundedState.CollectUnbounded unbounded;
    @Nullable private final Graph graph;
    @Nullable private final HashPrefixPass hashPrefixes;
    @Nullable private final SetOperatorHashes setHashes;
    @Nullable private final MergeGC.MergeRetain mergeRetain;
    @Nullable private final MergeGC.MergeNoops mergeNoops;
    @Nullable private final MergeGC.FindEquivalentNoops traceInputs;
    @Nullable private final MergeGC.FindMultipleRetains retentionGroups;
    @Nullable private final AppendOnly appendOnly;
    @Nullable private final ExpandStreamAggregates streamAggregates;
    @Nullable private final FindUnusedInputFields inputFields;
    @Nullable private final TrimInputs trimInputs;
    @Nullable private final TrimFilters trimFilters;
    @Nullable private final DeferCarriedColumns deferCarried;
    @Nullable private final FindCommonProjections commonProjections;
    @Nullable private final FuseExpensiveMaps.Fuse mapFusion;
    @Nullable private final CreateStarJoins.FindJoinTrees joinTrees;
    @Nullable private final CreateStarJoins.ReplaceStarJoins starJoins;
    @Nullable private final ReplaceCommonProjections replaceCommon;
    @Nullable private final LowerAsof.MoveGC asofRetention;
    @Nullable private final LowerAsof.OrderGcDependencies asofOrder;
    @Nullable private final MultisetAnalysis multiplicities;
    @Nullable private final EnsureDistinctOutputs distinctOutputs;
    @Nullable private final CircuitContainsNow clockUses;
    @Nullable private final CollapseSharedChains collapseChains;
    @Nullable private final FindWindowGroups windowGroups;
    @Nullable private final ImplementWindowGroups windowImplementation;
    @Nullable private final RemoveUselessLeftJoinsVisitor leftJoins;
    @Nullable private final OrderErrorConsumers errorOrder;
    @Nullable private final ProgramIdentifier tableToRemove;
    @Nullable private final Set<DBSPOperator> seenConstants;
    private static final byte[] EMPTY_CONTEXT = new byte[0];

    public RustCircuitPass(DBSPCompiler compiler, CircuitTransform original, OptimizerSnapshots snapshots) {
        this.compiler = compiler;
        this.name = original.getName();
        if (original instanceof DeadCode dead && dead.keepAllSources)
            this.nativeName = "DeadCodeKeepSources";
        else if (original instanceof RemoveViewOperators views && views.removesAllViews())
            this.nativeName = "RemoveAllViews";
        else
            this.nativeName = this.name;
        this.snapshots = snapshots;
        this.traceInputs = original instanceof MergeGC.FindEquivalentNoops inputs ? inputs : null;
        this.retentionGroups = original instanceof MergeGC.FindMultipleRetains groups ? groups : null;
        this.mergeRetain = original instanceof MergeGC.MergeRetain merge ? merge : null;
        this.mergeNoops = original instanceof MergeGC.MergeNoops merge ? merge : null;
        this.hashPrefixes = original instanceof HashPrefixPass prefix ? prefix : null;
        this.setHashes = original instanceof SetOperatorHashes set ? set : null;
        this.graph = original instanceof Graph graph ? graph : null;
        this.streaming = original instanceof FindUnboundedState.DetectStreamingOperations detector ? detector : null;
        this.bounded = original instanceof FindUnboundedState.FindBounded propagation ? propagation : null;
        this.unbounded = original instanceof FindUnboundedState.CollectUnbounded collect ? collect : null;
        this.garbageCollected = original instanceof FindUnboundedState.FindGCedStreams streams ? streams : null;
        this.mapFusion = original instanceof FuseExpensiveMaps.Fuse fuse ? fuse : null;
        this.joinTrees = original instanceof CreateStarJoins.FindJoinTrees find ? find : null;
        this.starJoins = original instanceof CreateStarJoins.ReplaceStarJoins joins ? joins : null;
        this.replaceCommon = original instanceof ReplaceCommonProjections replace ? replace : null;
        this.asofRetention = original instanceof LowerAsof.MoveGC move ? move : null;
        this.asofOrder = original instanceof LowerAsof.OrderGcDependencies order ? order : null;
        this.commonProjections = original instanceof FindCommonProjections common ? common : null;
        this.deferCarried = original instanceof DeferCarriedColumns defer ? defer : null;
        this.trimFilters = original instanceof TrimFilters trim ? trim : null;
        this.trimInputs = original instanceof TrimInputs trim ? trim : null;
        this.inputFields = original instanceof FindUnusedInputFields fields ? fields : null;
        this.streamAggregates = original instanceof ExpandStreamAggregates expand ? expand : null;
        this.appendOnly = original instanceof AppendOnly analysis ? analysis : null;
        this.multiplicities = original instanceof MultisetAnalysis analysis ? analysis : null;
        this.distinctOutputs = original instanceof EnsureDistinctOutputs distinct ? distinct : null;
        this.clockUses = original instanceof CircuitContainsNow clock ? clock : null;
        this.leftJoins = original instanceof RemoveUselessLeftJoinsVisitor joins ? joins : null;
        this.windowGroups = original instanceof FindWindowGroups windows ? windows : null;
        this.windowImplementation = original instanceof ImplementWindowGroups windows ? windows : null;
        this.collapseChains = original instanceof CollapseSharedChains collapse ? collapse : null;
        this.mapChains = original instanceof FindMapChains find ? find : null;
        this.errorOrder = original instanceof OrderErrorConsumers order ? order : null;
        this.tableToRemove = original instanceof RemoveTable table ? table.tableName() : null;
        this.seenConstants = original instanceof CSE ? Collections.newSetFromMap(new WeakHashMap<>()) : null;
    }

    private static native byte[] transformWithContext(String pass, byte[] circuit, long next, String errorView, byte[] state);

    /** Indexes of the aggregate implementations that can share each operator. */
    public static List<List<Integer>> aggregateGroups(List<IAggregate> aggregates, boolean appendOnly) {
        var kinds = aggregates.stream().map(a -> a.getClass().getSimpleName()).toList();
        try {
            byte[] reply = transformWithContext("AggregateGroups", MAPPER.writeValueAsBytes(kinds),
                    0, "", MAPPER.writeValueAsBytes(appendOnly));
            int[][] indexes = MAPPER.readValue(reply, int[][].class);
            return java.util.Arrays.stream(indexes)
                    .map(group -> java.util.Arrays.stream(group).boxed().toList()).toList();
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid aggregate groups from the Rust optimizer", error);
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    private static int applyAnnotations(Iterable<DBSPOperator> operators, JsonNode edits) {
        int count = 0;
        for (var operator : operators) {
            JsonNode change = edits.get(Long.toString(operator.id));
            if (change != null) {
                operator.annotations.replace(Annotations.fromJson(change));
                count++;
            }
            if (operator instanceof DBSPNestedOperator nested)
                count += applyAnnotations(nested.getAllOperators(), edits);
        }
        return count;
    }

    private static void visitConstants(Iterable<DBSPOperator> operators, Consumer<DBSPOperator> visit) {
        for (var op : operators) {
            if (op instanceof DBSPConstantOperator || op instanceof DBSPIncrementalConstantOperator)
                visit.accept(op);
            else if (op instanceof DBSPNestedOperator nested)
                visitConstants(nested.getAllOperators(), visit);
        }
    }

    private byte[] passState(DBSPCircuit circuit, ToJsonOuterVisitor writer) throws IOException {
        if (this.asofRetention != null)
            return MAPPER.writeValueAsBytes(this.asofRetention.retentionJoins());
        if (this.streamAggregates != null)
            return MAPPER.writeValueAsBytes(Map.of("weight_id", this.compiler.weightVar.getId(),
                    "weight_name", this.compiler.weightVar.variable,
                    "append_only_inputs", this.streamAggregates.appendOnlyInputs()));
        if (this.nativeName.equals("IndexedInputs"))
            return MAPPER.writeValueAsBytes(Map.of("dedup_keys", this.compiler.options.ioOptions.gen2,
                    "lazy_upsert", this.compiler.metadata.lazyUpsert()));
        if (this.nativeName.equals("ExpandAggregateZero"))
            return MAPPER.writeValueAsBytes(ExpandAggregateZero.nestedHoistOrder(this.compiler, circuit));
        if (this.seenConstants == null)
            return EMPTY_CONTEXT;
        var state = MAPPER.createObjectNode();
        var seen = state.putArray("seen_constants");
        visitConstants(circuit.getAllOperators(), op -> {
            if (this.seenConstants.contains(op))
                seen.add(op.id);
        });
        var groups = new IdentityHashMap<Shuffle, Integer>();
        var shuffles = state.putObject("shuffles");
        Objects.requireNonNull(writer.innerVisitor.liveShuffles).forEach((id, shuffle) ->
                shuffles.put(id.toString(), groups.computeIfAbsent(shuffle, key -> groups.size())));
        return MAPPER.writeValueAsBytes(state);
    }

    private void reportUnusedSources(DBSPCircuit circuit, JsonNode ids) {
        if (ids.isEmpty())
            return;
        var unused = new HashSet<Long>();
        for (var id : ids)
            unused.add(id.longValue());
        for (var table : circuit.sourceOperators.values()) {
            if (unused.remove(table.asOperator().id))
                this.compiler.reportWarning(table.getSourcePosition(), "Unused",
                        "Table " + table.getTableName().singleQuote() + " is not used");
        }
        if (!unused.isEmpty())
            throw new IllegalStateException("DeadCode reported an unknown input");
    }

    private DBSPCircuit readGraph(DBSPCircuit input, JsonNode result, ToJsonOuterVisitor writer) throws IOException {
        JsonNode graph = result.required("circuit");
        if (graph.isNull())
            return input;
        var shuffles = Objects.requireNonNull(writer.innerVisitor.liveShuffles);
        // Both trimming passes give inserted operators fresh IDs. The clone visitor
        // preserves other operators with unchanged inputs, including the identities
        // recorded by earlier iterations of unused-field removal.
        var originalOperators = this.trimFilters != null || this.nativeName.equals("TrimWindows")
                ? operatorsById(input) : Map.<Long, DBSPOperator>of();
        var decoder = new JsonDecoder(null, writer.liveProvenanceDecoder()) {
            @Override
            DBSPOperator decodedOperator(long id, DBSPOperator decoded) {
                var original = originalOperators.get(id);
                if (original == null || !original.inputs.equals(decoded.inputs))
                    return decoded;
                if (original instanceof DBSPNestedOperator nested &&
                        !nested.sameCircuit(decoded.to(DBSPNestedOperator.class)))
                    return decoded;
                return original;
            }

            @Override
            public Shuffle decodeShuffle(JsonNode flatmap) {
                return Shuffle.fromJson(flatmap.required("shuffle"), shuffles.get(flatmap.required("id").longValue()));
            }

            @Override
            public ProgramMetadata decodeProgramMetadata(JsonNode saved) {
                if (!saved.isNull())
                    throw new IllegalStateException(RustCircuitPass.this.name + " replaced the retained schema");
                return input.getMetadata();
            }
        };
        if (graph.has("retained_inner")) {
            var live = Objects.requireNonNull(writer.innerVisitor.liveNodes);
            for (long id : MAPPER.treeToValue(graph.required("retained_inner"), long[].class))
                decoder.inner.cache(id, Objects.requireNonNull(live.get(id), "Unknown retained inner node " + id));
            graph = graph.required("graph");
        }
        var output = decoder.decodeOuter(graph, DBSPCircuit.class);
        var weight = this.compiler.weightVar;
        JsonNode expandedWeight = result.get("weight_node");
        long id = expandedWeight == null || expandedWeight.isNull() ? weight.getId() : expandedWeight.longValue();
        if ((expandedWeight != null && !expandedWeight.isNull())
                || (writer.innerVisitor.serialized.contains(id) && decoder.inner.decoded.containsKey(id)))
            this.compiler.weightVar = decoder.decodeWeightVariable(MAPPER.createObjectNode()
                    .put("node", id).put("variable", weight.variable));
        return output;
    }

    @Nullable
    private static DBSPOperator findOperator(Iterable<DBSPOperator> operators, long id) {
        for (var op : operators) {
            if (op.id == id)
                return op;
            if (op instanceof DBSPNestedOperator nested) {
                var child = findOperator(nested.getAllOperators(), id);
                if (child != null)
                    return child;
            }
        }
        return null;
    }

    private void reportViolation(DBSPCircuit circuit, JsonNode violation) {
        if (violation.isNull())
            return;
        var op = findOperator(circuit.getAllOperators(), violation.required("operator").longValue());
        if (op == null)
            throw new IllegalStateException(this.name + " returned an unknown operator");
        String message = violation.required("message").textValue();
        if (this.nativeName.equals("NoIntegralVisitor"))
            throw new InternalCompilerError(message, op);
        if (violation.required("internal").booleanValue())
            throw new InternalCompilerError(message, op.getNode());
        throw new CompilationError(message, op.getNode());
    }

    private static InternalCompilerError retentionError(DBSPCircuit circuit, JsonNode finding) {
        var operator = findOperator(circuit.allOperators, finding.required("operator").longValue());
        if (!(operator instanceof DBSPBinaryOperator retainer))
            throw new IllegalStateException("Unknown trace retainer " + finding);
        JsonNode previous = finding.get("previous");
        return StrayGC.violation(StrayGC.Violation.valueOf(finding.required("reason").textValue()),
                retainer, previous == null ? null : findOperator(circuit.allOperators, previous.longValue()));
    }

    private void reportPersistentIds(DBSPCircuit circuit, JsonNode findings) {
        if (findings.isEmpty())
            return;
        var operators = operatorsById(circuit);
        for (var finding : findings) {
            var operator = Objects.requireNonNull(operators.get(finding.required("operator").longValue()));
            switch (finding.required("kind").textValue()) {
                case "missing" -> throw new NullPointerException();
                case "empty" -> throw new InternalCompilerError("Operator " + operator + " has an empty persistent id", operator);
                case "duplicate" -> {
                    var previous = Objects.requireNonNull(operators.get(finding.required("previous").longValue()));
                    throw new InternalCompilerError("Operators " + previous + " and " + operator +
                            " have the same persistent id " + OperatorHash.getHash(operator, true), operator);
                }
                case "identical_views" -> {
                    var previous = Objects.requireNonNull(operators.get(finding.required("previous").longValue()));
                    CheckPersistentIds.reportIdenticalViews(this.compiler, operator.to(DBSPSinkOperator.class),
                            previous.to(DBSPSinkOperator.class));
                }
                default -> throw new IllegalStateException("Unknown persistent ID finding " + finding);
            }
        }
    }

    private DBSPCircuit applyResult(DBSPCircuit circuit, @Nullable ToJsonOuterVisitor writer, JsonNode result) throws IOException {
        if (this.joinTrees != null) {
            JsonNode trees = result.required("join_trees");
            if (!trees.isArray())
                throw new IllegalStateException("Expected an array of star join replacements");
            this.joinTrees.setNativeReplacements(trees.isEmpty() ? EMPTY_CONTEXT : MAPPER.writeValueAsBytes(trees));
            return circuit;
        }
        if (result.has("duplicate_retention")) {
            JsonNode finding = result.required("duplicate_retention");
            if (!finding.isNull()) {
                var first = findOperator(circuit.allOperators, finding.required("first").longValue());
                var other = findOperator(circuit.allOperators, finding.required("other").longValue());
                if (!(first instanceof DBSPBinaryOperator retain) || other == null)
                    throw new IllegalStateException("Unknown duplicate retention policies " + finding);
                throw CheckRetain.duplicate(retain, other);
            }
            return circuit;
        }
        if (result.has("retention_violation"))
            throw retentionError(circuit, result.required("retention_violation"));
        if (result.has("unused_declarations")) {
            for (var id : result.required("unused_declarations")) {
                if (!(findOperator(circuit.allOperators, id.longValue()) instanceof DBSPViewDeclarationOperator declaration))
                    throw new IllegalStateException("Unknown recursive declaration " + id);
                this.compiler.reportWarning(declaration.getSourcePosition(), "View is not recursive",
                        "View " + declaration.originalViewName().singleQuote()
                                + " is declared recursive, but is not used in any recursive computation");
            }
            JsonNode late = result.required("late_column");
            if (!late.isNull()) {
                if (!(findOperator(circuit.allOperators, late.required("operator").longValue()) instanceof DBSPViewOperator view))
                    throw new IllegalStateException("Unknown recursive view " + late);
                throw new CompilationError("LATENESS on views inside recursive components is not supported: view "
                        + view.viewName.singleQuote(), view.metadata.columns.get(late.required("column").intValue()).getPositionRange());
            }
        }
        if (result.has("removed_left_joins"))
            for (var id : result.required("removed_left_joins")) {
                if (!(findOperator(circuit.allOperators, id.longValue()) instanceof DBSPJoinBaseOperator join))
                    throw new IllegalStateException("Rust returned an unknown left join");
                RemoveUselessLeftJoinsVisitor.reportRemoved(this.compiler, join);
            }
        if (result.has("statistics")) {
            CircuitStatistics.logCounters(result.required("statistics"));
            return circuit;
        }
        if (result.has("constant_views")) {
            for (var id : result.required("constant_views")) {
                if (!(findOperator(circuit.getAllOperators(), id.longValue()) instanceof DBSPSinkOperator sink))
                    throw new IllegalStateException("ConstantViews returned an unknown sink " + id);
                ConstantViews.warn(this.compiler, sink);
            }
            return circuit;
        }
        if (result.has("connector_call")) {
            JsonNode call = result.required("connector_call");
            if (!call.isNull())
                throw new CompilationError("Calls to function " + Utilities.singleQuote(call.required("name").textValue())
                        + " are only allowed in DEFAULT column initializers", SourcePositionRange.fromJson(call.required("position")));
            return circuit;
        }
        if (result.has("nondeterministic_initializers")) {
            for (var finding : result.required("nondeterministic_initializers")) {
                if (finding.has("declaration")) {
                    long id = finding.required("declaration").longValue();
                    var declaration = circuit.declarations.stream().filter(decl -> decl.id == id)
                            .findFirst().orElseThrow(() -> new IllegalStateException("Unknown function declaration " + id));
                    DeterministicFunctions.reportFunction(this.compiler, declaration);
                } else {
                    long id = finding.required("source").longValue();
                    if (!(findOperator(circuit.getAllOperators(), id) instanceof DBSPSourceMultisetOperator source))
                        throw new IllegalStateException("Unknown source for default initializer " + id);
                    int column = finding.required("column").intValue();
                    DeterministicFunctions.reportDefault(this.compiler, source.metadata.getColumnMetadata(column));
                }
            }
            return circuit;
        }
        if (result.has("violation")) {
            this.reportViolation(circuit, result.required("violation"));
            return circuit;
        }
        if (result.has("diagnostics")) {
            for (JsonNode message : result.required("diagnostics"))
                this.compiler.reportProblem(SourcePositionRange.fromJson(message.required("range")),
                        message.required("warning").booleanValue(), message.required("continuation").booleanValue(),
                        message.required("error_type").textValue(), message.required("message").textValue());
            if (!result.has("circuit"))
                return circuit;
        }
        if (result.has("annotations")) {
            JsonNode edits = result.required("annotations");
            if (!edits.isObject() || (!edits.isEmpty() && applyAnnotations(circuit.getAllOperators(), edits) != edits.size()))
                throw new IllegalStateException(this.name + " returned unknown annotation targets");
            return circuit;
        }
        if (writer == null)
            throw new IllegalStateException(this.nativeName + " attempted to replace a circuit from topology alone");
        DBSPCircuit output = this.readGraph(circuit, result, writer);
        this.reportUnusedSources(circuit, result.required("unused_sources"));
        return output;
    }

    /** Apply the order and input substitutions from Rust to the original operators.
     * The constructors retain the live type objects and allocate any new closure names. */
    private DBSPCircuit applyViewReferences(DBSPCircuit circuit, JsonNode plan) {
        if (plan.has("violation")) {
            this.reportViolation(circuit, plan.required("violation"));
            return circuit;
        }
        JsonNode order = plan.required("order");
        if (order.isEmpty())
            return circuit;
        var originals = new HashMap<Long, DBSPOperator>();
        circuit.allOperators.forEach(op -> originals.put(op.id, op));
        var mapped = new HashMap<Long, DBSPOperator>();
        var output = new DBSPCircuit(circuit.metadata, circuit.incremental);
        circuit.declarations.forEach(output::addDeclaration);
        for (var id : order) {
            var op = Objects.requireNonNull(originals.remove(id.longValue()), "Unknown or repeated operator in view order");
            var changes = plan.required("redirects").path(Long.toString(op.id));
            var inputs = new ArrayList<OutputPort>(op.inputs.size());
            for (int slot = 0; slot < op.inputs.size(); slot++) {
                var original = op.inputs.get(slot);
                var redirect = changes.get(Integer.toString(slot));
                long source = redirect == null ? original.operator.id : redirect.longValue();
                var producer = Objects.requireNonNull(mapped.get(source), "View reference precedes its producer");
                inputs.add(producer.getOutput(redirect == null ? original.outputNumber : 0));
            }
            var replacement = op.withInputs(inputs, false);
            output.addOperator(replacement);
            mapped.put(op.id, replacement);
        }
        if (!originals.isEmpty())
            throw new IllegalStateException("View reference order omitted an operator");
        return output;
    }

    private static void visitOperators(Iterable<DBSPOperator> operators, Consumer<DBSPOperator> visit) {
        for (var op : operators) {
            visit.accept(op);
            if (op instanceof DBSPNestedOperator nested)
                visitOperators(nested.getAllOperators(), visit);
        }
    }

    /** Apply one round to live operators. Rebuilt operators have fresh identities
     * and may become equivalent after their inputs have merged. */
    private DBSPCircuit applyCse(DBSPCircuit circuit) throws IOException {
        var seen = Objects.requireNonNull(this.seenConstants);
        int limit = Math.max(circuit.largestComponentSize(), 10);
        for (int round = 0; round < limit; round++) {
            long next = DBSPVariablePath.nextGeneratedName();
            var probe = this.invoke("Topology/CSE", OptimizerTopology.encode(circuit), next, EMPTY_CONTEXT);
            var payloads = new HashSet<Long>();
            probe.required("payloads").forEach(id -> payloads.add(id.longValue()));
            if (payloads.isEmpty()) {
                visitConstants(circuit.getAllOperators(), seen::add);
                return circuit;
            }
            var writer = OptimizerTopology.selectedExpressions(this.compiler, circuit, payloads, OptimizerTopology.Details.NONE);
            var result = this.invoke("CSE", writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                    next, this.passState(circuit, writer));
            visitConstants(circuit.getAllOperators(), seen::add);
            var pairs = result.required("aliases");
            if (pairs.isEmpty())
                return circuit;
            var live = new HashMap<Long, DBSPOperator>();
            visitOperators(circuit.getAllOperators(), op -> live.put(op.id, op));
            var aliases = new HashMap<DBSPOperator, DBSPOperator>();
            for (var pair : pairs) {
                var original = Objects.requireNonNull(live.get(pair.required(0).longValue()), "Unknown CSE operand");
                var base = Objects.requireNonNull(live.get(pair.required(1).longValue()), "Unknown CSE representative");
                aliases.put(original, base);
            }
            var output = new CSE.RemoveCSE(this.compiler, aliases).apply(circuit);
            if (output.sameCircuit(circuit))
                return circuit;
            circuit = output;
        }
        this.compiler.reportError(SourcePositionRange.INVALID, "InfiniteLoop",
                "Repeated optimization CSE " + limit + " times without convergence");
        return circuit;
    }

    private DBSPCircuit pruneCircuit(DBSPCircuit circuit, JsonNode pruning) {
        JsonNode removed = pruning.required("pruned_operators");
        if (removed.isEmpty())
            return circuit;
        var drop = new HashSet<Long>();
        for (var id : removed)
            drop.add(id.longValue());
        var keep = new HashSet<DBSPOperator>();
        visitOperators(circuit.getAllOperators(), op -> {
            if (!drop.remove(op.id))
                keep.add(op);
        });
        if (!drop.isEmpty())
            throw new IllegalStateException("DeadCode returned unknown operators " + drop);
        this.reportUnusedSources(circuit, pruning.required("unused_sources"));
        return new RemoveOperators(this.compiler, keep).apply(circuit);
    }

    /** Apply the Rust deletion list without decoding the surviving expressions.
     * Later expression visitors use object identity when sharing constants. */
    private DBSPCircuit removeSources(DBSPCircuit circuit, JsonNode sourceIds) {
        if (!sourceIds.isArray())
            throw new IllegalStateException("RemoveTable requires an array of source IDs");
        if (sourceIds.isEmpty())
            return circuit;
        var sources = new HashSet<Long>();
        for (var id : sourceIds)
            sources.add(id.longValue());
        DBSPCircuit result = new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void postorder(DBSPSourceMultisetOperator input) {
                if (!sources.remove(input.id))
                    super.postorder(input);
            }
        }.apply(circuit);
        if (!sources.isEmpty())
            throw new IllegalStateException("RemoveTable returned an unknown source");
        this.compiler.removeTable(Objects.requireNonNull(this.tableToRemove));
        return result;
    }

    private void findWindowGroups(DBSPCircuit circuit, FindWindowGroups analysis, long next) throws IOException {
        analysis.groups.clear();
        var windows = new HashMap<Long, DBSPWindowOperator>();
        var payloads = new HashSet<Long>();
        visitOperators(circuit.getAllOperators(), op -> {
            if (op instanceof DBSPWindowOperator window) {
                windows.put(op.id, window);
                payloads.add(op.id);
                if (window.left().node() instanceof DBSPMapIndexOperator index)
                    payloads.add(index.id);
            }
        });
        if (windows.isEmpty() || this.compiler.metadata.windowSharingDisabled())
            return;
        var writer = OptimizerTopology.selectedExpressions(this.compiler, circuit, payloads, OptimizerTopology.Details.NONE);
        var candidates = this.invoke("FindWindowGroups", writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                next, EMPTY_CONTEXT);
        var selected = new ArrayList<DBSPWindowOperator>();
        for (long id : MAPPER.treeToValue(candidates, long[].class))
            selected.add(Objects.requireNonNull(windows.get(id), "Rust returned an unknown window"));
        if (selected.isEmpty())
            return;
        NativeWindowGroups.install(this.compiler, analysis, selected, new NativeWindowGroups.Selection() {
            public int[][] group(List<String> inputs, int threshold) throws IOException {
                var groups = invoke("WindowGroups/Keys", MAPPER.writeValueAsBytes(inputs), next,
                        MAPPER.writeValueAsBytes(threshold));
                return MAPPER.treeToValue(groups, int[][].class);
            }

            public NativeWindowGroups.Fields share(List<List<String>> fields) throws IOException {
                var plan = invoke("WindowGroups/Fields", MAPPER.writeValueAsBytes(fields), next, EMPTY_CONTEXT);
                return MAPPER.treeToValue(plan, NativeWindowGroups.Fields.class);
            }
        });
    }

    private JsonNode invoke(String pass, byte[] input, long next, byte[] state) throws IOException {
        return MAPPER.readTree(transformWithContext(pass, input, next, DBSPCompiler.ERROR_VIEW_NAME.name(), state));
    }

    private static List<long[]> wirePorts(Set<OutputPort> ports) {
        return ports.stream().map(p -> new long[]{p.operator.id, p.outputNumber}).toList();
    }

    private static Set<OutputPort> readPorts(DBSPCircuit circuit, JsonNode result) throws IOException {
        var pending = new HashMap<Long, List<Integer>>();
        for (var pair : MAPPER.treeToValue(result, long[][].class)) {
            if (pair.length != 2)
                throw new IllegalStateException("Expected an operator id and output number");
            pending.computeIfAbsent(pair[0], id -> new ArrayList<>()).add(Math.toIntExact(pair[1]));
        }
        var ports = new HashSet<OutputPort>();
        visitOperators(circuit.allOperators, op -> {
            var outputs = pending.remove(op.id);
            if (outputs != null)
                for (int output : outputs)
                    ports.add(op.getOutput(output));
        });
        if (!pending.isEmpty())
            throw new IllegalStateException("Unknown port producers " + pending.keySet());
        return ports;
    }

    private JsonNode retentionFacts(DBSPCircuit circuit, long initial, byte[] state) throws IOException {
        var result = this.invoke("Topology/" + this.nativeName,
                OptimizerTopology.encode(circuit, OptimizerTopology.Details.GC_GROUPS), initial, state);
        if (result.has("needs_full_snapshot")) {
            var writer = this.snapshots.writer();
            writer.apply(circuit);
            result = this.invoke("Live/" + this.nativeName, writer.getJsonString().getBytes(StandardCharsets.UTF_8), initial, state);
        }
        return result.required("facts");
    }

    private static Map<Long, DBSPOperator> operatorsById(DBSPCircuit circuit) {
        var operators = new HashMap<Long, DBSPOperator>();
        visitOperators(circuit.getAllOperators(), op -> operators.put(op.id, op));
        return operators;
    }

    private record StateFinding(long operator, List<Integer> inputs) {}

    private record OwnedOperator(DBSPOperator operator, ICircuit body) {}

    private static void indexOwners(ICircuit body, Map<Long, OwnedOperator> owners) {
        for (var op : body.getAllOperators()) {
            owners.put(op.id, new OwnedOperator(op, body));
            if (op instanceof DBSPNestedOperator nested)
                indexOwners(nested, owners);
        }
    }

    private void collectUnbounded(DBSPCircuit circuit, long next, FindUnboundedState.CollectUnbounded collect) throws IOException {
        var bounded = wirePorts(collect.boundedPorts());
        var result = this.invoke(this.nativeName, OptimizerTopology.stateBounds(this.compiler, circuit), next,
                MAPPER.writeValueAsBytes(bounded));
        var findings = MAPPER.treeToValue(result, StateFinding[].class);
        if (findings.length == 0)
            return;
        var owners = new HashMap<Long, OwnedOperator>();
        indexOwners(circuit, owners);
        for (var finding : findings) {
            var owner = Objects.requireNonNull(owners.get(finding.operator), "Unknown unbounded operator");
            collect.record(new FindUnboundedState.UnboundedOperator(owner.operator, owner.body, finding.inputs));
        }
    }

    private record SharedProjection(long producer, long[] readers, int depth, List<List<Integer>> paths) {}

    private void findCommonProjections(DBSPCircuit circuit, long next, FindCommonProjections analysis) throws IOException {
        analysis.clear();
        var details = OptimizerTopology.Details.CONSUMERS;
        var payloads = this.invoke("Topology/FindCommonProjections", OptimizerTopology.encode(circuit, details), next, EMPTY_CONTEXT);
        var selected = MAPPER.convertValue(payloads, new TypeReference<Set<Long>>() {});
        if (selected.isEmpty())
            return;
        var writer = OptimizerTopology.selectedExpressions(this.compiler, circuit, selected, details);
        var result = this.invoke(this.nativeName, writer.getJsonString().getBytes(StandardCharsets.UTF_8), next, EMPTY_CONTEXT);
        var operators = operatorsById(circuit);
        for (var plan : MAPPER.treeToValue(result, SharedProjection[].class)) {
            var readers = java.util.Arrays.stream(plan.readers)
                    .mapToObj(id -> operators.get(id).to(DBSPUnaryOperator.class)).toList();
            analysis.record(operators.get(plan.producer), readers, plan.depth, plan.paths);
        }
    }

    private record InputFieldUse(long reader, List<List<Integer>> paths) {}

    private void findInputFields(DBSPCircuit circuit, long next, FindUnusedInputFields analysis) throws IOException {
        var readers = this.invoke("Topology/FindUnusedInputFields", OptimizerTopology.encode(circuit), next, EMPTY_CONTEXT);
        if (readers.isEmpty())
            return;
        var selected = MAPPER.convertValue(readers, new TypeReference<Set<Long>>() {});
        var writer = OptimizerTopology.selectedExpressions(this.compiler, circuit, selected, OptimizerTopology.Details.NONE);
        var uses = this.invoke(this.nativeName, writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                next, EMPTY_CONTEXT);
        var operators = operatorsById(circuit);
        for (var use : MAPPER.treeToValue(uses, InputFieldUse[].class)) {
            if (!selected.remove(use.reader))
                throw new IllegalStateException("Unexpected input projection " + use.reader);
            analysis.record(operators.get(use.reader).to(DBSPUnaryOperator.class), use.paths);
        }
        if (!selected.isEmpty())
            throw new IllegalStateException("Missing input projections " + selected);
    }

    private void analyzeMultiplicities(DBSPCircuit circuit, long next, MultisetAnalysis analysis) throws IOException {
        var plan = this.invoke("Topology/MultisetAnalysis", OptimizerTopology.encode(circuit), next, EMPTY_CONTEXT);
        final JsonNode result;
        if (plan.has("facts")) {
            result = plan.required("facts");
        } else {
            var payloads = new HashSet<Long>();
            plan.required("payloads").forEach(id -> payloads.add(id.longValue()));
            var writer = OptimizerTopology.selectedExpressions(this.compiler, circuit, payloads, OptimizerTopology.Details.NONE);
            result = this.invoke("MultisetAnalysis", writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                    next, EMPTY_CONTEXT);
        }
        var facts = new HashMap<OutputPort, Boolean>();
        visitOperators(circuit.getAllOperators(), op -> {
            for (int i = 0; i < op.outputCount(); i++) {
                if (op instanceof DBSPNestedOperator nested && nested.internalOutputs.get(i) == null)
                    continue;
                JsonNode value = result.required(Long.toString(op.id)).required(i);
                if (!value.isBoolean())
                    throw new IllegalStateException("Missing multiplicity for " + op.id + " output " + i);
                facts.put(op.getOutput(i), value.booleanValue());
            }
        });
        analysis.restore(facts);
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        long initial = DBSPVariablePath.nextGeneratedName();
        try {
            byte[] state = EMPTY_CONTEXT;
            ToJsonOuterVisitor writer = null;
            if (this.seenConstants != null)
                return this.applyCse(circuit);
            if (this.commonProjections != null) {
                this.findCommonProjections(circuit, initial, this.commonProjections);
                return circuit;
            }
            if (this.mapFusion != null && !this.mapFusion.hasClusters())
                return circuit;
            if (this.starJoins != null) {
                state = this.starJoins.nativeReplacements();
                if (state.length == 0)
                    return circuit;
            }
            if (this.replaceCommon != null && this.replaceCommon.analysis().outputProjection.isEmpty())
                return circuit;
            if (this.deferCarried != null) {
                var layouts = this.deferCarried.layouts();
                if (layouts.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(layouts);
            }
            if (this.trimFilters != null) {
                var readers = this.trimFilters.candidateReaders(circuit, true);
                if (readers.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(readers);
            }
            if (this.trimInputs != null) {
                var layouts = this.trimInputs.layouts();
                if (layouts.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(layouts);
            }
            if (this.inputFields != null) {
                this.findInputFields(circuit, initial, this.inputFields);
                return circuit;
            }
            if (this.multiplicities != null) {
                this.analyzeMultiplicities(circuit, initial, this.multiplicities);
                return circuit;
            }
            if (this.clockUses != null) {
                if (!this.clockUses.found()) {
                    var found = this.invoke(this.nativeName, OptimizerExpressions.operatorCalls(this.compiler, circuit),
                            initial, state);
                    if (!found.isBoolean())
                        throw new IllegalStateException("Expected a boolean clock-use result");
                    this.clockUses.recordClockUse(found.booleanValue());
                }
                return circuit;
            }
            if (this.hashPrefixes != null) {
                var inputs = this.hashPrefixes.inputs();
                if (inputs.hashes().isEmpty())
                    return circuit;
                var result = this.invoke(this.nativeName, OptimizerTopology.encode(circuit), initial,
                        MAPPER.writeValueAsBytes(inputs));
                Map<Long, String> prefixes = MAPPER.convertValue(result.required("prefixes"), new TypeReference<>() {});
                this.hashPrefixes.restore(prefixes, operatorsById(circuit));
                return circuit;
            }
            if (this.setHashes != null) {
                var result = this.invoke(this.nativeName, OptimizerTopology.encode(circuit), initial,
                        MAPPER.writeValueAsBytes(this.setHashes.computedHashes()));
                return this.applyResult(circuit, null, result);
            }
            if (this.traceInputs != null) {
                var plan = MAPPER.treeToValue(this.retentionFacts(circuit, initial, state), MergeGC.SharedTraceInputs.class);
                this.traceInputs.restore(plan, plan.aliases().isEmpty() && plan.removed().isEmpty() ? Map.of() : operatorsById(circuit));
                return circuit;
            }
            if (this.retentionGroups != null) {
                state = MAPPER.writeValueAsBytes(this.retentionGroups.groupedIds());
                var groups = MAPPER.treeToValue(this.retentionFacts(circuit, initial, state), long[][].class);
                if (groups.length != 0)
                    this.retentionGroups.restore(groups, operatorsById(circuit));
                return circuit;
            }
            if (this.graph != null) {
                var result = this.invoke(this.nativeName, OptimizerTopology.encode(circuit,
                        OptimizerTopology.Details.CONSUMERS), initial, state);
                var edges = MAPPER.convertValue(result, new TypeReference<Map<Long, int[][][]>>() {});
                this.graph.restore(circuit, edges);
                return circuit;
            }
            if (this.streaming != null) {
                var result = this.invoke(this.nativeName, OptimizerTopology.encode(circuit,
                        OptimizerTopology.Details.STREAMING), initial, state);
                if (!result.isBoolean())
                    throw new IllegalStateException("Expected a boolean streaming classification");
                this.streaming.record(result.booleanValue());
                return circuit;
            }
            if (this.bounded != null) {
                var context = Map.of("known", wirePorts(this.bounded.boundedPorts()),
                        "keys", wirePorts(this.bounded.indexedKeys(circuit)), "now_table", DBSPCompiler.NOW_TABLE_NAME.name());
                var result = this.invoke(this.nativeName, OptimizerTopology.stateBounds(this.compiler, circuit),
                        initial, MAPPER.writeValueAsBytes(context));
                this.bounded.record(readPorts(circuit, result));
                return circuit;
            }
            if (this.unbounded != null) {
                this.collectUnbounded(circuit, initial, this.unbounded);
                return circuit;
            }
            if (this.garbageCollected != null) {
                var result = this.invoke(this.nativeName, OptimizerTopology.encode(circuit), initial, state);
                this.garbageCollected.record(readPorts(circuit, result));
                return circuit;
            }
            if (this.appendOnly != null) {
                var result = this.invoke(this.nativeName, OptimizerTopology.encode(circuit,
                        OptimizerTopology.Details.TABLE_CHANGES), initial, state);
                var ids = new HashSet<Long>();
                for (long id : MAPPER.treeToValue(result, long[].class))
                    ids.add(id);
                visitOperators(circuit.getAllOperators(), op -> {
                    if (ids.remove(op.id))
                        this.appendOnly.appendOnly.add(new OutputPort(op, 0));
                });
                if (!ids.isEmpty())
                    throw new IllegalStateException("Unknown append-only operators " + ids);
                return circuit;
            }
            if (this.windowGroups != null) {
                this.findWindowGroups(circuit, this.windowGroups, initial);
                return circuit;
            }
            if (this.mapChains != null) {
                var result = this.invoke(this.nativeName, OptimizerTopology.encode(circuit,
                        OptimizerTopology.Details.SINK_KIND), initial, state);
                long[][] chains = MAPPER.treeToValue(result.required("map_chains"), long[][].class);
                if (chains.length > 0) {
                    var operators = new HashMap<Long, DBSPOperator>();
                    visitOperators(circuit.getAllOperators(), op -> operators.put(op.id, op));
                    this.mapChains.appendChains(chains, operators::get);
                }
                return circuit;
            }
            if (this.asofOrder != null) {
                var order = this.invoke(this.nativeName, OptimizerTopology.encode(circuit, OptimizerTopology.Details.CONSUMERS),
                        initial, MAPPER.writeValueAsBytes(this.asofOrder.edgeIds()));
                circuit.applyOperatorOrder(MAPPER.treeToValue(order, long[].class));
                return circuit;
            }
            if (this.errorOrder != null) {
                JsonNode order = this.invoke(this.nativeName, OptimizerTopology.encode(circuit, OptimizerTopology.Details.ERROR_CONSUMERS),
                        initial, MAPPER.writeValueAsBytes(DBSPCompiler.ERROR_TABLE_NAME.name()));
                this.errorOrder.applyOrder(circuit, MAPPER.treeToValue(order.required("order"), long[].class),
                        order.required("first_error").intValue());
                return circuit;
            }
            if (this.tableToRemove != null) {
                JsonNode selected = this.invoke(this.nativeName, OptimizerTopology.encode(circuit, OptimizerTopology.Details.TABLE_NAMES), initial,
                        MAPPER.writeValueAsBytes(this.tableToRemove.name()));
                return this.removeSources(circuit, selected.required("remove_sources"));
            }
            if (this.nativeName.equals("FixupViewReferences"))
                return this.applyViewReferences(circuit, this.invoke(this.nativeName,
                        OptimizerTopology.encode(circuit, OptimizerTopology.Details.RECURSIVE_VIEWS), initial, state));
            if (this.mergeRetain != null) {
                var groups = this.mergeRetain.memberIds();
                if (groups.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(groups);
            }
            if (this.mergeNoops != null) {
                var inputs = this.mergeNoops.sharedInputs();
                if (inputs.aliases().isEmpty() && inputs.removed().isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(inputs);
            }
            if (this.collapseChains != null) {
                var chains = this.collapseChains.chainIds();
                if (chains.stream().noneMatch(chain -> chain.size() > 1))
                    return circuit;
                state = MAPPER.writeValueAsBytes(chains);
            }
            if (this.windowImplementation != null) {
                var groups = this.windowImplementation.projections();
                if (groups.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(groups);
            }
            if (this.distinctOutputs != null) {
                var sinks = this.distinctOutputs.multisetSinks(circuit);
                if (sinks.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(sinks);
            }
            if (this.leftJoins != null) {
                var keyed = this.leftJoins.keyedLeftJoins(circuit);
                if (keyed.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(keyed);
            }
            JsonNode result = null;
            switch (this.nativeName) {
                case "UDFInliner" -> {
                    if (circuit.declarations.stream().noneMatch(decl ->
                            decl.item instanceof DBSPFunctionItem function && function.function.body != null))
                        return circuit;
                }
                case "SimplifyConditionals", "ExpandSafeCasts", "ExpandUnsafeCasts", "AdjustSqlIndex", "CreateRuntimeErrorWrappers" -> {
                    JsonNode probe = this.invoke("Expressions/" + this.nativeName,
                            OptimizerExpressions.inventory(this.compiler, circuit), initial, state);
                    if (!probe.required("needs_full_snapshot").booleanValue())
                        return circuit;
                }
                case "DeterministicFunctions" -> result = this.invoke(this.nativeName,
                        OptimizerExpressions.encode(this.compiler, circuit, OptimizerExpressions.Roots.INITIALIZERS),
                        initial, state);
                case "NoConnectorMetadata" -> result = this.invoke(this.nativeName,
                        OptimizerExpressions.executableCalls(this.compiler, circuit), initial, state);
                case "ComparatorDeclarations" -> {
                    byte[] types = OptimizerExpressions.encode(this.compiler, circuit, OptimizerExpressions.Roots.INDEX_TYPES);
                    if (!this.invoke("Topology/" + this.nativeName, types, initial, state)
                            .required("needs_full_snapshot").booleanValue())
                        return circuit;
                }
                case "CircuitStatistics" -> result = this.invoke(this.nativeName,
                        OptimizerTopology.statistics(this.compiler, circuit), initial, state);
                case "CheckPersistentIds" -> {
                    var findings = this.invoke(this.nativeName, OptimizerTopology.encode(circuit,
                            OptimizerTopology.Details.PERSISTENT_IDS), initial, state);
                    this.reportPersistentIds(circuit, findings.required("persistent_id_findings"));
                    return circuit;
                }
                case "ConstantViews" -> result = this.invoke(this.nativeName,
                        OptimizerTopology.encode(circuit, OptimizerTopology.Details.VIEW_NAMES), initial, state);
                case "CheckRetain", "ValidateRecursiveOperators", "NoIntegralVisitor", "CheckHints", "CompactNames", "TagRegions" ->
                        result = this.invoke(this.nativeName, OptimizerTopology.encode(circuit), initial, state);
                case "TrimWindows", "ExpandStreamAggregates", "IndexedInputs", "OptimizeProjectionVisitor", "PullFilterVisitor", "DesugarNowEquality", "BreakFilters", "SimplifyWaterline", "DecomposeExpensiveFilters", "FilterJoinVisitor", "ShareOutputIntegrators", "DuplicateSharedIndexes", "BalancedJoins", "DeadCode", "DeadCodeKeepSources", "RemoveNoops", "PushDifferentialsUp",
                     "FuseIntoArrayAgg", "PropagateConstants", "PropagateEmptySources", "RemoveConstantFilters", "CloneOperatorsWithFanout", "OptimizeDistinctVisitor",
                     "RemoveDeindexOperators", "RemoveIAfterD", "ImplementJoins", "MergeSums", "OptimizeIncrementalVisitor",
                     "RemoveViewOperators", "RemoveAllViews", "ExpandAggregateZero", "ExpandMetadataCasts",
                     "LinearPostprocessRetainKeys", "ExpandHop", "RemoveIdentityOperators", "BuildNestedOperators",
                     "SubstituteLeftJoins", "ChainVisitor", "ImplementChains", "StrayGC", "SeparateIntegrators" -> {
                    var details = switch (this.nativeName) {
                        case "SeparateIntegrators" -> OptimizerTopology.Details.TRACE_INTEGRATORS;
                        case "StrayGC" -> OptimizerTopology.Details.TRACE_RETENTION;
                        case "ExpandMetadataCasts" -> OptimizerTopology.Details.COLUMN_INITIALIZERS;
                        case "IndexedInputs" -> OptimizerTopology.Details.PRIMARY_KEYS;
                        case "PropagateEmptySources" -> OptimizerTopology.Details.EMPTY_LITERALS;
                        case "RemoveConstantFilters" -> OptimizerTopology.Details.FILTER_PREDICATES;
                        default -> OptimizerTopology.Details.NONE;
                    };
                    JsonNode probe = this.invoke("Topology/" + this.nativeName, OptimizerTopology.encode(circuit, details),
                            initial, state);
                    if (probe.has("stream_aggregate")) {
                        var aggregate = findOperator(circuit.allOperators, probe.required("stream_aggregate").longValue());
                        throw new InternalCompilerError("StreamAggregate operator should have been removed " + Objects.requireNonNull(aggregate));
                    }
                    if (probe.has("retention_violation"))
                        throw retentionError(circuit, probe.required("retention_violation"));
                    if (probe.has("pruned_operators"))
                        return this.pruneCircuit(circuit, probe);
                    if (!probe.required("needs_full_snapshot").booleanValue())
                        return circuit;
                }
            }
            if (result == null) {
                writer = this.snapshots.writer();
                writer.innerVisitor.liveNodes = new HashMap<>();
                writer.apply(circuit);
                if (this.mapFusion != null)
                    state = MapFusionState.encode(writer, this.mapFusion);
                else if (this.replaceCommon != null)
                    state = SharedProjectionState.encode(writer, this.replaceCommon.analysis());
                else if (state == EMPTY_CONTEXT)
                    state = this.passState(circuit, writer);
                result = this.invoke("Live/" + this.nativeName, writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                        initial, state);
            }
            JsonNode counter = result.required("next_generated_name");
            if (!counter.isIntegralNumber() || !counter.canConvertToLong()
                    || counter.longValue() < initial || counter.longValue() == Long.MAX_VALUE)
                throw new IllegalStateException(this.name + " returned an invalid name counter");
            DBSPCircuit output = this.applyResult(circuit, writer, result);
            DBSPVariablePath.restoreGeneratedNames(counter.longValue());
            return output;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid circuit from Rust pass " + this.name, error);
        }
    }
}
