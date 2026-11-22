package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.RawValue;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.ir.statement.DBSPComparatorItem;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.CheckPersistentIds;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindSourcePositions;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveUselessLeftJoinsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandMetadataCasts;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
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
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SimplifyWaterline;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CSE;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.MergeFilters;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ChainVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeWithGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CloneOperatorsWithFanout;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CreateStarJoins;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FuseExpensiveMaps;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.FindMapChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.FindSharedIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.ReplaceSharedIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.CollapseSharedChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitStatistics;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.PropagateEmptySources;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ExpandAggregateZero;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ExpandAggregates.ExpandStreamAggregates;
import org.dbsp.sqlCompiler.compiler.visitors.outer.EnsureDistinctOutputs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ConstantViews;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindDeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FuseIntoArrayAgg;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Gen2Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveStarJoins;
import org.dbsp.sqlCompiler.compiler.visitors.outer.TagRegions;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveIdentityOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizerStep;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeterministicFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindUnboundedState;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LinearPostprocessRetainKeys;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.HashPrefixPass;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.SetOperatorHashes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveTable;
import org.dbsp.sqlCompiler.compiler.visitors.outer.StrayGC;
import org.dbsp.sqlCompiler.compiler.visitors.outer.AppendOnly;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerAsof;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.ReorderTemporalFilters;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields.FindUnusedInputFields;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.DeferCarriedColumns;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.LeftJoinChainsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.TrimFilters;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields.TrimInputs;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindCommonProjections;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ReplaceCommonProjections;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.MultisetAnalysis;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.KeyColumns;
import org.dbsp.sqlCompiler.ir.type.CollectionShape;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.IndexedShape;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.CircuitContainsNow;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.CheckRetain;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneRefType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.WaterlineProjection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.LimiterInputs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.InsertLimiters;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.OrderErrorConsumers;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MergeGC;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveViewOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeProjectionVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ConstantExpressions;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Lineage;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.RustDeltaExpandOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.AggregateDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DistinctDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.OperatorDeltaExpansion;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.StarJoinFilterMapDeltaExpansion;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.StarJoinDeltaExpansion;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.compiler.visitors.inner.NativeValueNumbering;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.FindWindowGroups;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.ImplementWindowGroups;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.NativeWindowGroups;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.LinkedHashMap;
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
    @Nullable private final OptimizeProjectionVisitor outputProjections;
    public final boolean wrapsGraphOptimization;
    @Nullable private final Lineage lineage;
    private final OptimizerSnapshots snapshots;
    @Nullable private final FindMapChains mapChains;
    @Nullable private final FindSharedIndexes sharedIndexes;
    @Nullable private final ReplaceSharedIndexes replaceSharedIndexes;
    @Nullable private final MergeFilters mergeFilters;
    private boolean usedGeneralTemporalAnalysis;

    public boolean usedGeneralTemporalAnalysis() {
        return this.usedGeneralTemporalAnalysis;
    }

    @Nullable private final FindUnboundedState.DetectStreamingOperations streaming;
    @Nullable private final FindUnboundedState.FindGCedStreams garbageCollected;
    @Nullable private final FindUnboundedState.FindBounded bounded;
    @Nullable private final FindUnboundedState.CollectUnbounded unbounded;
    @Nullable private final Graph graph;
    @Nullable private final CloneOperatorsWithFanout fanout;
    @Nullable private final HashPrefixPass hashPrefixes;
    @Nullable private final SetOperatorHashes setHashes;
    @Nullable private final MergeGC.MergeRetain mergeRetain;
    @Nullable private final MergeGC.MergeNoops mergeNoops;
    @Nullable private final MergeGC.FindEquivalentNoops traceInputs;
    @Nullable private final MergeGC.FindMultipleRetains retentionGroups;
    @Nullable private final AppendOnly appendOnly;
    @Nullable private final ExpandStreamAggregates streamAggregates;
    @Nullable private final FindUnusedInputFields inputFields;
    @Nullable private final InsertLimiters insertLimiters;
    @Nullable private final TrimInputs trimInputs;
    @Nullable private final TrimFilters trimFilters;
    @Nullable private final DeferCarriedColumns deferCarried;
    @Nullable private final FindCommonProjections commonProjections;
    @Nullable private final FuseExpensiveMaps.Fuse mapFusion;
    @Nullable private final CreateStarJoins.FindJoinTrees joinTrees;
    @Nullable private final CreateStarJoins.ReplaceStarJoins starJoins;
    @Nullable private final ReplaceCommonProjections replaceCommon;
    @Nullable private final LowerAsof.LowerAsofInner asofLowering;
    @Nullable private final LowerAsof.MoveGC asofRetention;
    @Nullable private final LowerAsof.OrderGcDependencies asofOrder;
    @Nullable private final MultisetAnalysis multiplicities;
    @Nullable private final EnsureDistinctOutputs distinctOutputs;
    @Nullable private final CircuitContainsNow clockUses;
    @Nullable private final ReorderTemporalFilters.Scan temporalScan;
    @Nullable private final ReorderTemporalFilters.Reorder temporalOrder;
    @Nullable private final CollapseSharedChains collapseChains;
    @Nullable private final FindWindowGroups windowGroups;
    @Nullable private final ImplementWindowGroups windowImplementation;
    @Nullable private final RemoveUselessLeftJoinsVisitor leftJoins;
    @Nullable private final OrderErrorConsumers errorOrder;
    @Nullable private final ProgramIdentifier tableToRemove;
    @Nullable private final CircuitTransform stages;
    @Nullable private final FuseIntoArrayAgg.FindFusable arrayCandidates;
    @Nullable private final FuseIntoArrayAgg.Fuse arrayFusion;
    @Nullable private final TagRegions.DetectConflicts regionConflicts;
    @Nullable private final RemoveIdentityOperators.Decide identityMaps;
    @Nullable private final ExpandAggregateZero.ExpandAggregateZeros aggregateZeros;
    @Nullable private final FindDeadCode deadCode;
    @Nullable private final CSE.FindCSE commonExpressions;
    @Nullable private final Set<DBSPOperator> seenConstants;
    private static final byte[] EMPTY_CONTEXT = new byte[0];

    /** Expansion facts for an InsertLimiters checkpoint, including when Rust runs the pass. */
    @Nullable InsertLimiters limiterInputs() {
        return this.insertLimiters;
    }

    public RustCircuitPass(DBSPCompiler compiler, CircuitTransform original, OptimizerSnapshots snapshots) {
        this.compiler = compiler;
        this.name = original.getName();
        this.lineage = original instanceof Lineage report ? report : null;
        this.wrapsGraphOptimization = original instanceof OptimizeWithGraph;
        this.temporalScan = original instanceof ReorderTemporalFilters.Scan scan ? scan : null;
        this.temporalOrder = original instanceof ReorderTemporalFilters.Reorder reorder ? reorder : null;
        if (original instanceof FuseIntoArrayAgg.Fuse)
            this.nativeName = "FuseArrayReaders";
        else if (original instanceof ExpandAggregateZero.ExpandAggregateZeros)
            this.nativeName = "ExpandAggregateZero";
        else if (original instanceof RemoveIdentityOperators.Decide)
            this.nativeName = "IdentityDecisions";
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
        this.fanout = original instanceof CloneOperatorsWithFanout fanout ? fanout : null;
        this.streaming = original instanceof FindUnboundedState.DetectStreamingOperations detector ? detector : null;
        this.bounded = original instanceof FindUnboundedState.FindBounded propagation ? propagation : null;
        this.unbounded = original instanceof FindUnboundedState.CollectUnbounded collect ? collect : null;
        this.garbageCollected = original instanceof FindUnboundedState.FindGCedStreams streams ? streams : null;
        this.mapFusion = original instanceof FuseExpensiveMaps.Fuse fuse ? fuse : null;
        this.joinTrees = original instanceof CreateStarJoins.FindJoinTrees find ? find : null;
        this.starJoins = original instanceof CreateStarJoins.ReplaceStarJoins joins ? joins : null;
        this.replaceCommon = original instanceof ReplaceCommonProjections replace ? replace : null;
        this.asofLowering = original instanceof LowerAsof.LowerAsofInner lower ? lower : null;
        this.asofRetention = original instanceof LowerAsof.MoveGC move ? move : null;
        this.asofOrder = original instanceof LowerAsof.OrderGcDependencies order ? order : null;
        this.commonProjections = original instanceof FindCommonProjections common ? common : null;
        this.deferCarried = original instanceof DeferCarriedColumns defer ? defer : null;
        this.trimFilters = original instanceof TrimFilters trim ? trim : null;
        this.insertLimiters = original instanceof InsertLimiters limiters ? limiters : null;
        this.trimInputs = original instanceof TrimInputs trim ? trim : null;
        this.outputProjections = original instanceof OptimizeProjectionVisitor projections ? projections : null;
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
        this.sharedIndexes = original instanceof FindSharedIndexes find ? find : null;
        this.mergeFilters = original instanceof MergeFilters merge ? merge : null;
        this.replaceSharedIndexes = original instanceof ReplaceSharedIndexes replace ? replace : null;
        this.mapChains = original instanceof FindMapChains find ? find : null;
        this.errorOrder = original instanceof OrderErrorConsumers order ? order : null;
        this.tableToRemove = original instanceof RemoveTable table ? table.tableName() : null;
        this.stages = this.selectStages(original);
        this.arrayCandidates = original instanceof FuseIntoArrayAgg.FindFusable find ? find : null;
        this.arrayFusion = original instanceof FuseIntoArrayAgg.Fuse fuse ? fuse : null;
        this.aggregateZeros = original instanceof ExpandAggregateZero.ExpandAggregateZeros zeros ? zeros : null;
        this.regionConflicts = original instanceof TagRegions.DetectConflicts detect ? detect : null;
        this.identityMaps = original instanceof RemoveIdentityOperators.Decide decide ? decide : null;
        this.deadCode = original instanceof FindDeadCode dead ? dead : null;
        this.commonExpressions = original instanceof CSE.FindCSE common ? common : null;
        this.seenConstants = this.commonExpressions != null ? Collections.newSetFromMap(new WeakHashMap<>()) : null;
    }

    private static native void enterSnapshotScope();
    private static native void leaveSnapshotScope();

    /** Drive an observed pass to convergence. The step returns true when the circuit changed.
     * A positive result is the number of iterations that exhausted the convergence limit. */
    public static native int repeat(int largestComponent, int requested, IntPredicate step);

    public static List<OptimizerStep> optimizerOrder(boolean outputsAreSets, boolean incrementalize) {
        try {
            byte[] options = MAPPER.writeValueAsBytes(Map.of(
                    "outputs_are_sets", outputsAreSets, "incrementalize", incrementalize));
            byte[] reply = transformWithContext("OptimizerOrder", options, 0, "", EMPTY_CONTEXT);
            return MAPPER.readValue(reply, new TypeReference<>() {});
        } catch (IOException error) {
            throw new UncheckedIOException("Could not read the Rust optimizer pass order", error);
        }
    }

    /** Release cached expressions when this optimizer returns, including on pass failure. */
    public static DBSPCircuit withLiveSnapshots(Supplier<DBSPCircuit> optimize) {
        enterSnapshotScope();
        try {
            return optimize.get();
        } finally {
            leaveSnapshotScope();
        }
    }

    private static native byte[] transformWithContext(String pass, byte[] circuit, long next, String errorView, byte[] state);

    private static JsonDecoder expressionDecoder(ToJsonInnerVisitor writer) {
        var decoder = new JsonDecoder();
        Objects.requireNonNull(writer.liveNodes).forEach(decoder.inner::cache);
        return decoder;
    }

    private static <T extends DBSPExpression> T loweredFunction(DBSPCompiler compiler,
            ToJsonInnerVisitor writer, JsonNode result, Class<T> kind) {
        T function = expressionDecoder(writer).decodeInner(result.required("function"), kind);
        for (var warning : result.required("warnings"))
            compiler.reportWarning(SourcePositionRange.fromJson(warning.required("range")),
                    warning.required("error_type").textValue(), warning.required("message").textValue());
        return function;
    }

    @Nullable
    public static DBSPExpression unaryWaterline(DBSPCompiler compiler, DBSPExpression operation, DBSPExpression bound) {
        return tryLowerFunction(compiler, "Monotonicity/Unary", operation, bound, DBSPExpression.class);
    }

    @Nullable
    public static MonotoneExpression projectRow(DBSPCompiler compiler, DBSPExpression body,
                                                DBSPVariablePath bound, IMaybeMonotoneType input) {
        return projectRows(compiler, body, new DBSPVariablePath[]{bound}, new IMaybeMonotoneType[]{input}, false);
    }

    @Nullable
    public static MonotoneExpression projectRows(DBSPCompiler compiler, DBSPExpression body, DBSPVariablePath[] bounds,
                                                 IMaybeMonotoneType[] inputs, boolean indexed) {
        IMaybeMonotoneType[] parameters = Linq.map(inputs, MonotoneRefType::new, IMaybeMonotoneType.class);
        if (indexed)
            parameters = new IMaybeMonotoneType[]{new PartiallyMonotoneTuple(List.of(parameters),
                    bounds[0].getType().is(DBSPTypeRawTuple.class), false)};
        return projectClosureBody(compiler, body, bounds, parameters, indexed);
    }

    /** Analyze a closure against its live parameter types, retaining reference identity across reads. */
    @Nullable
    public static MonotoneExpression projectClosureBody(DBSPCompiler compiler, DBSPExpression body,
            DBSPVariablePath[] bounds, IMaybeMonotoneType[] parameters, boolean indexed) {
        try {
            var roots = new IDBSPInnerNode[bounds.length + 1];
            roots[0] = body;
            System.arraycopy(bounds, 0, roots, 1, bounds.length);
            var shapes = WaterlineProjection.inputShapes(parameters, indexed);
            String pass = indexed ? "Monotonicity/ProjectIndexedRow"
                    : shapes.size() == 1 ? "Monotonicity/ProjectRow" : "Monotonicity/ProjectJoinRow";
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(shapes.size() == 1 ? shapes.get(0) : shapes), roots);
            if (reply.value.isNull())
                return null;
            var nodes = Objects.requireNonNull(reply.writer.liveNodes);
            var result = WaterlineProjection.resolveType(reply.value.required("row_type"),
                    id -> Utilities.getExists(nodes, id).to(DBSPType.class), parameters);
            var reduced = reply.value.required("bound");
            return new MonotoneExpression(body, result, reduced.isNull() ? null
                    : expressionDecoder(reply.writer).decodeInner(reduced, DBSPExpression.class));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode a row waterline projection", error);
        }
    }

    public record ProjectedWaterline(IMaybeMonotoneType bodyType, @Nullable DBSPClosureExpression closure) {}

    @Nullable
    public static ProjectedWaterline projectWaterlineClosure(DBSPCompiler compiler, DBSPExpression body,
            DBSPParameter[] parameters, DBSPType[] projectedTypes, IMaybeMonotoneType[] argumentShapes, boolean indexed) {
        var roots = new ArrayList<IDBSPInnerNode>();
        roots.add(body);
        for (int i = 0; i < parameters.length; i++) {
            roots.add(parameters[i]);
            roots.add(projectedTypes[i]);
        }
        var request = MAPPER.createObjectNode();
        request.put("indexed", indexed);
        request.set("shapes", MAPPER.valueToTree(WaterlineProjection.inputShapes(argumentShapes, indexed)));
        try {
            var reply = invokeLowering(compiler, "Monotonicity/ProjectClosure",
                    MAPPER.writeValueAsBytes(request), roots.toArray(IDBSPInnerNode[]::new));
            if (!answered(compiler, "Monotonicity/ProjectClosure", reply.value))
                return null;
            var nodes = Objects.requireNonNull(reply.writer.liveNodes);
            var type = WaterlineProjection.resolveType(reply.value.required("row_type"),
                    id -> Utilities.getExists(nodes, id).to(DBSPType.class), argumentShapes);
            var closure = reply.value.required("closure");
            return new ProjectedWaterline(type, closure.isNull() ? null :
                    expressionDecoder(reply.writer).decodeInner(closure, DBSPClosureExpression.class));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode a waterline closure", error);
        }
    }

    public static DBSPExpression nullLiteralWaterline(DBSPCompiler compiler, DBSPType type) {
        return lowerFunction(compiler, "Monotonicity/NullLiteral", type, null, DBSPExpression.class);
    }

    public static DBSPFold lowerAggregateFold(DBSPCompiler compiler, DBSPAggregateList aggregates) {
        return lowerFunction(compiler, "LowerAggregateFold", aggregates,
                aggregates.isEmpty() ? DBSPTypeTuple.EMPTY : null, DBSPFold.class);
    }

    public static DBSPClosureExpression lagTransfer(DBSPCompiler compiler, DBSPLagOperator lag) {
        byte[] offset = Integer.toString(lag.offset).getBytes(StandardCharsets.US_ASCII);
        return loweredClosure(compiler, invokeLowering(compiler, "Monotonicity/Lag", offset,
                lag.getClosureFunction(), lag.comparator, lag.input().getOutputIndexedZSetType()));
    }

    public static DBSPClosureExpression asofTransfer(DBSPCompiler compiler, DBSPAsofJoinOperator join) {
        var site = expressionSite(join.getNode());
        site.putArray("timestamps").add(join.leftTimestampIndex).add(join.rightTimestampIndex);
        try {
            return loweredClosure(compiler, invokeLowering(compiler, "Monotonicity/Asof", MAPPER.writeValueAsBytes(site),
                    join.getKeyType(), join.getClosureFunction(),
                    join.right().getOutputIndexedZSetType().elementType));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode ASOF waterline projection", error);
        }
    }

    public static JsonNode combineBounds(JsonNode inputs, boolean union) {
        return exchangeBounds(inputs, union ? "Monotonicity/Union" : "Monotonicity/Intersection");
    }

    public static JsonNode indexedBounds(DBSPCompiler compiler, JsonNode inputs, boolean antiJoin) {
        String operation = antiJoin ? "Monotonicity/AntiJoinProjection" : "Monotonicity/JoinArguments";
        var result = exchangeBounds(inputs, operation);
        answered(compiler, operation, result);
        return result;
    }

    private static JsonNode exchangeBounds(JsonNode inputs, String pass) {
        try {
            return MAPPER.readTree(transformWithContext(pass, MAPPER.writeValueAsBytes(inputs), 0, "", EMPTY_CONTEXT));
        } catch (IOException error) {
            throw new UncheckedIOException("Could not combine waterline projections", error);
        }
    }

    public static DBSPClosureExpression hopTransfer(DBSPCompiler compiler, DBSPHopOperator hop, DBSPType inputRow) {
        var site = expressionSite(hop.getNode());
        site.put("timestamp_index", hop.timestampIndex);
        try {
            return loweredClosure(compiler, invokeLowering(compiler, "Monotonicity/Hop", MAPPER.writeValueAsBytes(site),
                    inputRow, hop.getOutputZSetElementType(), hop.interval, hop.size, hop.start));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode HOP timestamp position", error);
        }
    }

    /** Bind the projected inputs without serializing the already analyzed closure body. */
    public static DBSPClosureExpression bindWaterlineInputs(DBSPCompiler compiler, DBSPParameter[] parameters,
            DBSPType[] types, DBSPExpression body) {
        try {
            var names = Linq.map(parameters, DBSPParameter::getName, String.class);
            var reply = invokeLowering(compiler, "Monotonicity/WaterlineBindings", MAPPER.writeValueAsBytes(names), types);
            var decoder = expressionDecoder(reply.writer);
            var nodes = reply.value.elements();
            var input = decoder.decodeInner(nodes.next(), DBSPParameter.class);
            var statements = new ArrayList<DBSPStatement>();
            nodes.forEachRemaining(node -> statements.add(decoder.decodeInner(node, DBSPLetStatement.class)));
            return new DBSPBlockExpression(statements, body).closure(input);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode waterline parameter names", error);
        }
    }

    public static Pair<DBSPClosureExpression, DBSPClosureExpression> identityBounds(
            DBSPCompiler compiler, DBSPType row, DBSPType projected, boolean indexed) {
        String operation = indexed ? "Monotonicity/IndexedIdentityBounds" : "Monotonicity/RowIdentityBounds";
        return loweredClosurePair(invokeLowering(compiler, operation, row, projected));
    }

    public static DBSPClosureExpression aggregateTransfer(DBSPCompiler compiler, DBSPType input,
                                                            DBSPTypeIndexedZSet output, boolean rolling) {
        return lowerFunction(compiler, rolling ? "Monotonicity/Rolling" : "Monotonicity/Group",
                input, output, DBSPClosureExpression.class);
    }

    public static DBSPClosureExpression minimumTransfer(DBSPCompiler compiler, CalciteObject origin,
            DBSPType input, @Nullable DBSPClosureExpression postprocess) {
        try {
            return loweredClosure(compiler, invokeLowering(compiler, "Monotonicity/Minimum",
                    MAPPER.writeValueAsBytes(expressionSite(origin)), input, postprocess));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode aggregate source position", error);
        }
    }

    @Nullable
    public static Boolean isMinimumChain(DBSPCompiler compiler, DBSPChainAggregateOperator chain) {
        var result = invokeLowering(compiler, "Monotonicity/IsMinimumChain", chain.init, chain.getClosureFunction()).value;
        return answered(compiler, "Monotonicity/IsMinimumChain", result) ? result.booleanValue() : null;
    }

    @Nullable
    public static DBSPClosureExpression chainMinimumTransfer(DBSPCompiler compiler,
            DBSPChainAggregateOperator chain, DBSPType input) {
        try {
            var reply = invokeLowering(compiler, "Monotonicity/ChainMinimum",
                    MAPPER.writeValueAsBytes(expressionSite(chain.getNode())), input,
                    chain.getOutputIndexedZSetType().elementType, chain.getClosureFunction());
            if (reply.value.required("function").isNull()) {
                finishLowering(compiler, reply.value);
                return null;
            }
            return loweredClosure(compiler, reply);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode chain aggregate source position", error);
        }
    }

    public record ExpandedUnaryDelta(List<DBSPSimpleOperator> operators, DBSPSimpleOperator output,
                                     OperatorDeltaExpansion correspondence) {}

    public static ExpandedUnaryDelta unaryDelta(DBSPCompiler compiler, DBSPUnaryOperator original, OutputPort input) {
        var operands = new ArrayList<IDBSPInnerNode>();
        operands.add(input.outputType());
        String pass;
        if (original instanceof DBSPAggregateOperator) {
            pass = "DeltaExpansion/Aggregate";
            operands.add(original.outputType);
            if (original.function != null)
                operands.add(original.function);
        } else {
            pass = original instanceof DBSPPositiveOperator ? "DeltaExpansion/Positive" : "DeltaExpansion/Distinct";
        }
        var site = expressionSite(original.getRelNode());
        site.put("input_port", input.outputNumber);
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(site), operands.toArray(IDBSPInnerNode[]::new));
            var decoder = expressionDecoder(reply.writer);
            var operators = decodeOperatorFragment(decoder, reply, op -> original.getRelNode(), input.node());
            var integral = decoder.outer.lookup(reply.value.required("integral").longValue(), DBSPIntegrateOperator.class);
            var output = decoder.outer.lookup(reply.value.required("output").longValue(), DBSPSimpleOperator.class);
            var aggregate = reply.value.get("aggregate");
            var correspondence = aggregate == null
                    ? new DistinctDeltaExpansion(integral, output.to(DBSPBinaryDistinctOperator.class))
                    : new AggregateDeltaExpansion(integral, decoder.outer.lookup(aggregate.longValue(), DBSPPrimitiveAggregateOperator.class),
                            output.to(DBSPUpsertFeedbackOperator.class));
            return new ExpandedUnaryDelta(operators, output, correspondence);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode unary delta inputs", error);
        }
    }

    public record ExpandedBinaryDelta(List<DBSPSimpleOperator> operators,
                                      Map<RustDeltaExpandOperators.Term, DBSPSimpleOperator> terms) {}

    public static ExpandedBinaryDelta binaryDelta(DBSPCompiler compiler, DBSPJoinBaseOperator join,
            RustDeltaExpandOperators.JoinForm form, @Nullable DBSPExpression filter, OutputPort left, OutputPort right) {
        var site = expressionSite(join.getRelNode());
        site.put("form", form.name());
        site.put("balanced", join.balanced);
        site.putArray("ports").add(left.outputNumber).add(right.outputNumber);
        try {
            var reply = invokeLowering(compiler, "DeltaExpansion/BinaryJoin", MAPPER.writeValueAsBytes(site),
                    join.getFunction(), join.outputType, left.outputType(), right.outputType(), join.left().outputType(), filter);
            var decoder = expressionDecoder(reply.writer);
            var operators = decodeOperatorFragment(decoder, reply, op -> join.getRelNode(), left.node(), right.node());
            var terms = new EnumMap<RustDeltaExpandOperators.Term, DBSPSimpleOperator>(RustDeltaExpandOperators.Term.class);
            for (var entry : reply.value.required("terms").properties())
                terms.put(RustDeltaExpandOperators.Term.valueOf(entry.getKey()),
                        decoder.outer.lookup(entry.getValue().longValue(), DBSPSimpleOperator.class));
            terms.get(RustDeltaExpandOperators.Term.LEFT_INTEGRAL).copyAnnotations(join.left().node());
            terms.get(RustDeltaExpandOperators.Term.RIGHT_INTEGRAL).copyAnnotations(join.right().node());
            finishLowering(compiler, reply.value);
            return new ExpandedBinaryDelta(operators, terms);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode binary join inputs", error);
        }
    }

    public record ExpandedStarDelta(List<DBSPSimpleOperator> operators, DBSPSimpleOperator output,
                                    StarJoinDeltaExpansion correspondence) {}

    public static ExpandedStarDelta starDelta(DBSPCompiler compiler, DBSPStarJoinBaseOperator operator,
                                                   List<OutputPort> inputs) {
        var site = expressionSite(operator.getRelNode());
        var ports = site.putArray("ports");
        var roots = new ArrayList<IDBSPInnerNode>(inputs.size() + 2);
        roots.add(operator.getClosureFunction());
        roots.add(operator.outputType);
        for (var port : inputs) {
            ports.add(port.outputNumber);
            roots.add(port.outputType());
        }
        if (operator instanceof DBSPStarJoinFilterMapOperator filtered) {
            roots.add(Objects.requireNonNull(filtered.filter));
            if (filtered.map != null)
                roots.add(filtered.map);
        }
        try {
            var reply = invokeLowering(compiler, "DeltaExpansion/StarJoin",
                    MAPPER.writeValueAsBytes(site), roots.toArray(IDBSPInnerNode[]::new));
            var decoder = expressionDecoder(reply.writer);
            var operators = decodeOperatorFragment(decoder, reply, op -> operator.getRelNode(),
                    inputs.stream().map(OutputPort::node).toArray(DBSPOperator[]::new));
            var integrals = new ArrayList<DBSPDelayedIntegralOperator>();
            for (var id : reply.value.required("integrals"))
                integrals.add(decoder.outer.lookup(id.longValue(), DBSPDelayedIntegralOperator.class));
            var terms = new ArrayList<StarJoinDeltaExpansion.Term>();
            for (var term : reply.value.required("terms")) {
                var joins = new ArrayList<DBSPJoinBaseOperator>();
                for (var id : term.required("joins"))
                    joins.add(decoder.outer.lookup(id.longValue(), DBSPJoinBaseOperator.class));
                var projection = decoder.outer.lookup(term.required("projection").longValue(), DBSPSimpleOperator.class);
                terms.add(new StarJoinDeltaExpansion.Term(joins, projection));
            }
            finishLowering(compiler, reply.value);
            var sum = decoder.outer.lookup(reply.value.required("sum").longValue(), DBSPSumOperator.class);
            var expansion = new StarJoinDeltaExpansion(integrals, terms, sum);
            if (!reply.value.has("filter"))
                return new ExpandedStarDelta(operators, sum, expansion);
            var filter = decoder.outer.lookup(reply.value.required("filter").longValue(), DBSPFilterOperator.class);
            var map = reply.value.has("map")
                    ? decoder.outer.lookup(reply.value.required("map").longValue(), DBSPSimpleOperator.class)
                    : null;
            var filtered = new StarJoinFilterMapDeltaExpansion(expansion, filter, map);
            return new ExpandedStarDelta(operators, filtered.last(), filtered);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode star join inputs", error);
        }
    }

    public static DBSPClosureExpression applyWaterlines(DBSPCompiler compiler,
            DBSPClosureExpression function, List<OutputPort> inputs) {
        var roots = new IDBSPInnerNode[inputs.size() + 1];
        roots[0] = function;
        for (int i = 0; i < inputs.size(); i++)
            roots[i + 1] = inputs.get(i).outputType();
        return loweredClosure(compiler, invokeLowering(compiler, "Limiters/Apply", roots));
    }

    public static Pair<DBSPClosureExpression, DBSPClosureExpression> latenessFilter(DBSPCompiler compiler,
            ProgramIdentifier name, List<Integer> columns, DBSPType row, DBSPType waterline) {
        return lowerLatenessFilter(compiler, "Limiters/RowFilter", List.of(name.name(), columns), row, waterline);
    }

    public static Pair<DBSPClosureExpression, DBSPClosureExpression> lateKeyFilter(DBSPCompiler compiler,
            ProgramIdentifier name, List<Integer> keys, List<Integer> columns, DBSPType key, DBSPType waterline) {
        return lowerLatenessFilter(compiler, "Limiters/KeyFilter", List.of(name.name(), keys, columns), key, waterline);
    }

    private static Pair<DBSPClosureExpression, DBSPClosureExpression> lowerLatenessFilter(DBSPCompiler compiler,
            String pass, List<?> fields, DBSPType input, DBSPType waterline) {
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(fields), input, waterline);
            return loweredClosurePair(reply);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode LATENESS columns", error);
        }
    }

    public static DBSPClosureExpression joinWaterline(DBSPCompiler compiler,
            @Nullable OutputPort left, @Nullable IMaybeMonotoneType leftShape,
            @Nullable OutputPort right, @Nullable IMaybeMonotoneType rightShape) {
        var shapes = MAPPER.createArrayNode();
        var types = new ArrayList<DBSPType>(2);
        for (int side = 0; side < 2; side++) {
            var port = side == 0 ? left : right;
            if (port == null) {
                shapes.addNull();
            } else {
                types.add(port.outputType());
                shapes.add(LimiterInputs.projection(Objects.requireNonNull(side == 0 ? leftShape : rightShape)));
            }
        }
        try {
            return loweredClosure(compiler, invokeLowering(compiler, "Limiters/Join",
                    MAPPER.writeValueAsBytes(shapes), types.toArray(DBSPType[]::new)));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode join waterline projections", error);
        }
    }

    public record WindowWaterline(DBSPClosureExpression endpoint, @Nullable DBSPClosureExpression combine) {}

    public static WindowWaterline windowWaterline(DBSPCompiler compiler, DBSPClosureExpression endpoints,
            @Nullable OutputPort inputBound, boolean retainEmptyValue) {
        IDBSPInnerNode[] inputs = inputBound == null ? new IDBSPInnerNode[] { endpoints }
                : new IDBSPInnerNode[] { endpoints, inputBound.outputType() };
        try {
            var reply = invokeLowering(compiler, "Limiters/Window", MAPPER.writeValueAsBytes(retainEmptyValue), inputs);
            var decoder = expressionDecoder(reply.writer);
            var roots = reply.value.required("functions");
            var result = new WindowWaterline(decoder.decodeInner(roots.required(0), DBSPClosureExpression.class),
                    inputBound == null ? null : decoder.decodeInner(roots.required(1), DBSPClosureExpression.class));
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode window waterline projection", error);
        }
    }

    private static ObjectNode expressionSite(CalciteObject origin) {
        var site = MAPPER.createObjectNode();
        site.put("empty", origin.isEmpty());
        var range = origin.getPositionRange();
        if (!origin.isEmpty() || range.isValid())
            site.set("position", range.asJson());
        return site;
    }

    public static InsertLimiters.FinalityFunctions finalityFunctions(DBSPCompiler compiler, CalciteObject origin,
            DBSPType row, DBSPType bound, int rowIndex, int boundIndex) {
        var site = expressionSite(origin);
        site.put("row", rowIndex);
        site.put("waterline", boundIndex);
        try {
            var reply = invokeLowering(compiler, "Limiters/Finality", MAPPER.writeValueAsBytes(site), row, bound);
            var decoder = expressionDecoder(reply.writer);
            var roots = reply.value.required("functions");
            var result = new InsertLimiters.FinalityFunctions(
                    decoder.decodeInner(roots.required(0), DBSPClosureExpression.class),
                    roots.required(1).isNull() ? null : decoder.decodeInner(roots.get(1), DBSPClosureExpression.class),
                    decoder.decodeInner(roots.required(2), DBSPClosureExpression.class),
                    roots.required(3).isNull() ? null : decoder.decodeInner(roots.get(3), DBSPClosureExpression.class));
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode EMIT FINAL columns", error);
        }
    }

    public static InsertLimiters.DeclaredWaterline declaredWaterline(DBSPCompiler compiler, CalciteObject origin,
            DBSPVariablePath row, List<Integer> columns, List<DBSPExpression> intervals) {
        var site = expressionSite(origin);
        site.set("columns", MAPPER.valueToTree(columns));
        var inputs = new ArrayList<DBSPExpression>(intervals.size() + 1);
        inputs.add(row);
        inputs.addAll(intervals);
        try {
            var reply = invokeLowering(compiler, "Limiters/DeclaredWaterline",
                    MAPPER.writeValueAsBytes(site), inputs.toArray(DBSPExpression[]::new));
            var decoder = expressionDecoder(reply.writer);
            var roots = reply.value.required("expressions");
            var result = new InsertLimiters.DeclaredWaterline(
                    decoder.decodeInner(roots.required(0), DBSPTupleExpression.class),
                    decoder.decodeInner(roots.required(1), DBSPExpression.class),
                    decoder.decodeInner(roots.required(2), DBSPClosureExpression.class));
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode the declared waterline", error);
        }
    }

    public static DBSPClosureExpression minimumWaterline(DBSPCompiler compiler, List<OutputPort> inputs) {
        var types = inputs.stream().map(OutputPort::outputType).toArray(DBSPType[]::new);
        return loweredClosure(compiler, invokeLowering(compiler, "Limiters/Minimum", types));
    }

    public static DBSPClosureExpression projectWaterline(DBSPCompiler compiler, DBSPType input,
            IMaybeMonotoneType source, IMaybeMonotoneType destination) {
        return waterlineProjection(compiler, "Limiters/Project", input, destination.getType(), source, destination);
    }

    public static DBSPClosureExpression mergeViewWaterline(DBSPCompiler compiler,
            OutputPort inherited, OutputPort declared, IMaybeMonotoneType inheritedShape, IMaybeMonotoneType declaredShape) {
        return waterlineProjection(compiler, "Limiters/MergeView",
                inherited.outputType(), declared.outputType(), inheritedShape, declaredShape);
    }

    private static DBSPClosureExpression waterlineProjection(DBSPCompiler compiler, String pass,
            DBSPType first, DBSPType second, IMaybeMonotoneType firstShape, IMaybeMonotoneType secondShape) {
        try {
            var shapes = List.of(LimiterInputs.projection(firstShape), LimiterInputs.projection(secondShape));
            return loweredClosure(compiler, invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(shapes), first, second));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode waterline projection", error);
        }
    }

    private static DBSPClosureExpression loweredClosure(DBSPCompiler compiler, LoweringReply reply) {
        var result = loweredFunction(compiler, reply.writer, reply.value, DBSPClosureExpression.class);
        DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
        return result;
    }

    public enum BinaryDomain { Scalar, Temporal, TimeOfDay }
    public enum BinaryWaterline { None, Left, Right, Both, InheritLeft, InheritRight, Coalesce }
    public record WaterlineOperand(boolean monotone, boolean constant, boolean positiveLiteral, boolean neverNull) {}
    public enum CastWaterline { None, Identity, Ordinary, Saturating }

    public static CastWaterline castWaterline(DBSPCompiler compiler, DBSPType from, DBSPType to,
                                               DBSPType bound, boolean safe) {
        try {
            var reply = invokeLowering(compiler, "Monotonicity/Cast", MAPPER.writeValueAsBytes(safe), from, to, bound);
            return MAPPER.treeToValue(reply.value, CastWaterline.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid cast waterline from Rust", error);
        }
    }

    public record FunctionArgument(boolean monotone, boolean constant, boolean doubleLiteralAboveOne) {}

    @Nullable
    public static String functionWaterline(String function, List<FunctionArgument> arguments) {
        try {
            var facts = MAPPER.writeValueAsBytes(List.of(function, arguments));
            var reply = transformWithContext("Monotonicity/Function", facts, 0, "", EMPTY_CONTEXT);
            return MAPPER.readValue(reply, String.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid function waterline from Rust", error);
        }
    }

    public record BinaryBounds(BinaryDomain domain, DBSPOpcode opcode, WaterlineOperand left, WaterlineOperand right) {}

    public record BinaryTransfer(BinaryWaterline rule, @Nullable DBSPExpression bound) {}

    /** The reduced operand trees already live in the JVM. Rust needs their types
     * to build the parent and returns references to the original operands. */
    public static BinaryTransfer binaryWaterline(DBSPCompiler compiler, DBSPExpression expression,
            BinaryBounds facts, @Nullable DBSPExpression left, @Nullable DBSPExpression right) {
        var site = expressionSite(expression.getNode());
        site.set("operation", MAPPER.valueToTree(facts));
        var lhs = left == null ? null : new DBSPVariablePath("left", left.getType());
        var rhs = right == null ? null : new DBSPVariablePath("right", right.getType());
        try {
            var reply = invokeLowering(compiler, "Monotonicity/BinaryBound", MAPPER.writeValueAsBytes(site),
                    expression.getType(), lhs, rhs);
            var rule = MAPPER.treeToValue(reply.value.required("rule"), BinaryWaterline.class);
            var bound = reply.value.required("bound");
            if (bound.isNull())
                return new BinaryTransfer(rule, null);
            var live = Objects.requireNonNull(reply.writer.liveNodes);
            if (lhs != null)
                live.put(lhs.id, left);
            if (rhs != null)
                live.put(rhs.id, right);
            return new BinaryTransfer(rule, expressionDecoder(reply.writer).decodeInner(bound, DBSPExpression.class));
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid binary waterline from Rust", error);
        }
    }

    public static DBSPClosureExpression filterTransfer(DBSPCompiler compiler, DBSPFilterOperator filter, DBSPType row) {
        try {
            return loweredClosure(compiler, invokeLowering(compiler, "Monotonicity/Filter",
                    MAPPER.writeValueAsBytes(expressionSite(filter.getNode())), filter.getClosureFunction(), row));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode filter source position", error);
        }
    }

    public static DBSPClosureExpression flatmapWaterline(DBSPCompiler compiler, DBSPFlatmap flatmap) {
        return lowerFunction(compiler, "Monotonicity/Flatmap", flatmap, null,
                DBSPClosureExpression.class);
    }

    public static DBSPClosureExpression lowerFlatmap(DBSPCompiler compiler, DBSPFlatmap flatmap) {
        return lowerFunction(compiler, "LowerFlatmap", flatmap, flatmap.getCollectionElementType(),
                DBSPClosureExpression.class);
    }

    public static DBSPClosureExpression composeProjection(DBSPCompiler compiler,
            DBSPClosureExpression before, DBSPClosureExpression after) {
        return lowerFunction(compiler, "ComposeProjection", before, after, DBSPClosureExpression.class);
    }

    @Nullable
    public static DBSPClosureExpression tryComposeProjection(DBSPCompiler compiler,
            DBSPClosureExpression before, DBSPClosureExpression after) {
        return tryLowerFunction(compiler, "TryComposeProjection", before, after, DBSPClosureExpression.class);
    }

    public static DBSPClosureExpression composeIndexedJoinOutput(DBSPCompiler compiler,
            DBSPClosureExpression before, DBSPClosureExpression after) {
        return lowerFunction(compiler, "ComposeIndexedJoinOutput", before, after, DBSPClosureExpression.class);
    }

    public static DBSPClosureExpression antijoinProjection(DBSPCompiler compiler,
            DBSPTypeIndexedZSet input, @Nullable DBSPClosureExpression projection) {
        return lowerFunction(compiler, "AntijoinProjection", input, projection, DBSPClosureExpression.class);
    }

    public static boolean isMapProjection(DBSPCompiler compiler, DBSPClosureExpression closure, boolean allowNestedAndCasts) {
        String pass = allowNestedAndCasts ? "IsNestedProjection" : "IsStrictProjection";
        var result = invokeLowering(compiler, pass, closure, null).value;
        if (!result.isBoolean())
            throw new IllegalStateException("Expected a boolean projection decision from Rust");
        return result.booleanValue();
    }

    public static boolean isIdentityProjection(DBSPCompiler compiler, DBSPClosureExpression closure) {
        var reply = invokeLowering(compiler, "IsIdentityProjection", closure).value;
        DBSPVariablePath.restoreGeneratedNames(reply.required("next_generated_name").longValue());
        return reply.required("identity").booleanValue();
    }

    @Nullable
    public static DBSPFlatmap projectUnnest(DBSPCompiler compiler, DBSPFlatmap flatmap, DBSPClosureExpression reader) {
        return tryLowerFunction(compiler, "ProjectUnnest", flatmap, reader, DBSPFlatmap.class);
    }

    @Nullable
    public static DBSPExpression projectOutputFunction(DBSPCompiler compiler, DBSPSimpleOperator producer,
            DBSPSimpleOperator reader, boolean singleReader) {
        try {
            byte[] site = MAPPER.writeValueAsBytes(List.of(producer.getClass().getSimpleName(),
                    reader instanceof DBSPMapIndexOperator, singleReader));
            var reply = invokeLowering(compiler, "ProjectOutputs/Function", site, producer.getFunction(),
                    reader.getFunction(), reader.outputType);
            return reply.value.isNull() ? null : loweredFunction(compiler, reply.writer, reply.value, DBSPExpression.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode output projection", error);
        }
    }

    public static DBSPExpression projectAggregateZero(DBSPCompiler compiler, DBSPExpression zero, DBSPClosureExpression reader) {
        return lowerFunction(compiler, "ProjectAggregateZero", zero, reader, DBSPExpression.class);
    }

    public static Pair<DBSPClosureExpression, DBSPClosureExpression> splitAntijoinProjection(
            DBSPCompiler compiler, DBSPClosureExpression projection) {
        var reply = invokeLowering(compiler, "SplitAntijoinProjection", projection, null);
        return decodeClosurePair(reply);
    }

    @Nullable
    public static Pair<DBSPClosureExpression, DBSPClosureExpression> trimTopKProjection(
            DBSPCompiler compiler, DBSPClosureExpression producer, DBSPClosureExpression reader) {
        var reply = invokeLowering(compiler, "TrimTopKProjection", producer, reader);
        return reply.value.isNull() ? null : decodeClosurePair(reply);
    }

    private static Pair<DBSPClosureExpression, DBSPClosureExpression> loweredClosurePair(LoweringReply reply) {
        var pair = decodeClosurePair(new LoweringReply(reply.writer, reply.value.required("functions")));
        DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
        return pair;
    }

    private static Pair<DBSPClosureExpression, DBSPClosureExpression> decodeClosurePair(LoweringReply reply) {
        var functions = reply.value;
        if (!functions.isArray() || functions.size() != 2)
            throw new IllegalStateException("Expected two closures from Rust");
        var decoder = expressionDecoder(reply.writer);
        return Pair.of(decoder.decodeInner(functions.get(0), DBSPClosureExpression.class),
                decoder.decodeInner(functions.get(1), DBSPClosureExpression.class));
    }

    @Nullable
    public static Pair<DBSPAggregateList, DBSPClosureExpression> trimAggregateProjection(
            DBSPCompiler compiler, DBSPAggregateList aggregates, DBSPTypeIndexedZSet output, DBSPClosureExpression reader) {
        var reply = invokeLowering(compiler, "TrimAggregateProjection", reader, aggregates.getEmptySetResult(), output.keyType);
        if (reply.value.isNull())
            return null;
        List<Integer> retained = MAPPER.convertValue(reply.value.required("retained"), new TypeReference<>() {});
        var expressions = reply.value.required("expressions");
        var decoder = expressionDecoder(reply.writer);
        var emptyRow = decoder.decodeInner(expressions.required(0), DBSPTupleExpression.class);
        var filler = decoder.decodeInner(expressions.required(1), DBSPClosureExpression.class);
        var projected = aggregates.project(retained, emptyRow);
        DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
        return Pair.of(projected, filler);
    }

    @Nullable
    public static DBSPClosureExpression composeIndexedProjection(DBSPCompiler compiler,
            DBSPClosureExpression before, DBSPClosureExpression after, boolean onlyProjections) {
        return tryLowerFunction(compiler, onlyProjections ? "TryIndexedProjection" : "ComposeIndexedProjection",
                before, after, DBSPClosureExpression.class);
    }

    private static <T extends DBSPExpression> T lowerFunction(DBSPCompiler compiler, String pass,
            IDBSPInnerNode input, @Nullable IDBSPInnerNode second, Class<T> kind) {
        return Objects.requireNonNull(tryLowerFunction(compiler, pass, input, second, kind));
    }

    @Nullable
    private static <T extends DBSPExpression> T tryLowerFunction(DBSPCompiler compiler, String pass,
            IDBSPInnerNode input, @Nullable IDBSPInnerNode second, Class<T> kind) {
        var reply = invokeLowering(compiler, pass, input, second);
        if (reply.value.isNull())
            return null;
        var function = loweredFunction(compiler, reply.writer, reply.value, kind);
        DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
        return function;
    }

    @Nullable
    public static Pair<DBSPClosureExpression, DBSPClosureExpression> splitMapInput(DBSPCompiler compiler,
            DBSPUnaryOperator map, List<List<Integer>> paths) {
        try {
            String pass = map instanceof DBSPMapIndexOperator ? "SplitIndexedMapInput" : "SplitRowMapInput";
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(paths), map.getClosureFunction());
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            var functions = reply.value.required("functions");
            return functions.isNull() ? null : decodeClosurePair(new LoweringReply(reply.writer, functions));
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode map input fields", error);
        }
    }

    private record LoweringReply(ToJsonInnerVisitor writer, JsonNode value) {}

    private static boolean answered(DBSPCompiler compiler, String operation, JsonNode reply) {
        boolean answered = !reply.isNull();
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision(operation, answered);
        return answered;
    }

    private static LoweringReply invokeLowering(DBSPCompiler compiler, String pass, IDBSPInnerNode... inputs) {
        return invokeLowering(compiler, pass, EMPTY_CONTEXT, inputs);
    }

    private static LoweringReply invokeLowering(DBSPCompiler compiler, String pass, byte[] state, IDBSPInnerNode... inputs) {
        var json = JsonStream.compactCircuit();
        var inner = switch (pass) {
            case "MergeTemporalBounds", "TemporalGroups" -> new TemporalKeyWriter(compiler, json);
            default -> new ToJsonInnerVisitor(compiler, json, 1, true);
        };
        inner.liveNodes = new HashMap<>();
        json.beginArray();
        for (var input : inputs)
            if (input != null)
                input.accept(inner);
        json.endArray();
        try {
            byte[] reply = transformWithContext(pass, json.toString().getBytes(StandardCharsets.UTF_8),
                    DBSPVariablePath.nextGeneratedName(), "", state);
            return new LoweringReply(inner, MAPPER.readTree(reply));
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid function from Rust pass " + pass, error);
        }
    }

    /** Clock calls that require a map or filter to be rewritten, keyed by its function ID. */
    public static Map<Long, DBSPExpression> temporalClockSites(DBSPCompiler compiler, DBSPCircuit circuit) {
        var functions = new LinkedHashMap<Long, DBSPExpression>();
        visitOperators(circuit.allOperators, operator -> {
            if (operator instanceof DBSPMapOperator || operator instanceof DBSPFilterOperator) {
                var function = operator.to(DBSPSimpleOperator.class).getFunction();
                functions.putIfAbsent(function.getId(), function);
            }
        });
        return temporalClockSites(compiler, functions.values());
    }

    public static Map<Long, DBSPExpression> temporalClockSites(DBSPCompiler compiler,
                                                               Iterable<DBSPExpression> expressions) {
        var calls = OptimizerExpressions.callSites(compiler, expressions);
        if (calls.sites().isEmpty())
            return Map.of();
        try {
            byte[] reply = transformWithContext("RewriteNow/ClockSites", MAPPER.writeValueAsBytes(calls.sites()),
                    0, "", EMPTY_CONTEXT);
            Map<Long, Long> selected = MAPPER.readValue(reply, new TypeReference<>() {});
            var sites = new HashMap<Long, DBSPExpression>();
            selected.forEach((root, call) -> sites.put(root, Utilities.getExists(calls.expressions(), call)));
            return sites;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid clock call sites from the Rust optimizer", error);
        }
    }

    public static List<DBSPSimpleOperator> clockJoin(DBSPCompiler compiler, DBSPUnaryOperator consumer,
            DBSPSimpleOperator rows, DBSPSimpleOperator indexedClock, DBSPClosureExpression function,
            DBSPType timestamp, DBSPExpression call) {
        String pass = consumer instanceof DBSPFilterOperator ? "RewriteNow/JoinedFilter" : "RewriteNow/JoinedMap";
        var reply = invokeLowering(compiler, pass, rows.getOutputZSetElementType(), DBSPTypeTuple.EMPTY,
                timestamp, function, consumer.getOutputZSetType());
        var unsupported = reply.value.get("unsupported_lambda");
        if (unsupported != null) {
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            var lambda = expressionDecoder(reply.writer).decodeInner(unsupported, DBSPClosureExpression.class);
            throw new CompilationError("NOW() is not supported inside a lambda expression", lambda.getNode());
        }
        var origin = consumer.getRelNode();
        var joinOrigin = clockJoinOrigin(origin, call);
        return decodeOperatorFragment(reply, op -> op instanceof DBSPStreamJoinOperator ? joinOrigin : origin,
                rows, indexedClock);
    }

    private static CalciteRelNode clockJoinOrigin(CalciteRelNode origin, DBSPExpression call) {
        var join = origin.copy();
        join.addSourcePositions(List.of(call.getSourcePosition()));
        return join;
    }

    public static DBSPClosureExpression unmatchedRows(DBSPCompiler compiler, DBSPJoinBaseOperator join) {
        return lowerFunction(compiler, "DeltaExpansion/UnmatchedRows", join.getClosureFunction(),
                join.left().outputType(), DBSPClosureExpression.class);
    }

    public record ClockSource(List<DBSPSimpleOperator> operators, DBSPSimpleOperator collection,
                              DBSPSimpleOperator indexed) {}

    public static ClockSource clockSource(DBSPCompiler compiler, CalciteRelNode origin, DBSPType timestamp,
                                          @Nullable DBSPSimpleOperator table) {
        var reply = table == null
                ? invokeLowering(compiler, "RewriteNow/BuiltinClock", timestamp, DBSPTypeTuple.EMPTY)
                : invokeLowering(compiler, "RewriteNow/TableClock", timestamp, compiler.weightVar);
        var decoder = expressionDecoder(reply.writer);
        var inputs = table == null ? new DBSPSimpleOperator[0] : new DBSPSimpleOperator[]{table};
        var operators = decodeOperatorFragment(decoder, reply, op -> origin, inputs);
        return new ClockSource(operators,
                decoder.outer.lookup(reply.value.required("collection").longValue(), DBSPSimpleOperator.class),
                decoder.outer.lookup(reply.value.required("indexed").longValue(), DBSPSimpleOperator.class));
    }

    private static List<DBSPSimpleOperator> decodeOperatorFragment(LoweringReply reply,
            Function<DBSPSimpleOperator, CalciteRelNode> origin, DBSPOperator... inputs) {
        return decodeOperatorFragment(expressionDecoder(reply.writer), reply, origin, inputs);
    }

    private static List<DBSPSimpleOperator> decodeOperatorFragment(JsonDecoder decoder, LoweringReply reply,
            Function<DBSPSimpleOperator, CalciteRelNode> origin, DBSPOperator... inputs) {
        var ports = reply.value.required("inputs");
        if (ports.size() != inputs.length)
            throw new IllegalStateException("Operator fragment has the wrong number of inputs");
        for (int i = 0; i < inputs.length; ++i)
            decoder.outer.cache(ports.required(i).longValue(), inputs[i]);
        var result = new ArrayList<DBSPSimpleOperator>();
        for (var node : reply.value.required("operators")) {
            var operator = decoder.decodeOuter(node, DBSPSimpleOperator.class);
            operator.restoreProvenance(origin.apply(operator));
            result.add(operator);
        }
        DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
        return result;
    }

    private static CalciteRelNode windowOrigin(DBSPCompiler compiler, CalciteRelNode origin,
                                                DBSPSimpleOperator clock, DBSPSimpleOperator op) {
        if (op instanceof DBSPWaterlineOperator
                || (op instanceof DBSPDifferentiateOperator diff && diff.input().node() == clock))
            return clock.getRelNode();
        if (!(op instanceof DBSPWindowOperator window))
            return origin;
        var finder = new FindSourcePositions(compiler, false);
        finder.apply(window.right().simpleNode().getFunction());
        var copy = origin.copy();
        copy.addSourcePositions(finder.positions);
        return copy;
    }

    public static List<DBSPSimpleOperator> temporalWindow(DBSPCompiler compiler, CalciteRelNode origin,
            DBSPSimpleOperator rows, DBSPSimpleOperator clock, DBSPParameter parameter,
            DBSPExpression key, DBSPType keyType, DBSPType clockType, DBSPExpression lowerInitial,
            DBSPExpression upperInitial, List<DBSPOpcode> comparisons, List<DBSPExpression> bounds) {
        var inputs = new ArrayList<IDBSPInnerNode>(List.of(rows.outputType, clock.outputType,
                DBSPTypeRawTuple.EMPTY, clockType, parameter, key, keyType, lowerInitial, upperInitial));
        inputs.addAll(bounds);
        try {
            var request = expressionSite(origin);
            request.set("comparisons", MAPPER.valueToTree(comparisons.stream().map(Enum::name).toList()));
            var reply = invokeLowering(compiler, "RewriteNow/Window", MAPPER.writeValueAsBytes(request),
                    inputs.toArray(IDBSPInnerNode[]::new));
            return decodeOperatorFragment(reply, op -> windowOrigin(compiler, origin, clock, op), rows, clock);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode temporal window comparisons", error);
        }
    }

    public record ClockFilter(List<DBSPSimpleOperator> operators, @Nullable DBSPExpression warningPredicate,
                              @Nullable DBSPClosureExpression unsupportedLambda) {}

    /** Lower the windows and residual clock join of one predicate in their original order. */
    @Nullable
    public static ClockFilter temporalFilter(DBSPCompiler compiler, DBSPFilterOperator filter,
            DBSPSimpleOperator rows, DBSPSimpleOperator clock, DBSPSimpleOperator indexedClock, DBSPType timestamp) {
        var origin = filter.getRelNode();
        try {
            var reply = invokeLowering(compiler, "RewriteNow/Filter", MAPPER.writeValueAsBytes(expressionSite(origin)),
                    rows.outputType, clock.outputType, DBSPTypeRawTuple.EMPTY, timestamp, DBSPTypeTuple.EMPTY,
                    filter.getOutputZSetType(), filter.getClosureFunction());
            if (!answered(compiler, "RewriteNow/Filter", reply.value))
                return null;
            var rejection = reply.value.get("rejected_lambda");
            if (rejection != null) {
                var decoder = expressionDecoder(reply.writer);
                var predicate = decoder.decodeInner(rejection.required(0), DBSPExpression.class);
                var lambda = decoder.decodeInner(rejection.required(1), DBSPClosureExpression.class);
                DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
                return new ClockFilter(List.of(), predicate, lambda);
            }
            var warning = reply.value.get("warning_predicate");
            DBSPExpression predicate = warning == null ? null :
                    expressionDecoder(reply.writer).decodeInner(warning, DBSPExpression.class);
            var join = predicate == null ? origin : clockJoinOrigin(origin,
                    Objects.requireNonNull(temporalClockSites(compiler, List.of(predicate)).get(predicate.getId())));
            return new ClockFilter(decodeOperatorFragment(reply,
                    op -> op instanceof DBSPStreamJoinOperator ? join : windowOrigin(compiler, origin, clock, op),
                    rows, clock, indexedClock), predicate, null);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode temporal filter origin", error);
        }
    }

    public static DBSPClosureExpression mergeTemporalPredicates(DBSPCompiler compiler,
            DBSPClosureExpression source, DBSPClosureExpression filter) {
        return lowerFunction(compiler, "MergeFilters/Predicate", source, filter, DBSPClosureExpression.class);
    }

    private DBSPCircuit expandHop(DBSPCircuit circuit) {
        return new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void postorder(DBSPHopOperator operator) {
                var input = this.mapped(operator.input());
                var origin = operator.getRelNode();
                var site = expressionSite(origin);
                site.put("timestamp_index", operator.timestampIndex).put("input_port", input.outputNumber);
                try {
                    var reply = invokeLowering(this.compiler, "ExpandHop/Window", MAPPER.writeValueAsBytes(site),
                            input.getOutputZSetElementType(), operator.getOutputZSetElementType(),
                            operator.interval, operator.size, operator.start);
                    var operators = decodeOperatorFragment(reply,
                            op -> op instanceof DBSPMapOperator ? origin.intermediate() : origin, input.node());
                    this.addOperator(operators.get(0));
                    this.map(operator, operators.get(1));
                } catch (IOException error) {
                    throw new UncheckedIOException("Could not encode HOP window inputs", error);
                }
            }
        }.apply(circuit);
    }

    private DBSPCircuit expandStreamAggregates(DBSPCircuit circuit, ExpandStreamAggregates pass) {
        return new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void postorder(DBSPStreamAggregateOperator operator) {
                if (operator.aggregateList == null) {
                    super.postorder(operator);
                    return;
                }
                var input = this.mapped(operator.input());
                var site = expressionSite(operator.getRelNode());
                site.put("append_only", pass.isAppendOnly(operator.input())).put("input_port", input.outputNumber);
                try {
                    var reply = invokeLowering(this.compiler, "ExpandStreamAggregates/List",
                            MAPPER.writeValueAsBytes(site), input.outputType(), operator.outputType,
                            operator.aggregateList, this.compiler.weightVar);
                    var decoder = expressionDecoder(reply.writer);
                    var operators = decodeOperatorFragment(decoder, reply, op -> operator.getRelNode(), input.node());
                    operators.forEach(this::addOperator);
                    var result = decoder.outer.lookup(reply.value.required("output").longValue(), DBSPSimpleOperator.class);
                    this.map(operator, result, false);
                    finishLowering(this.compiler, reply.value);
                } catch (IOException error) {
                    throw new UncheckedIOException("Could not encode stream aggregate inputs", error);
                }
            }
        }.apply(circuit);
    }

    public static DBSPClosureExpression lowerJoinFilterMap(DBSPCompiler compiler, DBSPCircuit circuit,
            DBSPSimpleOperator operator, DBSPClosureExpression filter, @Nullable DBSPClosureExpression map) {
        var json = JsonStream.compactCircuit();
        var inner = new ToJsonInnerVisitor(compiler, json, 1, true);
        inner.liveNodes = new HashMap<>();
        json.beginObject().appendClass(circuit).label("id").append(circuit.id)
                .label("declarations").beginArray();
        json.endArray().label("allOperators").beginArray();
        json.beginObject().appendClass(operator).label("id").append(operator.id);
        json.label("function");
        operator.getClosureFunction().accept(inner);
        json.label("filter");
        filter.accept(inner);
        if (map != null) {
            json.label("map");
            map.accept(inner);
        }
        json.endObject();
        json.endArray().endObject();
        try {
            byte[] reply = transformWithContext("LowerJoinFilterMap", json.toString().getBytes(StandardCharsets.UTF_8),
                    0, "", EMPTY_CONTEXT);
            return loweredFunction(compiler, inner, MAPPER.readTree(reply), DBSPClosureExpression.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid join-filter-map closure from Rust", error);
        }
    }

    public record OperandUse(long closure, long parent, int operand, boolean forwards) {}
    public record NumberedExpression(long value, boolean candidate, List<OperandUse> users) {}
    public record ScopedValue(long closure, long value) {}

    public interface LazyBinding {
        DBSPVariablePath var();
        DBSPExpression expression();
    }

    /** Keep live references shared between the consumer and all its initializers. */
    private static LoweringReply lowerBindings(DBSPCompiler compiler, List<? extends LazyBinding> bindings,
            @Nullable DBSPExpression consumer) {
        var inputs = new ArrayList<IDBSPInnerNode>();
        if (consumer != null)
            inputs.add(consumer);
        for (var binding : bindings) {
            inputs.add(binding.var());
            inputs.add(binding.expression());
        }
        return invokeLowering(compiler, consumer == null ? "InnerCSE/Statements" : "InnerCSE/Let",
                inputs.toArray(IDBSPInnerNode[]::new));
    }

    public static DBSPExpression bindLazyExpression(DBSPCompiler compiler, List<? extends LazyBinding> bindings,
            DBSPExpression body) {
        var reply = lowerBindings(compiler, bindings, body);
        return loweredFunction(compiler, reply.writer, reply.value, DBSPExpression.class);
    }

    public static List<DBSPLetStatement> lazyStatements(DBSPCompiler compiler, List<? extends LazyBinding> bindings) {
        var reply = lowerBindings(compiler, bindings, null);
        Utilities.enforce(reply.value.isArray() && reply.value.size() == bindings.size());
        var decoder = expressionDecoder(reply.writer);
        var statements = new ArrayList<DBSPLetStatement>();
        for (var node : reply.value)
            statements.add(decoder.decodeInner(node, DBSPLetStatement.class));
        return statements;
    }

    public record PendingBinding(long owner, List<Long> dependencies) {}
    public record BindingScope(List<Long> declarations, @Nullable Long closure, boolean outermost) {}

    public static Integer[] bindingPlacements(BindingScope scope, List<PendingBinding> bindings) {
        try {
            byte[] reply = transformWithContext("InnerCSE/Bindings", MAPPER.writeValueAsBytes(bindings),
                    0, "", MAPPER.writeValueAsBytes(scope));
            var placements = MAPPER.readValue(reply, Integer[].class);
            Utilities.enforce(placements.length == bindings.size());
            return placements;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid lazy binding placements from Rust", error);
        }
    }

    private record ClosureSnapshot(byte[] graph, Map<Long, IDBSPInnerNode> nodes) {
        static ClosureSnapshot capture(DBSPCompiler compiler, DBSPClosureExpression function) {
            var writer = new ToJsonInnerVisitor(compiler, JsonStream.compactCircuit(), 0);
            var nodes = new HashMap<Long, IDBSPInnerNode>();
            writer.liveNodes = nodes;
            function.accept(writer);
            return new ClosureSnapshot(writer.stream.toString().getBytes(StandardCharsets.UTF_8), nodes);
        }

        byte[] analyze(String pass) {
            return transformWithContext(pass, this.graph, 0, "", EMPTY_CONTEXT);
        }
    }

    public static List<DBSPExpression> mapFusionFragments(DBSPCompiler compiler, DBSPClosureExpression function) {
        var snapshot = ClosureSnapshot.capture(compiler, function);
        try {
            var fragments = new ArrayList<DBSPExpression>();
            for (long id : MAPPER.readValue(snapshot.analyze("FuseExpensiveMaps/Fragments"), long[].class))
                fragments.add(Utilities.getExists(snapshot.nodes, id).to(DBSPExpression.class));
            return fragments;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid map fusion fragments from Rust", error);
        }
    }

    public record StaticPlan(Set<Long> candidates, Set<Long> cells, Map<Long, Long> reads) {}

    public record RenumberedInitializer(DBSPExpression expression, int next) {}

    public static RenumberedInitializer renumberStaticErrors(DBSPCompiler compiler, DBSPExpression initializer, int first) {
        try {
            var reply = invokeLowering(compiler, "Statics/RenumberErrors", MAPPER.writeValueAsBytes(first), initializer);
            var expression = expressionDecoder(reply.writer).decodeInner(reply.value.required("initializer"), DBSPExpression.class);
            return new RenumberedInitializer(expression, reply.value.required("next").intValue());
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode the next static error index", error);
        }
    }

    public record StaticItem(DBSPStaticItem declaration, DBSPPathExpression reference) {}

    private static StaticItem decodeStaticItem(LoweringReply reply) {
        var decoder = expressionDecoder(reply.writer);
        return new StaticItem(decoder.decodeInner(reply.value.required(0), DBSPStaticItem.class),
                decoder.decodeInner(reply.value.required(1), DBSPPathExpression.class));
    }

    public static StaticItem declareStatic(DBSPCompiler compiler, DBSPStaticExpression existing) {
        return decodeStaticItem(invokeLowering(compiler, "Statics/Declare", existing));
    }

    public static DBSPExpression defineStatic(DBSPCompiler compiler, DBSPExpression origin, DBSPExpression initializer,
            String rendered, boolean declare, List<DBSPStaticItem> declarations) {
        var reply = invokeLowering(compiler, declare ? "Statics/Define" : "Statics/Inline",
                rendered.getBytes(StandardCharsets.UTF_8), origin, initializer);
        if (!declare)
            return expressionDecoder(reply.writer).decodeInner(reply.value.required(0), DBSPStaticExpression.class);
        StaticItem item = decodeStaticItem(reply);
        declarations.add(item.declaration);
        return item.reference;
    }

    public static StaticPlan staticCandidates(DBSPCompiler compiler, DBSPClosureExpression closure) {
        var snapshot = ClosureSnapshot.capture(compiler, closure);
        try {
            var result = MAPPER.readTree(snapshot.analyze("ImplementStatics/Candidates"));
            if (result.has("untyped_array")) {
                var array = Utilities.getExists(snapshot.nodes, result.required("untyped_array").longValue());
                throw ConstantExpressions.untypedArray(array.to(DBSPArrayExpression.class));
            }
            return MAPPER.treeToValue(result.required("plan"), StaticPlan.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid static initializer candidates from Rust", error);
        }
    }

    public static int[][] mapFusionGroups(List<int[]> fingerprints) {
        try {
            byte[] reply = transformWithContext("FuseExpensiveMaps/Groups", MAPPER.writeValueAsBytes(fingerprints),
                    0, "", EMPTY_CONTEXT);
            return MAPPER.readValue(reply, int[][].class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid map fusion groups from Rust", error);
        }
    }

    public static NativeValueNumbering.Group[] numberValues(NativeValueNumbering.Request request) {
        try {
            byte[] reply = transformWithContext("InnerCSE/NumberValues", MAPPER.writeValueAsBytes(request),
                    0, "", EMPTY_CONTEXT);
            return MAPPER.readValue(reply, NativeValueNumbering.Group[].class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid expression value numbers from Rust", error);
        }
    }

    public static Set<ScopedValue> sharedExpressionUses(List<NumberedExpression> graph) {
        try {
            byte[] reply = transformWithContext("InnerCSE/Uses", MAPPER.writeValueAsBytes(graph), 0, "", EMPTY_CONTEXT);
            var shared = new HashSet<ScopedValue>();
            for (long[] pair : MAPPER.readValue(reply, long[][].class)) {
                if (pair.length != 2)
                    throw new IllegalStateException("Expected a closure and canonical expression ID");
                shared.add(new ScopedValue(pair[0], pair[1]));
            }
            return shared;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid closure-local expression uses from Rust", error);
        }
    }

    public static LeftJoinChainsVisitor.KeyChoice chooseChainKey(LeftJoinChainsVisitor.KeyInput facts) {
        try {
            byte[] reply = transformWithContext("LeftJoinChains/Key", MAPPER.writeValueAsBytes(facts), 0, "", EMPTY_CONTEXT);
            return MAPPER.readValue(reply, LeftJoinChainsVisitor.KeyChoice.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid left-join chain key from the Rust optimizer", error);
        }
    }

    public enum PredicateKind { NoNow, Residual, LT, LTE, GT, GTE }
    public record TemporalComparison(PredicateKind kind, DBSPExpression expression, @Nullable DBSPExpression clock) {}

    public record ConjunctWindow(DBSPExpression key, boolean lower) {}

    /** Return the window for each conjunct, preserving the live key's source position. */
    @Nullable
    public static List<ConjunctWindow> conjunctWindows(DBSPCompiler compiler, DBSPClosureExpression condition) {
        var reply = invokeLowering(compiler, "WindowGroups/Conjuncts", condition.parameters[0], condition.body);
        if (!answered(compiler, "WindowGroups/Conjuncts", reply.value))
            return null;
        var nodes = Objects.requireNonNull(reply.writer.liveNodes);
        var windows = new ArrayList<ConjunctWindow>(reply.value.size());
        for (var window : reply.value) {
            windows.add(window.isNull() ? null : new ConjunctWindow(
                    Utilities.getExists(nodes, window.required("key").longValue()).to(DBSPExpression.class),
                    window.required("lower").booleanValue()));
        }
        return windows;
    }

    @Nullable
    public static List<TemporalComparison> inferTemporalPredicates(DBSPCompiler compiler,
            DBSPParameter parameter, DBSPExpression body) {
        var reply = invokeLowering(compiler, "TemporalComparisons", parameter, body);
        return answered(compiler, "TemporalComparisons", reply.value) ? temporalComparisons(reply) : null;
    }

    public static List<TemporalComparison> classifyTemporalPredicates(DBSPCompiler compiler,
            DBSPExpression body, Set<Long> monotoneOperands) {
        try {
            var reply = invokeLowering(compiler, "TemporalComparisons", MAPPER.writeValueAsBytes(monotoneOperands), body);
            return temporalComparisons(reply);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode temporal monotonicity facts", error);
        }
    }

    private static List<TemporalComparison> temporalComparisons(LoweringReply reply) {
        var decoder = expressionDecoder(reply.writer);
        var parts = new ArrayList<TemporalComparison>(reply.value.size());
        for (var part : reply.value) {
            var kind = PredicateKind.valueOf(part.required("kind").textValue());
            var expression = decoder.decodeInner(part.required("expression"), DBSPExpression.class);
            var clock = part.has("clock") ? decoder.decodeInner(part.required("clock"), DBSPExpression.class) : null;
            parts.add(new TemporalComparison(kind, expression, clock));
        }
        return parts;
    }

    public record TemporalGroup(int first, int count, @Nullable DBSPExpression expression) {}

    @Nullable
    public static List<TemporalGroup> groupTemporalFilters(DBSPCompiler compiler, List<String> kinds,
                                                            List<IDBSPInnerNode> roots) {
        var reply = temporalGroupRequest(compiler, "TemporalGroups", kinds, roots);
        if (!answered(compiler, "TemporalGroups", reply.value))
            return null;
        var decoder = expressionDecoder(reply.writer);
        var groups = new ArrayList<TemporalGroup>();
        for (var group : reply.value) {
            var value = group.required("expression");
            groups.add(new TemporalGroup(group.required("first").intValue(), group.required("count").intValue(),
                    value.isNull() ? null : decoder.decodeInner(value, DBSPExpression.class)));
        }
        return groups;
    }

    @Nullable
    public static Boolean acceptsTemporalBound(DBSPCompiler compiler, List<String> kinds, List<IDBSPInnerNode> roots) {
        var value = temporalGroupRequest(compiler, "MergeTemporalBounds", kinds, roots).value;
        return answered(compiler, "MergeTemporalBounds", value) ? value.booleanValue() : null;
    }

    private static LoweringReply temporalGroupRequest(DBSPCompiler compiler, String pass,
                                                       List<String> kinds, List<IDBSPInnerNode> roots) {
        try {
            return invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(kinds), roots.toArray(IDBSPInnerNode[]::new));
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode temporal filter groups", error);
        }
    }

    private record TemporalPath(long filter, long source, int port, List<Long> maps) {}

    public static DBSPClosureExpression composeTimestamp(DBSPCompiler compiler, DBSPClosureExpression timestamp,
                                                         List<DBSPClosureExpression> maps) {
        var inputs = new ArrayList<DBSPClosureExpression>(maps.size() + 1);
        inputs.add(timestamp);
        inputs.addAll(maps);
        return loweredClosure(compiler, invokeLowering(compiler, "WindowGroups/ComposeTimestamp",
                inputs.toArray(DBSPClosureExpression[]::new)));
    }

    private static void scanTemporalWindows(DBSPCompiler compiler, DBSPCircuit circuit, ReorderTemporalFilters.Scan scan) {
        var operators = operatorsById(circuit);
        var payloads = new HashSet<Long>();
        for (var op : operators.values())
            if (op instanceof DBSPFilterOperator || op instanceof DBSPMapOperator || op instanceof DBSPMapIndexOperator)
                payloads.add(op.id);
        var writer = OptimizerTopology.selectedExpressions(compiler, circuit, payloads, OptimizerTopology.Details.NONE);
        try {
            byte[] result = transformWithContext("WindowGroups/Scan",
                    writer.getJsonString().getBytes(StandardCharsets.UTF_8), 0, "",
                    MAPPER.writeValueAsBytes(compiler.metadata.windowSharingThreshold()));
            var reply = MAPPER.readTree(result);
            var groups = reply.get("groups");
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision("WindowGroups/Scan", groups != null);
            if (groups != null) {
                var chosen = new LinkedHashMap<DBSPFilterOperator, Integer>();
                for (var group : groups)
                    for (var member : group) {
                        var filter = Utilities.getExists(operators, member.required(0).longValue()).to(DBSPFilterOperator.class);
                        Utilities.putNew(chosen, filter, member.required(1).intValue());
                    }
                scan.useSharedInputs(chosen, groups.size());
                return;
            }
            List<TemporalPath> paths = MAPPER.convertValue(reply.required("paths"), new TypeReference<>() {});
            var sources = new LinkedHashMap<DBSPFilterOperator, ReorderTemporalFilters.SourceAndMaps>();
            for (var path : paths) {
                var source = Utilities.getExists(operators, path.source()).getOutput(path.port());
                var maps = path.maps().stream().map(id -> Utilities.getExists(operators, id)
                        .to(DBSPSimpleOperator.class).getClosureFunction()).toList();
                var filter = Utilities.getExists(operators, path.filter()).to(DBSPFilterOperator.class);
                Utilities.putNew(sources, filter, new ReorderTemporalFilters.SourceAndMaps(source, maps));
            }
            scan.chooseFromPaths(sources);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid temporal filter source paths", error);
        }
    }

    public record TemporalCandidate(String fingerprint, int conjunct) {}
    public record TemporalBound(String fingerprint, boolean lower) {}

    /** Each sharing group contains pairs of filter and conjunct ordinals. */
    public static int[][][] sharedTemporalInputs(List<List<TemporalCandidate>> candidates, int threshold) {
        try {
            byte[] reply = transformWithContext("WindowGroups/TemporalInputs", MAPPER.writeValueAsBytes(candidates),
                    0, "", MAPPER.writeValueAsBytes(threshold));
            return MAPPER.readValue(reply, int[][][].class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid temporal filter groups from the Rust optimizer", error);
        }
    }

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
        if (this.insertLimiters != null) {
            var state = MAPPER.createObjectNode();
            state.put("next_generated_name", DBSPVariablePath.nextGeneratedName());
            state.put("gen2", this.compiler.options.ioOptions.gen2);
            state.putArray("singletons");
            state.set("operators", LimiterInputs.capture(this.insertLimiters, circuit));
            var functions = LimiterInputs.transferRoots(this.insertLimiters, circuit);
            if (!functions.isEmpty())
                state.putObject("transfer_roots").putRawValue("checkpoint",
                        new RawValue(LimiterInputs.encodeTransferRoots(writer, functions)));
            return MAPPER.writeValueAsBytes(state);
        }
        if (this.asofRetention != null)
            return MAPPER.writeValueAsBytes(this.asofRetention.retentionJoins());
        if (this.nativeName.equals("LowerCircuitVisitor"))
            return CircuitLoweringTypes.encode(writer, circuit, this.compiler.options.ioOptions.gen2);
        if (this.nativeName.equals("IndexedInputs"))
            return MAPPER.writeValueAsBytes(Map.of("dedup_keys", this.compiler.options.ioOptions.gen2,
                    "lazy_upsert", this.compiler.metadata.lazyUpsert()));
        if (this.aggregateZeros != null)
            return MAPPER.writeValueAsBytes(this.aggregateZeros.hoistOrder());
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

    private record AsofRetention(long retainer, long join, boolean rebuilt) {}

    private boolean reusesOperators() {
        return this.nativeName.equals("TrimWindows") || this.insertLimiters != null || this.mergeFilters != null;
    }

    private DBSPCircuit readGraph(DBSPCircuit input, JsonNode result, ToJsonOuterVisitor writer) throws IOException {
        JsonNode graph = result.required("circuit");
        if (graph.isNull())
            return input;
        var shuffles = Objects.requireNonNull(writer.innerVisitor.liveShuffles);
        // The clone passes preserve unchanged operators. Later projection passes
        // use their object identity to recognize maps they have already analyzed.
        // A keyed source can keep its wire ID when it becomes a waterline operator.
        var originalOperators = this.reusesOperators()
                ? operatorsById(input) : Map.<Long, DBSPOperator>of();
        var decoder = new JsonDecoder(null, writer.liveProvenanceDecoder()) {
            @Override
            DBSPOperator decodedOperator(long id, DBSPOperator decoded) {
                var original = originalOperators.get(id);
                if (original == null || original.getClass() != decoded.getClass()
                        || !original.inputs.equals(decoded.inputs))
                    return decoded;
                if (original instanceof DBSPNestedOperator nested &&
                        (!nested.sameCircuit(decoded.to(DBSPNestedOperator.class))
                                || !nested.internalOutputs.equals(decoded.to(DBSPNestedOperator.class).internalOutputs)))
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
        if (this.asofLowering != null) {
            for (var binding : MAPPER.treeToValue(result.required("asof_retention"), AsofRetention[].class)) {
                var retainer = decoder.outer.lookup(binding.retainer, DBSPOperator.class).to(IGCOperator.class);
                var source = binding.rebuilt
                        ? Objects.requireNonNull(findOperator(input.allOperators, binding.retainer)).to(IGCOperator.class)
                        : retainer;
                var join = decoder.outer.lookup(binding.join, DBSPConcreteAsofJoinOperator.class);
                this.asofLowering.recordRetainer(source, retainer, join);
            }
        }
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
        if (this.identityMaps != null) {
            var live = operatorsById(circuit);
            this.identityMaps.remove.clear();
            Map<Long, Boolean> actions = MAPPER.convertValue(result.required("identity_actions"), new TypeReference<>() {});
            actions.forEach((id, forward) -> this.identityMaps.remove.put(
                    Objects.requireNonNull(live.get(id), "Unknown identity map " + id), forward));
            return circuit;
        }
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
            if (result.has("limiter_refusal")) {
                JsonNode refusal = result.required("limiter_refusal");
                var operator = findOperator(circuit.allOperators, refusal.required("operator").longValue());
                String message = result.required("message").textValue();
                DBSPVariablePath.restoreGeneratedNames(result.required("next_generated_name").longValue());
                switch (refusal.required("kind").textValue()) {
                    case "gen2_key_lateness" -> {
                        if (operator instanceof DBSPSourceMultisetOperator table)
                            throw new UnimplementedException(message, table.getRelNode());
                    }
                    case "missing_waterline" -> {
                        if (operator instanceof DBSPSinkOperator sink)
                            throw new CompilationError(message, sink.metadata.columns
                                    .get(refusal.required("column").intValue()).getPositionRange());
                    }
                }
                throw new IllegalStateException("Unknown limiter diagnostic site " + refusal);
            }
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

    @Nullable
    static DBSPClosureExpression pulledPredicate(DBSPCompiler compiler, FilterPullEdits.Site site,
                                                 DBSPSimpleOperator producer, DBSPClosureExpression predicate) {
        var reply = switch (site) {
            case Map, MapIndex -> invokeLowering(compiler, "PullFilters/" + site, producer.getClosureFunction(), predicate);
            case Aggregate -> invokeLowering(compiler, "PullFilters/Aggregate",
                    producer.inputs.get(0).outputType(), producer.outputPort().outputType(), predicate);
        };
        var function = reply.value.required("function");
        var result = function.isNull() ? null : expressionDecoder(reply.writer).decodeInner(function, DBSPClosureExpression.class);
        // Even a rejected aggregate predicate can consume a variable name and emit warnings.
        finishLowering(compiler, reply.value);
        return result;
    }

    private DBSPCircuit applyStreamEdits(DBSPCircuit circuit, JsonNode edits) {
        if (edits.isEmpty())
            return circuit;
        var originals = new HashMap<Long, DBSPOperator>();
        visitOperators(circuit.getAllOperators(), op -> originals.put(op.id, op));
        return new CircuitCloneVisitor(this.compiler, false) {
            private OutputPort input(JsonNode port) {
                var source = Objects.requireNonNull(originals.get(port.required(0).longValue()), "Unknown stream input");
                return this.mapped(source.getOutput(port.required(1).intValue()));
            }

            @Override
            public void replace(DBSPSimpleOperator op) {
                var edit = edits.get(Long.toString(op.id));
                if (edit == null) {
                    super.replace(op);
                    return;
                }
                if (!this.visited.add(op))
                    return;
                switch (edit.required("kind").textValue()) {
                    case "MapWithFunction" -> {
                        var input = this.mapped(op.to(DBSPUnaryOperator.class).input());
                        this.map(op, new DBSPMapOperator(op.getRelNode(), op.getFunction(), op.getOutputZSetType(), input));
                    }
                    case "Delta" -> this.map(op, PropagateEmptySources.emptyStream(op, StreamKind.DELTA));
                    case "Collection" -> this.map(op, PropagateEmptySources.emptyStream(op, StreamKind.COLLECTION));
                    case "Forward" -> this.map(op.outputPort(), this.input(edit.required("inputs").required(0)), false);
                    case "Discard" -> { /* The shared integral no longer needs this retention policy. */ }
                    case "LeftJoin" -> {
                        var left = this.input(edit.required("inputs").required(0));
                        var function = lowerFunction(RustCircuitPass.this.compiler, "EmptyRightJoin",
                                op.getClosureFunction(), left.outputType(), DBSPClosureExpression.class);
                        this.map(op, new DBSPMapOperator(op.getRelNode(), function, left));
                    }
                    case "Sum" -> {
                        var inputs = new ArrayList<OutputPort>();
                        edit.required("inputs").forEach(port -> inputs.add(this.input(port)));
                        this.map(op, op.withInputs(inputs, false).to(DBSPSimpleOperator.class));
                    }
                    default -> throw new IllegalStateException("Unknown stream edit " + edit);
                }
            }
        }.apply(circuit);
    }

    private DBSPCircuit trimFilterInputs(DBSPCircuit circuit, TrimFilters analysis, long next) throws IOException {
        var candidates = analysis.candidateReaders(circuit, true);
        if (candidates.isEmpty())
            return circuit;
        var reply = this.invoke("Topology/TrimFilters", OptimizerTopology.encode(circuit), next,
                MAPPER.writeValueAsBytes(candidates));
        Set<Long> selected = MAPPER.convertValue(reply, new TypeReference<>() {});
        if (selected.isEmpty())
            return circuit;
        return new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void replace(DBSPSimpleOperator reader) {
                if (!selected.contains(reader.id)) {
                    super.replace(reader);
                    return;
                }
                if (this.visited.contains(reader))
                    return;
                var filter = reader.inputs.get(0).operator.to(DBSPFilterOperator.class);
                var lowered = invokeLowering(RustCircuitPass.this.compiler, "TrimFilters/Closures",
                        reader.getClosureFunction(), filter.getClosureFunction());
                DBSPVariablePath.restoreGeneratedNames(lowered.value.required("next_generated_name").longValue());
                var functions = lowered.value.required("functions");
                if (functions.isNull()) {
                    super.replace(reader);
                    return;
                }
                this.visited.add(reader);
                var decoder = expressionDecoder(lowered.writer);
                var project = decoder.decodeInner(functions.required(0), DBSPClosureExpression.class);
                var predicate = decoder.decodeInner(functions.required(1), DBSPClosureExpression.class);
                var consume = decoder.decodeInner(functions.required(2), DBSPClosureExpression.class);
                var pre = new DBSPMapOperator(filter.getRelNode(), project, this.mapped(filter.input()));
                this.addOperator(pre);
                var narrowed = new DBSPFilterOperator(filter.getRelNode(), predicate, pre.outputPort());
                this.addOperator(narrowed);
                this.map(reader, reader instanceof DBSPMapIndexOperator
                        ? new DBSPMapIndexOperator(reader.getRelNode(), consume, narrowed.outputPort())
                        : new DBSPMapOperator(reader.getRelNode(), consume, narrowed.outputPort()));
            }
        }.apply(circuit);
    }

    /** Keep the scheduler around stateful native analyses so each consumer sees
     * the current round's results and every nested boundary remains observable. */
    private RustCircuitPass nativeStage(CircuitTransform stage) {
        return new RustCircuitPass(this.compiler, stage, this.snapshots);
    }

    /** Keep independent analysis state for the native sequence. The differential
     * oracle can run the original sequence alongside it on the same compiler. */
    @Nullable private CircuitTransform selectStages(CircuitTransform original) {
        if (original instanceof RemoveStarJoins group)
            return new Repeat(this.compiler, this.nativeStage(group.transform), group.repeats);
        if (original instanceof OptimizeWithGraph group && group.transform instanceof Passes iteration) {
            var steps = new Passes(iteration.getName(), this.compiler);
            for (var step : iteration.passes)
                steps.add(this.nativeStage(step));
            return new Repeat(this.compiler, steps, group.repeats);
        }
        if (original instanceof Gen2Passes) {
            var stages = new Gen2Passes(this.compiler);
            stages.passes.replaceAll(this::nativeStage);
            return stages;
        }
        if (original instanceof CSE || original instanceof FuseIntoArrayAgg) {
            Repeat repeat = original instanceof CSE ? new CSE(this.compiler) : new FuseIntoArrayAgg(this.compiler);
            ((Passes) repeat.transform).passes.replaceAll(this::selectAnalysis);
            return repeat;
        }

        Passes stages;
        if (original instanceof ExpandAggregateZero)
            stages = new ExpandAggregateZero(this.compiler);
        else if (original instanceof RemoveIdentityOperators)
            stages = new RemoveIdentityOperators(this.compiler);
        else if (original instanceof TagRegions)
            stages = new TagRegions(this.compiler);
        else if (original instanceof DeadCode dead)
            stages = new DeadCode(this.compiler, dead.keepAllSources);
        else
            return null;
        stages.passes.replaceAll(this::selectAnalysis);
        return stages;
    }

    private CircuitTransform selectAnalysis(CircuitTransform stage) {
        if (stage instanceof Graph || stage instanceof DeadCode || stage instanceof FindDeadCode
                || stage instanceof CSE.FindCSE || stage instanceof FuseIntoArrayAgg.FindFusable
                || stage instanceof FuseIntoArrayAgg.Fuse || stage instanceof ExpandAggregateZero.ExpandAggregateZeros
                || stage instanceof TagRegions.DetectConflicts || stage instanceof RemoveIdentityOperators.Decide)
            return this.nativeStage(stage);
        return stage;
    }

    /** Populate the alias map shared with RemoveCSE for this iteration. */
    private void findCommonExpressions(DBSPCircuit circuit, CSE.FindCSE finder, long next) throws IOException {
        var probe = this.invoke("CSE/Candidates", OptimizerTopology.operatorInputs(circuit), next, EMPTY_CONTEXT);
        Set<Long> payloads = MAPPER.convertValue(probe, new TypeReference<Set<Long>>() {});
        var seen = Objects.requireNonNull(this.seenConstants);
        if (!payloads.isEmpty()) {
            var writer = OptimizerTopology.selectedExpressions(this.compiler, circuit, payloads, OptimizerTopology.Details.NONE);
            var result = this.invoke("CSE", writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                    next, this.passState(circuit, writer));
            var live = operatorsById(circuit);
            for (var pair : result.required("aliases")) {
                var original = Objects.requireNonNull(live.get(pair.required(0).longValue()), "Unknown CSE operand");
                var base = Objects.requireNonNull(live.get(pair.required(1).longValue()), "Unknown CSE representative");
                finder.setCanonical(original, base);
            }
        }
        visitConstants(circuit.getAllOperators(), seen::add);
    }

    private void collectKeptOperators(DBSPCircuit circuit, JsonNode pruning, Set<DBSPOperator> keep) {
        var drop = new HashSet<Long>();
        for (var id : pruning.required("pruned_operators"))
            drop.add(id.longValue());
        visitOperators(circuit.getAllOperators(), op -> {
            if (!drop.remove(op.id))
                keep.add(op);
        });
        if (!drop.isEmpty())
            throw new IllegalStateException("DeadCode returned unknown operators " + drop);
        this.reportUnusedSources(circuit, pruning.required("unused_sources"));
    }

    private DBSPCircuit replaceSharedProjections(DBSPCircuit circuit, FindCommonProjections analysis)
            throws IOException {
        if (analysis.outputProjection.isEmpty())
            return circuit;
        var candidates = new HashSet<>(analysis.inputProjection.keySet());
        candidates.addAll(analysis.outputProjection.keySet());
        var sites = MAPPER.createArrayNode();
        for (var op : candidates) {
            var site = sites.addObject().put("id", op.id).put("kind", op.getClass().getSimpleName())
                    .put("reader", analysis.inputProjection.containsKey(op));
            var projection = analysis.outputProjection.get(op);
            if (projection != null)
                site.put("indexed", projection.getResultType().is(DBSPTypeRawTuple.class));
        }
        var edits = this.invoke("SharedProjections/Edits", MAPPER.writeValueAsBytes(sites),
                DBSPVariablePath.nextGeneratedName(), EMPTY_CONTEXT);
        return LiveSharedProjections.apply(this.compiler, circuit, analysis, MAPPER.convertValue(edits,
                new TypeReference<Map<Long, LiveSharedProjections.Edits>>() {}));
    }

    private DBSPCircuit retainLinearAggregateKeys(DBSPCircuit circuit, long next) throws IOException {
        var graph = new Graph(this.compiler);
        var buildGraph = new RustCircuitPass(this.compiler, graph, this.snapshots);
        this.compiler.applyObservedPass(this.name, 0, 0, buildGraph, circuit);
        var plan = this.invoke("Topology/LinearPostprocessRetainKeys",
                OptimizerTopology.encode(circuit, OptimizerTopology.Details.CONSUMERS), next, EMPTY_CONTEXT);
        if (plan.isNull())
            return circuit;

        circuit.applyOperatorOrder(MAPPER.treeToValue(plan.required("order"), long[].class));
        this.compiler.observeCircuit(this.name, 1, "OrderWaterlineDependencies", circuit);
        this.compiler.applyObservedPass(this.name, 2, 0, buildGraph, circuit);

        var operators = operatorsById(circuit);
        var retainers = new HashMap<DBSPAggregateLinearPostprocessOperator, DBSPIntegrateTraceRetainKeysOperator>();
        Map<Long, Long> selected = MAPPER.convertValue(plan.required("retainers"), new TypeReference<>() {});
        selected.forEach((aggregate, retainer) -> retainers.put(
                Objects.requireNonNull(operators.get(aggregate)).to(DBSPAggregateLinearPostprocessOperator.class),
                Objects.requireNonNull(operators.get(retainer)).to(DBSPIntegrateTraceRetainKeysOperator.class)));
        var replace = new LinearPostprocessRetainKeys.ReplaceLinear(this.compiler, graph.getGraphs(), retainers);
        return this.compiler.applyObservedPass(this.name, 3, 0, replace, circuit);
    }

    private DBSPCircuit bypassNoops(DBSPCircuit circuit, long next) throws IOException {
        var reply = this.invoke("Topology/BypassNoops", OptimizerTopology.encode(circuit), next, EMPTY_CONTEXT);
        Set<Long> bypassed = MAPPER.convertValue(reply, new TypeReference<>() {});
        if (bypassed.isEmpty())
            return circuit;
        return new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void postorder(DBSPNoopOperator op) {
                if (bypassed.contains(op.id))
                    this.map(op.outputPort(), this.mapped(op.input()), false);
                else
                    super.postorder(op);
            }
        }.apply(circuit);
    }

    private DBSPCircuit separateTraceInputs(DBSPCircuit circuit, JsonNode selected) {
        if (selected.isEmpty())
            return circuit;
        Map<Long, List<Integer>> readers = MAPPER.convertValue(selected, new TypeReference<>() {});
        var result = new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void replace(DBSPSimpleOperator operator) {
                var slots = readers.remove(operator.id);
                if (slots == null) {
                    super.replace(operator);
                    return;
                }
                if (!this.visited.add(operator))
                    return;
                var inputs = new ArrayList<>(Linq.map(operator.inputs, this::mapped));
                for (int slot : slots) {
                    var noop = new DBSPNoopOperator(operator.getRelNode(), inputs.get(slot));
                    this.addOperator(noop);
                    inputs.set(slot, noop.outputPort());
                }
                this.map(operator, operator.withInputs(inputs, false).to(DBSPSimpleOperator.class));
            }
        }.apply(circuit);
        if (!readers.isEmpty())
            throw new IllegalStateException("Unknown trace readers " + readers.keySet());
        return result;
    }

    private DBSPCircuit fuseChains(DBSPCircuit circuit, long next) throws IOException {
        var topology = OptimizerTopology.encode(circuit, OptimizerTopology.Details.CHAIN_FUNCTIONS);
        var plan = this.invoke("Topology/ChainReaders", topology, next, EMPTY_CONTEXT);
        if (plan.isEmpty())
            return circuit;
        Set<Long> readers = MAPPER.convertValue(plan, new TypeReference<>() {});
        return new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void replace(DBSPSimpleOperator operator) {
                if (!readers.contains(operator.id)) {
                    super.replace(operator);
                    return;
                }
                if (!this.visited.add(operator))
                    return;
                var reader = operator.to(DBSPUnaryOperator.class);
                var chain = ChainVisitor.extendChain(reader, this.mapped(reader.input()));
                this.map(reader, Objects.requireNonNull(chain, "Invalid chain reader " + reader.id));
            }
        }.apply(circuit);
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

    private record ViewFacts(long id, String name, boolean recursive, boolean lateness, long[] input) {}

    public boolean removesAllViews() {
        return this.nativeName.equals("RemoveAllViews");
    }

    private DBSPCircuit removeViews(DBSPCircuit circuit, long next) throws IOException {
        var views = new ArrayList<ViewFacts>();
        visitOperators(circuit.getAllOperators(), op -> {
            if (op instanceof DBSPViewOperator view) {
                var input = view.input();
                views.add(new ViewFacts(view.id, view.viewName.name(), view.metadata.recursive,
                        view.metadata.hasLateness(), new long[]{input.operator.id, input.outputNumber}));
            }
        });
        if (views.isEmpty())
            return circuit;
        String pass = this.removesAllViews() ? "Views/RemoveAll" : "Views/RemoveOrdinary";
        var edits = this.invoke(pass, MAPPER.writeValueAsBytes(views), next, EMPTY_CONTEXT);
        return this.applyStreamEdits(circuit, edits);
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
        var reply = this.invoke("WindowGroups/Indexes", writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                next, MAPPER.writeValueAsBytes(this.compiler.metadata.windowSharingThreshold()));
        var plans = reply.get("plans");
        if (this.compiler.stateObserver != null)
            this.compiler.stateObserver.nativeDecision("WindowGroups/Indexes", plans != null);
        if (plans != null) {
            NativeWindowGroups.installPlans(analysis, windows, MAPPER.convertValue(plans, new TypeReference<>() {}));
            return;
        }
        var selected = new ArrayList<DBSPWindowOperator>();
        for (long id : MAPPER.treeToValue(reply.required("candidates"), long[].class))
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

    public record KeyColumnCopy(int parameter, Column source, Column output) {}

    /** Returns null when the reference visitor must handle nesting depth or report a diagnostic. */
    @Nullable
    public static List<KeyColumnCopy> columnCopies(DBSPCompiler compiler, DBSPClosureExpression closure,
                                                   CollectionShape shape, boolean lossless) {
        return analyzeKeyClosure(compiler, "KeyAnalysis/ColumnCopies", closure, shape, lossless,
                null, null, new TypeReference<>() {});
    }

    public enum KeyTransformKind { Identity, Projection }
    public record KeyTransform(KeyTransformKind kind, @Nullable List<KeyColumnCopy> columns) {}
    public record PortKeyFacts(long operator, List<KeyColumns> keys, @Nullable KeyTransform transform) {}

    public static boolean circuitKeys(DBSPCompiler compiler, DBSPCircuit circuit, boolean lossless,
                                      BiConsumer<DBSPSimpleOperator, PortKeyFacts> install) {
        try {
            String pass = "KeyAnalysis/Circuit";
            var bytes = transformWithContext(pass, OptimizerTopology.candidateKeys(compiler, circuit), 0, "",
                    MAPPER.writeValueAsBytes(lossless));
            var result = MAPPER.readTree(bytes);
            if (!answered(compiler, pass, result))
                return false;
            if (!result.isEmpty()) {
                var operators = operatorsById(circuit);
                for (var facts : MAPPER.treeToValue(result, PortKeyFacts[].class))
                    install.accept(Utilities.getExists(operators, facts.operator()).to(DBSPSimpleOperator.class), facts);
            }
            return true;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not read circuit candidate keys", error);
        }
    }

    public enum KeyProjection { Row, Ranked }

    /** Analyses of one closure can succeed independently. Projected keys require known column copies. */
    public record OutputColumns(@Nullable List<KeyColumnCopy> copies, @Nullable List<List<Column>> groups,
                                @Nullable List<KeyColumns> keys) {}

    public static OutputColumns outputColumns(DBSPCompiler compiler, DBSPClosureExpression closure,
                                              CollectionShape shape, boolean lossless) {
        return outputColumns(compiler, closure, shape, lossless, null, null);
    }

    public static OutputColumns outputColumns(DBSPCompiler compiler, DBSPClosureExpression closure,
                                              CollectionShape shape, boolean lossless,
                                              @Nullable KeyProjection projection, @Nullable List<KeyColumns> keys) {
        OutputColumns result = analyzeKeyClosure(compiler, "KeyAnalysis/OutputColumns", closure, shape, lossless,
                projection, keys, new TypeReference<>() {});
        if (projection != null && compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision("KeyAnalysis/" + projection, result.keys() != null);
        return result;
    }

    @Nullable
    public static Set<Column> fixedColumns(DBSPCompiler compiler, DBSPClosureExpression closure,
                                           CollectionShape shape, boolean lossless) {
        return analyzeKeyClosure(compiler, "KeyAnalysis/FixedColumns", closure, shape, lossless,
                null, null, new TypeReference<>() {});
    }

    @Nullable
    public static List<KeyColumns> filterKeys(DBSPCompiler compiler, DBSPClosureExpression closure,
                                              CollectionShape shape, boolean lossless, List<KeyColumns> keys) {
        List<KeyColumns> result = analyzeKeyClosure(compiler, "KeyAnalysis/FilterKeys", closure, shape, lossless,
                null, keys, new TypeReference<>() {});
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision("KeyAnalysis/FilterKeys", result != null);
        return result;
    }

    @Nullable
    private static <T> T analyzeKeyClosure(DBSPCompiler compiler, String pass, DBSPClosureExpression closure,
                                           CollectionShape shape, boolean lossless, @Nullable KeyProjection projection,
                                           @Nullable List<KeyColumns> keys, TypeReference<T> type) {
        record Request(int width, @Nullable Integer indexFields, boolean lossless,
                       @Nullable KeyProjection projection, @Nullable List<KeyColumns> keys) {}
        var request = new Request(shape.width(), shape instanceof IndexedShape i ? i.indexFields() : null, lossless,
                projection, keys);
        return exchangeKeyClosure(compiler, pass, closure, request, type);
    }

    public static OutputColumns joinOutputColumns(DBSPCompiler compiler, DBSPClosureExpression closure,
                                                  CollectionShape shape, boolean lossless,
                                                  List<List<KeyColumns>> inputs, boolean leftJoin) {
        record Join(List<List<KeyColumns>> inputs, boolean leftJoin) {}
        record Request(int width, @Nullable Integer indexFields, boolean lossless, Join join) {}
        var request = new Request(shape.width(), shape instanceof IndexedShape i ? i.indexFields() : null, lossless,
                new Join(inputs, leftJoin));
        OutputColumns result = exchangeKeyClosure(compiler, "KeyAnalysis/JoinColumns", closure, request,
                new TypeReference<>() {});
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision("KeyAnalysis/JoinColumns", result.keys() != null);
        return result;
    }

    @Nullable
    private static <T> T exchangeKeyClosure(DBSPCompiler compiler, String pass, DBSPClosureExpression closure,
                                            Object request, TypeReference<T> type) {
        var json = JsonStream.compactCircuit();
        json.beginArray();
        closure.accept(new ToJsonInnerVisitor(compiler, json, 0));
        try {
            var graph = json.endArray().toString().getBytes(StandardCharsets.UTF_8);
            var reply = transformWithContext(pass, graph, 0, "", MAPPER.writeValueAsBytes(request));
            return MAPPER.readValue(reply, type);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not exchange column provenance with Rust", error);
        }
    }

    /** The lineage interpreter supplies plain column copies from the join function. */
    public static List<KeyColumns> joinKeys(List<List<KeyColumns>> inputs, List<KeyColumnCopy> copies,
                                            boolean leftJoin) {
        record Request(List<List<KeyColumns>> inputs, List<KeyColumnCopy> copies, boolean leftJoin) {}
        try {
            var request = MAPPER.writeValueAsBytes(new Request(inputs, copies, leftJoin));
            var result = transformWithContext("KeyAnalysis/Join", request, 0, "", EMPTY_CONTEXT);
            return MAPPER.readValue(result, new TypeReference<List<KeyColumns>>() {});
        } catch (IOException error) {
            throw new UncheckedIOException("Could not exchange join keys with Rust", error);
        }
    }

    public enum TransferRule { Identity, DeclaredColumns, ApplyClosure, ProjectIndex, JoinClosure,
        AggregateKey, RollingTimestamp, IntersectRows, IntersectKeys, PrimitiveMinimumOrKey,
        ChainMinimumOrKey, InvalidIndexedSource, Analyze }
    public enum ClosureBody { Operator, IndexedValue, Filter, Hop, Lag }
    public enum FieldWaterline { Whole, Unbounded, Unit, AllColumns }
    public record IndexedWaterline(boolean requireInputKey, FieldWaterline key, FieldWaterline value) {}
    public record MonotonicityTransfer(DBSPOperator operator, TransferRule rule, int input,
                                       @Nullable List<Boolean> lateness, @Nullable IndexedWaterline projection,
                                       @Nullable ClosureBody function) {}

    /** Validate the graph after each pass. Consecutive passes often leave its ports,
     * source kinds and sink modes identical, even when they rewrite expressions. */
    public static final class StreamValidator {
        @Nullable private byte[] previous;

        public void validate(DBSPCircuit circuit) {
            byte[] topology = OptimizerTopology.encode(circuit, OptimizerTopology.Details.STREAM_KINDS);
            if (java.util.Arrays.equals(this.previous, topology))
                return;
            try {
                var finding = MAPPER.readTree(transformWithContext(
                        "Topology/ValidateStreamKinds", topology, 0, "", EMPTY_CONTEXT));
                if (!finding.isNull())
                    report(circuit, finding);
                this.previous = topology;
            } catch (IOException error) {
                throw new UncheckedIOException("Invalid stream-kind validation result", error);
            }
        }

        private static void report(DBSPCircuit circuit, JsonNode finding) {
            var operator = Objects.requireNonNull(findOperator(circuit.allOperators, finding.required("operator").longValue()));
            switch (finding.required("reason").textValue()) {
                case "no_inputs" -> Utilities.fail("Operator " + operator + " has no inputs to derive its stream kind from");
                case "input" -> {
                    int slot = finding.required("input").intValue();
                    var kinds = MAPPER.convertValue(finding.required("expected"), StreamKind[].class);
                    Utilities.fail("Operator " + operator + " requires input " + slot + " to be one of " +
                            List.of(kinds) + ", but " + operator.inputs.get(slot) + " produces " + finding.required("actual").textValue());
                }
                case "table" -> throw new InternalCompilerError("Table " + operator + " produces a stream of " +
                        finding.required("actual").textValue() + " in a circuit whose tables must produce " +
                        finding.required("expected").textValue(), operator);
                case "output" -> {
                    var sink = operator.to(DBSPSinkOperator.class);
                    throw new InternalCompilerError("Output " + sink.viewName.singleQuote() + " receives a stream of " +
                            finding.required("actual").textValue() + " but the circuit must produce " +
                            finding.required("expected").textValue(), sink);
                }
                default -> throw new IllegalStateException("Unknown stream-kind violation " + finding);
            }
        }
    }

    /** Reconnect the transfer schedule to the live expressions of the expanded circuit. */
    public static List<MonotonicityTransfer> monotonicityTransfers(DBSPCircuit circuit) {
        record Step(long operator, TransferRule rule, int input, @Nullable List<Boolean> lateness,
                    @Nullable IndexedWaterline projection, @Nullable ClosureBody function) {}
        byte[] topology = OptimizerTopology.encode(circuit, OptimizerTopology.Details.MONOTONICITY);
        byte[] result = transformWithContext("Topology/Monotonicity", topology, 0, "", EMPTY_CONTEXT);
        try {
            var steps = MAPPER.readValue(result, Step[].class);
            if (steps.length == 0)
                return List.of();
            var operators = operatorsById(circuit);
            var transfers = new ArrayList<MonotonicityTransfer>(steps.length);
            for (var step : steps)
                transfers.add(new MonotonicityTransfer(Utilities.getExists(operators, step.operator()),
                        step.rule(), step.input(), step.lateness(), step.projection(), step.function()));
            return transfers;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid monotonicity transfer schedule", error);
        }
    }

    /** Row renderings are inputs to the stable names of ASOF comparator declarations. */
    public static Map<Long, List<String>> asofRowTypes(DBSPCircuit circuit) {
        Map<Long, List<String>> rows = new HashMap<>();
        visitOperators(circuit.getAllOperators(), op -> {
            if (op instanceof DBSPAsofJoinOperator join)
                rows.put(op.id, List.of(join.getLeftInputValueType().toString(), join.getRightInputValueType().toString()));
        });
        return rows;
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

    @Nullable
    public static int[][] splitSharedIndexes(DBSPCompiler compiler, List<DBSPClosureExpression> functions, List<Boolean> fixed) {
        try {
            var reply = invokeLowering(compiler, "SharedIndexes/Split",
                    MAPPER.writeValueAsBytes(fixed), functions.toArray(DBSPClosureExpression[]::new)).value;
            if (reply.isNull())
                return null;
            var groups = MAPPER.treeToValue(reply.required("groups"), int[][].class);
            finishLowering(compiler, reply);
            return groups;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid shared index groups", error);
        }
    }

    private static void finishLowering(DBSPCompiler compiler, JsonNode state) {
        for (var warning : state.required("warnings"))
            compiler.reportWarning(SourcePositionRange.fromJson(warning.required("range")),
                    warning.required("error_type").textValue(), warning.required("message").textValue());
        DBSPVariablePath.restoreGeneratedNames(state.required("next_generated_name").longValue());
    }

    private static IDBSPInnerNode rewriteInnerRoot(DBSPCompiler compiler, String pass, IDBSPInnerNode root) {
        // Expression translators leave types untouched.
        if (root instanceof DBSPType)
            return root;
        var reply = invokeLowering(compiler, pass, root);
        var result = expressionDecoder(reply.writer).decodeInner(reply.value.required("root"), IDBSPInnerNode.class);
        finishLowering(compiler, reply.value);
        return result;
    }

    /** The first root is the reader. Each following pair binds a narrow parameter to its wide row. */
    public static IDBSPInnerNode rewriteIndexReader(DBSPCompiler compiler, List<IDBSPInnerNode> roots,
                                                     List<List<Integer>> columns) {
        try {
            var reply = invokeLowering(compiler, "SharedIndexes/Reader",
                    MAPPER.writeValueAsBytes(columns), roots.toArray(IDBSPInnerNode[]::new));
            return expressionDecoder(reply.writer).decodeInner(reply.value, IDBSPInnerNode.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode shared-index reader columns", error);
        }
    }

    public record WideIndexFields(DBSPVariablePath variable, DBSPExpression key, boolean nullable,
                                  List<DBSPExpression> fields, @Nullable List<List<Integer>> layout,
                                  List<List<DBSPExpression>> rows) {}

    public static WideIndexFields wideIndexFields(DBSPCompiler compiler, List<DBSPClosureExpression> functions) {
        var reply = invokeLowering(compiler, "SharedIndexes/WideFields", functions.toArray(DBSPClosureExpression[]::new));
        var decoder = expressionDecoder(reply.writer);
        var expressions = reply.value.required("expressions");
        var variable = decoder.decodeInner(expressions.required(0), DBSPVariablePath.class);
        var key = decoder.decodeInner(expressions.required(1), DBSPExpression.class);
        var fields = new ArrayList<DBSPExpression>(expressions.size() - 2);
        for (int i = 2; i < expressions.size(); i++)
            fields.add(decoder.decodeInner(expressions.required(i), DBSPExpression.class));
        List<List<Integer>> layout = MAPPER.convertValue(reply.value.required("layout"), new TypeReference<>() {});
        var rows = new ArrayList<List<DBSPExpression>>();
        if (layout == null) {
            int offset = 0;
            for (var width : reply.value.required("widths")) {
                int end = Math.addExact(offset, width.intValue());
                rows.add(fields.subList(offset, end));
                offset = end;
            }
            if (rows.size() != functions.size() || offset != fields.size())
                throw new IllegalStateException("Invalid column counts in Rust shared index");
        }
        finishLowering(compiler, reply.value);
        return new WideIndexFields(variable, key, reply.value.required("nullable").booleanValue(), fields, layout, rows);
    }

    @FunctionalInterface
    private interface KeyEquality {
        boolean equivalent(byte[] closures) throws IOException;
    }

    private static native byte[] discoverIndexes(byte[] snapshot, long next, byte[] visited, KeyEquality equality);

    private DBSPCircuit declareComparators(DBSPCircuit circuit, long next) throws IOException {
        byte[] types = OptimizerExpressions.encode(this.compiler, circuit, OptimizerExpressions.Roots.INDEX_TYPES);
        long[] indexes = MAPPER.treeToValue(this.invoke("Topology/OrderedIndexes", types, next, EMPTY_CONTEXT), long[].class);
        if (indexes.length == 0)
            return circuit;

        var operators = operatorsById(circuit);
        var roots = new DBSPExpression[indexes.length];
        for (int i = 0; i < indexes.length; i++)
            roots[i] = operators.get(indexes[i]).to(DBSPUnaryOperator.class).getFunction();
        var names = circuit.declarations.stream().map(DBSPDeclaration::getName).toList();
        var reply = invokeLowering(this.compiler, "ComparatorDeclarations/Functions", MAPPER.writeValueAsBytes(names), roots);
        var decoder = expressionDecoder(reply.writer);
        var functions = reply.value.required("functions");
        if (functions.size() != indexes.length)
            throw new IllegalStateException("Expected one function per ordered index");
        var replacements = new HashMap<Long, DBSPExpression>();
        for (int i = 0; i < indexes.length; i++)
            replacements.put(indexes[i], decoder.decodeInner(functions.required(i), DBSPExpression.class));
        var items = new ArrayList<DBSPComparatorItem>();
        for (var item : reply.value.required("items"))
            items.add(decoder.decodeInner(item, DBSPComparatorItem.class));

        return new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void replace(DBSPSimpleOperator operator) {
                var function = replacements.get(operator.id);
                if (function == null) {
                    super.replace(operator);
                    return;
                }
                var index = operator.to(DBSPUnaryOperator.class);
                var input = this.mapped(index.input());
                var type = index.getOutputIndexedZSetType();
                if (index instanceof DBSPMapIndexOperator)
                    this.map(index, new DBSPMapIndexOperator(index.getRelNode(), function, type, input));
                else
                    this.map(index, new DBSPFlatMapIndexOperator(index.getRelNode(), function, type, input));
            }

            @Override
            public void postorder(DBSPCircuit circuit) {
                for (var item : items)
                    this.getUnderConstructionCircuit().addDeclaration(new DBSPDeclaration(item));
                super.postorder(circuit);
            }
        }.apply(circuit);
    }

    private void findSharedIndexes(DBSPCircuit circuit, long next, FindSharedIndexes find) throws IOException {
        var details = OptimizerTopology.Details.SHARED_INDEXES;
        var payloads = this.invoke("Topology/FindSharedIndexes", OptimizerTopology.encode(circuit, details), next, EMPTY_CONTEXT);
        var selected = MAPPER.convertValue(payloads, new TypeReference<Set<Long>>() {});
        if (selected.isEmpty())
            return;
        var writer = OptimizerTopology.selectedForRewrite(this.compiler, circuit, selected, details);
        byte[] result = discoverIndexes(writer.getJsonString().getBytes(StandardCharsets.UTF_8), next,
                MAPPER.writeValueAsBytes(find.visitedIds()), bytes -> {
                    var keys = decodeClosurePair(new LoweringReply(writer.innerVisitor, MAPPER.readTree(bytes)));
                    return keys.left.equivalent(keys.right);
                });
        var state = MAPPER.readTree(result);
        find.record(MAPPER.treeToValue(state.required("groups"), FindSharedIndexes.IndexReader[][].class),
                MAPPER.treeToValue(state.required("visited"), long[].class), operatorsById(circuit));
        finishLowering(this.compiler, state);
    }

    private DBSPCircuit collapseSharedChains(DBSPCircuit circuit, long next, CollapseSharedChains analysis) throws IOException {
        var chains = analysis.chainIds();
        var payloads = new HashSet<Long>();
        for (var chain : chains)
            if (chain.size() > 1)
                payloads.addAll(chain);
        if (payloads.isEmpty())
            return circuit;
        var writer = OptimizerTopology.selectedForRewrite(this.compiler, circuit, payloads, OptimizerTopology.Details.NONE);
        var result = this.invoke("CollapseSharedChains/Closures", writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                next, MAPPER.writeValueAsBytes(chains));
        if (result.isEmpty())
            return circuit;
        var decoder = expressionDecoder(writer.innerVisitor);
        var operators = operatorsById(circuit);
        record Index(OutputPort source, DBSPClosureExpression function) {}
        var indexes = new HashMap<Long, Index>();
        for (var entry : result) {
            var source = Utilities.getExists(operators, entry.required("source").longValue());
            var port = source.getOutput(entry.required("port").intValue());
            var closure = decoder.decodeInner(entry.required("function"), DBSPClosureExpression.class);
            Utilities.putNew(indexes, entry.required("tail").longValue(), new Index(port, closure));
        }
        return new CircuitCloneVisitor(this.compiler, false) {
            @Override
            public void postorder(DBSPMapIndexOperator operator) {
                var index = indexes.get(operator.id);
                if (index == null)
                    super.postorder(operator);
                else
                    this.map(operator, new DBSPMapIndexOperator(operator.getRelNode(), index.function(),
                            operator.getOutputIndexedZSetType(), this.mapped(index.source())));
            }
        }.apply(circuit);
    }

    private void findCommonProjections(DBSPCircuit circuit, long next, FindCommonProjections analysis) throws IOException {
        analysis.clear();
        var details = OptimizerTopology.Details.CONSUMERS;
        var payloads = this.invoke("Topology/FindCommonProjections", OptimizerTopology.encode(circuit, details), next, EMPTY_CONTEXT);
        var selected = MAPPER.convertValue(payloads, new TypeReference<Set<Long>>() {});
        if (selected.isEmpty())
            return;
        var writer = OptimizerTopology.selectedForRewrite(this.compiler, circuit, selected, details);
        var result = this.invoke(this.nativeName, writer.getJsonString().getBytes(StandardCharsets.UTF_8), next, EMPTY_CONTEXT);
        var decoder = expressionDecoder(writer.innerVisitor);
        var operators = operatorsById(circuit);
        for (var entry : result.required("closures")) {
            var operator = Objects.requireNonNull(operators.get(entry.required("operator").longValue()));
            var closure = decoder.decodeInner(entry.required("function"), DBSPClosureExpression.class);
            var width = entry.required("original_output_size");
            if (width.isNull())
                Utilities.putNew(analysis.inputProjection, operator, closure);
            else {
                Utilities.putNew(analysis.outputProjection, operator, closure);
                Utilities.putNew(analysis.originalOutputSize, operator, width.intValue());
            }
        }
        DBSPVariablePath.restoreGeneratedNames(result.required("next_generated_name").longValue());
    }

    @Nullable
    public static List<List<List<Integer>>> parameterFieldDependencies(DBSPCompiler compiler, DBSPClosureExpression closure) {
        var result = invokeLowering(compiler, "ParameterFieldDependencies", closure).value;
        return answered(compiler, "ParameterFieldDependencies", result)
                ? MAPPER.convertValue(result, new TypeReference<>() {}) : null;
    }

    /** Analyze each distinct reader closure once per unused-field iteration. */
    public static Map<Long, List<List<Integer>>> mapFieldDependencies(DBSPCompiler compiler, DBSPCircuit circuit,
                                                                       Predicate<DBSPUnaryOperator> eligible) {
        var readers = new LinkedHashMap<Long, DBSPUnaryOperator>();
        visitOperators(circuit.getAllOperators(), op -> {
            if (op instanceof DBSPMapOperator || op instanceof DBSPMapIndexOperator) {
                var map = op.to(DBSPUnaryOperator.class);
                if (eligible.test(map))
                    readers.putIfAbsent(map.getFunction().id, map);
            }
        });
        return mapFieldDependencies(compiler, circuit, readers);
    }

    private static Map<Long, List<List<Integer>>> mapFieldDependencies(DBSPCompiler compiler, DBSPCircuit circuit,
                                                                      Map<Long, DBSPUnaryOperator> readers) {
        if (readers.isEmpty())
            return Map.of();
        try {
            var input = OptimizerExpressions.mapFunctions(compiler, circuit, readers.values());
            var reply = transformWithContext("MapFieldDependencies", input, 0, "", EMPTY_CONTEXT);
            Map<Long, List<List<Integer>>> paths = MAPPER.readValue(reply, new TypeReference<>() {});
            if (!paths.keySet().equals(readers.keySet()))
                throw new IllegalStateException("Incomplete map closure dependencies");
            return paths;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid map field dependencies from the Rust optimizer", error);
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

    private record LineageFinding(String kind, List<Lineage.TableColumn> columns, long comparison, long literal) {}

    private DBSPCircuit reportLineage(DBSPCircuit circuit, Lineage report, long initial) throws IOException {
        var writer = this.snapshots.writer();
        var nodes = new HashMap<Long, IDBSPInnerNode>();
        writer.innerVisitor.liveNodes = nodes;
        writer.apply(circuit);
        var result = this.invoke("Lineage", writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                initial, EMPTY_CONTEXT);
        if (!answered(this.compiler, "Lineage", result))
            return report.apply(circuit);
        for (var finding : MAPPER.treeToValue(result, LineageFinding[].class)) {
            var columns = new java.util.TreeSet<>(finding.columns());
            switch (finding.kind()) {
                case "correlated" -> report.reportCorrelated(new Lineage.ColumnSet(columns));
                case "compared" -> {
                    var comparison = Utilities.getExists(nodes, finding.comparison()).to(DBSPBinaryExpression.class);
                    var literal = Utilities.getExists(nodes, finding.literal()).to(DBSPLiteral.class);
                    for (var column : columns)
                        report.reportCompared(new Lineage.ColumnCompared(column, comparison.opcode, literal));
                }
                default -> throw new IllegalStateException("Unknown lineage finding " + finding.kind());
            }
        }
        return circuit;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        this.usedGeneralTemporalAnalysis = false;
        long initial = DBSPVariablePath.nextGeneratedName();
        try {
            byte[] state = EMPTY_CONTEXT;
            ToJsonOuterVisitor writer = null;
            if (this.lineage != null)
                return this.reportLineage(circuit, this.lineage, initial);
            if (this.stages != null)
                return this.stages.apply(circuit);
            if (this.outputProjections != null)
                return this.outputProjections.applyNative(circuit);
            if (this.streamAggregates != null)
                return this.expandStreamAggregates(circuit, this.streamAggregates);
            if (this.arrayCandidates != null) {
                var topology = OptimizerTopology.encode(circuit, OptimizerTopology.Details.AGGREGATE_RESULTS);
                var probe = this.invoke("Topology/FindFusable", topology, initial, EMPTY_CONTEXT);
                byte[] matches = EMPTY_CONTEXT;
                if (probe.required("needs_full_snapshot").booleanValue()) {
                    var snapshot = this.snapshots.writer();
                    snapshot.apply(circuit);
                    var found = this.invoke("FindFusable", snapshot.getJsonString().getBytes(StandardCharsets.UTF_8),
                            initial, EMPTY_CONTEXT);
                    if (!found.isEmpty())
                        matches = MAPPER.writeValueAsBytes(found);
                }
                this.arrayCandidates.setNativeMatches(matches);
                return circuit;
            }
            if (this.arrayFusion != null) {
                state = this.arrayFusion.nativeMatches();
                if (state.length == 0)
                    return circuit;
            }
            if (this.regionConflicts != null) {
                var conflicts = this.invoke("DetectConflicts", OptimizerTopology.encode(circuit), initial, state);
                this.regionConflicts.recordConflicts(MAPPER.treeToValue(conflicts, int[].class));
                return circuit;
            }
            if (this.commonExpressions != null) {
                this.findCommonExpressions(circuit, this.commonExpressions, initial);
                return circuit;
            }
            if (this.deadCode != null) {
                var mode = this.deadCode.keepAllSources ? "Prune/AllTables" : "Prune/UsedTables";
                var plan = this.invoke(mode, OptimizerTopology.operatorInputs(circuit), initial, state);
                this.deadCode.toKeep.clear();
                this.collectKeptOperators(circuit, plan, this.deadCode.toKeep);
                return circuit;
            }
            if (this.nativeName.equals("ChainVisitor"))
                return this.fuseChains(circuit, initial);
            if (this.fanout != null) {
                var producers = this.invoke("Topology/FanoutProducers", OptimizerTopology.encode(circuit), initial, state);
                if (producers.isEmpty())
                    return circuit;
                var selected = MAPPER.convertValue(producers, new TypeReference<Set<Long>>() {});
                var payload = OptimizerTopology.selectedExpressions(this.compiler, circuit, selected, OptimizerTopology.Details.NONE);
                var readers = this.invoke("FanoutReaders", payload.getJsonString().getBytes(StandardCharsets.UTF_8), initial, state);
                return this.fanout.cloneSelected(circuit, MAPPER.convertValue(readers, new TypeReference<Set<Long>>() {}));
            }
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
            if (this.replaceCommon != null)
                return this.replaceSharedProjections(circuit, this.replaceCommon.analysis());
            if (this.deferCarried != null) {
                var layouts = this.deferCarried.layouts();
                if (layouts.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(layouts);
            }
            if (this.trimFilters != null)
                return this.trimFilterInputs(circuit, this.trimFilters, initial);
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
            if (this.temporalScan != null) {
                scanTemporalWindows(this.compiler, circuit, this.temporalScan);
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
                byte[] result = transformWithContext("Graph/Inputs", OptimizerTopology.consumerInputs(circuit),
                        initial, "", EMPTY_CONTEXT);
                var edges = MAPPER.readValue(result, new TypeReference<Map<Long, int[][][]>>() {});
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
            if (this.sharedIndexes != null) {
                this.findSharedIndexes(circuit, initial, this.sharedIndexes);
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
            if (this.asofLowering != null) {
                var rows = asofRowTypes(circuit);
                if (rows.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(rows);
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
                var edits = this.invoke("Topology/MergeNoops", OptimizerTopology.encode(circuit),
                        initial, MAPPER.writeValueAsBytes(inputs));
                return this.applyStreamEdits(circuit, edits);
            }
            if (this.replaceSharedIndexes != null) {
                var groups = this.replaceSharedIndexes.candidateIds();
                if (groups.isEmpty())
                    return circuit;
                state = MAPPER.writeValueAsBytes(groups);
            }
            if (this.collapseChains != null)
                return this.collapseSharedChains(circuit, initial, this.collapseChains);
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
                case "Reorder" -> {
                    var reorder = Objects.requireNonNull(this.temporalOrder);
                    var chosen = new LinkedHashMap<Long, Integer>();
                    reorder.chosenConjunct.forEach((filter, index) -> chosen.put(filter.id, index));
                    writer = this.snapshots.writer(this.reusesOperators());
                    writer.innerVisitor.liveNodes = new HashMap<>();
                    writer.apply(circuit);
                    byte[] input = writer.getJsonString().getBytes(StandardCharsets.UTF_8);
                    result = this.invoke("Live/WindowGroups/Reorder", input, initial, MAPPER.writeValueAsBytes(chosen));
                    if (!answered(this.compiler, "WindowGroups/Reorder", result)) {
                        var orders = new LinkedHashMap<Long, ReorderTemporalFilters.Reorder.TemporalOrder>();
                        visitOperators(circuit.getAllOperators(), op -> {
                            if (op instanceof DBSPFilterOperator filter) {
                                var order = reorder.classify(filter);
                                if (order != null)
                                    orders.put(filter.id, order);
                            }
                        });
                        if (orders.isEmpty())
                            return circuit;
                        result = this.invoke("Live/Reorder", input, initial, MAPPER.writeValueAsBytes(orders));
                    }
                }
                case "Simplify" -> {
                    return new Simplify(this.compiler) {
                        @Override
                        public IDBSPInnerNode apply(IDBSPInnerNode root) {
                            return rewriteInnerRoot(this.compiler, "Simplify/Root", root);
                        }
                    }.circuitRewriter(true).apply(circuit);
                }
                case "SimplifyWaterline" -> {
                    return new SimplifyWaterline(this.compiler) {
                        @Override
                        public IDBSPInnerNode apply(IDBSPInnerNode root) {
                            return rewriteInnerRoot(this.compiler, "SimplifyWaterline/Root", root);
                        }
                    }.circuitRewriter().apply(circuit);
                }
                case "ExpandMetadataCasts" -> {
                    return new ExpandMetadataCasts(this.compiler, initializer -> lowerFunction(this.compiler,
                            "ExpandMetadataCasts/Initializer", initializer, null, DBSPExpression.class)).apply(circuit);
                }
                case "RemoveDeindexOperators" -> {
                    var edits = this.invoke("Topology/DeindexMaps", OptimizerTopology.encode(circuit), initial, state);
                    return this.applyStreamEdits(circuit, edits);
                }
                case "RemoveNoops" -> {
                    return this.bypassNoops(circuit, initial);
                }
                case "LinearPostprocessRetainKeys" -> {
                    return this.retainLinearAggregateKeys(circuit, initial);
                }
                case "RemoveViewOperators", "RemoveAllViews" -> {
                    return this.removeViews(circuit, initial);
                }
                case "FindJoinTrees" -> {
                    var isolated = this.invoke("Topology/FindJoinTrees", OptimizerTopology.encode(circuit,
                            OptimizerTopology.Details.CONSUMERS), initial, state);
                    if (!isolated.isNull())
                        result = isolated;
                }
                case "UDFInliner" -> {
                    if (circuit.declarations.stream().noneMatch(decl ->
                            decl.item instanceof DBSPFunctionItem function && function.function.body != null))
                        return circuit;
                    var functions = OptimizerExpressions.encode(this.compiler, circuit, OptimizerExpressions.Roots.DECLARED_FUNCTIONS);
                    var calls = OptimizerExpressions.operatorCalls(this.compiler, circuit);
                    if (!this.invoke("UDFInliner/Declarations", functions, initial, calls)
                            .required("needs_full_snapshot").booleanValue())
                        return circuit;
                }
                case "SimplifyConditionals", "ExpandSafeCasts", "ExpandUnsafeCasts", "AdjustSqlIndex" -> {
                    Predicate<DBSPExpression> candidate = switch (this.nativeName) {
                        case "SimplifyConditionals" -> expression -> expression instanceof DBSPIfExpression;
                        case "AdjustSqlIndex" -> expression -> expression instanceof DBSPBinaryExpression binary
                                && (binary.opcode == DBSPOpcode.SQL_INDEX || binary.opcode == DBSPOpcode.SAFE_RUST_INDEX);
                        default -> {
                            var safety = this.nativeName.equals("ExpandSafeCasts")
                                    ? DBSPCastExpression.CastType.SqlSafe : DBSPCastExpression.CastType.SqlUnsafe;
                            yield expression -> expression instanceof DBSPCastExpression cast && cast.safe == safety;
                        }
                    };
                    String pass = this.nativeName + "/Root";
                    return OptimizerExpressions.rewriteSelected(this.compiler, circuit,
                            !this.nativeName.equals("SimplifyConditionals"), candidate,
                            root -> rewriteInnerRoot(this.compiler, pass, root));
                }
                case "CreateRuntimeErrorWrappers" -> {
                    JsonNode probe = this.invoke("Expressions/" + this.nativeName,
                            OptimizerExpressions.inventory(this.compiler, circuit), initial, state);
                    if (!probe.required("needs_full_snapshot").booleanValue())
                        return circuit;
                }
                case "IncrementalizeVisitor" -> {
                    var plan = this.invoke("Incrementalize/Boundaries", OptimizerTopology.operatorInputs(circuit), initial, state);
                    Map<Long, List<IncrementalBoundaryEdits.Step>> edits = MAPPER.convertValue(plan, new TypeReference<>() {});
                    return new IncrementalBoundaryEdits(this.compiler, edits).apply(circuit);
                }
                case "ExpandHop" -> { return this.expandHop(circuit); }
                case "DecomposeExpensiveFilters" -> {
                    var predicates = OptimizerExpressions.encode(this.compiler, circuit, OptimizerExpressions.Roots.FILTER_PREDICATES);
                    if (!this.invoke("DecomposeExpensiveFilters/Predicates", predicates, initial, state)
                            .required("needs_full_snapshot").booleanValue())
                        return circuit;
                }
                case "DeterministicFunctions" -> result = this.invoke(this.nativeName,
                        OptimizerExpressions.encode(this.compiler, circuit, OptimizerExpressions.Roots.INITIALIZERS),
                        initial, state);
                case "NoConnectorMetadata" -> result = this.invoke(this.nativeName,
                        OptimizerExpressions.executableCalls(this.compiler, circuit), initial, state);
                case "ComparatorDeclarations" -> { return this.declareComparators(circuit, initial); }
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
                case "LowerCircuitVisitor", "TrimWindows", "IndexedInputs", "PullFilterVisitor", "DesugarNowEquality", "BreakFilters", "MergeFilters", "FilterJoinVisitor", "ShareOutputIntegrators", "DuplicateSharedIndexes", "BalancedJoins", "PushDifferentialsUp",
                     "FuseIntoArrayAgg", "PropagateConstants", "PropagateEmptySources", "RemoveConstantFilters", "CloneOperatorsWithFanout", "OptimizeDistinctVisitor",
                     "RemoveIAfterD", "ImplementJoins", "MergeSums", "OptimizeIncrementalVisitor",
                     "ExpandAggregateZero",
                     "RemoveIdentityOperators", "BuildNestedOperators",
                     "SubstituteLeftJoins", "ImplementChains", "StrayGC", "SeparateIntegrators" -> {
                    var details = switch (this.nativeName) {
                        case "SeparateIntegrators" -> OptimizerTopology.Details.TRACE_INTEGRATORS;
                        case "StrayGC" -> OptimizerTopology.Details.TRACE_RETENTION;
                        case "IndexedInputs" -> OptimizerTopology.Details.PRIMARY_KEYS;
                        case "FuseIntoArrayAgg" -> OptimizerTopology.Details.AGGREGATE_RESULTS;
                        case "PropagateEmptySources" -> OptimizerTopology.Details.EMPTY_LITERALS;
                        case "RemoveConstantFilters" -> OptimizerTopology.Details.FILTER_PREDICATES;
                        default -> OptimizerTopology.Details.NONE;
                    };
                    JsonNode probe = this.invoke("Topology/" + this.nativeName, OptimizerTopology.encode(circuit, details),
                            initial, state);
                    if (probe.has("pull_filters")) {
                        Map<Long, FilterPullEdits.Site> sites = MAPPER.convertValue(probe.required("pull_filters"),
                                new TypeReference<>() {});
                        return sites.isEmpty() ? circuit : new FilterPullEdits(this.compiler, sites).apply(circuit);
                    }
                    if (probe.has("integral_edits")) {
                        Map<Long, String> edits = MAPPER.convertValue(probe.required("integral_edits"), new TypeReference<>() {});
                        return edits.isEmpty() ? circuit : new IntegralPushEdits(this.compiler, edits).apply(circuit);
                    }
                    if (probe.has("empty_stream_edits"))
                        return this.applyStreamEdits(circuit, probe.required("empty_stream_edits"));
                    if (probe.has("stream_aggregate")) {
                        var aggregate = findOperator(circuit.allOperators, probe.required("stream_aggregate").longValue());
                        throw new InternalCompilerError("StreamAggregate operator should have been removed " + Objects.requireNonNull(aggregate));
                    }
                    if (probe.has("trace_inputs"))
                        return this.separateTraceInputs(circuit, probe.required("trace_inputs"));
                    if (probe.has("retention_violation"))
                        throw retentionError(circuit, probe.required("retention_violation"));
                    if (!probe.required("needs_full_snapshot").booleanValue())
                        return circuit;
                    if (this.nativeName.equals("OptimizeIncrementalVisitor")) {
                        writer = this.snapshots.integralPushWriter();
                        writer.apply(circuit);
                        var edits = this.invoke("Live/OptimizeIncrementalVisitor/Signatures",
                                writer.getJsonString().getBytes(StandardCharsets.UTF_8), initial, state);
                        if (!edits.has("needs_full_snapshot"))
                            result = edits;
                    }
                }
            }
            if (result == null) {
                writer = this.snapshots.writer(this.reusesOperators());
                writer.innerVisitor.liveNodes = new HashMap<>();
                writer.apply(circuit);
                if (this.mapFusion != null)
                    state = MapFusionState.encode(writer, this.mapFusion);
                else if (state == EMPTY_CONTEXT)
                    state = this.passState(circuit, writer);
                result = this.invoke("Live/" + this.nativeName, writer.getJsonString().getBytes(StandardCharsets.UTF_8),
                        initial, state);
            }
            if (this.mergeFilters != null && !answered(this.compiler, "MergeFilters", result)) {
                this.usedGeneralTemporalAnalysis = true;
                return this.mergeFilters.apply(circuit);
            }
            if (this.replaceSharedIndexes != null && !answered(this.compiler, "ReplaceSharedIndexes", result))
                return this.replaceSharedIndexes.apply(circuit);
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
