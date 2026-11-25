package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.RawValue;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.ir.statement.DBSPComparatorItem;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.CheckPersistentIds;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.ProvenanceCodec;
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
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.RepeatedExpressions;
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
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FieldUseMap;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindUsedFields;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.BorrowedField;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FieldSet;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.IUsedFields;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ParameterField;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.TupleOfFields;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields.FindUnusedInputFields;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.DeferCarriedColumns;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.LeftJoinChainsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.NativeLeftJoinChains;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.TrimFilters;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields.TrimInputs;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindCommonProjections;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ReplaceCommonProjections;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.MultisetAnalysis;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.KeyColumns;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampTzLiteral;
import org.dbsp.sqlCompiler.ir.type.CollectionShape;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.IndexedShape;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.CircuitContainsNow;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.CheckRetain;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneFunction;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneRefType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.ScalarMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.WaterlineProjection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.LimiterInputs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.InsertLimiters;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.OrderErrorConsumers;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MergeGC;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.Monotonicity;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveViewOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeProjectionVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ConstantExpressions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.compiler.visitors.inner.NativeValueNumbering;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ValueNumbering;
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
    private static final byte[] POSTORDER_BOUNDS = "checked-postorder".getBytes(StandardCharsets.US_ASCII);

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

    public enum ProjectionRule {
        KEEP, COMPOSE_MAP, COMPOSE_INDEX, PUSH_THROUGH, PROJECT_SUM, PROJECT_JOIN,
        PROJECT_ASOF, PROJECT_INDEXED_JOIN, PROJECT_FILTERED_JOIN, PROJECT_FILTERED_STAR,
        PROJECT_FLAT_MAP, PROJECT_DEINDEX, PROJECT_AGGREGATE_ZERO, TRIM_AGGREGATE,
        TRIM_TOP_K, PROJECT_ANTI_JOIN
    }

    private record ProjectionRules(ProjectionRule row, ProjectionRule sharedRow, ProjectionRule index) {}

    // Rules depend only on the mapped producer's class. Never cache the fanout
    // or the producer of a particular input across optimizer iterations.
    private static final ClassValue<ProjectionRules> PROJECTION_RULES = new ClassValue<>() {
        @Override
        protected ProjectionRules computeValue(Class<?> producer) {
            try {
                var classes = new ArrayList<String>();
                for (Class<?> type = producer; DBSPOperator.class.isAssignableFrom(type); type = type.getSuperclass())
                    classes.add(type.getSimpleName());
                var reply = transformWithContext("OptimizeProjections/Producer",
                        MAPPER.writeValueAsBytes(classes), 0, "", EMPTY_CONTEXT);
                return MAPPER.readValue(reply, ProjectionRules.class);
            } catch (IOException error) {
                throw new UncheckedIOException("Could not read projection rules for " + producer.getName(), error);
            }
        }
    };

    /** MapIndex readers reach this point only when the original producer has one reader. */
    public static ProjectionRule projectionRule(DBSPCompiler compiler, DBSPOperator producer, boolean indexed, int fanout) {
        var rules = PROJECTION_RULES.get(producer.getClass());
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision("OptimizeProjections/Producer", true);
        if (indexed)
            return rules.index;
        return fanout == 1 ? rules.row : rules.sharedRow;
    }

    public static DBSPSimpleOperator projectFilteredJoin(DBSPCompiler compiler, DBSPSimpleOperator producer,
                                                          DBSPMapOperator reader) {
        String pass = "OptimizeProjections/FilteredJoin";
        var site = expressionSite(producer.getRelNode());
        site.set("ports", MAPPER.valueToTree(producer.inputs.stream().map(p -> p.outputNumber).toList()));
        DBSPClosureExpression filter, map;
        if (producer instanceof DBSPJoinFilterMapOperator binary) {
            site.put("balanced", binary.balanced);
            filter = binary.filter;
            map = binary.map;
        } else {
            var star = producer.to(DBSPStarJoinFilterMapOperator.class);
            filter = star.filter;
            map = star.map;
        }
        site.put("has_filter", filter != null);
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(site),
                    reader.outputType, producer.getFunction(), reader.getClosureFunction(), filter, map);
            if (reply.value.has("binding_error"))
                checkBindingErrors(reply.writer, reply.value.required("binding_error"));
            var operators = decodeOperatorFragment(reply, op -> producer.getRelNode(),
                    producer.inputs.stream().map(OutputPort::node).toArray(DBSPOperator[]::new));
            Utilities.enforce(operators.size() == 1);
            reportWarnings(compiler, reply.value.required("warnings"));
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return operators.getFirst().copyAnnotations(producer);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode filtered join projection", error);
        }
    }

    public static List<DBSPSimpleOperator> pushProjection(DBSPCompiler compiler,
            DBSPUnaryOperator reader, DBSPSimpleOperator producer) {
        String pass = "OptimizeProjections/PushThrough";
        var request = expressionSite(producer.getRelNode());
        request.put("producer", producer.getClass().getSimpleName());
        request.set("ports", MAPPER.valueToTree(producer.inputs.stream().map(p -> p.outputNumber).toList()));
        var roots = new ArrayList<IDBSPInnerNode>();
        roots.add(reader.getFunction());
        roots.add(reader.outputType);
        if ((producer instanceof DBSPDelayOperator || producer instanceof DBSPAggregateZeroOperator)
                && producer.function != null)
            roots.add(producer.function);
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(request),
                    roots.toArray(IDBSPInnerNode[]::new));
            var operators = decodeOperatorFragment(reply,
                    op -> op instanceof DBSPMapOperator || op instanceof DBSPMapIndexOperator
                            ? reader.getRelNode() : producer.getRelNode(),
                    producer.inputs.stream().map(OutputPort::node).toArray(DBSPOperator[]::new));
            Utilities.enforce(operators.size() == producer.inputs.size() + 1);
            operators.subList(0, producer.inputs.size()).forEach(op -> op.copyAnnotations(reader));
            if (!(producer instanceof DBSPAggregateZeroOperator))
                operators.getLast().copyAnnotations(producer);
            reportWarnings(compiler, reply.value.required("warnings"));
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return operators;
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode projection inputs", error);
        }
    }

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

    /** Resolve a copied flatmap's shuffle against the input of this optimizer call.
     * Copies of a newly composed shuffle can also share an owner within the reply. */
    private static class LiveDecoder extends JsonDecoder {
        private final ToJsonInnerVisitor writer;
        private final Map<Long, Shuffle> newShuffles = new HashMap<>();

        LiveDecoder(ToJsonInnerVisitor writer, ProvenanceCodec.Decoder provenance) {
            super(null, provenance);
            this.writer = writer;
        }

        @Override
        protected RuntimeException constructorFailure(String nodeClass, long id, Throwable cause) {
            // These constructors run as part of the live optimizer pass.
            if (cause instanceof BaseCompilerException diagnostic)
                return diagnostic;
            return super.constructorFailure(nodeClass, id, cause);
        }

        @Override
        public Shuffle decodeShuffle(JsonNode flatmap) {
            long owner = flatmap.path("shuffleIdentity").asLong(flatmap.required("id").longValue());
            var shape = flatmap.required("shuffle");
            if (this.writer.liveNodes != null && this.writer.liveNodes.get(owner) instanceof DBSPFlatmap source)
                return Shuffle.fromJson(shape, source.shuffle);
            if (this.writer.liveShuffles != null && this.writer.liveShuffles.containsKey(owner))
                return Shuffle.fromJson(shape, this.writer.liveShuffles.get(owner));
            return this.newShuffles.computeIfAbsent(owner, ignored -> Shuffle.fromJson(shape));
        }
    }

    private static JsonDecoder expressionDecoder(ToJsonInnerVisitor writer) {
        var decoder = new LiveDecoder(writer, new ProvenanceCodec.Decoder());
        Objects.requireNonNull(writer.liveNodes).forEach(decoder.inner::cache);
        return decoder;
    }

    @Nullable
    private static <T extends IDBSPInnerNode> T decodeLiveRoot(ToJsonInnerVisitor writer, JsonNode result, Class<T> kind) {
        if (result.isNull())
            return null;
        return decodeLiveRoot(expressionDecoder(writer), result, kind);
    }

    private static <T extends IDBSPInnerNode> T decodeLiveRoot(JsonDecoder decoder, JsonNode result, Class<T> kind) {
        if (result.isArray()) {
            // Rust emits the dependencies of a deep expression before its root.
            int last = result.size() - 1;
            Utilities.enforce(last >= 0);
            for (int i = 0; i < last; i++)
                decoder.decodeInner(result.get(i), IDBSPInnerNode.class);
            result = result.get(last);
        }
        return decoder.decodeInner(result, kind);
    }

    private static <T extends DBSPExpression> T loweredFunction(DBSPCompiler compiler,
            ToJsonInnerVisitor writer, JsonNode result, Class<T> kind) {
        T function = Objects.requireNonNull(decodeLiveRoot(writer, result.required("function"), kind));
        reportWarnings(compiler, result.required("warnings"));
        return function;
    }

    @Nullable
    public static DBSPExpression unaryWaterline(DBSPCompiler compiler, DBSPExpression operation, DBSPExpression bound) {
        return tryLowerFunction(compiler, "Monotonicity/Unary", operation, bound, DBSPExpression.class);
    }

    public static boolean unaryPreservesOrder(DBSPOpcode opcode) {
        try {
            var reply = transformWithContext("Monotonicity/UnaryPreservesOrder",
                    MAPPER.writeValueAsBytes(opcode.name()), 0, "", EMPTY_CONTEXT);
            return MAPPER.readValue(reply, Boolean.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid Rust unary waterline decision", error);
        }
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
            return projectedValue(body, reply, parameters);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode a row waterline projection", error);
        }
    }

    @Nullable
    public static MonotoneExpression projectRead(DBSPCompiler compiler, DBSPExpression read,
            IMaybeMonotoneType source, @Nullable DBSPExpression bound) {
        boolean wrapper = read instanceof DBSPCloneExpression || read instanceof DBSPSomeExpression || read instanceof DBSPUnwrapExpression;
        String pass = wrapper ? "Monotonicity/ProjectWrapper" : "Monotonicity/ProjectRead";
        try {
            var operation = MAPPER.createObjectNode();
            if (read instanceof DBSPFieldExpression field)
                operation.put("kind", "field").put("index", field.fieldNo);
            else if (read instanceof DBSPBorrowExpression)
                operation.put("kind", "borrow");
            else if (read instanceof DBSPDerefExpression)
                operation.put("kind", "deref");
            else if (read instanceof DBSPCloneExpression)
                operation.put("kind", "clone");
            else if (read instanceof DBSPSomeExpression)
                operation.put("kind", "some");
            else if (read instanceof DBSPUnwrapExpression)
                operation.put("kind", "unwrap");
            else
                throw new IllegalArgumentException("Expected a row read or value wrapper");
            // Field simplification can return one of a tuple's own fields or move an unwrap.
            // All other reads need only the bound's type and source location.
            boolean inspectBound = read instanceof DBSPFieldExpression &&
                    (bound instanceof DBSPBaseTupleExpression || bound instanceof DBSPUnwrapExpression);
            var input = bound == null || inspectBound ? bound : new DBSPVariablePath(bound.getNode(), "bound", bound.getType());
            var state = MAPPER.writeValueAsBytes(List.of(operation, WaterlineProjection.encode(source)));
            // A clone of the bound inherits the wrapper's source location.
            IDBSPInnerNode site = wrapper ? new DBSPVariablePath(read.getNode(), "site", read.getType()) : read.getType();
            var reply = invokeLowering(compiler, pass, state, site, input);
            if (!answered(compiler, pass, reply.value))
                return null;
            if (reply.value.has("invalid_projection"))
                throw invalidWaterlineProjection(reply.writer, reply.value.required("invalid_projection"),
                        new IMaybeMonotoneType[]{source}, List.of());
            if (reply.value.has("type"))
                throw waterlineTypeError(reply.writer, reply.value);
            if (reply.value.has("missing_bound"))
                throw new NullPointerException();
            if (input != bound)
                Objects.requireNonNull(reply.writer.liveNodes).put(Objects.requireNonNull(input).id, bound);
            return projectedValue(read, reply, source);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode a row waterline", error);
        }
    }

    private static MonotoneExpression projectedValue(DBSPExpression body, LoweringReply reply,
                                                       IMaybeMonotoneType... parameters) {
        var nodes = Objects.requireNonNull(reply.writer.liveNodes);
        var result = WaterlineProjection.resolveType(reply.value.required("row_type"),
                id -> Utilities.getExists(nodes, id).to(DBSPType.class), parameters);
        var reduced = reply.value.required("bound");
        return new MonotoneExpression(body, result, decodeLiveRoot(reply.writer, reduced, DBSPExpression.class));
    }

    public sealed interface WaterlineAnalysis permits ProjectedWaterline, WaterlineInspection, FailedWaterline {}

    public enum WaterlineError { MissingOperand, TargetMaximum, FlatmapInput }

    public record FailedWaterline(WaterlineError error, DBSPExpression expression) implements WaterlineAnalysis {}

    public record ProjectedWaterline(IMaybeMonotoneType bodyType, @Nullable DBSPClosureExpression closure)
            implements WaterlineAnalysis {}

    public record WaterlineInspection(ReferenceMap references, List<IDBSPInnerNode> repeatedPath)
            implements WaterlineAnalysis {
        public void checkTree() {
            if (!this.repeatedPath.isEmpty())
                throw repeatedExpressionError(this.repeatedPath);
        }
    }

    /** Resolve bindings and check expression occurrences in the same serialized graph. */
    public static WaterlineInspection inspectWaterline(DBSPCompiler compiler, IDBSPInnerNode root) {
        var reply = invokeLowering(compiler, "Monotonicity/InspectBody", root);
        return readWaterlineInspection(reply.writer, reply.value);
    }

    @Nullable
    private static InternalCompilerError bindingError(ToJsonInnerVisitor writer, JsonNode value) {
        var nodes = Objects.requireNonNull(writer.liveNodes);
        if (value.has("free")) {
            var variable = Utilities.getExists(nodes, value.required("free").longValue());
            return new InternalCompilerError("Could not resolve " + variable);
        }
        if (value.has("conflict")) {
            var conflict = value.required("conflict");
            var variable = Utilities.getExists(nodes, conflict.required(0).longValue()).to(DBSPVariablePath.class);
            var previous = Utilities.getExists(nodes, conflict.required(1).longValue()).to(IDBSPDeclaration.class);
            var current = Utilities.getExists(nodes, conflict.required(2).longValue()).to(IDBSPDeclaration.class);
            return ReferenceMap.conflictingDeclaration(variable, previous, current);
        }
        return null;
    }

    private static void checkBindingErrors(ToJsonInnerVisitor writer, JsonNode value) {
        var error = bindingError(writer, value);
        if (error != null)
            throw error;
    }

    private static void readBindings(ToJsonInnerVisitor writer, JsonNode pairs, ReferenceMap references) {
        var nodes = Objects.requireNonNull(writer.liveNodes);
        for (var pair : pairs) {
            var variable = Utilities.getExists(nodes, pair.required(0).longValue()).to(DBSPVariablePath.class);
            var declaration = Utilities.getExists(nodes, pair.required(1).longValue()).to(IDBSPDeclaration.class);
            references.declare(variable, declaration);
        }
    }

    public record ReferenceResult(boolean hasFree, @Nullable InternalCompilerError diagnostic) {}

    public static ReferenceResult resolveReferences(DBSPCompiler compiler, IDBSPInnerNode root,
                                                   boolean allowFree, ReferenceMap references) {
        var flags = (allowFree ? "true" : "false").getBytes(StandardCharsets.US_ASCII);
        var reply = invokeLowering(compiler, "ResolveReferences", flags, root);
        readBindings(reply.writer, reply.value.required("bindings"), references);
        return new ReferenceResult(reply.value.required("has_free").booleanValue(),
                bindingError(reply.writer, reply.value.required("diagnostic")));
    }

    private static WaterlineInspection readWaterlineInspection(ToJsonInnerVisitor writer, JsonNode value) {
        checkBindingErrors(writer, value);
        var references = new ReferenceMap();
        readBindings(writer, value.required("bindings"), references);
        return new WaterlineInspection(references, repeatedPath(writer, value.required("repeated")));
    }

    private static List<IDBSPInnerNode> repeatedPath(ToJsonInnerVisitor writer, JsonNode ids) {
        var nodes = Objects.requireNonNull(writer.liveNodes);
        var path = new ArrayList<IDBSPInnerNode>(ids.size());
        for (var id : ids)
            path.add(Utilities.getExists(nodes, id.longValue()));
        return path;
    }

    private static RuntimeException repeatedExpressionError(List<IDBSPInnerNode> path) {
        return RepeatedExpressions.duplicateError(path.getFirst(), path.getLast(), path.subList(0, path.size() - 1));
    }

    public static WaterlineAnalysis projectWaterlineClosure(DBSPCompiler compiler, DBSPExpression body,
            DBSPParameter[] parameters, DBSPType[] projectedTypes, IMaybeMonotoneType[] argumentShapes, boolean indexed) {
        return projectWaterlineClosure(compiler, body, parameters, projectedTypes, argumentShapes, indexed, null);
    }

    public static WaterlineAnalysis projectWaterlineClosure(DBSPCompiler compiler, DBSPExpression body,
            DBSPParameter[] parameters, DBSPType[] projectedTypes, IMaybeMonotoneType[] argumentShapes, boolean indexed,
            @Nullable BiConsumer<DBSPExpression, MonotoneExpression> observe) {
        return projectWaterlineClosure(compiler, body, parameters, projectedTypes, argumentShapes, indexed,
                observe == null, observe);
    }

    /** A result-only caller still receives the prefix of visited facts if analysis fails. */
    public static WaterlineAnalysis projectWaterlineClosure(DBSPCompiler compiler, DBSPExpression body,
            DBSPParameter[] parameters, DBSPType[] projectedTypes, IMaybeMonotoneType[] argumentShapes, boolean indexed,
            boolean resultOnly, @Nullable BiConsumer<DBSPExpression, MonotoneExpression> observe) {
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
            String pass = resultOnly ? "Monotonicity/ProjectClosure" : "Monotonicity/ExpressionFacts";
            var reply = invokeLowering(compiler, pass,
                    MAPPER.writeValueAsBytes(request), roots.toArray(IDBSPInnerNode[]::new));
            var inspection = reply.value.get("inspection");
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, inspection == null);
            if (inspection != null)
                return readWaterlineInspection(reply.writer, inspection);
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            if (reply.value.has("facts")) {
                var observer = resultOnly && !reply.value.has("failure") ? null : observe;
                return readExpressionFacts(reply, parameters, argumentShapes, observer);
            }
            var nodes = Objects.requireNonNull(reply.writer.liveNodes);
            var type = WaterlineProjection.resolveType(reply.value.required("row_type"),
                    id -> Utilities.getExists(nodes, id).to(DBSPType.class), argumentShapes);
            var closure = reply.value.required("closure");
            return new ProjectedWaterline(type, decodeLiveRoot(reply.writer, closure, DBSPClosureExpression.class));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode a waterline closure", error);
        }
    }

    private static RuntimeException invalidWaterlineProjection(ToJsonInnerVisitor writer, JsonNode projection,
            IMaybeMonotoneType[] inputs, List<MonotoneExpression> facts) {
        var nodes = Objects.requireNonNull(writer.liveNodes);
        WaterlineProjection.resolveFactType(projection,
                id -> Utilities.getExists(nodes, id).to(DBSPType.class), inputs, facts);
        return new IllegalStateException("Expected an invalid waterline projection " + projection);
    }

    private static RuntimeException waterlineTypeError(ToJsonInnerVisitor writer, JsonNode failure) {
        var type = Utilities.getExists(Objects.requireNonNull(writer.liveNodes),
                failure.required("type").longValue()).to(DBSPType.class);
        return waterlineTypeError(type, failure.required("kind").textValue());
    }

    private static RuntimeException waterlineTypeError(DBSPType type, String kind) {
        return switch (kind) {
            case "RequiredType" -> {
                type.withMayBeNull(false);
                yield new IllegalStateException("Expected a type with no nonnullable form " + type);
            }
            case "NullableRawTuple" -> {
                Utilities.enforce(!type.mayBeNull);
                yield new IllegalStateException("Expected a nullable raw tuple " + type);
            }
            case "ExpectedScalar" -> ScalarMonotoneType.expectedScalar(type);
            case "UnsupportedType" -> NonMonotoneType.unsupportedType(type);
            case "TargetMaximum" -> new UnsupportedException(type.withMayBeNull(false).getNode());
            default -> throw new IllegalArgumentException("Unknown waterline type error " + kind);
        };
    }

    /** Decode all bounds with one identity map, including operands shared with the reduced closure. */
    private static WaterlineAnalysis readExpressionFacts(LoweringReply reply,
            DBSPParameter[] parameters, IMaybeMonotoneType[] argumentShapes,
            @Nullable BiConsumer<DBSPExpression, MonotoneExpression> observe) {
        var decoder = expressionDecoder(reply.writer);
        var bounds = new ArrayList<DBSPExpression>();
        for (var encoded : reply.value.required("bounds"))
            bounds.add(decodeLiveRoot(decoder, encoded, DBSPExpression.class));
        var nodes = Objects.requireNonNull(reply.writer.liveNodes);
        var facts = new ArrayList<MonotoneExpression>();
        var inputs = new MonotoneExpression[parameters.length];
        for (var entry : reply.value.required("facts")) {
            var synthetic = entry.get("synthesized");
            var expression = synthetic == null
                    ? Utilities.getExists(nodes, entry.required("id").longValue()).to(DBSPExpression.class)
                    : bounds.get(synthetic.intValue());
            MonotoneExpression value;
            if (synthetic != null) {
                value = new MonotoneExpression(expression, NonMonotoneType.nonMonotone(expression.getType()), null);
            } else if (entry.has("alias")) {
                value = facts.get(entry.required("alias").intValue());
            } else if (entry.has("parameter")) {
                int index = entry.required("parameter").intValue();
                if (inputs[index] == null) {
                    var bound = bounds.get(entry.required("bound").intValue());
                    inputs[index] = new MonotoneExpression(parameters[index].asVariable(), argumentShapes[index], bound);
                }
                value = inputs[index];
            } else {
                var type = WaterlineProjection.resolveFactType(entry.required("type"),
                        id -> Utilities.getExists(nodes, id).to(DBSPType.class), argumentShapes, facts);
                var slot = entry.required("bound");
                value = new MonotoneExpression(expression, type, slot.isNull() ? null : bounds.get(slot.intValue()));
            }
            facts.add(value);
            if (observe != null)
                observe.accept(expression, value);
        }
        var failure = reply.value.get("failure");
        if (failure != null) {
            checkBindingErrors(reply.writer, failure);
            if (failure.has("repeated"))
                throw repeatedExpressionError(repeatedPath(reply.writer, failure.required("repeated")));
            if ("FlatmapResult".equals(failure.required("kind").textValue())) {
                var flatmap = Utilities.getExists(nodes, failure.required("expression").longValue()).to(DBSPFlatmap.class);
                var computed = decodeLiveRoot(reply.writer, failure.required("computed"), DBSPType.class);
                flatmap.checkResultType(Objects.requireNonNull(computed));
                throw new IllegalStateException("Expected a flatmap result type mismatch for " + flatmap);
            }
            if ("VariableTypeMismatch".equals(failure.required("kind").textValue())) {
                var read = Utilities.getExists(nodes, failure.required("expression").longValue()).to(DBSPVariablePath.class);
                Utilities.last(facts).checkVariableType(read);
                throw new IllegalStateException("Expected the recorded variable type mismatch for " + read);
            }
            if ("MissingReference".equals(failure.required("kind").textValue()))
                throw Utilities.missingKey(Utilities.getExists(nodes, failure.required("expression").longValue()));
            if ("FunctionArgument".equals(failure.required("kind").textValue())) {
                var call = Utilities.getExists(nodes, failure.required("expression").longValue()).to(DBSPApplyExpression.class);
                var argument = MAPPER.convertValue(failure.required("argument"), FunctionArgumentError.class);
                throw argument.exception(call);
            }
            if (failure.has("type"))
                throw waterlineTypeError(reply.writer, failure);
            if ("InvalidBoundCast".equals(failure.required("kind").textValue())) {
                var operation = Utilities.getExists(nodes, failure.required("expression").longValue()).to(DBSPExpression.class);
                var bound = Objects.requireNonNull(decodeLiveRoot(reply.writer, failure.required("bound"), DBSPExpression.class));
                bound.cast(operation.getNode(), operation.getType().withMayBeNull(false), DBSPCastExpression.CastType.SqlUnsafe);
                throw new IllegalStateException("Expected an invalid waterline cast for " + operation);
            }
            if ("ExpectedScalarOperand".equals(failure.required("kind").textValue())) {
                var source = WaterlineProjection.resolveFactType(failure.required("source"),
                        id -> Utilities.getExists(nodes, id).to(DBSPType.class), argumentShapes, facts);
                source.to(ScalarMonotoneType.class);
                throw new IllegalStateException("Expected a non-scalar waterline operand " + source);
            }
            if (failure.has("projection"))
                throw invalidWaterlineProjection(reply.writer, failure.required("projection"), argumentShapes, facts);
            var kind = WaterlineError.valueOf(failure.required("kind").textValue());
            var expression = Utilities.getExists(nodes, failure.required("expression").longValue());
            return new FailedWaterline(kind, expression.to(DBSPExpression.class));
        }
        var result = reply.value.required("closure");
        var closure = result.isNull() ? null : bounds.get(result.intValue()).to(DBSPClosureExpression.class);
        return new ProjectedWaterline(Utilities.last(facts).getMonotoneType(), closure);
    }

    public static DBSPExpression ensureExpressionTree(DBSPCompiler compiler, DBSPExpression root) {
        return lowerFunction(compiler, "EnsureExpressionTree", root, null, DBSPExpression.class);
    }

    public static DBSPExpression clockWaterline(DBSPCompiler compiler, DBSPApplyExpression call) {
        var clock = lowerFunction(compiler, "Monotonicity/Clock", call, null, DBSPApplyExpression.class);
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision("Monotonicity/Clock", true);
        return clock;
    }

    /** Send just the field types. The reduced tuple refers back to the live field expressions. */
    public static DBSPExpression tupleWaterline(DBSPCompiler compiler, DBSPBaseTupleExpression tuple,
                                                DBSPExpression[] fields) {
        var site = expressionSite(tuple.getNode());
        site.put("raw", tuple.isRaw());
        var inputs = Linq.map(fields, field -> new DBSPVariablePath("field", field.getType()), DBSPVariablePath.class);
        String pass = "Monotonicity/TupleBound";
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(site), inputs);
            var live = Objects.requireNonNull(reply.writer.liveNodes);
            for (int i = 0; i < inputs.length; i++)
                live.put(inputs[i].id, fields[i]);
            var result = loweredFunction(compiler, reply.writer, reply.value, DBSPBaseTupleExpression.class);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode a tuple waterline", error);
        }
    }

    public record LiteralWaterline(@Nullable DBSPExpression bound, boolean constant, boolean positive) {}

    public static LiteralWaterline literalWaterline(DBSPCompiler compiler, DBSPLiteral literal) {
        String pass = "Monotonicity/Literal";
        var reply = invokeLowering(compiler, pass, literal);
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision(pass, true);
        return switch (reply.value.required("kind").textValue()) {
            case "unbounded" -> new LiteralWaterline(null, false, false);
            case "target_maximum" -> throw new UnsupportedException(literal.getType().withMayBeNull(false).getNode());
            case "bounded" -> new LiteralWaterline(
                    Objects.requireNonNull(decodeLiveRoot(reply.writer, reply.value.required("bound"), DBSPExpression.class)),
                    reply.value.required("constant").booleanValue(), reply.value.required("positive").booleanValue());
            default -> throw new IllegalStateException("Unknown literal waterline " + reply.value);
        };
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

    public static JsonNode combineBounds(DBSPCompiler compiler, JsonNode inputs, boolean union) {
        return exchangeBounds(compiler, inputs, union ? "Monotonicity/Union" : "Monotonicity/Intersection");
    }

    public static JsonNode indexedBounds(DBSPCompiler compiler, JsonNode inputs, boolean antiJoin) {
        String operation = antiJoin ? "Monotonicity/AntiJoinProjection" : "Monotonicity/JoinArguments";
        return exchangeBounds(compiler, inputs, operation);
    }

    private static JsonNode exchangeBounds(DBSPCompiler compiler, JsonNode inputs, String pass) {
        try {
            var result = MAPPER.readTree(transformWithContext(pass, MAPPER.writeValueAsBytes(inputs), 0, "", POSTORDER_BOUNDS));
            answered(compiler, pass, result);
            return result;
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

    /** Bind the projected inputs while retaining the analyzed body in the JVM.
     * Joined and indexed inputs unpack the single Apply parameter. */
    public static DBSPClosureExpression bindWaterlineInputs(DBSPCompiler compiler, DBSPParameter[] parameters,
            DBSPType[] types, DBSPExpression body, boolean packInputs) {
        var slot = new DBSPVariablePath("body", body.getType());
        String pass = packInputs ? "Monotonicity/WaterlineBindings" : "Monotonicity/RowClosure";
        try {
            LoweringReply reply;
            if (packInputs) {
                var names = Linq.map(parameters, DBSPParameter::getName, String.class);
                var roots = new IDBSPInnerNode[types.length + 1];
                roots[0] = slot;
                System.arraycopy(types, 0, roots, 1, types.length);
                reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(names), roots);
            } else {
                Utilities.enforce(parameters.length == 1);
                reply = invokeLowering(compiler, pass, slot, parameters[0]);
            }
            Objects.requireNonNull(reply.writer.liveNodes).put(slot.id, body);
            var closure = loweredClosure(compiler, reply);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return closure;
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
    public static DBSPClosureExpression minimumTransferForOutput(DBSPCompiler compiler, CalciteObject origin,
            DBSPType input, DBSPType output, @Nullable DBSPClosureExpression postprocess) {
        try {
            var reply = invokeLowering(compiler, "Monotonicity/MinimumForOutput",
                    MAPPER.writeValueAsBytes(expressionSite(origin)), input, output, postprocess);
            return optionalLoweredClosure(compiler, reply);
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode aggregate source position", error);
        }
    }

    @Nullable
    public static Boolean isMinimumChain(DBSPCompiler compiler, DBSPChainAggregateOperator chain) {
        String pass = "Monotonicity/IsMinimumChain";
        var reply = invokeLowering(compiler, pass, chain.init, chain.getClosureFunction());
        var result = reply.value;
        if (!answered(compiler, pass, result))
            return null;
        if (result.has("comparison_failure"))
            reportComparisonFailure(result.required("comparison_failure"));
        if (result.has("conflict"))
            throw Objects.requireNonNull(bindingError(reply.writer, result));
        if (result.has("first_field")) {
            var value = Utilities.getExists(Objects.requireNonNull(reply.writer.liveNodes),
                    result.required("first_field").longValue()).to(DBSPExpression.class);
            Monotonicity.firstField(value);
            throw new IllegalStateException(result.required("error").textValue());
        }
        return result.booleanValue();
    }

    @Nullable
    public static DBSPClosureExpression chainMinimumTransfer(DBSPCompiler compiler,
            DBSPChainAggregateOperator chain, DBSPType input) {
        try {
            var reply = invokeLowering(compiler, "Monotonicity/ChainMinimum",
                    MAPPER.writeValueAsBytes(expressionSite(chain.getNode())), input,
                    chain.getOutputIndexedZSetType().elementType, chain.getClosureFunction());
            return optionalLoweredClosure(compiler, reply);
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

    @Nullable
    private static DBSPClosureExpression optionalLoweredClosure(DBSPCompiler compiler, LoweringReply reply) {
        if (reply.value.required("function").isNull()) {
            finishLowering(compiler, reply.value);
            return null;
        }
        return loweredClosure(compiler, reply);
    }

    private static DBSPClosureExpression loweredClosure(DBSPCompiler compiler, LoweringReply reply) {
        var result = loweredFunction(compiler, reply.writer, reply.value, DBSPClosureExpression.class);
        DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
        return result;
    }

    public enum BinaryDomain { Scalar, Temporal, TimeOfDay }
    public enum BinaryWaterline { None, Left, Right, Both, InheritLeft, InheritRight, Coalesce }
    public record WaterlineOperand(boolean monotone, boolean constant, boolean positiveLiteral, boolean neverNull) {}

    /** Encode only the operand types; the projected cast must reference the live bound. */
    @Nullable
    public static DBSPExpression castWaterline(DBSPCompiler compiler, DBSPCastExpression cast, DBSPExpression bound) {
        var source = new DBSPVariablePath("source", cast.source.getType());
        var reduced = new DBSPVariablePath("bound", bound.getType());
        var operation = new DBSPCastExpression(cast.getNode(), source, cast.getType(), cast.safe);
        var reply = invokeLowering(compiler, "Monotonicity/Cast", operation, reduced);
        if (reply.value.isNull())
            return null;
        Objects.requireNonNull(reply.writer.liveNodes).put(reduced.id, bound);
        return loweredFunction(compiler, reply.writer, reply.value, DBSPExpression.class);
    }

    public record FunctionArgument(boolean monotone, boolean constant, boolean doubleLiteralAboveOne) {}

    public record FunctionArgumentError(Kind kind, int index) {
        public enum Kind { Missing, NonConstant }

        public RuntimeException exception(DBSPApplyExpression call) {
            if (this.kind == Kind.Missing) {
                Objects.checkIndex(this.index, call.arguments.length);
                return new IllegalStateException("Expected a missing argument at index " + this.index);
            }
            return MonotoneFunction.nonConstantArgument(call.arguments[this.index]);
        }
    }

    public record FunctionRule(boolean registered, @Nullable String target, @Nullable FunctionArgumentError argumentError) {
        public FunctionRule(boolean registered, @Nullable String target) {
            this(registered, target, null);
        }
    }

    public static FunctionRule functionRule(String function, List<FunctionArgument> arguments) {
        try {
            var facts = MAPPER.writeValueAsBytes(List.of(function, arguments));
            var reply = transformWithContext("Monotonicity/FunctionRule", facts, 0, "", EMPTY_CONTEXT);
            return MAPPER.readValue(reply, FunctionRule.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid function waterline from Rust", error);
        }
    }

    @Nullable
    public static String functionWaterline(String function, List<FunctionArgument> arguments) {
        return functionRule(function, arguments).target();
    }

    /** A null slot copies the corresponding constant from the original call. */
    public static DBSPExpression buildFunctionWaterline(DBSPCompiler compiler, DBSPApplyExpression call,
            String target, DBSPExpression[] bounds) {
        var inputs = new ArrayList<DBSPExpression>();
        inputs.add(call);
        var present = new boolean[bounds.length];
        for (int i = 0; i < bounds.length; i++) {
            if (bounds[i] != null) {
                present[i] = true;
                inputs.add(bounds[i]);
            }
        }
        String pass = "Monotonicity/FunctionBound";
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(List.of(target, present)),
                    inputs.toArray(DBSPExpression[]::new));
            var result = loweredFunction(compiler, reply.writer, reply.value, DBSPExpression.class);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode scalar function waterline", error);
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
        site.putArray("present").add(left != null).add(right != null);
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

    public static DBSPClosureExpression antijoinProjection(DBSPCompiler compiler,
            DBSPTypeIndexedZSet input, @Nullable DBSPClosureExpression projection) {
        return lowerFunction(compiler, "AntijoinProjection", input, projection, DBSPClosureExpression.class);
    }

    /** Return the projected inputs in insertion order, followed by the rebound join. */
    public static List<DBSPSimpleOperator> narrowJoinInputs(DBSPCompiler compiler, DBSPSimpleOperator join,
            List<OutputPort> mappedInputs) {
        String pass = "RemoveUnusedFields/NarrowJoinOperators";
        var site = expressionSite(join.getRelNode());
        site.put("kind", join.getClass().getSimpleName());
        var ports = site.putArray("ports");
        var roots = new ArrayList<IDBSPInnerNode>();
        roots.add(join.getClosureFunction());
        roots.add(join.outputType);
        for (int i = 0; i < join.inputs.size(); ++i) {
            ports.add(mappedInputs.get(i).outputNumber);
            roots.add(join.inputs.get(i).getOutputIndexedZSetType().getKVRefType());
        }
        var extra = site.putArray("extra");
        if (join instanceof DBSPAsofJoinOperator asof) {
            site.putArray("timestamps").add(asof.leftTimestampIndex).add(asof.rightTimestampIndex);
            site.put("left", asof.isLeft);
            extra.add("comparator");
            roots.add(asof.comparator);
        } else if (join instanceof DBSPJoinBaseOperator binary) {
            site.put("balanced", binary.balanced);
            DBSPClosureExpression filter = null;
            DBSPClosureExpression map = null;
            if (join instanceof DBSPJoinFilterMapOperator filtered) {
                filter = filtered.filter;
                map = filtered.map;
            } else if (join instanceof DBSPLeftJoinFilterMapOperator filtered) {
                filter = filtered.filter;
                map = filtered.map;
            }
            if (filter != null) {
                extra.add("filter");
                roots.add(filter);
            }
            if (map != null) {
                extra.add("map");
                roots.add(map);
            }
        }
        try {
            var inputs = roots.toArray(IDBSPInnerNode[]::new);
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(site), inputs);
            if (reply.value.isNull()) {
                var reader = join instanceof DBSPAsofJoinOperator asof
                        ? asof.fieldUseClosure() : join.getClosureFunction();
                site.set("paths", MAPPER.valueToTree(requiredParameterFieldDependencies(compiler, reader)));
                reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(site), inputs);
            }
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            if (reply.value.has("diagnostic") || reply.value.has("invalid_input")) {
                DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
                if (reply.value.has("diagnostic"))
                    checkBindingErrors(reply.writer, reply.value.required("diagnostic"));
                else
                    FindUsedFields.reportInvalidJoinInput(join.getClosureFunction().parameters[
                            reply.value.required("invalid_input").intValue()],
                            MAPPER.convertValue(reply.value.required("paths"), new TypeReference<>() {}));
            }
            if (!reply.value.required("narrowed").booleanValue())
                return List.of();
            var operators = decodeOperatorFragment(reply, op -> join.getRelNode(),
                    mappedInputs.stream().map(OutputPort::node).toArray(DBSPOperator[]::new));
            if (!(join instanceof DBSPAsofJoinOperator))
                operators.getLast().copyAnnotations(join);
            return operators;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode join input projections", error);
        }
    }

    public static DBSPClosureExpression projectJoinInput(DBSPCompiler compiler, CalciteObject origin,
            DBSPType input, List<Integer> columns) {
        var site = expressionSite(origin);
        site.set("columns", MAPPER.valueToTree(columns));
        String pass = "RemoveUnusedFields/JoinInput";
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(site), input);
            var function = loweredClosure(compiler, reply);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return function;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode projection of join input", error);
        }
    }

    public static DBSPClosureExpression projectUsedFields(DBSPCompiler compiler, DBSPType row,
            List<Boolean> used, @Nullable DBSPType indexedInput) {
        String pass = indexedInput == null ? "RemoveUnusedFields/Row" : "RemoveUnusedFields/IndexedValue";
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(used), row, indexedInput);
            var result = loweredClosure(compiler, reply);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode input column usage", error);
        }
    }

    @Nullable
    public static List<Integer> topKComparedFields(DBSPCompiler compiler,
            DBSPComparatorExpression rank, DBSPComparatorExpression ties) {
        String pass = "RemoveUnusedFields/TopKFields";
        var reply = invokeLowering(compiler, pass, rank, ties);
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision(pass, true);
        return reply.value.isNull() ? null : MAPPER.convertValue(reply.value, new TypeReference<>() {});
    }

    public static Pair<DBSPComparatorExpression, DBSPComparatorExpression> projectTopKComparators(
            DBSPCompiler compiler, DBSPComparatorExpression rank, DBSPComparatorExpression ties,
            DBSPType row, List<Boolean> used) {
        String pass = "RemoveUnusedFields/TopKComparators";
        record Projection(String row, List<Boolean> used) {}
        try {
            var state = MAPPER.writeValueAsBytes(new Projection(row.toString(), used));
            var reply = invokeLowering(compiler, pass, state, rank, ties, row);
            var decoder = expressionDecoder(reply.writer);
            var order = decoder.decodeInner(reply.value.required(0), DBSPComparatorExpression.class);
            var equality = decoder.decodeInner(reply.value.required(1), DBSPComparatorExpression.class);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return Pair.of(order, equality);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode TopK input projection", error);
        }
    }

    public static DBSPClosureExpression rebindProjectedReader(DBSPCompiler compiler,
            DBSPClosureExpression closure, List<DBSPParameter> parameters, List<List<Boolean>> usage) {
        return rebindReader(compiler, "RemoveUnusedFields/Reader", closure, parameters, usage);
    }

    public static DBSPClosureExpression rebindNestedReader(DBSPCompiler compiler,
            DBSPClosureExpression closure, List<DBSPParameter> parameters, List<List<FieldUseMap.TupleUsage>> usage) {
        return rebindReader(compiler, "RemoveUnusedFields/NestedReader", closure, parameters, usage);
    }

    private static DBSPClosureExpression rebindReader(DBSPCompiler compiler, String pass,
            DBSPClosureExpression closure, List<DBSPParameter> parameters, List<?> usage) {
        var inputs = new ArrayList<IDBSPInnerNode>(1 + parameters.size());
        inputs.add(closure);
        inputs.addAll(parameters);
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(usage), inputs.toArray(IDBSPInnerNode[]::new));
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            checkBindingErrors(reply.writer, reply.value);
            return loweredClosure(compiler, reply);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode projected reader parameters", error);
        }
    }

    private record IndexedReaderProjection(DBSPClosureExpression projection, DBSPClosureExpression reader,
                                          List<Boolean> used) {}

    public static List<DBSPSimpleOperator> trimLinearAggregateInput(DBSPCompiler compiler,
            DBSPAggregateLinearPostprocessOperator operator, OutputPort input) {
        var reader = operator.getClosureFunction();
        var reads = requiredParameterFieldDependencies(compiler, reader).getFirst();
        return narrowedUnary(compiler, operator, input, "LinearAggregate", reads, operator.outputType,
                reader, operator.input().getOutputIndexedZSetType().getKVRefType(), operator.postProcess);
    }

    public static List<DBSPSimpleOperator> trimFlatmapInput(DBSPCompiler compiler,
            DBSPFlatMapOperator operator, OutputPort input, DBSPFlatmap flatmap) {
        var reads = requiredParameterFieldDependencies(compiler, flatmap.collectionExpression).getFirst();
        return narrowedUnary(compiler, operator, input, "Flatmap", reads, operator.outputType, flatmap);
    }

    private static List<DBSPSimpleOperator> narrowedUnary(DBSPCompiler compiler, DBSPUnaryOperator operator,
            OutputPort input, String kind, List<?> reads, IDBSPInnerNode... operands) {
        String pass = "RemoveUnusedFields/UnaryOperators";
        var request = expressionSite(operator.getRelNode());
        request.put("kind", kind);
        request.put("port", input.outputNumber);
        request.set("reads", MAPPER.valueToTree(reads));
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(request), operands);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            if (reply.value.isNull())
                return List.of();
            if (reply.value.has("diagnostic")) {
                // UNNEST allocates its projection before checking the original reader.
                if (operator instanceof DBSPFlatMapOperator)
                    DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
                checkBindingErrors(reply.writer, reply.value.required("diagnostic"));
            }
            var operators = decodeOperatorFragment(reply, op -> operator.getRelNode(), input.node());
            Utilities.enforce(operators.size() == 2);
            int width = operator instanceof DBSPFlatMapOperator
                    ? operator.input().getOutputZSetElementType().to(DBSPTypeTuple.class).getToplevelFieldCount()
                    : operator.getType().getToplevelFieldCount();
            operators.getFirst().addAnnotation(new IsProjection(width), DBSPSimpleOperator.class);
            if (operator instanceof DBSPStreamAggregateOperator)
                operators.getLast().copyAnnotations(operator);
            reportWarnings(compiler, reply.value.required("warnings"));
            return operators;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode fields for " + kind, error);
        }
    }

    public static List<DBSPSimpleOperator> narrowedTopKOperators(DBSPCompiler compiler,
            DBSPIndexedTopKOperator operator, OutputPort input, DBSPClosureExpression producer, DBSPComparatorExpression rank) {
        var plan = trimTopKInput(compiler, producer, operator.input().getOutputIndexedZSetType().getKVRefType(),
                rank, operator.equalityComparator.comparator);
        if (plan == null)
            return List.of();
        String pass = "RemoveUnusedFields/TopKOperators";
        var request = expressionSite(operator.getRelNode());
        request.put("port", input.outputNumber);
        request.put("numbering", operator.numbering.name());
        request.put("width", operator.getType().getToplevelFieldCount());
        // Comparator type names include the debug spelling of the projected value type.
        request.put("row", plan.projection.getResultType().to(DBSPTypeRawTuple.class).getFieldType(1).toString());
        request.set("used", MAPPER.valueToTree(plan.used));
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(request),
                    plan.projection, plan.reader, rank, operator.equalityComparator, operator.limit);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            var operators = decodeOperatorFragment(reply, op -> operator.getRelNode(), input.node());
            Utilities.enforce(operators.size() == 2);
            operators.getLast().copyAnnotations(operator);
            return operators;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode TopK input", error);
        }
    }

    @Nullable
    private static IndexedReaderProjection trimTopKInput(DBSPCompiler compiler, DBSPClosureExpression producer,
            DBSPType input, DBSPComparatorExpression rank, DBSPComparatorExpression ties) {
        var paths = requiredParameterFieldDependencies(compiler, producer).get(1);
        var columns = topKComparedFields(compiler, rank, ties);
        if (columns == null)
            return null;
        int width = producer.parameters[1].getType().deref().getToplevelFieldCount();
        columns.forEach(column -> Objects.checkIndex(column, width));
        return trimIndexedReader(compiler, "RemoveUnusedFields/TopKInput", producer, input, List.of(columns, paths));
    }

    @Nullable
    private static IndexedReaderProjection trimIndexedReader(DBSPCompiler compiler, String pass,
            DBSPClosureExpression reader, DBSPType input, List<?> usage) {
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(usage), reader, input);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            if (reply.value.isNull())
                return null;
            if (reply.value.has("diagnostic"))
                checkBindingErrors(reply.writer, reply.value.required("diagnostic"));
            var functions = decodeClosurePair(new LoweringReply(reply.writer, reply.value.required("functions")));
            finishLowering(compiler, reply.value);
            return new IndexedReaderProjection(functions.left, functions.right,
                    MAPPER.convertValue(reply.value.required("used"), new TypeReference<>() {}));
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode field paths for " + pass, error);
        }
    }

    @Nullable
    public static Pair<DBSPClosureExpression, DBSPFlatmap> trimFlatmapInput(DBSPCompiler compiler, DBSPFlatmap flatmap) {
        String pass = "RemoveUnusedFields/FlatmapInput";
        var paths = requiredParameterFieldDependencies(compiler, flatmap.collectionExpression).getFirst();
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(paths), flatmap);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            if (reply.value.isNull())
                return null;
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            if (reply.value.has("diagnostic"))
                checkBindingErrors(reply.writer, reply.value.required("diagnostic"));
            var functions = reply.value.required("functions");
            var decoder = expressionDecoder(reply.writer);
            var projection = decoder.decodeInner(functions.required(0), DBSPClosureExpression.class);
            var narrowed = decoder.decodeInner(functions.required(1), DBSPFlatmap.class);
            return Pair.of(projection, narrowed);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode UNNEST collection field paths", error);
        }
    }

    public static List<DBSPSimpleOperator> trimStreamAggregateInput(
            DBSPCompiler compiler, DBSPStreamAggregateOperator operator, OutputPort input, DBSPAggregateList list) {
        var parts = new ArrayList<IDBSPInnerNode>();
        parts.add(operator.outputType);
        parts.add(operator.input().getOutputIndexedZSetType().getKVRefType());
        parts.add(list.rowVar);
        parts.add(list.getEmptySetResult());
        var reads = new ArrayList<List<List<Integer>>>();
        for (var part : list.aggregates) {
            if (part instanceof MinMaxAggregate minMax) {
                reads.add(requiredParameterFieldDependencies(compiler, minMax.comparedValue).getFirst());
                parts.add(minMax.withCopiedIncrement());
            } else if (part instanceof NonLinearAggregate aggregate) {
                reads.add(requiredParameterFieldDependencies(compiler, aggregate.increment).get(1));
                parts.add(aggregate);
            } else {
                var aggregate = part.to(LinearAggregate.class);
                reads.add(requiredParameterFieldDependencies(compiler, aggregate.map).getFirst());
                parts.add(aggregate);
            }
        }

        return narrowedUnary(compiler, operator, input, "StreamAggregate", reads, parts.toArray(IDBSPInnerNode[]::new));
    }

    public static IAggregate rebindProjectedAggregate(DBSPCompiler compiler, IAggregate aggregate,
            List<DBSPParameter> rows, List<Boolean> usage) {
        String pass = "RemoveUnusedFields/AggregateReader";
        var inputs = new ArrayList<IDBSPInnerNode>(1 + rows.size());
        inputs.add(aggregate);
        inputs.addAll(rows);
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(usage),
                    inputs.toArray(IDBSPInnerNode[]::new));
            var result = expressionDecoder(reply.writer).decodeInner(reply.value.required(0), IAggregate.class);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode aggregate input fields", error);
        }
    }

    public static List<DBSPType> compressedParameterTypes(DBSPCompiler compiler, int depth,
            List<DBSPType> types, List<List<Boolean>> usage) {
        String pass = "RemoveUnusedFields/ParameterTypes";
        record Request(int depth, List<List<Boolean>> usage) {}
        try {
            var state = MAPPER.writeValueAsBytes(new Request(depth, usage));
            var reply = invokeLowering(compiler, pass, state, types.toArray(DBSPType[]::new));
            if (!reply.value.isArray() || reply.value.size() != types.size())
                throw new IllegalStateException("Rust returned the wrong number of compressed parameter types");
            var decoder = expressionDecoder(reply.writer);
            var result = new ArrayList<DBSPType>(types.size());
            for (var type : reply.value)
                result.add(decoder.decodeInner(type, DBSPType.class));
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode parameter field usage", error);
        }
    }

    public static boolean isMapProjection(DBSPCompiler compiler, DBSPExpression function, boolean allowNestedAndCasts) {
        return isMapProjection(compiler, function, allowNestedAndCasts ? "IsNestedProjection" : "IsStrictProjection");
    }

    public static boolean isOutputProjection(DBSPCompiler compiler, DBSPSimpleOperator reader) {
        String pass = reader instanceof DBSPMapIndexOperator ? "IsFlatProjectionWithCasts" : "IsNestedProjection";
        return isMapProjection(compiler, reader.getFunction(), pass);
    }

    private static boolean isMapProjection(DBSPCompiler compiler, DBSPExpression function, String pass) {
        var reply = invokeLowering(compiler, pass, function);
        var result = reply.value;
        if (result.isBoolean())
            return result.booleanValue();
        throw projectionError(reply.writer, result, function);
    }

    private static RuntimeException projectionError(ToJsonInnerVisitor writer, JsonNode result, DBSPExpression function) {
        checkBindingErrors(writer, result);
        if (result.has("invalid_field_count")) {
            var type = function.to(DBSPClosureExpression.class).body.getType().to(DBSPTypeTupleBase.class);
            Projection.checkFieldCount(type, result.required("invalid_field_count").intValue());
        }
        return new IllegalStateException("Invalid projection decision from Rust: " + result);
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
    public static DBSPSimpleOperator foldOutputProjection(DBSPCompiler compiler, DBSPSimpleOperator producer,
            DBSPSimpleOperator reader, boolean singleReader) {
        return projectedOutput(compiler, "ProjectOutputs/Fold", producer, reader, singleReader);
    }

    public static DBSPSimpleOperator projectJoinOutput(DBSPCompiler compiler, DBSPSimpleOperator producer,
            DBSPSimpleOperator reader) {
        if (reader.getClosureFunction().parameters.length != 1) {
            throw new InternalCompilerError("Expected closure with 1 parameter",
                    producer.outputType instanceof DBSPTypeIndexedZSet ? reader : reader.getFunction());
        }
        return Objects.requireNonNull(projectedOutput(compiler, "OptimizeProjections/JoinOutput", producer, reader, true));
    }

    @Nullable
    private static DBSPSimpleOperator projectedOutput(DBSPCompiler compiler, String pass,
            DBSPSimpleOperator producer, DBSPSimpleOperator reader, boolean singleReader) {
        boolean indexed = reader instanceof DBSPMapIndexOperator;
        var site = expressionSite(producer.getRelNode());
        site.put("producer", producer.getClass().getSimpleName());
        site.put("indexed", indexed);
        site.put("single_reader", singleReader);
        site.set("ports", MAPPER.valueToTree(producer.inputs.stream().map(p -> p.outputNumber).toList()));
        DBSPExpression comparator = null;
        if (producer instanceof DBSPAsofJoinOperator asof) {
            site.putArray("timestamps").add(asof.leftTimestampIndex).add(asof.rightTimestampIndex);
            site.put("is_left", asof.isLeft);
            comparator = asof.comparator;
        } else if (producer instanceof DBSPJoinBaseOperator join) {
            site.put("balanced", join.balanced);
        }
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(site),
                    producer.getFunction(), reader.getFunction(), reader.outputType, comparator);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            if (reply.value.isNull())
                return null;
            if (reply.value.has("binding_error"))
                checkBindingErrors(reply.writer, reply.value.required("binding_error"));
            boolean indexedConstant = indexed && producer instanceof DBSPConstantOperator;
            CalciteRelNode origin = indexedConstant ? reader.getRelNode()
                    : indexed ? reader.getRelNode().after(producer.getRelNode()) : producer.getRelNode();
            var operators = decodeOperatorFragment(reply, op -> origin,
                    producer.inputs.stream().map(OutputPort::node).toArray(DBSPOperator[]::new));
            Utilities.enforce(operators.size() == 1);
            reportWarnings(compiler, reply.value.required("warnings"));
            var result = operators.getFirst();
            return indexedConstant ? result : result.copyAnnotations(producer);
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
        Utilities.enforce(!reply.value.isNull(), () -> "splitClosure cannot decompose closure " + projection);
        return decodeClosurePair(reply);
    }

    @Nullable
    public static Pair<DBSPClosureExpression, DBSPClosureExpression> trimTopKProjection(
            DBSPCompiler compiler, DBSPClosureExpression producer, DBSPClosureExpression reader) {
        var reply = invokeLowering(compiler, "TrimTopKProjection", producer, reader);
        return reply.value.isNull() ? null : decodeClosurePair(reply);
    }

    public static List<DBSPSimpleOperator> trimProjectedOutput(DBSPCompiler compiler,
            DBSPUnaryOperator producer, DBSPMapIndexOperator reader) {
        var site = expressionSite(producer.getRelNode());
        site.put("port", producer.input().outputNumber);
        final String pass;
        final IDBSPInnerNode[] roots;
        if (producer instanceof DBSPIndexedTopKOperator topK) {
            pass = "OptimizeProjections/TopKOutput";
            site.put("numbering", topK.numbering.name());
            roots = new IDBSPInnerNode[]{reader.getClosureFunction(), producer.input().outputType(),
                    topK.outputProducer, topK.getFunction(), topK.equalityComparator, topK.limit};
        } else {
            var aggregate = producer.to(DBSPStreamAggregateOperator.class);
            pass = "OptimizeProjections/AggregateOutput";
            roots = new IDBSPInnerNode[]{reader.getClosureFunction(), aggregate.outputType,
                    aggregate.getAggregateList(), reader.outputType};
        }
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(site), roots);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            if (reply.value.isNull())
                return List.of();
            var operators = decodeOperatorFragment(reply,
                    op -> op instanceof DBSPMapIndexOperator ? reader.getRelNode() : producer.getRelNode(),
                    producer.input().node());
            if (producer instanceof DBSPIndexedTopKOperator)
                operators.getFirst().copyAnnotations(producer);
            operators.getLast().copyAnnotations(reader);
            return operators;
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot encode projected output", error);
        }
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
        return Pair.of(decodeLiveRoot(decoder, functions.get(0), DBSPClosureExpression.class),
                decodeLiveRoot(decoder, functions.get(1), DBSPClosureExpression.class));
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
        if (reply.value.has("binding_error"))
            checkBindingErrors(reply.writer, reply.value.required("binding_error"));
        if (reply.value.has("projection_error"))
            throw projectionError(reply.writer, reply.value.required("projection_error"),
                    Objects.requireNonNull(second).to(DBSPExpression.class));
        var function = loweredFunction(compiler, reply.writer, reply.value, kind);
        DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
        return function;
    }

    @Nullable
    public static Pair<DBSPClosureExpression, DBSPClosureExpression> splitMapInput(DBSPCompiler compiler,
            DBSPUnaryOperator map, @Nullable List<List<Integer>> paths) {
        try {
            boolean indexed = map instanceof DBSPMapIndexOperator;
            var closure = map.getClosureFunction();
            String pass;
            byte[] state;
            if (paths == null) {
                pass = "SplitInspectedMapInput";
                var inputs = requiredParameterFieldDependencies(compiler, closure);
                state = MAPPER.writeValueAsBytes(List.of(indexed, inputs.getFirst()));
            } else {
                pass = indexed ? "SplitIndexedMapInput" : "SplitRowMapInput";
                state = MAPPER.writeValueAsBytes(paths);
            }
            var reply = invokeLowering(compiler, pass, state, closure);
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            if (compiler.stateObserver != null && paths == null)
                compiler.stateObserver.nativeDecision(pass, true);
            if (reply.value.has("diagnostic"))
                checkBindingErrors(reply.writer, reply.value.required("diagnostic"));
            var functions = reply.value.required("functions");
            return functions.isNull() ? null : decodeClosurePair(new LoweringReply(reply.writer, functions));
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode map input fields", error);
        }
    }

    /** Decode the two operators of a map split against the actual upstream port. */
    public static List<DBSPSimpleOperator> splitMapOperators(DBSPCompiler compiler, DBSPUnaryOperator map,
            OutputPort input, @Nullable List<List<Integer>> paths) {
        String pass = "RemoveUnusedFields/SplitMap";
        boolean inspected = paths == null;
        var reader = map.getClosureFunction();
        if (inspected)
            paths = requiredParameterFieldDependencies(compiler, reader).getFirst();
        var request = expressionSite(map.getRelNode());
        request.put("port", input.outputNumber);
        request.put("inspected", inspected);
        request.set("paths", MAPPER.valueToTree(paths));
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(request),
                    reader, input.outputType(), map.outputType);
            if (compiler.stateObserver != null) {
                compiler.stateObserver.nativeDecision(pass, true);
                if (inspected)
                    compiler.stateObserver.nativeDecision("SplitInspectedMapInput", true);
            }
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            if (reply.value.has("diagnostic"))
                checkBindingErrors(reply.writer, reply.value.required("diagnostic"));
            var operators = decodeOperatorFragment(reply, op -> map.getRelNode(), input.node());
            if (!operators.isEmpty()) {
                Utilities.enforce(operators.size() == 2);
                var projection = operators.getFirst();
                if (reply.value.required("inherit_annotations").booleanValue())
                    projection.copyAnnotations(map);
                projection.addAnnotation(new IsProjection(reader.getType().getToplevelFieldCount()), DBSPSimpleOperator.class);
            }
            return operators;
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
        var inner = new OptimizerExpressionWriter(compiler, !pass.equals("RewriteNow/ClockSites"));
        try {
            byte[] reply = transformWithContext(pass, inner.encode(inputs), DBSPVariablePath.nextGeneratedName(), "", state);
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
        var roots = Linq.list(expressions).toArray(IDBSPInnerNode[]::new);
        if (roots.length == 0)
            return Map.of();
        var reply = invokeLowering(compiler, "RewriteNow/ClockSites", roots);
        var nodes = Objects.requireNonNull(reply.writer.liveNodes);
        var sites = new HashMap<Long, DBSPExpression>();
        for (var pair : reply.value) {
            var call = Utilities.getExists(nodes, pair.required(1).longValue()).to(DBSPApplyExpression.class);
            sites.put(pair.required(0).longValue(), call);
        }
        return sites;
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
            var lambda = decodeLiveRoot(expressionDecoder(reply.writer), unsupported, DBSPClosureExpression.class);
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
                var predicate = decodeLiveRoot(decoder, rejection.required(0), DBSPExpression.class);
                var lambda = decodeLiveRoot(decoder, rejection.required(1), DBSPClosureExpression.class);
                DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
                return new ClockFilter(List.of(), predicate, lambda);
            }
            var warning = reply.value.get("warning_predicate");
            DBSPExpression predicate = warning == null ? null :
                    decodeLiveRoot(reply.writer, warning, DBSPExpression.class);
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
        var inner = new OptimizerExpressionWriter(compiler, json, 1, true);
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
        json.endArray();
        inner.writeDeferredDefinitions();
        json.endObject();
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

    public record LazyRead(DBSPVariablePath variable, DBSPExpression value) {}

    /** The first read allocates the cell. Later reads refer to that same live variable. */
    public static LazyRead readLazyValue(DBSPCompiler compiler, DBSPType resultType,
                                         @Nullable DBSPVariablePath variable, boolean borrowed) {
        try {
            String pass = "InnerCSE/ReadLazy";
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(borrowed),
                    variable == null ? resultType : variable);
            var decoder = expressionDecoder(reply.writer);
            var roots = reply.value.required("roots");
            var cell = decodeLiveRoot(decoder, roots.required(0), DBSPVariablePath.class);
            var read = decodeLiveRoot(decoder, roots.required(1), DBSPExpression.class);
            DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return new LazyRead(cell, read);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode the CSE borrow mode", error);
        }
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
            var writer = new OptimizerExpressionWriter(compiler, false);
            return new ClosureSnapshot(writer.encode(function), Objects.requireNonNull(writer.liveNodes));
        }

        byte[] analyze(String pass) {
            return transformWithContext(pass, this.graph, 0, "", EMPTY_CONTEXT);
        }
    }

    public record MapFusionScan(List<FuseExpensiveMaps.MapInfo> candidates, @Nullable int[][] clusters,
                                @Nullable JsonNode comparisonFailure) {
        /** Cluster membership is observable when a later field comparison fails. */
        public void finish() {
            if (this.comparisonFailure != null && !this.comparisonFailure.isNull())
                reportComparisonFailure(this.comparisonFailure);
        }
    }

    public static MapFusionScan scanMapFusion(DBSPCompiler compiler, List<DBSPMapOperator> maps) {
        String pass = "FuseExpensiveMaps/Scan";
        var ports = new HashMap<OutputPort, Integer>();
        var inputs = new int[maps.size()];
        var functions = new DBSPClosureExpression[maps.size()];
        for (int i = 0; i < maps.size(); i++) {
            var map = maps.get(i);
            inputs[i] = ports.computeIfAbsent(map.input(), ignored -> ports.size());
            functions[i] = map.getClosureFunction();
        }
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(inputs), functions);
            reportWarnings(compiler, reply.value.required("warnings"));
            var roots = reply.value.required("roots").elements();
            var decoder = expressionDecoder(reply.writer);
            var candidates = new ArrayList<FuseExpensiveMaps.MapInfo>();
            for (var member : reply.value.required("members")) {
                var map = maps.get(member.required(0).intValue());
                var function = decodeLiveRoot(decoder, roots.next(), DBSPClosureExpression.class);
                var fragments = new ArrayList<DBSPExpression>();
                for (int i = 0; i < member.required(1).intValue(); i++)
                    fragments.add(decodeLiveRoot(decoder, roots.next(), DBSPExpression.class));
                candidates.add(new FuseExpensiveMaps.MapInfo(map, function, fragments));
            }
            Utilities.enforce(!roots.hasNext());
            var clusters = reply.value.required("clusters");
            return new MapFusionScan(candidates, answered(compiler, pass, clusters)
                    ? MAPPER.treeToValue(clusters, int[][].class) : null, reply.value.get("comparison_failure"));
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid map fusion scan from Rust", error);
        }
    }

    @Nullable
    public static Boolean hasDuplicateExpensiveField(DBSPCompiler compiler, DBSPClosureExpression function) {
        String pass = "FuseExpensiveMaps/DuplicateField";
        var result = invokeLowering(compiler, pass, function).value;
        if (!answered(compiler, pass, result))
            return null;
        if (result.has("comparison_failure"))
            reportComparisonFailure(result.required("comparison_failure"));
        return result.booleanValue();
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
            var expression = Objects.requireNonNull(decodeLiveRoot(reply.writer, reply.value.required("initializer"),
                    DBSPExpression.class));
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

    /** Index of the matching declaration, or -1 when this literal needs a new static. */
    public static int findStaticLiteral(DBSPCompiler compiler, DBSPLiteral literal, List<DBSPLiteral> candidates) {
        var inputs = new IDBSPInnerNode[candidates.size() + 1];
        inputs[0] = literal;
        for (int i = 0; i < candidates.size(); i++)
            inputs[i + 1] = candidates.get(i);
        String pass = "Statics/FindLiteral";
        var result = invokeLowering(compiler, pass, inputs).value;
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision(pass, true);
        if (result.isNull())
            return -1;
        if (!result.isInt() || result.intValue() < 0 || result.intValue() >= candidates.size())
            throw new IllegalStateException("Invalid static literal index " + result);
        return result.intValue();
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

    @Nullable
    public static int[][] siblingMapGroups(DBSPCompiler compiler, List<FuseExpensiveMaps.MapInfo> siblings) {
        String pass = "FuseExpensiveMaps/Siblings";
        var roots = new ArrayList<IDBSPInnerNode>();
        int[] counts = new int[siblings.size()];
        for (int i = 0; i < counts.length; i++) {
            var map = siblings.get(i);
            roots.add(map.function().parameters[0]);
            roots.addAll(map.minimal());
            counts[i] = map.minimal().size();
        }
        try {
            var result = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(counts),
                    roots.toArray(IDBSPInnerNode[]::new)).value;
            return answered(compiler, pass, result) ? MAPPER.treeToValue(result, int[][].class) : null;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid sibling map groups from Rust", error);
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

    /** Send the original DAG once. Rust collects operand uses and chooses value numbers in lexical scope. */
    public static void numberValues(DBSPCompiler compiler, IDBSPInnerNode root, ValueNumbering analysis, boolean rewrite) {
        var reply = analyzeCse(compiler, root, rewrite, "InnerCSE/NumberGraph");
        try {
            var result = MAPPER.treeToValue(reply.value, NativeValueNumbering.Result.class);
            NativeValueNumbering.bind(result, Objects.requireNonNull(reply.writer.liveNodes), analysis, rewrite);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid CSE graph value numbers", error);
        }
    }

    /** An unknown write keeps the original DAG. Other roots use the Rust translation of the normalized tree. */
    public static DBSPExpression rewriteCse(DBSPCompiler compiler, DBSPExpression tree, DBSPExpression original) {
        var reply = analyzeCse(compiler, tree, true, "InnerCSE/Rewrite");
        DBSPVariablePath.restoreGeneratedNames(reply.value.required("next_generated_name").longValue());
        if (reply.value.has("unimplemented_for"))
            throw new UnimplementedException();
        var result = decodeLiveRoot(reply.writer, reply.value.required("root"), DBSPExpression.class);
        return result == null ? original : result;
    }

    private static LoweringReply analyzeCse(DBSPCompiler compiler, IDBSPInnerNode root, boolean rewrite, String pass) {
        var writer = new OptimizerExpressionWriter(compiler, pass.equals("InnerCSE/Rewrite"));
        var graph = writer.encode(root);
        try {
            var spellings = NativeValueNumbering.spellings(Objects.requireNonNull(writer.liveNodes).values(), rewrite);
            var bytes = transformWithContext(pass, graph, DBSPVariablePath.nextGeneratedName(), "", MAPPER.writeValueAsBytes(spellings));
            var value = MAPPER.readTree(bytes);
            checkBindingErrors(writer, value);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return new LoweringReply(writer, value);
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid Rust CSE analysis", error);
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

    @Nullable
    public static List<NativeLeftJoinChains.Step> chainSteps(DBSPCompiler compiler, List<NativeLeftJoinChains.Operator> graph) {
        String pass = "LeftJoinChains/Steps";
        try {
            var reply = MAPPER.readTree(transformWithContext(pass, MAPPER.writeValueAsBytes(graph), 0, "", EMPTY_CONTEXT));
            return answered(compiler, pass, reply) ? MAPPER.convertValue(reply, new TypeReference<>() {}) : null;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not read left-join chain steps", error);
        }
    }

    public static List<NativeLeftJoinChains.Endpoint> carriedColumns(DBSPCompiler compiler, NativeLeftJoinChains.Inputs inputs) {
        String pass = "LeftJoinChains/Columns";
        try {
            var bytes = transformWithContext(pass, MAPPER.writeValueAsBytes(inputs), 0, "", EMPTY_CONTEXT);
            List<NativeLeftJoinChains.Endpoint> result = MAPPER.readValue(bytes, new TypeReference<>() {});
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(pass, true);
            return result;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not read carried left-join columns", error);
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
        advanceTemporalNames(reply.value);
        var entries = reply.value.required("windows");
        var windows = new ArrayList<ConjunctWindow>(entries.size());
        for (var window : entries) {
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

    private static void advanceTemporalNames(JsonNode reply) {
        long count = reply.required("generated_names").longValue();
        DBSPVariablePath.restoreGeneratedNames(Math.addExact(DBSPVariablePath.nextGeneratedName(), count));
    }

    private static List<TemporalComparison> temporalComparisons(LoweringReply reply) {
        advanceTemporalNames(reply.value);
        var decoder = expressionDecoder(reply.writer);
        var entries = reply.value.required("comparisons");
        var parts = new ArrayList<TemporalComparison>(entries.size());
        for (var part : entries) {
            var kind = PredicateKind.valueOf(part.required("kind").textValue());
            var expression = decodeLiveRoot(decoder, part.required("expression"), DBSPExpression.class);
            var clock = part.has("clock") ? decodeLiveRoot(decoder, part.required("clock"), DBSPExpression.class) : null;
            parts.add(new TemporalComparison(kind, expression, clock));
        }
        return parts;
    }

    public record TemporalGroup(int first, int count, @Nullable DBSPExpression expression) {}

    @Nullable
    public static List<TemporalGroup> groupTemporalFilters(DBSPCompiler compiler, List<String> kinds,
                                                            List<IDBSPInnerNode> roots) {
        var reply = temporalGroupRequest(compiler, "TemporalGroups", kinds, roots, true);
        if (reply == null)
            return null;
        var decoder = expressionDecoder(reply.writer);
        var groups = new ArrayList<TemporalGroup>();
        for (var group : reply.value) {
            var value = group.required("expression");
            groups.add(new TemporalGroup(group.required("first").intValue(), group.required("count").intValue(),
                    value.isNull() ? null : decodeLiveRoot(decoder, value, DBSPExpression.class)));
        }
        return groups;
    }

    @Nullable
    public static Boolean acceptsTemporalBound(DBSPCompiler compiler, List<String> kinds, List<IDBSPInnerNode> roots) {
        return acceptsTemporalBound(compiler, kinds, roots, false);
    }

    /** A speculative comparison can decline an unbound key. The merge pass reports it at the original site. */
    @Nullable
    public static Boolean acceptsTemporalBound(DBSPCompiler compiler, List<String> kinds, List<IDBSPInnerNode> roots,
                                              boolean reportComparisonFailure) {
        var reply = temporalGroupRequest(compiler, "MergeTemporalBounds", kinds, roots, reportComparisonFailure);
        return reply == null ? null : reply.value.booleanValue();
    }

    @Nullable
    private static LoweringReply temporalGroupRequest(DBSPCompiler compiler, String pass,
                                                       List<String> kinds, List<IDBSPInnerNode> roots,
                                                       boolean reportComparisonFailure) {
        try {
            var reply = invokeLowering(compiler, pass, MAPPER.writeValueAsBytes(kinds),
                    roots.toArray(IDBSPInnerNode[]::new));
            if (!answered(compiler, pass, reply.value))
                return null;
            var failure = reply.value.get("comparison_failure");
            if (failure != null) {
                if (reportComparisonFailure)
                    reportComparisonFailure(failure);
                return null;
            }
            return reply;
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode temporal filter groups", error);
        }
    }

    private record TemporalPath(long filter, long source, int port, List<Long> maps) {}

    public static DBSPClosureExpression composeTimestamp(DBSPCompiler compiler, DBSPClosureExpression timestamp,
                                                         List<DBSPClosureExpression> maps) {
        // A later map's diagnostic can refer to an expression created by an earlier map.
        for (var map : maps) {
            var reply = invokeLowering(compiler, "WindowGroups/ComposeTimestamp", timestamp, map);
            if (reply.value.has("binding_error"))
                checkBindingErrors(reply.writer, reply.value.required("binding_error"));
            timestamp = loweredClosure(compiler, reply);
        }
        return timestamp;
    }

    private static void scanTemporalWindows(DBSPCompiler compiler, DBSPCircuit circuit, ReorderTemporalFilters.Scan scan) {
        var operators = operatorsById(circuit);
        var payloads = new HashSet<Long>();
        for (var op : operators.values())
            if (op instanceof DBSPFilterOperator || op instanceof DBSPMapOperator || op instanceof DBSPMapIndexOperator)
                payloads.add(op.id);
        var writer = OptimizerTopology.temporalScan(compiler, circuit, payloads);
        try {
            byte[] result = transformWithContext("WindowGroups/Scan",
                    writer.getJsonString().getBytes(StandardCharsets.UTF_8), 0, "",
                    MAPPER.writeValueAsBytes(compiler.metadata.windowSharingThreshold()));
            var reply = MAPPER.readTree(result);
            var groups = reply.get("groups");
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision("WindowGroups/Scan", groups != null);
            if (groups != null) {
                advanceTemporalNames(reply);
                reportWarnings(compiler, reply.required("warnings"));
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
                state.putRawValue("transfer_roots", new RawValue(writer.liveTransferRoots(functions)));
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
        // The clone passes preserve unchanged operators. Later projection passes
        // use their object identity to recognize maps they have already analyzed.
        // A keyed source can keep its wire ID when it becomes a waterline operator.
        var originalOperators = this.reusesOperators()
                ? operatorsById(input) : Map.<Long, DBSPOperator>of();
        var decoder = new LiveDecoder(writer.innerVisitor, writer.liveProvenanceDecoder()) {
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
        if (this.trimInputs != null && result.has("binding_error")) {
            DBSPVariablePath.restoreGeneratedNames(result.required("next_generated_name").longValue());
            checkBindingErrors(Objects.requireNonNull(writer).innerVisitor, result.required("binding_error"));
            throw new IllegalStateException("Expected a source reader binding diagnostic");
        }
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
        var result = decodeLiveRoot(reply.writer, reply.value.required("function"), DBSPClosureExpression.class);
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
                var project = decodeLiveRoot(decoder, functions.required(0), DBSPClosureExpression.class);
                var predicate = decodeLiveRoot(decoder, functions.required(1), DBSPClosureExpression.class);
                var consume = decodeLiveRoot(decoder, functions.required(2), DBSPClosureExpression.class);
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
        try {
            var writer = new OptimizerExpressionWriter(compiler, false);
            var graph = writer.encode(closure);
            var reply = transformWithContext(pass, graph, 0, "", MAPPER.writeValueAsBytes(request));
            var value = MAPPER.readTree(reply);
            if (value.has("diagnostic")) {
                if (compiler.stateObserver != null)
                    compiler.stateObserver.nativeDecision(pass, true);
                reportLineageFailure(writer, value.required("diagnostic"));
            }
            return MAPPER.readerFor(type).readValue(value);
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
        AggregateKey, RollingTimestamp, InvalidRollingOffset, IntersectRows, IntersectKeys, PrimitiveMinimumOrKey,
        ChainMinimumOrKey, InvalidIndexedSource, Analyze }
    public enum ClosureBody { Operator, IndexedValue, Filter, Hop, Lag }
    public enum ColumnWaterline { Declared, InheritIfPresent, Unbounded }
    public enum FieldWaterline { Whole, Unbounded, Unit, AllColumns }
    public record IndexedWaterline(boolean requireInputKey, FieldWaterline key, FieldWaterline value) {}
    public record MonotonicityTransfer(DBSPOperator operator, TransferRule rule, int input,
                                       @Nullable List<ColumnWaterline> columns, @Nullable IndexedWaterline projection,
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

    /** The live result of a transfer, including a declined MIN value projection. */
    public enum WaterlineResult {
        Absent(0), Row(1), Key(2), Value(3), KeyAndValue(4), TryGroupKey(5);

        final int wireValue;
        WaterlineResult(int wireValue) {
            this.wireValue = wireValue;
        }

        public static WaterlineResult indexed(boolean key, boolean value) {
            if (key)
                return value ? KeyAndValue : Key;
            return value ? Value : Absent;
        }
    }

    @FunctionalInterface
    private interface WaterlineStep {
        int apply(long operator, byte[] transfer);
    }

    private static native void propagateWaterlines(byte[] topology, WaterlineStep step);

    /** Report whether each live transfer produced a bound before Rust visits its consumers. */
    public static void visitWaterlines(DBSPCompiler compiler, DBSPCircuit circuit,
                                       Function<MonotonicityTransfer, WaterlineResult> visit) {
        record Step(TransferRule rule, int input, @Nullable List<ColumnWaterline> columns,
                    @Nullable IndexedWaterline projection, @Nullable ClosureBody function) {}
        propagateWaterlines(OptimizerTopology.monotonicity(compiler, circuit), new WaterlineStep() {
            @Nullable Map<Long, DBSPOperator> operators;

            @Override
            public int apply(long operator, byte[] transfer) {
                if (this.operators == null)
                    this.operators = operatorsById(circuit);
                try {
                    var step = MAPPER.readValue(transfer, Step.class);
                    return visit.apply(new MonotonicityTransfer(Utilities.getExists(this.operators, operator),
                            step.rule(), step.input(), step.columns(), step.projection(), step.function())).wireValue;
                } catch (IOException error) {
                    throw new UncheckedIOException("Invalid monotonicity transfer", error);
                }
            }
        });
    }

    /** Inspect the possible transfers without evaluating their closures. */
    public static List<MonotonicityTransfer> monotonicityTransfers(DBSPCompiler compiler, DBSPCircuit circuit) {
        var transfers = new ArrayList<MonotonicityTransfer>();
        visitWaterlines(compiler, circuit, transfer -> {
            transfers.add(transfer);
            return WaterlineResult.Row;
        });
        return transfers;
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
    static native byte[] planIndexFields(byte[] functions, long next, byte[] fixed, LiveOperands operands);

    private static LoweringReply sharedIndexPlan(DBSPCompiler compiler, List<DBSPClosureExpression> functions,
                                                 @Nullable List<Boolean> fixed) {
        String operation = fixed == null ? "SharedIndexes/WideFields" : "SharedIndexes/Split";
        var writer = new OptimizerExpressionWriter(compiler);
        try {
            var inputs = writer.encode(functions.toArray(DBSPClosureExpression[]::new));
            var bytes = planIndexFields(inputs, DBSPVariablePath.nextGeneratedName(), MAPPER.writeValueAsBytes(fixed),
                    liveOperands(compiler, writer));
            var state = MAPPER.readTree(bytes);
            if (compiler.stateObserver != null)
                compiler.stateObserver.nativeDecision(operation, true);
            finishLowering(compiler, state);
            if (state.has("comparison_failure"))
                reportComparisonFailure(state.required("comparison_failure"));
            return new LoweringReply(writer, state);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not plan shared-index fields", error);
        }
    }

    public static int[][] splitSharedIndexes(DBSPCompiler compiler, List<DBSPClosureExpression> functions, List<Boolean> fixed) {
        var reply = sharedIndexPlan(compiler, functions, fixed);
        return MAPPER.convertValue(reply.value.required("groups"), int[][].class);
    }

    private static void reportWarnings(DBSPCompiler compiler, JsonNode warnings) {
        for (var warning : warnings)
            compiler.reportWarning(SourcePositionRange.fromJson(warning.required("range")),
                    warning.required("error_type").textValue(), warning.required("message").textValue());
    }

    private static void finishLowering(DBSPCompiler compiler, JsonNode state) {
        reportWarnings(compiler, state.required("warnings"));
        DBSPVariablePath.restoreGeneratedNames(state.required("next_generated_name").longValue());
    }

    private static IDBSPInnerNode rewriteInnerRoot(DBSPCompiler compiler, String pass, IDBSPInnerNode root) {
        // Expression translators leave types untouched.
        if (root instanceof DBSPType)
            return root;
        var reply = invokeLowering(compiler, pass, root);
        var result = Objects.requireNonNull(decodeLiveRoot(reply.writer, reply.value.required("root"), IDBSPInnerNode.class));
        finishLowering(compiler, reply.value);
        return result;
    }

    /** The first root is the reader. Each following pair binds a narrow parameter to its wide row. */
    public static IDBSPInnerNode rewriteIndexReader(DBSPCompiler compiler, List<IDBSPInnerNode> roots,
                                                     List<List<Integer>> columns) {
        try {
            var reply = invokeLowering(compiler, "SharedIndexes/Reader",
                    MAPPER.writeValueAsBytes(columns), roots.toArray(IDBSPInnerNode[]::new));
            return decodeLiveRoot(expressionDecoder(reply.writer), reply.value, IDBSPInnerNode.class);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode shared-index reader columns", error);
        }
    }

    public record WideIndexFields(DBSPVariablePath variable, DBSPExpression key, boolean nullable,
                                  List<DBSPExpression> fields, List<List<Integer>> layout) {}

    public static WideIndexFields wideIndexFields(DBSPCompiler compiler, List<DBSPClosureExpression> functions) {
        var reply = sharedIndexPlan(compiler, functions, null);
        var decoder = expressionDecoder(reply.writer);
        var expressions = reply.value.required("expressions");
        var variable = decodeLiveRoot(decoder, expressions.required(0), DBSPVariablePath.class);
        var key = decodeLiveRoot(decoder, expressions.required(1), DBSPExpression.class);
        var fields = new ArrayList<DBSPExpression>(expressions.size() - 2);
        for (int i = 2; i < expressions.size(); i++)
            fields.add(decodeLiveRoot(decoder, expressions.required(i), DBSPExpression.class));
        List<List<Integer>> layout = MAPPER.convertValue(reply.value.required("layout"), new TypeReference<>() {});
        return new WideIndexFields(variable, key, reply.value.required("nullable").booleanValue(), fields, layout);
    }

    @FunctionalInterface
    interface LiveOperands {
        byte[] capture(byte[] roots) throws IOException;
    }

    /** Re-encode both operands together so shared references, shuffle owners and
     * JDK timestamp conversions are available in a single expression snapshot. */
    static LiveOperands liveOperands(DBSPCompiler compiler, ToJsonInnerVisitor writer) {
        return bytes -> {
            var roots = MAPPER.readTree(bytes);
            var decoder = expressionDecoder(writer);
            var operands = new DBSPExpression[roots.size()];
            for (int i = 0; i < operands.length; i++)
                operands[i] = decodeLiveRoot(decoder, roots.required(i), DBSPExpression.class);
            return new OptimizerExpressionWriter(compiler, false).encode(operands);
        };
    }

    private static native byte[] discoverIndexes(byte[] snapshot, long next, byte[] visited, LiveOperands operands);

    private static native byte[] replaceIndexes(byte[] snapshot, long next, byte[] groups, LiveOperands operands);

    private static native byte[] fuseMaps(byte[] snapshot, long next, byte[] clusters, LiveOperands operands);

    @FunctionalInterface
    private interface TemporalAnalysis {
        byte[] analyze(byte[] predicates) throws IOException;
    }

    private static native byte[] mergeFilters(byte[] snapshot, long next, TemporalAnalysis analysis);

    private JsonNode mergeTemporalFilters(DBSPCircuit circuit, ToJsonOuterVisitor writer,
                                          byte[] snapshot, long next) throws IOException {
        var result = mergeFilters(snapshot, next, bytes -> {
            if (!this.usedGeneralTemporalAnalysis && this.compiler.stateObserver != null)
                this.compiler.stateObserver.nativeDecision("MergeFilters", false);
            this.usedGeneralTemporalAnalysis = true;
            var request = MAPPER.readTree(bytes);
            finishLowering(this.compiler, request);
            var operators = request.required("operators");
            var closures = request.required("closures");
            var decoder = expressionDecoder(writer.innerVisitor);
            var source = Objects.requireNonNull(findOperator(circuit.allOperators, operators.required(0).longValue()));
            var target = Objects.requireNonNull(findOperator(circuit.allOperators, operators.required(1).longValue()));
            boolean merge = Objects.requireNonNull(this.mergeFilters).canBeMerged(source.to(DBSPFilterOperator.class),
                    decodeLiveRoot(decoder, closures.required(0), DBSPClosureExpression.class),
                    target.to(DBSPFilterOperator.class),
                    decodeLiveRoot(decoder, closures.required(1), DBSPClosureExpression.class));
            var reply = MAPPER.createObjectNode();
            reply.put("merge", merge);
            reply.put("next_generated_name", DBSPVariablePath.nextGeneratedName());
            return MAPPER.writeValueAsBytes(reply);
        });
        var reply = MAPPER.readTree(result);
        var failure = reply.get("failure");
        if (failure != null) {
            finishLowering(this.compiler, reply);
            if (!this.usedGeneralTemporalAnalysis && this.compiler.stateObserver != null)
                this.compiler.stateObserver.nativeDecision("MergeFilters", true);
            if (failure.has("predicate"))
                throw temporalPredicateError(this.compiler, writer.innerVisitor, failure.required("predicate"));
            if (failure.has("comparison_failure"))
                reportComparisonFailure(failure.required("comparison_failure"));
            var typeError = failure.get("type_error");
            if (typeError != null) {
                var type = decodeLiveRoot(expressionDecoder(writer.innerVisitor), typeError.required("type"), DBSPType.class);
                throw waterlineTypeError(type, typeError.required("kind").textValue());
            }
            if (failure.has("argument_error")) {
                var call = decodeLiveRoot(expressionDecoder(writer.innerVisitor), failure.required("call"), DBSPApplyExpression.class);
                throw MAPPER.convertValue(failure.required("argument_error"), FunctionArgumentError.class).exception(call);
            }
            if (failure.has("missing_clock_bound"))
                // MonotoneExpression.getReducedExpression requires a non-null bound.
                throw new NullPointerException();
            var expression = decodeLiveRoot(expressionDecoder(writer.innerVisitor),
                    failure.required("missing_clock_key"), DBSPExpression.class);
            throw Utilities.missingKey(failure.required("key_is_node_id").booleanValue() ? expression.getId() : expression);
        }
        return reply;
    }

    /** Materialize the Rust waterline diagnostic against the failing predicate's
     * live types, including facts produced by earlier statements in that predicate. */
    private static RuntimeException temporalPredicateError(DBSPCompiler compiler, ToJsonInnerVisitor writer, JsonNode roots) {
        var decoder = expressionDecoder(writer);
        var parameter = decodeLiveRoot(decoder, roots.required(0), DBSPParameter.class);
        var body = decodeLiveRoot(decoder, roots.required(1), DBSPExpression.class);
        var row = NonMonotoneType.nonMonotone(parameter.getType().deref());
        var analysis = projectWaterlineClosure(compiler, body, new DBSPParameter[]{parameter},
                new DBSPType[]{row.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(row)}, false);
        if (analysis instanceof FailedWaterline failed && failed.error() == WaterlineError.TargetMaximum)
            return new UnsupportedException(failed.expression().getType().withMayBeNull(false).getNode());
        return new IllegalStateException("Expected a waterline diagnostic for temporal predicate " + body);
    }

    private static void reportComparisonFailure(JsonNode failure) {
        if (failure.isTextual() && failure.textValue().equals("emptyContext"))
            throw new InternalCompilerError("Empty context", CalciteObject.EMPTY);
        var variable = failure.get("missingDeclaration");
        if (variable != null)
            Utilities.fail("Declaration for variable " + Utilities.singleQuote(variable.textValue()) + " not found");
        // The snapshot recorded that the JDK could not resolve this literal. Let
        // the same conversion render its exception only when a comparison reads it.
        var spelling = failure.required("invalidTimestamp").textValue();
        DBSPTimestampTzLiteral.getMicros(new TimestampWithTimeZoneString(spelling));
        throw new IllegalStateException("Expected the recorded timestamp conversion to fail for " + spelling);
    }

    private JsonNode columnRewriteReply(String operation, byte[] result) throws IOException {
        var reply = MAPPER.readTree(result);
        if (this.compiler.stateObserver != null)
            this.compiler.stateObserver.nativeDecision(operation, true);
        var failure = reply.get("comparison_failure");
        if (failure != null) {
            finishLowering(this.compiler, reply);
            reportComparisonFailure(failure);
        }
        return reply;
    }

    private JsonNode fuseMapClusters(ToJsonOuterVisitor writer, byte[] snapshot, long next) throws IOException {
        byte[] clusters = MapFusionState.encode(writer, Objects.requireNonNull(this.mapFusion));
        byte[] result = fuseMaps(snapshot, next, clusters, liveOperands(this.compiler, writer.innerVisitor));
        return this.columnRewriteReply("FuseExpensiveMaps/Columns", result);
    }

    private JsonNode replaceIndexGroups(ToJsonOuterVisitor writer, byte[] snapshot, long next, byte[] groups) throws IOException {
        byte[] result = replaceIndexes(snapshot, next, groups,
                liveOperands(this.compiler, writer.innerVisitor));
        return this.columnRewriteReply("ReplaceSharedIndexes", result);
    }

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
                MAPPER.writeValueAsBytes(find.visitedIds()), liveOperands(this.compiler, writer.innerVisitor));
        var state = MAPPER.readTree(result);
        this.finishIndexDiscovery(circuit, find, state);
        if (state.has("binding_error"))
            checkBindingErrors(writer.innerVisitor, state.required("binding_error"));
        var failure = state.get("comparison_failure");
        if (failure != null)
            reportComparisonFailure(failure);
    }

    private void finishIndexDiscovery(DBSPCircuit circuit, FindSharedIndexes find, JsonNode state) throws IOException {
        if (this.compiler.stateObserver != null)
            this.compiler.stateObserver.nativeDecision("SharedIndexes/Discover", true);
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

    /** A failing field-use lookup must name the node in the tree the compiler actually evaluates. */
    public static List<List<List<Integer>>> requiredParameterFieldDependencies(DBSPCompiler compiler,
                                                                                DBSPClosureExpression closure) {
        String pass = "ParameterFieldDependencies";
        var result = invokeLowering(compiler, pass, closure).value;
        if (result.isNull()) {
            var tree = closure.ensureTree(compiler).to(DBSPClosureExpression.class);
            var reply = invokeLowering(compiler, "CheckedParameterFieldDependencies", tree);
            result = reply.value;
            if (!result.isArray()) {
                if (compiler.stateObserver != null)
                    compiler.stateObserver.nativeDecision(pass, true);
                checkBindingErrors(reply.writer, result);
                if (result.has("no_parameter_value"))
                    Utilities.enforce(false);
                if (result.has("let_statement"))
                    throw new UnimplementedException();
                if (result.has("missing_value"))
                    throw Utilities.missingKey(result.required("missing_value").longValue());
                if (result.has("pair_index"))
                    List.of(0, 1).get(result.required("pair_index").intValue());
                if (result.has("index"))
                    Objects.checkIndex(result.required("index").intValue(), result.required("width").intValue());
                if (result.has("changed_value")) {
                    var nodes = Objects.requireNonNull(reply.writer.liveNodes);
                    var target = Utilities.getExists(nodes, result.required("target").longValue());
                    var conflict = result.required("changed_value");
                    var members = new ArrayList<IUsedFields>();
                    for (var member : conflict.required("members"))
                        members.add(fieldUseValue(member, tree, members));
                    throw new InternalCompilerError("Changing value of " + target + " from\n" +
                            fieldUseValue(conflict.required("previous"), tree, members) + " to\n" +
                            fieldUseValue(conflict.required("assigned"), tree, members), target.getNode());
                }
                throw new IllegalStateException("Unknown field-use diagnostic " + result);
            }
        }
        if (compiler.stateObserver != null)
            compiler.stateObserver.nativeDecision(pass, true);
        return MAPPER.convertValue(result, new TypeReference<>() {});
    }

    private static IUsedFields fieldUseValue(JsonNode value, DBSPClosureExpression closure, List<IUsedFields> members) {
        boolean borrowed = value.has("borrowed");
        if (borrowed || value.has("field")) {
            var path = value.required(borrowed ? "borrowed" : "field");
            var field = new ParameterField(closure.parameters[path.required("parameter").intValue()],
                    MAPPER.convertValue(path.required("indices"), new TypeReference<List<Integer>>() {}));
            return borrowed ? new BorrowedField(field) : field;
        }
        if (value.has("tuple") || value.has("parameter_pair")) {
            var fields = new ArrayList<IUsedFields>();
            value.required(value.has("tuple") ? "tuple" : "parameter_pair").forEach(offset -> fields.add(members.get(offset.intValue())));
            return new TupleOfFields(fields);
        }
        var fields = new HashSet<IUsedFields>();
        value.required("computed").forEach(offset -> fields.add(members.get(offset.intValue())));
        return new FieldSet(fields);
    }

    public enum MapInputAction { Keep, Project, Inspect }
    public record MapInputLayout(MapInputAction kind, @Nullable List<List<Integer>> paths) {}

    /** Analyze each distinct reader closure once per unused-field iteration. */
    public static Map<Long, MapInputLayout> mapFieldDependencies(DBSPCompiler compiler, DBSPCircuit circuit,
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

    private static Map<Long, MapInputLayout> mapFieldDependencies(DBSPCompiler compiler, DBSPCircuit circuit,
            Map<Long, DBSPUnaryOperator> readers) {
        if (readers.isEmpty())
            return Map.of();
        try {
            var input = OptimizerExpressions.mapFunctions(compiler, circuit, readers.values());
            var reply = transformWithContext("MapFieldDependencies", input, 0, "", EMPTY_CONTEXT);
            Map<Long, MapInputLayout> paths = MAPPER.readValue(reply, new TypeReference<>() {});
            if (!paths.keySet().equals(readers.keySet()))
                throw new IllegalStateException("Incomplete map closure dependencies");
            return paths;
        } catch (IOException error) {
            throw new UncheckedIOException("Invalid map field dependencies from the Rust optimizer", error);
        }
    }

    private enum SourceColumnAction { Keep, Trim, Inspect }
    private record SourceColumns(SourceColumnAction kind, @Nullable List<Boolean> used) {}
    private record SourceReaderColumns(long reader, SourceColumnAction kind, @Nullable List<Boolean> used) {}

    private void findInputFields(DBSPCircuit circuit, long next, FindUnusedInputFields analysis) throws IOException {
        var readers = this.invoke("Topology/FindUnusedInputFields", OptimizerTopology.encode(circuit), next, EMPTY_CONTEXT);
        if (readers.isEmpty())
            return;
        var selected = MAPPER.convertValue(readers, new TypeReference<Set<Long>>() {});
        var writer = OptimizerTopology.selectedExpressions(this.compiler, circuit, selected, OptimizerTopology.Details.NONE);
        String pass = "FindUnusedInputFields/Columns";
        var uses = this.invoke(pass, writer.getJsonString().getBytes(StandardCharsets.UTF_8), next, EMPTY_CONTEXT);
        var operators = operatorsById(circuit);
        for (var use : MAPPER.treeToValue(uses, SourceReaderColumns[].class)) {
            if (!selected.remove(use.reader))
                throw new IllegalStateException("Unexpected input projection " + use.reader);
            var reader = operators.get(use.reader).to(DBSPUnaryOperator.class);
            var columns = new SourceColumns(use.kind(), use.used());
            if (columns.kind() == SourceColumnAction.Inspect) {
                var function = reader.getClosureFunction();
                var paths = requiredParameterFieldDependencies(this.compiler, function).getFirst();
                var checked = invokeLowering(this.compiler, "FindUnusedInputFields/CheckedColumns",
                        MAPPER.writeValueAsBytes(paths), function.parameters[0].getType());
                columns = MAPPER.treeToValue(checked.value, SourceColumns.class);
            }
            if (columns.kind() == SourceColumnAction.Trim)
                analysis.recordColumns(reader, Objects.requireNonNull(columns.used()));
            else
                Utilities.enforce(columns.kind() == SourceColumnAction.Keep);
            if (this.compiler.stateObserver != null)
                this.compiler.stateObserver.nativeDecision(pass, true);
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

    /** Rebuild just the source printed by a failed cast, including its live SQL literals. */
    private static Lineage.ValueSource lineageDiagnosticSource(ToJsonInnerVisitor writer, JsonNode encoded) {
        var sources = new ArrayList<Lineage.ValueSource>(encoded.size());
        for (var entry : encoded) {
            Lineage.ValueSource source = switch (entry.required("kind").textValue()) {
                case "unknown" -> Lineage.Unknown.INSTANCE;
                case "constant" -> {
                    var nodes = Objects.requireNonNull(writer.liveNodes);
                    var expr = Utilities.getExists(nodes, entry.required("literal").longValue()).to(DBSPExpression.class);
                    yield new Lineage.Constant(expr instanceof DBSPBaseTupleExpression tuple && tuple.fields == null
                            ? expr.getType().none() : expr);
                }
                case "columns" -> new Lineage.Atom<>(new Lineage.ColumnSet(MAPPER.convertValue(
                        entry.required("columns"), new TypeReference<java.util.TreeSet<Lineage.TableColumn>>() {})));
                case "ref" -> new Lineage.Ref(sources.get(entry.required("value").intValue()));
                case "tuple" -> {
                    var fields = new ArrayList<Lineage.ValueSource>();
                    for (var field : entry.required("fields"))
                        fields.add(sources.get(field.intValue()));
                    yield new Lineage.Tuple(fields);
                }
                default -> throw new IllegalStateException("Unknown lineage value " + entry);
            };
            sources.add(source);
        }
        return sources.get(sources.size() - 1);
    }

    private static void reportLineageFailure(ToJsonInnerVisitor writer, JsonNode diagnostic) {
        if (diagnostic.has("shape_assertion"))
            Utilities.enforce(false);
        if (diagnostic.has("join_parameter_count")) {
            int size = diagnostic.required("join_parameter_count").intValue();
            throw new ArrayIndexOutOfBoundsException("Index " + size + " out of bounds for length " + size);
        }
        if (diagnostic.has("field_index")) {
            var bounds = diagnostic.required("field_index");
            Objects.checkIndex(bounds.required("index").intValue(), bounds.required("size").intValue());
        }
        if (diagnostic.has("source_cast")) {
            var source = lineageDiagnosticSource(writer, diagnostic.required("values"));
            switch (diagnostic.required("source_cast").textValue()) {
                case "tuple" -> source.to(Lineage.Tuple.class);
                case "ref" -> source.to(Lineage.Ref.class);
                default -> throw new IllegalStateException("Unknown lineage cast " + diagnostic);
            }
        }
        if (diagnostic.has("unsupported")) {
            var nodes = Objects.requireNonNull(writer.liveNodes);
            Lineage.InnerLineage.rejectUnsupported(
                    Utilities.getExists(nodes, diagnostic.required("unsupported").longValue()).to(DBSPExpression.class));
        }
        if (diagnostic.has("comparison"))
            reportComparisonFailure(diagnostic.required("comparison"));
        checkBindingErrors(writer, diagnostic);
        throw new IllegalStateException("Unknown lineage diagnostic " + diagnostic);
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
            throw new IllegalStateException("Missing lineage result for the live optimizer snapshot");
        var findings = result.isArray() ? result : result.required("findings");
        for (var finding : MAPPER.treeToValue(findings, LineageFinding[].class)) {
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
        if (result.has("diagnostic")) {
            var diagnostic = result.required("diagnostic");
            var port = diagnostic.get("missing_input");
            if (port != null) {
                var producer = Utilities.getExists(operatorsById(circuit), port.required(0).longValue());
                throw Utilities.missingKey(producer.getOutput(port.required(1).intValue()));
            }
            var width = diagnostic.get("output_width");
            if (width != null) {
                var producer = Utilities.getExists(operatorsById(circuit), width.required("operator").longValue());
                Lineage.checkOutputWidth(producer.getOutput(0), width.required("fields").intValue());
            }
            reportLineageFailure(writer.innerVisitor, diagnostic);
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
                byte[] snapshot = writer.getJsonString().getBytes(StandardCharsets.UTF_8);
                if (this.replaceSharedIndexes != null) {
                    result = this.replaceIndexGroups(writer, snapshot, initial, state);
                } else if (this.mapFusion != null) {
                    result = this.fuseMapClusters(writer, snapshot, initial);
                } else if (this.mergeFilters != null) {
                    result = this.mergeTemporalFilters(circuit, writer, snapshot, initial);
                } else {
                    if (state == EMPTY_CONTEXT)
                        state = this.passState(circuit, writer);
                    result = this.invoke("Live/" + this.nativeName, snapshot, initial, state);
                }
            }
            if (this.mergeFilters != null && !this.usedGeneralTemporalAnalysis && this.compiler.stateObserver != null)
                this.compiler.stateObserver.nativeDecision("MergeFilters", true);
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
