/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MergeGC;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.InsertLimiters;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CreateRuntimeErrorWrappers;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ImplementStatics;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SimplifyConditionals;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SimplifyWaterline;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandCasts;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandMetadataCasts;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandSafeCasts;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandUnsafeCasts;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.DuplicateSharedIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.FindMapChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.FindSharedIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.CollapseSharedChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.ReplaceSharedIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.ShareIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.ShareOutputIntegrators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.LeftJoinChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.MultisetAnalysis;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.CheckPersistentIds;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.HashPrefixPass;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.MerkleHashes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.SetOperatorHashes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MonotoneAnalyzer;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.ReorderTemporalFilters;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.CheckRetain;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.OrderErrorConsumers;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.SeparateIntegrators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.recursive.BuildNestedOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.recursive.FixupViewReferences;
import org.dbsp.sqlCompiler.compiler.visitors.outer.recursive.RecursiveComponents;
import org.dbsp.sqlCompiler.compiler.visitors.outer.recursive.SubstituteLeftJoins;
import org.dbsp.sqlCompiler.compiler.visitors.outer.recursive.ValidateRecursiveOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.ImplementNow;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.CircuitContainsNow;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.BreakFilters;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.DesugarNowEquality;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.MergeFilters;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.ShareWindowIntegrals;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.FindWindowGroups;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.ImplementWindowGroups;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.DeferCarriedColumns;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.TrimFilters;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.TrimWindows;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.Utilities;

import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/** All optimizations applied to circuits. */
public class CircuitOptimizer extends Passes {
    /** Names of the passes that {@link ProgramMetadata#DISABLE_OPTIMIZATIONS} lists. */
    final Set<String> disabled;
    /** Names of all the passes, sorted. */
    final Set<String> passNames;
    private final OptimizerSnapshots snapshots;

    public CircuitOptimizer(DBSPCompiler compiler) {
        super("Optimizer", compiler);
        this.disabled = new HashSet<>(compiler.metadata.disabledOptimizations());
        this.passNames = new TreeSet<>();
        this.snapshots = new OptimizerSnapshots(compiler);
        this.createOptimizer();
        this.checkDisabled();
    }

    /** Add {@code pass} unless {@link ProgramMetadata#DISABLE_OPTIMIZATIONS} lists its name. */
    @Override
    public void add(CircuitTransform pass) {
        String name = pass.getName();
        this.passNames.add(name);
        if (this.disabled.contains(name))
            return;
        super.add(this.selectImplementation(pass));
    }

    private CircuitTransform selectImplementation(CircuitTransform pass) {
        if (!this.compiler.options.ioOptions.rustOptimizerPasses)
            return pass;
        if (pass instanceof Conditional conditional) {
            if (conditional.transform instanceof Lineage || conditional.transform instanceof Gen2Passes || conditional.transform instanceof InsertWeightValidation
                    || conditional.transform instanceof RemoveStarJoins)
                return conditional.withTransform(new RustCircuitPass(this.compiler, conditional.transform, this.snapshots));
            return pass;
        }
        if (pass instanceof LeftJoinChains || pass instanceof FuseExpensiveMaps || pass instanceof MergeGC || pass instanceof FindUnboundedState || pass instanceof ShareIndexes || pass instanceof ExpandIndexedInputs || pass instanceof ExpandAggregates || pass instanceof MerkleHashes
                || pass instanceof RemoveUselessLeftJoins || pass instanceof ShareWindowIntegrals) {
            ((Passes) pass).passes.replaceAll(this::selectImplementation);
            return pass;
        }
        boolean supported;
        if (pass instanceof OptimizeWithGraph graph) {
            supported = switch (pass.getName()) {
                case "PushDifferentialsUp", "BalancedJoins", "ChainVisitor", "StrayGC" -> true;
                default -> false;
            };
            if (!supported && graph.transform instanceof Passes iteration)
                iteration.passes.replaceAll(this::selectImplementation);
        } else {
            supported = pass instanceof LowerCircuitVisitor
                    || pass instanceof CloneOperatorsWithFanout
                    || pass instanceof Graph || pass instanceof CreateStarJoins.FindJoinTrees
                    || pass instanceof FuseExpensiveMaps.Fuse || pass instanceof CreateStarJoins.ReplaceStarJoins
                    || pass instanceof LowerAsof.LowerAsofInner || pass instanceof LowerAsof.MoveGC || pass instanceof LowerAsof.OrderGcDependencies
                    || pass instanceof MergeGC.MergeNoops || pass instanceof MergeGC.MergeRetain
                    || pass instanceof HashPrefixPass || pass instanceof SetOperatorHashes || pass instanceof CheckPersistentIds
                    || pass instanceof MergeGC.FindEquivalentNoops
                    || pass instanceof MergeGC.FindMultipleRetains || pass instanceof CollapseSharedChains || pass instanceof ReplaceSharedIndexes || pass instanceof RemoveUselessLeftJoinsVisitor
                    || pass instanceof FindWindowGroups || pass instanceof ImplementWindowGroups
                    || pass instanceof FindMapChains || pass instanceof FindSharedIndexes || pass instanceof DuplicateSharedIndexes
                    || pass instanceof org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ReplaceCommonProjections
                    || pass instanceof org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindCommonProjections
                    || pass instanceof DeferCarriedColumns || pass instanceof TrimFilters || pass instanceof TrimWindows || pass instanceof UnusedFields.FindUnusedInputFields || pass instanceof UnusedFields.TrimInputs
                    || pass instanceof ExpandIndexedInputs.IndexedInputs || pass instanceof ExpandAggregates.ExpandStreamAggregates
                    || pass instanceof OptimizeProjectionVisitor || pass instanceof PullFilterVisitor
                    || pass instanceof ShareOutputIntegrators || pass instanceof FilterJoinVisitor || pass instanceof AppendOnly
                    || pass instanceof DecomposeExpensiveFilters || pass instanceof CircuitContainsNow || pass instanceof BreakFilters
                    || pass instanceof ReorderTemporalFilters.Scan || pass instanceof ReorderTemporalFilters.Reorder
                    || pass instanceof MergeFilters || pass instanceof DesugarNowEquality
                    || pass instanceof FuseIntoArrayAgg || pass instanceof ComparatorDeclarations
                    || pass instanceof CompactNames || pass instanceof TagRegions
                    || pass instanceof RemoveDeindexOperators || pass instanceof RemoveNoops || pass instanceof DeadCode
                    || pass instanceof OptimizeIncrementalVisitor || pass instanceof IncrementalizeVisitor
                    || pass instanceof NoIntegralVisitor || pass instanceof ValidateRecursiveOperators
                    || pass instanceof CSE || pass instanceof MergeSums || pass instanceof RemoveIAfterD
                    || pass instanceof ImplementChains || pass instanceof ImplementJoins || pass instanceof RemoveTable
                    || pass instanceof ExpandAggregateZero || pass instanceof DeterministicFunctions
                    || pass instanceof ConstantViews || pass instanceof CircuitStatistics || pass instanceof ExpandMetadataCasts
                    || pass instanceof LinearPostprocessRetainKeys || pass instanceof ExpandHop
                    || pass instanceof InsertLimiters || pass instanceof CheckRetain || pass instanceof SeparateIntegrators || pass instanceof OrderErrorConsumers
                    || pass instanceof RemoveIdentityOperators || pass instanceof FixupViewReferences || pass instanceof BuildNestedOperators
                    || pass instanceof SubstituteLeftJoins || pass instanceof EnsureDistinctOutputs || pass instanceof MultisetAnalysis
                    || pass instanceof FindUnboundedState.DetectStreamingOperations || pass instanceof FindUnboundedState.FindGCedStreams
                    || pass instanceof FindUnboundedState.FindBounded || pass instanceof FindUnboundedState.CollectUnbounded
                    || pass instanceof OptimizeDistinctVisitor
                    || (pass instanceof CircuitDispatcher dispatcher && dispatcher.innerVisitor instanceof NoConnectorMetadata)
                    || (pass instanceof CircuitRewriter rewriter && (rewriter.transform instanceof AdjustSqlIndex
                        || rewriter.getName().equals("EnsureExpressionTrees") || rewriter.transform instanceof SimplifyWaterline
                        || rewriter.transform instanceof CanonicalForm || rewriter.transform instanceof SimplifyConditionals
                        || rewriter.transform.getClass() == Simplify.class
                        || rewriter.transform instanceof ExpandSafeCasts || rewriter.transform instanceof ExpandUnsafeCasts
                        || rewriter.transform instanceof CreateRuntimeErrorWrappers || rewriter.transform instanceof UDFInliner))
                    || pass instanceof RemoveViewOperators || pass instanceof CheckHints
                    || pass instanceof PropagateConstants || pass instanceof PropagateEmptySources || pass instanceof RemoveConstantFilters;
        }
        return supported ? new RustCircuitPass(this.compiler, pass, this.snapshots) : pass;
    }

    /** Warn about each name in {@link ProgramMetadata#DISABLE_OPTIMIZATIONS} that is not
     * the name of a pass. */
    void checkDisabled() {
        for (String name : this.compiler.metadata.disabledOptimizations()) {
            if (this.passNames.contains(name))
                continue;
            this.compiler.reportWarning(
                    this.compiler.metadata.getPosition(ProgramMetadata.DISABLE_OPTIMIZATIONS),
                    "Unknown optimization", Utilities.singleQuote(name) +
                            " is not the name of a compiler pass; the passes are: " +
                            String.join(", ", this.passNames));
        }
    }

    /** Build a pass preceded by {@link MultisetAnalysis}. */
    private CircuitTransform withMultisets(Function<MultisetAnalysis, CircuitTransform> create) {
        MultisetAnalysis multisets = new MultisetAnalysis(this.compiler);
        CircuitTransform pass = create.apply(multisets);
        return new Passes(pass.getName(), this.compiler, this.selectImplementation(multisets), this.selectImplementation(pass));
    }

    static class StopOnError extends CircuitVisitor {
        public StopOnError(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public Token startVisit(IDBSPOuterNode node) {
            if (this.compiler().hasErrors())
                throw new CompilationError("Stopping compilation");
            return super.startVisit(node);
        }
    }

    void createOptimizer() {
        var options = this.compiler.options.languageOptions;
        var order = this.compiler.options.ioOptions.rustOptimizerPasses
                ? RustCircuitPass.optimizerOrder(options.outputsAreSets, options.incrementalize)
                : OptimizerStep.referenceOrder(options.outputsAreSets, options.incrementalize);
        var operatorsAnalyzed = new AnalyzedSet<DBSPOperator>();
        for (var step : order)
            this.add(this.createPass(step, operatorsAnalyzed));
    }

    private CircuitTransform createPass(OptimizerStep step, AnalyzedSet<DBSPOperator> operatorsAnalyzed) {
        CompilerOptions options = this.compiler.options;
        return switch (step) {
            case UdfInliner -> new UDFInliner(compiler).getCircuitRewriter(false);
            case DecomposeExpensiveFilters -> new DecomposeExpensiveFilters(compiler);
            case ImplementNow -> new ImplementNow(compiler, this::selectImplementation);
            case Lineage ->
                    new Conditional(compiler, new Lineage(compiler, true), () -> compiler.options.ioOptions.correlatedColumns);
            case DeterministicFunctions -> new DeterministicFunctions(compiler);
            case NoConnectorMetadata -> new NoConnectorMetadata(compiler).getCircuitVisitor(true);
            case StopOnError -> new StopOnError(compiler);
            case RecursiveComponents -> new RecursiveComponents(compiler, this::selectImplementation);
            case PruneUnusedInputs -> new DeadCode(compiler, options.languageOptions.generateInputForEveryTable);
            case EnsureDistinctOutputs ->
                    this.withMultisets(multisets -> new EnsureDistinctOutputs(compiler, multisets));
            case PropagateConstants -> this.withMultisets(multisets -> new PropagateConstants(compiler, multisets));
            case PropagateEmptySources -> new PropagateEmptySources(compiler);
            case MergeSums -> new MergeSums(compiler);
            case CreateStarJoins ->
                    new Conditional(compiler, new CreateStarJoins(compiler, this::selectImplementation), () -> !this.compiler.metadata.noStarJoins());
            case RemoveNoops -> new OptimizeWithGraph(compiler, g -> new RemoveNoops(compiler, g));
            case OptimizeProjectionsOnce -> new OptimizeWithGraph(compiler,
                    g -> new OptimizeProjections(compiler, true, g, operatorsAnalyzed), 1);
            case FuseExpensiveMaps -> new FuseExpensiveMaps(compiler);
            case RemoveIntermediateViews -> new RemoveViewOperators(compiler, false);
            case LeftJoinChains -> new LeftJoinChains(compiler);
            case UnusedFields -> new UnusedFields(compiler, this::selectImplementation);
            case RemoveUselessLeftJoins -> new RemoveUselessLeftJoins(compiler);
            case Cse -> new CSE(compiler);
            case FuseIntoArrayAgg -> new FuseIntoArrayAgg(compiler);
            case ExpandAggregates -> new ExpandAggregates(compiler, compiler.weightVar);
            case ExpandAggregateZero -> new ExpandAggregateZero(compiler);
            case RemoveStarJoins ->
                    new Conditional(compiler, new RemoveStarJoins(compiler), this.compiler.metadata::noStarJoins);
            case DeadCode -> new DeadCode(compiler, true);
            case OptimizeDistinct -> this.withMultisets(multisets -> new OptimizeDistinctVisitor(compiler, multisets));
            case OptimizeIncremental -> new OptimizeIncrementalVisitor(compiler);
            case Incrementalize -> new IncrementalizeVisitor(compiler);
            case RemoveIAfterD -> new RemoveIAfterD(compiler);
            case Simplify -> new Simplify(compiler).circuitRewriter(true);
            case RemoveConstantFilters -> new RemoveConstantFilters(compiler);
            case OptimizeProjection -> new OptimizeWithGraph(compiler, g -> new OptimizeProjectionVisitor(compiler, g));
            case PullFilter -> new OptimizeWithGraph(compiler, g -> new PullFilterVisitor(compiler, g));
            case OptimizeOnlyProjections -> new OptimizeWithGraph(compiler,
                    g -> new OptimizeProjections(compiler, true, g, operatorsAnalyzed));
            case ShareIndexes -> new ShareIndexes(compiler);
            case ShareWindowIntegrals -> new ShareWindowIntegrals(compiler);
            case FilterJoin -> new OptimizeWithGraph(compiler, g -> new FilterJoinVisitor(compiler, g));
            case ShareOutputIntegrators -> new ShareOutputIntegrators(compiler);
            case MonotoneAnalyzer -> new MonotoneAnalyzer(compiler, this::selectImplementation);
            case RemoveErrorTable -> new RemoveTable(compiler, DBSPCompiler.ERROR_TABLE_NAME);
            case CloneOperatorsWithFanout ->
                    new OptimizeWithGraph(compiler, g -> new CloneOperatorsWithFanout(compiler, g));
            case LinearPostprocessRetainKeys -> new LinearPostprocessRetainKeys(compiler);
            case ExpandIndexedInputs -> new ExpandIndexedInputs(compiler);
            case FindUnboundedState -> new FindUnboundedState(compiler);
            case InsertWeightValidation -> new Conditional(compiler, new InsertWeightValidation(compiler),
                    this.compiler.metadata::enforcePositiveInputs);
            case NoIntegral -> new NoIntegralVisitor(compiler);
            case ExpandHop -> new ExpandHop(compiler);
            case RemoveDeindexOperators -> new RemoveDeindexOperators(compiler);
            case RemoveIdentityOperators -> new RemoveIdentityOperators(compiler);
            case ChainVisitor -> new OptimizeWithGraph(compiler, g -> new ChainVisitor(compiler, g));
            case OptimizeProjections -> new OptimizeWithGraph(compiler,
                    g -> new OptimizeProjections(compiler, false, g, operatorsAnalyzed));
            case SimplifyWaterline -> new SimplifyWaterline(compiler).circuitRewriter();
            case ValidateRecursiveOperators -> new ValidateRecursiveOperators(compiler);
            case LowerAsof -> new LowerAsof(compiler, this::selectImplementation);
            case LowerCircuit -> new LowerCircuitVisitor(compiler);
            case AdjustSqlIndex -> new AdjustSqlIndex(compiler).circuitRewriter(true);
            case BalancedJoins -> new OptimizeWithGraph(compiler, g -> new BalancedJoins(compiler, g), 1);
            case ImplementChains -> new ImplementChains(compiler);
            case ExpandCasts -> new ExpandCasts(compiler, this::selectImplementation);
            case SimplifyConditionals -> new CircuitRewriter(compiler, new SimplifyConditionals(compiler), false);
            case ImplementJoins -> new ImplementJoins(compiler);
            case RemoveAllViews -> new RemoveViewOperators(compiler, true);
            case PushDifferentialsUp -> new OptimizeWithGraph(compiler, g -> new PushDifferentialsUp(compiler, g));
            case InnerCse -> new CircuitRewriter(compiler, new InnerCSE(compiler), true, InnerCSE::process);
            case CreateRuntimeErrorWrappers -> new CreateRuntimeErrorWrappers(compiler).getCircuitRewriter(true);
            case StrayGc -> new OptimizeWithGraph(compiler, g -> new StrayGC(compiler, g));
            case CanonicalForm -> new CanonicalForm(compiler).getCircuitRewriter(false);
            case ConstantViews -> new ConstantViews(compiler);
            case StaticDeclarations -> new Conditional(compiler,
                    new StaticDeclarations(compiler, new ImplementStatics(compiler, !options.ioOptions.multiCrates())),
                    () -> !options.ioOptions.gen2);
            case CheckHints -> new CheckHints(compiler);
            case ComparatorDeclarations -> new ComparatorDeclarations(compiler, new DeclareComparators(compiler));
            case Gen2Passes -> new Conditional(compiler, new Gen2Passes(compiler), () -> options.ioOptions.gen2);
            case CompactNames -> new CompactNames(compiler);
            case MerkleHashes -> new MerkleHashes(compiler);
            case TagRegions -> new TagRegions(compiler);
            case CircuitStatistics -> new CircuitStatistics(compiler);
        };
    }

    private final RustCircuitPass.StreamValidator streamValidator = new RustCircuitPass.StreamValidator();

    /** Every pass must leave a circuit with consistent stream kinds; when the testing option
     * asks for it, the circuit must also survive a round trip through JSON. */
    @Override
    protected void afterPass(DBSPCircuit circuit) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            this.streamValidator.validate(circuit);
        else
            new ValidateStreamKinds(this.compiler).apply(circuit);
        if (this.compiler.options.ioOptions.checkSerialization)
            new TestSerialize(this.compiler).apply(circuit);
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit input) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.withLiveSnapshots(() -> super.apply(input));
        return super.apply(input);
    }

    public DBSPCircuit optimize(DBSPCircuit input) {
        DBSPCircuit result = this.apply(input);
        // A pass can stop before incrementalization after reporting a user error.
        // Its partial circuit need not satisfy the final optimizer invariant.
        if (this.compiler.hasErrors())
            return result;
        boolean requested = this.compiler.options.languageOptions.incrementalize;
        Utilities.enforce(result.incremental == requested, () -> "The circuit is " +
                (result.incremental ? "" : "not ") + "incremental, but the compilation " +
                (requested ? "requires" : "does not allow") + " an incremental circuit");
        return result;
    }

    /** Index of the {@code occurrence}-th pass of class {@code anchor}, counting from 1 */
    private int indexOf(Class<? extends CircuitTransform> anchor, int occurrence) {
        Utilities.enforce(occurrence >= 1, () -> "Occurrence " + occurrence + " is not positive");
        int seen = 0;
        for (int i = 0; i < this.passes.size(); i++) {
            if (!anchor.isInstance(this.passes.get(i)))
                continue;
            seen++;
            if (seen == occurrence)
                return i;
        }
        throw new InternalCompilerError("Only " + seen + " passes of class " + anchor.getSimpleName() +
                " in " + this.name + ", occurrence " + occurrence + " requested");
    }

    /** Insert {@code pass} before the first pass of class {@code anchor}, which must be present.
     * Testing hook: production code adds its passes in order and must not use it. */
    public void insertBefore(Class<? extends CircuitTransform> anchor, CircuitTransform pass) {
        this.insertBefore(anchor, 1, pass);
    }

    /** Insert {@code pass} before the {@code occurrence}-th pass of class {@code anchor}, counting
     * from 1; at least that many must be present.  Testing hook, like
     * {@link #insertBefore(Class, CircuitTransform)}. */
    public void insertBefore(Class<? extends CircuitTransform> anchor, int occurrence, CircuitTransform pass) {
        this.passes.add(this.indexOf(anchor, occurrence), pass);
    }

    /** Insert {@code pass} after the first pass of class {@code anchor}, which must be present.
     * Testing hook, like {@link #insertBefore(Class, CircuitTransform)}. */
    public void insertAfter(Class<? extends CircuitTransform> anchor, CircuitTransform pass) {
        this.insertAfter(anchor, 1, pass);
    }

    /** Insert {@code pass} after the {@code occurrence}-th pass of class {@code anchor}, counting
     * from 1; at least that many must be present.  Testing hook, like
     * {@link #insertBefore(Class, CircuitTransform)}. */
    public void insertAfter(Class<? extends CircuitTransform> anchor, int occurrence, CircuitTransform pass) {
        this.passes.add(this.indexOf(anchor, occurrence) + 1, pass);
    }
}
