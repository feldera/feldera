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

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.Waterline;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.backend.MerkleOuter;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EliminateDump;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpandCasts;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpandWriteLog;
import org.dbsp.sqlCompiler.compiler.visitors.inner.LazyStatics;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SimplifyWaterline;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MonotoneAnalyzer;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

import java.util.ArrayList;
import java.util.List;

/** Very high level circuit-level optimizations.
 * Does not really look at the functions inside the circuit. */
public record CircuitOptimizer(DBSPCompiler compiler) implements ICompilerComponent {
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

    CircuitTransform getOptimizer() {
        List<CircuitTransform> passes = new ArrayList<>();
        DBSPCompiler compiler = this.compiler();
        CompilerOptions options = this.compiler().options;
        // Example dumping circuit to a png file
        // passes.add(ToDot.dumper(compiler, "x.png", 2));
        // First part of optimizations may still synthesize some circuit components
        passes.add(new ImplementNow(compiler));
        passes.add(new DeterministicDefault(compiler));
        passes.add(new StopOnError(compiler));
        passes.add(new RecursiveComponents(compiler));
        passes.add(new DeadCode(compiler, options.languageOptions.generateInputForEveryTable, true));
        if (options.languageOptions.outputsAreSets)
            passes.add(new EnsureDistinctOutputs(compiler));
        passes.add(new UnusedFields(compiler));
        passes.add(new CSE(compiler));
        passes.add(new MinMaxOptimize(compiler, compiler.weightVar));
        passes.add(new ExpandAggregateZero(compiler));
        passes.add(new MergeSums(compiler));
        passes.add(new OptimizeWithGraph(compiler, g -> new RemoveNoops(compiler, g)));
        passes.add(new PropagateEmptySources(compiler));
        passes.add(new DeadCode(compiler, true, false));
        passes.add(new OptimizeDistinctVisitor(compiler));
        // This is useful even without incrementalization if we have recursion
        passes.add(new OptimizeIncrementalVisitor(compiler));
        passes.add(new DeadCode(compiler, true, false));
        if (options.languageOptions.incrementalize) {
            passes.add(new IncrementalizeVisitor(compiler));
        }
        passes.add(new OptimizeIncrementalVisitor(compiler));
        passes.add(new RemoveIAfterD(compiler));
        passes.add(new DeadCode(compiler, true, false));
        passes.add(new Simplify(compiler).circuitRewriter(true));
        passes.add(new OptimizeWithGraph(compiler, g -> new OptimizeProjectionVisitor(compiler, g)));
        passes.add(new OptimizeWithGraph(compiler, g -> new OptimizeMaps(compiler, true, g)));
        passes.add(new OptimizeWithGraph(compiler, g -> new FilterJoinVisitor(compiler, g)));
        passes.add(new MonotoneAnalyzer(compiler));
        passes.add(new RemoveTable(compiler, DBSPCompiler.ERROR_TABLE_NAME));
        // The circuit is complete here, start optimizing for real.
        // Doing this after the monotone analysis only

        passes.add(new LinearPostprocessRetainKeys(compiler));
        if (!options.ioOptions.emitHandles)
            passes.add(new IndexedInputs(compiler));
        passes.add(new OptimizeWithGraph(compiler, g -> new FilterJoinVisitor(compiler, g)));
        passes.add(new DeadCode(compiler, true, false));
        passes.add(new Simplify(compiler).circuitRewriter(true));
        // The predicate below controls which nodes have their output dumped at runtime
        passes.add(new InstrumentDump(compiler, t -> false));
        if (options.languageOptions.incrementalize)
            passes.add(new NoIntegralVisitor(compiler));
        passes.add(new ExpandHop(compiler));
        passes.add(new RemoveDeindexOperators(compiler));
        passes.add(new OptimizeWithGraph(compiler, g -> new RemoveNoops(compiler, g)));
        passes.add(new RemoveViewOperators(compiler, false));
        passes.add(new RemoveIdentityOperators(compiler));
        passes.add(new Repeat(compiler, new ExpandCasts(compiler).circuitRewriter(true)));
        passes.add(new OptimizeWithGraph(compiler, g -> new ChainVisitor(compiler, g)));
        passes.add(new OptimizeWithGraph(compiler, g -> new OptimizeMaps(compiler, false, g)));
        passes.add(new SimplifyWaterline(compiler)
                .circuitRewriter(node -> node.hasAnnotation(a -> a.is(Waterline.class))));
        passes.add(new EliminateDump(compiler).circuitRewriter(false));
        passes.add(new ExpandWriteLog(compiler).circuitRewriter(false));
        passes.add(new Simplify(compiler).circuitRewriter(true));
        passes.add(new CSE(compiler));
        passes.add(new RecursiveComponents.ValidateRecursiveOperators(compiler));
        // Lowering implements aggregates and inlines some calls.
        passes.add(new LowerCircuitVisitor(compiler));
        // Lowering may surface additional casts that need to be expanded
        passes.add(new Repeat(compiler, new ExpandCasts(compiler).circuitRewriter(true)));
        // Beta reduction after implementing aggregates.
        passes.add(new BetaReduction(compiler).getCircuitVisitor(false));
        passes.add(new LazyStatics(compiler).circuitRewriter(false));
        passes.add(new ExpandJoins(compiler));
        passes.add(new CSE(compiler));
        passes.add(new RemoveViewOperators(compiler, true));
        // passes.add(new TestSerialize(compiler));
        // The canonical form is needed if we want the Merkle hashes to be "stable".
        passes.add(new CanonicalForm(compiler).getCircuitVisitor(false));
        passes.add(new CompactNames(compiler));
        passes.add(new MerkleOuter(compiler));
        return new Passes("CircuitOptimizer", compiler, passes);
    }

    public DBSPCircuit optimize(DBSPCircuit input) {
        CircuitTransform optimizer = this.getOptimizer();
        return optimizer.apply(input);
    }
}
