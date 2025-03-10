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

/** Very high level circuit-level optimizations.
 * Does not really look at the functions inside the circuit. */
public class CircuitOptimizer extends Passes {
    public CircuitOptimizer(DBSPCompiler compiler) {
        super("Optimizer", compiler);
        this.createOptimizer();
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
        CompilerOptions options = this.compiler().options;
        // Example dumping circuit to a png file
        // this.add(ToDot.dumper(compiler, "x.png", 2));
        // First part of optimizations may still synthesize some circuit components
        this.add(new ImplementNow(compiler));
        this.add(new DeterministicDefault(compiler));
        this.add(new StopOnError(compiler));
        this.add(new RecursiveComponents(compiler));
        this.add(new DeadCode(compiler, options.languageOptions.generateInputForEveryTable, true));
        if (options.languageOptions.outputsAreSets)
            this.add(new EnsureDistinctOutputs(compiler));
        this.add(new UnusedFields(compiler));
        this.add(new CSE(compiler));
        this.add(new MinMaxOptimize(compiler, compiler.weightVar));
        this.add(new ExpandAggregateZero(compiler));
        this.add(new MergeSums(compiler));
        this.add(new OptimizeWithGraph(compiler, g -> new RemoveNoops(compiler, g)));
        this.add(new PropagateEmptySources(compiler));
        this.add(new DeadCode(compiler, true, false));
        this.add(new OptimizeDistinctVisitor(compiler));
        // This is useful even without incrementalization if we have recursion
        this.add(new OptimizeIncrementalVisitor(compiler));
        this.add(new DeadCode(compiler, true, false));
        if (options.languageOptions.incrementalize) {
            this.add(new IncrementalizeVisitor(compiler));
        }
        this.add(new OptimizeIncrementalVisitor(compiler));
        this.add(new RemoveIAfterD(compiler));
        this.add(new DeadCode(compiler, true, false));
        this.add(new Simplify(compiler).circuitRewriter(true));
        this.add(new OptimizeWithGraph(compiler, g -> new OptimizeProjectionVisitor(compiler, g)));
        this.add(new OptimizeWithGraph(compiler, g -> new OptimizeMaps(compiler, true, g)));
        this.add(new OptimizeWithGraph(compiler, g -> new FilterJoinVisitor(compiler, g)));
        this.add(new MonotoneAnalyzer(compiler));
        this.add(new RemoveTable(compiler, DBSPCompiler.ERROR_TABLE_NAME));
        // The circuit is complete here, start optimizing for real.
        // Doing this after the monotone analysis only

        this.add(new LinearPostprocessRetainKeys(compiler));
        if (!options.ioOptions.emitHandles)
            this.add(new IndexedInputs(compiler));
        this.add(new OptimizeWithGraph(compiler, g -> new FilterJoinVisitor(compiler, g)));
        this.add(new DeadCode(compiler, true, false));
        this.add(new Simplify(compiler).circuitRewriter(true));
        // The predicate below controls which nodes have their output dumped at runtime
        this.add(new InstrumentDump(compiler, t -> false));
        if (options.languageOptions.incrementalize)
            this.add(new NoIntegralVisitor(compiler));
        this.add(new ExpandHop(compiler));
        this.add(new RemoveDeindexOperators(compiler));
        this.add(new OptimizeWithGraph(compiler, g -> new RemoveNoops(compiler, g)));
        this.add(new RemoveViewOperators(compiler, false));
        this.add(new RemoveIdentityOperators(compiler));
        this.add(new Repeat(compiler, new ExpandCasts(compiler).circuitRewriter(true)));
        this.add(new OptimizeWithGraph(compiler, g -> new ChainVisitor(compiler, g)));
        this.add(new OptimizeWithGraph(compiler, g -> new OptimizeMaps(compiler, false, g)));
        this.add(new SimplifyWaterline(compiler)
                .circuitRewriter(node -> node.hasAnnotation(a -> a.is(Waterline.class))));
        this.add(new EliminateDump(compiler).circuitRewriter(false));
        this.add(new ExpandWriteLog(compiler).circuitRewriter(false));
        this.add(new Simplify(compiler).circuitRewriter(true));
        this.add(new CSE(compiler));
        this.add(new RecursiveComponents.ValidateRecursiveOperators(compiler));
        // Lowering implements aggregates and inlines some calls.
        this.add(new LowerCircuitVisitor(compiler));
        // Lowering may surface additional casts that need to be expanded
        this.add(new Repeat(compiler, new ExpandCasts(compiler).circuitRewriter(true)));
        // Beta reduction after implementing aggregates.
        this.add(new BetaReduction(compiler).getCircuitVisitor(false));
        this.add(new CSE(compiler));
        this.add(new StaticDeclarations(compiler, new LazyStatics(compiler)));
        this.add(new ExpandJoins(compiler));
        this.add(new ComparatorDeclarations(compiler, new FindComparators(compiler)));
        this.add(new RemoveViewOperators(compiler, true));
        this.add(new CircuitRewriter(compiler, new InnerCSE(compiler), false, InnerCSE::process));
        this.add(new LazyStatics(compiler).circuitRewriter(false));
        // this.add(new TestSerialize(compiler));
        // The canonical form is needed if we want the Merkle hashes to be "stable".
        this.add(new CanonicalForm(compiler).getCircuitVisitor(false));
        this.add(new CompactNames(compiler));
        this.add(new MerkleOuter(compiler));
    }

    public DBSPCircuit optimize(DBSPCircuit input) {
        return this.apply(input);
    }
}
