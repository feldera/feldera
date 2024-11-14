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
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EliminateDump;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpandCasts;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpandWriteLog;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SimplifyWaterline;
import org.dbsp.sqlCompiler.circuit.annotation.Waterline;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MonotoneAnalyzer;

import java.util.ArrayList;
import java.util.List;

/** Very high level circuit-level optimizations.
 * Does not really look at the functions inside the circuit. */
public record CircuitOptimizer(DBSPCompiler compiler) implements ICompilerComponent {
    CircuitTransform getOptimizer() {
        List<CircuitTransform> passes = new ArrayList<>();
        IErrorReporter reporter = this.compiler();
        CompilerOptions options = this.compiler().options;

        passes.add(new ImplementNow(reporter, compiler));
        passes.add(new RecursiveComponents(reporter));
        if (options.languageOptions.outputsAreSets)
            passes.add(new EnsureDistinctOutputs(reporter));
        passes.add(new MinMaxOptimize(reporter, compiler.weightVar));
        passes.add(new ExpandAggregateZero(reporter));
        if (options.languageOptions.optimizationLevel < 2) {
            if (options.languageOptions.incrementalize)
                passes.add(new IncrementalizeVisitor(this.compiler()));
            if (!options.ioOptions.emitHandles)
                passes.add(new IndexedInputs(reporter));
        } else {
            passes.add(new MergeSums(reporter));
            passes.add(new OptimizeWithGraph(reporter, g -> new RemoveNoops(reporter, g)));
            passes.add(new PropagateEmptySources(reporter));
            passes.add(new DeadCode(reporter, options.languageOptions.generateInputForEveryTable, true));
            passes.add(new OptimizeDistinctVisitor(reporter));
            // This is useful even without incrementalization if we have recursion
            passes.add(new OptimizeIncrementalVisitor(reporter));
            passes.add(new DeadCode(reporter, true, false));
            if (options.languageOptions.incrementalize) {
                passes.add(new IncrementalizeVisitor(reporter));
            }
            passes.add(new OptimizeIncrementalVisitor(reporter));
            passes.add(new RemoveIAfterD(reporter));
            passes.add(new DeadCode(reporter, true, false));
            passes.add(new Simplify(reporter).circuitRewriter());
            passes.add(new OptimizeWithGraph(reporter, g -> new OptimizeProjectionVisitor(reporter, g)));
            passes.add(new OptimizeWithGraph(reporter, g -> new OptimizeMaps(reporter, true, g)));
            passes.add(new OptimizeWithGraph(reporter, g -> new FilterJoinVisitor(reporter, g)));
            if (options.languageOptions.incrementalize) {
                // Monotonicity analysis only makes sense for incremental programs
                passes.add(new MonotoneAnalyzer(reporter));
            }
            passes.add(new NarrowJoins(reporter));
            // Doing this after the monotone analysis only
            if (!options.ioOptions.emitHandles)
                passes.add(new IndexedInputs(reporter));
            passes.add(new OptimizeWithGraph(reporter, g -> new FilterJoinVisitor(reporter, g)));
            passes.add(new DeadCode(reporter, true, false));
            passes.add(new Simplify(reporter).circuitRewriter());
            // The predicate below controls which nodes have their output dumped at runtime
            passes.add(new InstrumentDump(reporter, t -> false));
            if (options.languageOptions.incrementalize)
                passes.add(new NoIntegralVisitor(reporter));
        }
        passes.add(new ExpandHop(reporter));
        passes.add(new RemoveDeindexOperators(reporter));
        passes.add(new OptimizeWithGraph(reporter, g -> new RemoveNoops(reporter, g)));
        passes.add(new RemoveViewOperators(reporter));
        passes.add(new Repeat(reporter, new ExpandCasts(reporter).circuitRewriter()));
        if (options.languageOptions.optimizationLevel >= 2) {
            passes.add(new OptimizeWithGraph(reporter, g -> new FilterMapVisitor(reporter, g)));
            // optimize the maps introduced by the deindex removal
            passes.add(new OptimizeWithGraph(reporter, g -> new OptimizeMaps(reporter, false, g)));
            passes.add(new SimplifyWaterline(reporter)
                    .circuitRewriter(node -> node.hasAnnotation(a -> a.is(Waterline.class))));
        }
        passes.add(new EliminateDump(reporter).circuitRewriter());
        passes.add(new ExpandWriteLog(reporter).circuitRewriter());
        if (options.languageOptions.optimizationLevel >= 2) {
            passes.add(new Simplify(reporter).circuitRewriter());
            passes.add(new CSE(reporter));
        }
        passes.add(new RecursiveComponents.ValidateRecursiveOperators(reporter));
        // Lowering implements aggregates and inlines some calls.
        passes.add(new LowerCircuitVisitor(reporter));
        // Lowering may surface additional casts that need to be expanded
        passes.add(new Repeat(reporter, new ExpandCasts(reporter).circuitRewriter()));
        // Beta reduction after implementing aggregates.
        passes.add(new BetaReduction(compiler).getCircuitVisitor());
        passes.add(new CompactNames(compiler));
        return new Passes("optimize", reporter, passes);
    }

    public DBSPCircuit optimize(DBSPCircuit input) {
        CircuitTransform optimizer = this.getOptimizer();
        return optimizer.apply(input);
    }
}
