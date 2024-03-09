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
import org.dbsp.sqlCompiler.compiler.backend.ToDotVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EliminateFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpandWriteLog;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ExpandOperators;

import java.util.ArrayList;
import java.util.List;

/**
 * Very high level circuit-level optimizations.
 * Does not really look at the functions inside the circuit.
 */
public class CircuitOptimizer implements ICompilerComponent {
    public final DBSPCompiler compiler;

    public CircuitOptimizer(DBSPCompiler compiler) {
        this.compiler = compiler;
    }

    static class MonotoneAnalyzer implements CircuitTransform {
        final IErrorReporter reporter;

        public MonotoneAnalyzer(IErrorReporter reporter) {
            this.reporter = reporter;
        }

        @SuppressWarnings("ConstantValue")
        @Override
        public DBSPCircuit apply(DBSPCircuit circuit) {
            final boolean debug = false;

            if (debug)
                ToDotVisitor.toDot(this.reporter, "original.jpg", true, "jpg", circuit);
            ExpandOperators expander = new ExpandOperators(this.reporter, 1);
            Repeat repeat = new Repeat(this.reporter, expander);
            DBSPCircuit expanded = repeat.apply(circuit);
            if (debug)
                ToDotVisitor.toDot(reporter, "expanded.jpg", false, "jpg", expanded);

            MonotoneOperators monotone = new MonotoneOperators(this.reporter);
            expanded = monotone.apply(expanded); // this does not really mutate the circuit

            InsertLimiters limiters = new InsertLimiters(
                    this.reporter, expanded, monotone.operatorMonotoneValue, expander.expansion);
            // Notice that we apply the limiters to the original circuit, not to the expanded circuit!
            DBSPCircuit result = limiters.apply(circuit);
            if (debug)
                ToDotVisitor.toDot(reporter, "limited.jpg", true, "jpg", result);
            return result;
        }

        @Override
        public String toString() {
            return "MonotoneAnalyzer";
        }
    }

    CircuitTransform getOptimizer() {
        List<CircuitTransform> passes = new ArrayList<>();
        IErrorReporter reporter = this.getCompiler();
        CompilerOptions options = this.getCompiler().options;

        if (!options.ioOptions.emitHandles)
            passes.add(new IndexedInputs(reporter));
        if (options.languageOptions.outputsAreSets)
            passes.add(new EnsureDistinctOutputs(reporter));
        if (options.languageOptions.optimizationLevel < 2) {
            if (options.languageOptions.incrementalize) {
                passes.add(new IncrementalizeVisitor(this.getCompiler()));
            }
        } else {
            // only on optimization level 2
            passes.add(new MergeSums(reporter));
            passes.add(new PropagateEmptySources(reporter));
            passes.add(new DeadCode(reporter, options.languageOptions.generateInputForEveryTable, true));
            passes.add(new OptimizeProjections(reporter));
            passes.add(new OptimizeDistinctVisitor(reporter));
            if (options.languageOptions.incrementalize) {
                passes.add(new IncrementalizeVisitor(reporter));
                passes.add(new OptimizeIncrementalVisitor(reporter));
            }
            passes.add(new DeadCode(reporter, true, false));
            if (options.languageOptions.incrementalize)
                passes.add(new NoIntegralVisitor(reporter));
            passes.add(new Simplify(reporter).circuitRewriter());
            // The predicate below controls which nodes have their output dumped at runtime
            passes.add(new InstrumentDump(reporter, t -> false));
        }
        passes.add(new MonotoneAnalyzer(reporter));
        // debugging aid
        passes.add(new RemoveDeindexOperator(reporter));
        passes.add(new EliminateFunctions(reporter).circuitRewriter());
        passes.add(new ExpandWriteLog(reporter).circuitRewriter());
        passes.add(new Simplify(reporter).circuitRewriter());
        return new Passes(reporter, passes);
    }

    public DBSPCircuit optimize(DBSPCircuit input) {
        CircuitTransform optimizer = this.getOptimizer();
        return optimizer.apply(input);
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
