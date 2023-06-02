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

package org.dbsp.sqlCompiler.compiler.optimizer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.optimize.*;
import org.dbsp.sqlCompiler.compiler.backend.visitors.*;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;

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

    CircuitVisitor getOptimizer() {
        if (this.compiler.options.optimizerOptions.optimizationLevel < 2) {
            if (this.compiler.options.optimizerOptions.incrementalize) {
                return new IncrementalizeVisitor(this.getCompiler());
            } else {
                // Nothing.
                return new PassesVisitor(this.getCompiler());
            }
        }

        List<CircuitVisitor> passes = new ArrayList<>();
        passes.add(new OptimizeDistinctVisitor(this.getCompiler()));
        if (this.getCompiler().options.optimizerOptions.incrementalize) {
            passes.add(new IncrementalizeVisitor(this.getCompiler()));
            passes.add(new OptimizeIncrementalVisitor(this.getCompiler()));
        }
        DeadCodeVisitor dead = new DeadCodeVisitor(this.getCompiler());
        passes.add(dead);
        passes.add(new RemoveOperatorsVisitor(this.getCompiler(), dead.toKeep));
        if (this.getCompiler().options.optimizerOptions.incrementalize)
            passes.add(new NoIntegralVisitor(this.getCompiler()));
        passes.add(new Simplify(this.getCompiler()).circuitRewriter());
        return new PassesVisitor(this.getCompiler(), passes);
    }

    public DBSPCircuit optimize(DBSPCircuit input) {
        return this.getOptimizer().apply(input);
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
