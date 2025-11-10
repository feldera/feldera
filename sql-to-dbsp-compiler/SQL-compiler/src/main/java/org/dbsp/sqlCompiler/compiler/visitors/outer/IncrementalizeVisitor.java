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
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** This visitor converts a {@link DBSPCircuit} into a new circuit which
 * computes the incremental version of the same query.
 * The generated circuit is not efficient, though, it should be
 * further optimized. */
public class IncrementalizeVisitor extends CircuitCloneVisitor {
    public IncrementalizeVisitor(DBSPCompiler compiler) {
        super(compiler, false);
    }

    public void input(DBSPSimpleOperator operator) {
        if (this.visited.contains(operator))
            return;
        this.addOperator(operator);
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), operator.outputPort());
        this.map(operator, integral);
    }

    @Override
    public void postorder(DBSPSourceMapOperator operator) {
        this.input(operator);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        this.input(operator);
    }

    @Override
    public void postorder(DBSPNowOperator operator) {
        // Treat like a constant
        this.addOperator(operator);
        DBSPSimpleOperator replacement = new DBSPDifferentiateOperator(operator.getRelNode(), operator.outputPort());
        this.addOperator(replacement);
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), replacement.outputPort());
        this.map(operator, integral);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        OutputPort source = this.mapped(operator.input());
        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(operator.getRelNode(), source);
        DBSPSinkOperator sink = new DBSPSinkOperator(operator.getRelNode(), operator.viewName,
                operator.query, operator.originalRowType, operator.metadata, diff.outputPort());
        this.addOperator(diff);
        this.map(operator, sink);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        DBSPSimpleOperator replacement;
        this.addOperator(operator);
        replacement = new DBSPDifferentiateOperator(operator.getRelNode(), operator.outputPort());
        this.addOperator(replacement);
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), replacement.outputPort());
        this.map(operator, integral);
    }
}
