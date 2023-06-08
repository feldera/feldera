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

import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.util.Linq;
import org.dbsp.util.UnsupportedException;

import java.util.List;

/**
 * This visitor optimizes incremental circuits by pushing integral operators
 * forward.
 */
public class OptimizeIncrementalVisitor extends CircuitCloneVisitor {
    public OptimizeIncrementalVisitor(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPDifferentialOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPIntegralOperator.class)) {
            DBSPIntegralOperator integral = source.to(DBSPIntegralOperator.class);
            this.map(operator, integral.input(), false);  // It should already be there
            return;
        }
        super.postorder(operator);
    }

    public void linear(DBSPUnaryOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPIntegralOperator.class)) {
            DBSPOperator replace = operator.withInputs(source.inputs, true);
            this.addOperator(replace);
            DBSPIntegralOperator integral = new DBSPIntegralOperator(operator.getNode(), replace);
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        this.linear(operator);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        this.linear(operator);
    }

    @Override
    public void postorder(DBSPIndexOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPFlatMapOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPNoopOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPWindowAggregateOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        this.linear(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        if (Linq.all(sources, s -> s.is(DBSPIntegralOperator.class))) {
            List<DBSPOperator> sourceSource = Linq.map(sources, s -> s.inputs.get(0));
            DBSPOperator replace = new DBSPIncrementalJoinOperator(operator.getNode(), operator.elementResultType,
                    operator.weightType, operator.getFunction(), operator.isMultiset,
                    sourceSource.get(0), sourceSource.get(1));
            this.addOperator(replace);
            DBSPIntegralOperator integral = new DBSPIntegralOperator(operator.getNode(), replace);
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        if (Linq.all(sources, s -> s.is(DBSPIntegralOperator.class))) {
            List<DBSPOperator> sourceSource = Linq.map(sources, s -> s.inputs.get(0));
            DBSPOperator replace = new DBSPSumOperator(operator.getNode(), sourceSource);
            this.addOperator(replace);
            DBSPIntegralOperator integral = new DBSPIntegralOperator(operator.getNode(), replace);
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        if (Linq.all(sources, s -> s.is(DBSPIntegralOperator.class))) {
            List<DBSPOperator> sourceSource = Linq.map(sources, s -> s.inputs.get(0));
            DBSPOperator replace = new DBSPSubtractOperator(operator.getNode(), sourceSource.get(0), sourceSource.get(1));
            this.addOperator(replace);
            DBSPIntegralOperator integral = new DBSPIntegralOperator(operator.getNode(), replace);
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPIntegralOperator.class)) {
            DBSPOperator replace = new DBSPIncrementalDistinctOperator(operator.getNode(), source.inputs.get(0));
            this.addOperator(replace);
            DBSPIntegralOperator integral = new DBSPIntegralOperator(operator.getNode(), replace);
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPIntegralOperator.class)) {
            DBSPOperator replace = new DBSPIncrementalAggregateOperator(
                    source.getNode(), operator.keyType, operator.outputElementType, operator.weightType,
                    operator.function, operator.aggregate, source.inputs.get(0));
            this.addOperator(replace);
            DBSPIntegralOperator integral = new DBSPIntegralOperator(operator.getNode(), replace);
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        this.addOperator(operator);
        DBSPDifferentialOperator diff = new DBSPDifferentialOperator(operator.getNode(), operator);
        this.addOperator(diff);
        DBSPIntegralOperator integral = new DBSPIntegralOperator(operator.getNode(), diff);
        this.map(operator, integral);
    }

    @Override
    public void postorder(DBSPOperator operator) {
        throw new UnsupportedException(operator);
    }
}
