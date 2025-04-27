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

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.circuit.annotation.NoInc;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** This visitor optimizes incremental circuits by pushing integral operators
 * forward. */
public class OptimizeIncrementalVisitor extends CircuitCloneVisitor {
    public OptimizeIncrementalVisitor(DBSPCompiler compiler) {
        super(compiler, false);
    }

    /** If a nested operator is here the integrators from the input will be
     * pushed to its output. */
    final Set<ICircuit> pushIntegators = new HashSet<>();

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        if (operator.hasAnnotation(p -> p.is(NoInc.class))) {
            this.linear(operator);
            return;
        }

        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPIntegrateOperator.class)) {
            DBSPIntegrateOperator integral = source.node().to(DBSPIntegrateOperator.class);
            this.map(operator.outputPort(), integral.input(), false);  // It should already be there
            return;
        }
        super.postorder(operator);
    }

    public void linear(DBSPUnaryOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPIntegrateOperator.class)) {
            DBSPSimpleOperator replace = operator.withInputs(source.node().inputs, true);
            this.addOperator(replace);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(
                    operator.getRelNode(), replace.outputPort());
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) { this.linear(operator); }

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
    public void postorder(DBSPFlatMapOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPNoopOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPHopOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPViewOperator operator) {
        this.linear(operator);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) { this.linear(operator); }

    // It's not linear, but it behaves like one
    @Override
    public void postorder(DBSPChainAggregateOperator operator) { this.linear(operator); }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        this.linear(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (Linq.all(sources, s -> s.node().is(DBSPIntegrateOperator.class))) {
            List<OutputPort> sourceSource = Linq.map(sources, s -> s.node().inputs.get(0));
            DBSPSimpleOperator replace = new DBSPJoinOperator(operator.getRelNode(),
                    operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset,
                    sourceSource.get(0), sourceSource.get(1));
            this.addOperator(replace);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(
                    operator.getRelNode(), replace.outputPort());
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (Linq.all(sources, s -> s.node().is(DBSPIntegrateOperator.class))) {
            List<OutputPort> sourceSource = Linq.map(sources, s -> s.node().inputs.get(0));
            DBSPSimpleOperator replace = new DBSPJoinIndexOperator(operator.getRelNode(),
                    operator.getOutputIndexedZSetType(),
                    operator.getFunction(), operator.isMultiset,
                    sourceSource.get(0), sourceSource.get(1));
            this.addOperator(replace);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), replace.outputPort());
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (Linq.all(sources, s -> s.node().is(DBSPIntegrateOperator.class))) {
            List<OutputPort> sourceSource = Linq.map(sources, s -> s.node().inputs.get(0));
            DBSPSimpleOperator replace = new DBSPAntiJoinOperator(operator.getRelNode(),
                    sourceSource.get(0), sourceSource.get(1));
            this.addOperator(replace);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), replace.outputPort());
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (Linq.all(sources, s -> s.node().is(DBSPIntegrateOperator.class))) {
            List<OutputPort> sourceSource = Linq.map(sources, s -> s.node().inputs.get(0));
            DBSPSimpleOperator replace = new DBSPSumOperator(operator.getRelNode(), sourceSource);
            this.addOperator(replace);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), replace.outputPort());
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (Linq.all(sources, s -> s.node().is(DBSPIntegrateOperator.class))) {
            List<OutputPort> sourceSource = Linq.map(sources, s -> s.node().inputs.get(0));
            DBSPSimpleOperator replace = new DBSPSubtractOperator(operator.getRelNode(), sourceSource.get(0), sourceSource.get(1));
            this.addOperator(replace);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), replace.outputPort());
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPIntegrateOperator.class)) {
            DBSPSimpleOperator replace = new DBSPDistinctOperator(operator.getRelNode(), source.node().inputs.get(0));
            this.addOperator(replace);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), replace.outputPort());
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPIntegrateOperator.class)) {
            DBSPSimpleOperator replace = new DBSPAggregateOperator(
                    source.node().getRelNode(), operator.getOutputIndexedZSetType(),
                    operator.function, operator.aggregate, source.node().inputs.get(0));
            this.addOperator(replace);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), replace.outputPort());
            this.map(operator, integral);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDeltaOperator operator) {
        // If the parent has integrators on all inputs, consume them here.
        // They will be "resurfaced" in postorder(DBSPNestedOperator)
        ICircuit parent = this.getParent();
        if (this.pushIntegators.contains(parent)) {
            OutputPort source = this.mapped(operator.input());
            Utilities.enforce(source.node().is(DBSPIntegrateOperator.class));
            DBSPSimpleOperator result = operator.withInputs(
                    Linq.list(source.node().to(DBSPUnaryOperator.class).input()), true);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator operator) {
        if (this.visited.contains(operator))
            return VisitDecision.STOP;
        super.preorder(operator);
        // Check if all inputs are integrators
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        boolean allIntegrators = Linq.all(sources, s -> s.node().is(DBSPIntegrateOperator.class));
        if (allIntegrators) {
            this.pushIntegators.add(operator);
        }
        return VisitDecision.CONTINUE;
    }

    public void postorder(DBSPNestedOperator operator) {
        if (!this.pushIntegators.contains(operator)) {
            super.postorder(operator);
            return;
        }

        // The rest is a copy of super.postorder, with the added integrators
        DBSPNestedOperator result = Utilities.removeLast(this.underConstruction).to(DBSPNestedOperator.class);
        result.setDerivedFrom(operator.derivedFrom);
        result.copyAnnotations(operator);

        if (result.sameCircuit(operator))
            result = operator;
        // Must insert operator before the integrators in topological order.
        this.map(operator, result);

        for (int i = 0; i < operator.outputCount(); i++) {
            OutputPort originalOutput = operator.internalOutputs.get(i);
            @Nullable OutputPort newPort = this.remap.get(originalOutput);
            if (result != operator) {
                result.addOutput(operator.outputViews.get(i), newPort);
            }
            if (newPort != null) {
                // The integrator receives the input from 'result', not from 'newPort'
                DBSPIntegrateOperator integral = new DBSPIntegrateOperator(
                        operator.getRelNode(), new OutputPort(result, i));
                // The integral will be inserted in the next circuit
                OutputPort nestedPort = new OutputPort(operator, i);
                this.map(nestedPort, integral.outputPort());
            }
        }
    }
}
