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

package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Materialize the stream boundaries selected by Rust using the live operator constructors. */
final class IncrementalBoundaryEdits extends CircuitCloneVisitor {
    enum Step { DeltaSource, DeltaConstant, Original, Differentiate, Integrate, AnnotatedIntegral, Sink }
    private final Map<Long, List<Step>> edits;

    IncrementalBoundaryEdits(DBSPCompiler compiler, Map<Long, List<Step>> edits) {
        super(compiler, false);
        this.preservesKinds = false;
        this.edits = edits;
    }

    @Override
    protected boolean resultIncremental(DBSPCircuit circuit) {
        return true;
    }

    @Override
    public void replace(DBSPSimpleOperator operator) {
        var steps = this.edits.get(operator.id);
        if (steps == null) {
            super.replace(operator);
            return;
        }
        if (!this.visited.add(operator))
            return;
        OutputPort port = operator.inputs.isEmpty() ? null : this.mapped(operator.inputs.get(0));
        var origin = operator.getRelNode();
        for (var step : steps) {
            DBSPSimpleOperator result = switch (step) {
                case DeltaSource -> operator.to(DBSPSourceBaseOperator.class).withKind(StreamKind.DELTA);
                case DeltaConstant -> new DBSPIncrementalConstantOperator(origin, operator.getFunction()).copyAnnotations(operator);
                case Original -> operator;
                case Differentiate -> new DBSPDifferentiateOperator(origin, Objects.requireNonNull(port));
                case Integrate -> new DBSPIntegrateOperator(origin, Objects.requireNonNull(port));
                case AnnotatedIntegral -> new DBSPIntegrateOperator(origin, Objects.requireNonNull(port)).copyAnnotations(operator);
                case Sink -> {
                    var sink = operator.to(DBSPSinkOperator.class);
                    yield new DBSPSinkOperator(origin, sink.viewName, sink.query, sink.originalRowType,
                            sink.metadata, Objects.requireNonNull(port));
                }
            };
            this.addOperator(result);
            port = result.outputPort();
        }
        this.map(operator.outputPort(), Objects.requireNonNull(port), false);
    }
}
