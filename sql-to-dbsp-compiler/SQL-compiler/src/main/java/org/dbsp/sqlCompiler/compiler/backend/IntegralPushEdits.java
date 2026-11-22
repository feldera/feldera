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

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.util.Linq;

import java.util.Map;

/** Materialize the integral moves selected by Rust, retaining the live functions and origins. */
final class IntegralPushEdits extends CircuitCloneVisitor {
    private final Map<Long, String> edits;

    IntegralPushEdits(DBSPCompiler compiler, Map<Long, String> edits) {
        super(compiler, false);
        this.edits = edits;
    }

    @Override
    public void replace(DBSPSimpleOperator operator) {
        var action = this.edits.get(operator.id);
        if (action == null) {
            super.replace(operator);
            return;
        }
        if (!this.visited.add(operator))
            return;
        var integrals = Linq.map(operator.inputs, p -> this.mapped(p).node().to(DBSPIntegrateOperator.class));
        var inputs = Linq.map(integrals, DBSPIntegrateOperator::input);
        if (action.equals("Bypass")) {
            this.map(operator.outputPort(), inputs.get(0), false);
            return;
        }
        var origin = operator.getRelNode();
        DBSPSimpleOperator delta = switch (action) {
            case "Push" -> operator.withInputs(inputs, true).to(DBSPSimpleOperator.class);
            case "DBSPJoinOperator" -> new DBSPJoinOperator(origin, operator.getOutputZSetType(),
                    operator.getFunction(), inputs.get(0), inputs.get(1), operator.to(DBSPStreamJoinOperator.class).balanced);
            case "DBSPJoinIndexOperator" -> new DBSPJoinIndexOperator(origin, operator.getOutputIndexedZSetType(),
                    operator.getFunction(), inputs.get(0), inputs.get(1), operator.to(DBSPStreamJoinIndexOperator.class).balanced);
            case "DBSPAntiJoinOperator" -> new DBSPAntiJoinOperator(origin, inputs.get(0), inputs.get(1));
            case "DBSPDistinctOperator" -> new DBSPDistinctOperator(origin, inputs.get(0));
            case "DBSPAggregateOperator" -> {
                var aggregate = operator.to(DBSPStreamAggregateOperator.class);
                yield new DBSPAggregateOperator(integrals.get(0).getRelNode(), aggregate.getOutputIndexedZSetType(),
                        aggregate.getAggregator(), aggregate.aggregateList, inputs.get(0));
            }
            default -> throw new IllegalArgumentException("Unknown integral edit " + action);
        };
        this.addOperator(delta.copyAnnotations(operator));
        this.map(operator, new DBSPIntegrateOperator(origin, delta.outputPort()));
    }
}
