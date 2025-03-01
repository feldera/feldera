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

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Linq;

import java.util.List;

public class OptimizeDistinctVisitor extends CircuitCloneVisitor {
    public OptimizeDistinctVisitor(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator distinct) {
        // distinct (distinct) = distinct
        OutputPort input = this.mapped(distinct.input());
        if (input.node().is(DBSPStreamDistinctOperator.class) ||
            input.node().is(DBSPDistinctOperator.class) ||
            !input.isMultiset()) {
            this.map(distinct.outputPort(), input, false);
            return;
        }
        if (input.node().is(DBSPStreamJoinOperator.class) ||
            input.node().is(DBSPMapOperator.class) ||
            input.node().is(DBSPSumOperator.class)) {
            boolean allDistinct = Linq.all(input.node().inputs, i -> i.node().is(DBSPStreamDistinctOperator.class));
            if (allDistinct) {
                // distinct(map(distinct)) = distinct(map)
                List<OutputPort> newInputs = Linq.map(input.node().inputs, i -> i.node().inputs.get(0));
                DBSPSimpleOperator newInput = input.simpleNode().withInputs(newInputs, false);
                this.addOperator(newInput);
                DBSPSimpleOperator newDistinct = distinct.withInputs(Linq.list(newInput.outputPort()), false);
                this.map(distinct, newDistinct);
                return;
            }
        }
        super.postorder(distinct);
    }

    @Override
    public void postorder(DBSPDistinctOperator distinct) {
        // distinct (distinct) = distinct
        OutputPort input = this.mapped(distinct.input());
        if (input.node().is(DBSPStreamDistinctOperator.class) ||
            input.node().is(DBSPDistinctOperator.class) ||
            !input.isMultiset()) {
            this.map(distinct.outputPort(), input, false);
            return;
        }
        if (input.node().is(DBSPStreamJoinOperator.class) ||
                input.node().is(DBSPMapOperator.class) ||
                input.node().is(DBSPSumOperator.class)) {
            boolean allDistinct = Linq.all(input.node().inputs, i -> i.node().is(DBSPStreamDistinctOperator.class));
            if (allDistinct) {
                // distinct(map(distinct)) = distinct(map)
                List<OutputPort> newInputs = Linq.map(input.node().inputs, i -> i.node().inputs.get(0));
                DBSPSimpleOperator newInput = input.simpleNode().withInputs(newInputs, false);
                this.addOperator(newInput);
                DBSPSimpleOperator newDistinct = distinct.withInputs(Linq.list(newInput.outputPort()), false);
                this.map(distinct, newDistinct);
                return;
            }
        }
        super.postorder(distinct);
    }

    public void postorder(DBSPFilterOperator filter) {
        OutputPort input = this.mapped(filter.input());
        if (input.node().is(DBSPStreamDistinctOperator.class)) {
            // swap distinct after filter
            DBSPSimpleOperator newFilter = filter.withInputs(input.node().inputs, false);
            this.addOperator(newFilter);
            DBSPSimpleOperator result = input.simpleNode().withInputs(Linq.list(newFilter.outputPort()), false);
            this.map(filter, result);
        } else {
            super.postorder(filter);
        }
    }

    public void postorder(DBSPStreamJoinOperator join) {
        OutputPort left = this.mapped(join.left());
        OutputPort right = this.mapped(join.right());
        // join(distinct) = distinct(join)
        if (left.node().is(DBSPStreamDistinctOperator.class) &&
            right.node().is(DBSPStreamDistinctOperator.class)) {
            OutputPort newLeft = left.node().inputs.get(0);
            OutputPort newRight = right.node().inputs.get(0);
            DBSPSimpleOperator result = join.withInputs(Linq.list(newLeft, newRight), false);
            this.addOperator(result);
            DBSPSimpleOperator distinct = new DBSPStreamDistinctOperator(join.getRelNode(), result.outputPort());
            this.map(join, distinct);
            return;
        }
        super.postorder(join);
    }
}
