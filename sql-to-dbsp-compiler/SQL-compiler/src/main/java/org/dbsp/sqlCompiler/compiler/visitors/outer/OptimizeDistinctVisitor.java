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
import org.dbsp.util.Linq;

import java.util.List;

public class OptimizeDistinctVisitor extends CircuitCloneVisitor {
    public OptimizeDistinctVisitor(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator distinct) {
        // distinct (distinct) = distinct
        DBSPOperator input = this.mapped(distinct.input());
        if (input.is(DBSPStreamDistinctOperator.class)) {
            this.map(distinct, input, false);
            return;
        }
        if (input.is(DBSPStreamJoinOperator.class) ||
            input.is(DBSPMapOperator.class) ||
            input.is(DBSPSumOperator.class)) {
            boolean allDistinct = Linq.all(input.inputs, i -> i.is(DBSPStreamDistinctOperator.class));
            if (allDistinct) {
                // distinct(map(distinct)) = distinct(map)
                List<DBSPOperator> newInputs = Linq.map(input.inputs, i -> i.inputs.get(0));
                DBSPOperator newInput = input.withInputs(newInputs, false);
                this.addOperator(newInput);
                DBSPOperator newDistinct = distinct.withInputs(Linq.list(newInput), false);
                this.map(distinct, newDistinct);
                return;
            }
        }
        super.postorder(distinct);
    }

    public void postorder(DBSPFilterOperator filter) {
        DBSPOperator input = this.mapped(filter.input());
        if (input.is(DBSPStreamDistinctOperator.class)) {
            // swap distinct after filter
            DBSPOperator newFilter = filter.withInputs(input.inputs, false);
            this.addOperator(newFilter);
            DBSPOperator result = input.withInputs(Linq.list(newFilter), false);
            this.map(filter, result);
        } else {
            super.postorder(filter);
        }
    }

    public void postorder(DBSPStreamJoinOperator join) {
        DBSPOperator left = this.mapped(join.inputs.get(0));
        DBSPOperator right = this.mapped(join.inputs.get(1));
        // join(distinct) = distinct(join)
        if (left.is(DBSPStreamDistinctOperator.class) &&
            right.is(DBSPStreamDistinctOperator.class)) {
            // swap distinct after filter
            DBSPOperator newLeft = left.inputs.get(0);
            DBSPOperator newRight = right.inputs.get(0);
            DBSPOperator result = join.withInputs(Linq.list(newLeft, newRight), false);
            this.addOperator(result);
            DBSPOperator distinct = new DBSPStreamDistinctOperator(join.getNode(), result);
            this.map(join, distinct);
            return;
        }
        super.postorder(join);
    }
}
