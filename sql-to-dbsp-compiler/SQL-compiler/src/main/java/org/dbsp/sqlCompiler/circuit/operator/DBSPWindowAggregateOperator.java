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

package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This operator does not correspond to any standard DBSP operator currently.
 * It is implemented as a sequence of 2 DBSP operators: partitioned_rolling_aggregate and
 * map_index.
 * This operator only operates correctly on deltas.  To operate on collections it
 * must differentiate its input, and integrate its output. */
public final class DBSPWindowAggregateOperator extends DBSPAggregateOperatorBase {
    public final DBSPExpression window;
    // TODO: these fields should not be here
    public final boolean ascending;
    public final boolean nullsLast;

    public DBSPWindowAggregateOperator(
            CalciteObject node,
            @Nullable DBSPExpression function, @Nullable DBSPAggregate aggregate,
            DBSPExpression window,
            DBSPTypeIndexedZSet outputType,
            boolean ascending, boolean nullsLast,
            DBSPOperator input) {
        super(node, "window_aggregate", outputType, function, aggregate, true, input, false);
        this.window = window;
        this.ascending = ascending;
        this.nullsLast = nullsLast;
        // Expect a tuple with 2 fields
        DBSPTypeTuple partAndTime = outputType.keyType.to(DBSPTypeTuple.class);
        if (partAndTime.size() != 2)
            throw new InternalCompilerError("Unexpected type for Window aggregate operator " + outputType);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPWindowAggregateOperator(
                this.getNode(), expression, this.aggregate, this.window,
                outputType.to(DBSPTypeIndexedZSet.class),
                this.ascending, this.nullsLast,
                this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWindowAggregateOperator(
                    this.getNode(), this.function, this.aggregate, this.window,
                    this.getOutputIndexedZSetType(),
                    this.ascending, this.nullsLast, newInputs.get(0));
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPWindowAggregateOperator otherOperator = other.as(DBSPWindowAggregateOperator.class);
        if (otherOperator == null)
            return false;
        return this.nullsLast == otherOperator.nullsLast &&
                this.ascending == otherOperator.ascending &&
                this.window.equivalent(otherOperator.window);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }
}
