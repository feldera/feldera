/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;

import java.util.function.Predicate;

/** Visitor tailored for optimizing the functions in Apply nodes produced by
 * the InsertLimiters pass.  These nodes have the closures of the shape:
 * clo = |v: (bool, T) -> expression.  These expressions are optimized using the following
 * rewrite:
 *
 * <p>
 * |v: (bool, T) -> if v.0 { clo((true, v.1)) } else { clo((false, v.1)) }
 */
public class SimplifyWaterline extends Simplify {
    public SimplifyWaterline(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.push(expression);
        DBSPExpression body = this.transform(expression.body);
        this.pop(expression);
        DBSPClosureExpression transformed = body.closure(expression.parameters);

        DBSPExpression result = transformed;
        if (transformed.parameters.length != 1) {
            this.map(expression, result);
            return VisitDecision.STOP;
        }
        DBSPType type = transformed.parameters[0].getType();
        if (!type.is(DBSPTypeRef.class)) {
            this.map(expression, result);
            return VisitDecision.STOP;
        }
        DBSPTypeTupleBase tuple = type.deref().as(DBSPTypeTupleBase.class);
        if (tuple == null || tuple.size() != 2) {
            this.map(expression, result);
            return VisitDecision.STOP;
        }

        if (!tuple.tupFields[0].is(DBSPTypeBool.class) || tuple.tupFields[0].mayBeNull) {
            this.map(expression, result);
            return VisitDecision.STOP;
        }

        DBSPVariablePath var = type.var();
        DBSPExpression ifTrue = transformed.call(
                tuple.makeTuple(new DBSPBoolLiteral(true), var.deref().field(1)).borrow())
                .reduce(this.errorReporter);
        DBSPExpression ifFalse = transformed.call(
                tuple.makeTuple(new DBSPBoolLiteral(false), var.deref().field(1)).borrow())
                .reduce(this.errorReporter);
        result = new DBSPIfExpression(expression.getNode(),
                var.deref().field(0), ifTrue, ifFalse).closure(var.asParameter());
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
