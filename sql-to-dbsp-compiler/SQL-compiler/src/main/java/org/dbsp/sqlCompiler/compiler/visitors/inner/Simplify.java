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

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;

import java.util.Objects;

/**
 * Visitor which does some Rust-level expression simplifications.
 * - is_null() called on non-nullable values is simplified to 'false'
 * - Boolean && and || with constant arguments are simplified
 * - 'if' expressions with constant arguments are simplified to the corresponding branch
 * - cast(NULL, T) is converted to a NULL value of type T
 * - 1 * x = x * 1 = x
 * - 0 + x = x + 0 = x
 * - 0 * x = x * 0 = 0
 */
public class Simplify extends InnerRewriteVisitor {
    // You would think that Calcite has done these optimizations, but apparently not.

    public Simplify(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public VisitDecision preorder(DBSPIsNullExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (!source.getNonVoidType().mayBeNull)
            result = DBSPBoolLiteral.FALSE;
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        DBSPExpression source = this.transform(expression.source);
        DBSPLiteral lit = source.as(DBSPLiteral.class);
        if (lit != null) {
            if (lit.getNonVoidType().is(DBSPTypeNull.class)) {
                // This is a literal with type "NULL".
                // Convert it to a literal of the resulting type
                DBSPExpression result = DBSPLiteral.none(expression.getNonVoidType());
                this.map(expression, result);
                return VisitDecision.STOP;
            }
        }
        this.map(expression, expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression expression) {
        DBSPExpression result = expression;
        if (expression.expression.is(DBSPBaseTupleExpression.class)) {
            result = expression.expression.to(DBSPBaseTupleExpression.class).get(expression.fieldNo);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIfExpression expression) {
        DBSPExpression condition = this.transform(expression.condition);
        DBSPExpression negative = this.transform(expression.negative);
        DBSPExpression positive = this.transform(expression.positive);
        DBSPExpression result = expression;
        if (condition.is(DBSPBoolLiteral.class)) {
            DBSPBoolLiteral cond = condition.to(DBSPBoolLiteral.class);
            if (!cond.isNull) {
                if (Objects.requireNonNull(cond.value)) {
                    result = positive;
                } else {
                    result = negative;
                }
            }
        } else if (condition != expression.condition ||
                positive != expression.positive ||
                negative != expression.negative) {
            result = new DBSPIfExpression(expression.getNode(), condition, positive, negative);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryExpression expression) {
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        DBSPType leftType = left.getNonVoidType();
        DBSPType rightType = right.getNonVoidType();
        boolean leftMayBeNull = leftType.mayBeNull;
        boolean rightMayBeNull = rightType.mayBeNull;
        DBSPExpression result = expression;
        if (expression.operation.equals(DBSPOpcode.AND)) {
            if (left.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral bLeft = left.to(DBSPBoolLiteral.class);
                if (bLeft.isNull) {
                    result = bLeft;
                } else if (Objects.requireNonNull(bLeft.value)) {
                    result = right;
                } else {
                    result = left;
                }
            } else if (right.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral bRight = right.to(DBSPBoolLiteral.class);
                if (bRight.isNull) {
                    result = left;
                } else if (Objects.requireNonNull(bRight.value)) {
                    result = left;
                } else {
                    result = right;
                }
            }
        } else if (expression.operation.equals(DBSPOpcode.OR)) {
            if (left.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral bLeft = left.to(DBSPBoolLiteral.class);
                if (bLeft.isNull) {
                    result = bLeft;
                } else if (Objects.requireNonNull(bLeft.value)) {
                    result = left;
                } else {
                    result = right;
                }
            } else if (right.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral bRight = right.to(DBSPBoolLiteral.class);
                if (bRight.isNull) {
                    result = left;
                } else if (Objects.requireNonNull(bRight.value)) {
                    result = right;
                } else {
                    result = left;
                }
            }
        } else if (expression.operation.equals(DBSPOpcode.ADD)) {
            if (left.is(DBSPLiteral.class)) {
                DBSPLiteral leftLit = left.to(DBSPLiteral.class);
                IsNumericType iLeftType = leftType.to(IsNumericType.class);
                if (iLeftType.isZero(leftLit)) {
                    result = right;
                } else if (leftLit.isNull) {
                    result = left;
                }
            } else if (right.is(DBSPLiteral.class)) {
                DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                IsNumericType iRightType = rightType.to(IsNumericType.class);
                if (iRightType.isZero(rightLit)) {
                    result = left;
                } else if (rightLit.isNull) {
                    result = right;
                }
            }
        } else if (expression.operation.equals(DBSPOpcode.MUL)) {
            if (left.is(DBSPLiteral.class)) {
                DBSPLiteral leftLit = left.to(DBSPLiteral.class);
                IsNumericType iLeftType = leftType.to(IsNumericType.class);
                if (iLeftType.isOne(leftLit)) {
                    // This works even for null
                    result = right.cast(expression.getNonVoidType());
                } else if (iLeftType.isZero(leftLit) && !rightMayBeNull) {
                    // This is not true for null values
                    result = left;
                } else if (leftLit.isNull) {
                    result = left;
                }
            } else if (right.is(DBSPLiteral.class)) {
                DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                IsNumericType iRightType = rightType.to(IsNumericType.class);
                if (iRightType.isOne(rightLit)) {
                    result = left;
                } else if (iRightType.isZero(rightLit) && !leftMayBeNull) {
                    // This is not true for null values
                    result = right;
                } else if (rightLit.isNull) {
                    result = right;
                }
            }
        } else if (left != expression.left || right != expression.right) {
            result = new DBSPBinaryExpression(expression.getNode(), expression.getNonVoidType(),
                    expression.operation, left, right, expression.primitive);
        }
        this.map(expression, result.cast(expression.getNonVoidType()));
        return VisitDecision.STOP;
    }
}
