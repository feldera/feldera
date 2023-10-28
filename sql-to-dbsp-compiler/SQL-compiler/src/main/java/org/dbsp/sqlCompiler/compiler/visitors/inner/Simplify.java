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

import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;
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
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = source.is_null();
        if (!source.getType().mayBeNull)
            result = new DBSPBoolLiteral(false);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        DBSPType type = this.transform(expression.getType());
        DBSPExpression result = source.cast(type);
        this.pop(expression);
        DBSPLiteral lit = source.as(DBSPLiteral.class);
        if (lit != null) {
            DBSPType litType = lit.getType();
            if (type.setMayBeNull(false).sameType(litType)) {
                // Cast from type to Option<type>
                result = lit.getWithNullable(type.mayBeNull);
            } else if (lit.isNull) {
                result = DBSPLiteral.none(type);
            } else if (litType.is(DBSPTypeNull.class)) {
                // This is a literal with type "NULL".
                // Convert it to a literal of the resulting type
                result = DBSPLiteral.none(type);
            } else if (lit.is(DBSPStringLiteral.class)) {
                // Constant folding string values
                // This is mostly useful for testing code, to fold various literals.
                DBSPStringLiteral str = lit.to(DBSPStringLiteral.class);
                Objects.requireNonNull(str.value);
                if (type.is(DBSPTypeDate.class)) {
                    // We have to validate the date, it may be invalid, and in this case
                    // the result is 'null'.  This pattern is hardwired in Calcite.
                    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.US)
                            .withResolverStyle(ResolverStyle.STRICT);
                    try {
                        //noinspection ResultOfMethodCallIgnored
                        LocalDate.parse(str.value, dateFormatter); // executed for exception
                        result = new DBSPDateLiteral(lit.getNode(), type, new DateString(str.value));
                    } catch (DateTimeParseException ex) {
                        result = DBSPLiteral.none(type);
                    }
                } else if (type.is(DBSPTypeTime.class)) {
                    try {
                        TimeString ts = new TimeString(str.value);
                        result = new DBSPTimeLiteral(lit.getNode(), type, ts);
                    } catch (DateTimeParseException ex) {
                        result = DBSPLiteral.none(type);
                    }
                } else if (type.is(DBSPTypeDecimal.class)) {
                    try {
                        result = new DBSPDecimalLiteral(type, new BigDecimal(str.value));
                    } catch (NumberFormatException ex) {
                        // on parse error return 0.
                        result = new DBSPDecimalLiteral(type, BigDecimal.ZERO);
                    }
                } else if (type.is(DBSPTypeString.class)) {
                    DBSPTypeString typeString = type.to(DBSPTypeString.class);
                    if (typeString.precision == DBSPTypeString.UNLIMITED_PRECISION) {
                        result = lit;
                    } else {
                        String value = str.value.substring(0, Math.min(str.value.length(), typeString.precision));
                        result = new DBSPStringLiteral(value, str.charset);
                    }
                } else if (type.is(DBSPTypeInteger.class)) {
                    DBSPTypeInteger ti = type.to(DBSPTypeInteger.class);
                    try {
                        switch (ti.getWidth()) {
                            case 8: {
                                byte value = Byte.parseByte(str.value);
                                result = new DBSPI8Literal(lit.getNode(), type, value);
                                break;
                            }
                            case 16: {
                                short value = Short.parseShort(str.value);
                                result = new DBSPI16Literal(lit.getNode(), type, value);
                                break;
                            }
                            case 32: {
                                int value = Integer.parseInt(str.value);
                                result = new DBSPI32Literal(lit.getNode(), type, value);
                                break;
                            }
                            case 64: {
                                long value = Long.parseLong(str.value);
                                result = new DBSPI64Literal(lit.getNode(), type, value);
                                break;
                            }
                        }
                    } catch (NumberFormatException ex) {
                        result = DBSPLiteral.none(type);
                    }
                }
            } else if (lit.is(DBSPI32Literal.class)) {
                DBSPI32Literal i = lit.to(DBSPI32Literal.class);
                Objects.requireNonNull(i.value);
                if (type.is(DBSPTypeDecimal.class)) {
                    result = new DBSPDecimalLiteral(source.getNode(), type, new BigDecimal(i.value));
                }
            } else if (lit.is(DBSPDecimalLiteral.class)) {
                DBSPDecimalLiteral dec = lit.to(DBSPDecimalLiteral.class);
                BigDecimal value = Objects.requireNonNull(dec.value);
                if (type.is(DBSPTypeDecimal.class)) {
                    // must adjust precision and scale
                    DBSPTypeDecimal decType = type.to(DBSPTypeDecimal.class);
                    value = value.setScale(decType.scale, RoundingMode.DOWN);
                    if (value.precision() > decType.precision) {
                        throw new IllegalArgumentException("Value " + value +
                                " cannot be represented with precision " + decType.precision);
                    }
                    result = new DBSPDecimalLiteral(source.getNode(), type, value);
                }
            }
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = source.field(expression.fieldNo);
        if (source.is(DBSPBaseTupleExpression.class)) {
            result = source.to(DBSPBaseTupleExpression.class).get(expression.fieldNo);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIfExpression expression) {
        this.push(expression);
        DBSPExpression condition = this.transform(expression.condition);
        DBSPExpression negative = this.transform(expression.negative);
        DBSPExpression positive = this.transform(expression.positive);
        this.pop(expression);
        DBSPExpression result = new DBSPIfExpression(expression.getNode(), condition, positive, negative);
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
        this.push(expression);
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        boolean leftMayBeNull = leftType.mayBeNull;
        boolean rightMayBeNull = rightType.mayBeNull;
        DBSPExpression result = new DBSPBinaryExpression(
                expression.getNode(), type, expression.operation, left, right);
        if (expression.operation.equals(DBSPOpcode.AND)) {
            if (left.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral bLeft = left.to(DBSPBoolLiteral.class);
                if (!bLeft.isNull) {
                    if (Objects.requireNonNull(bLeft.value)) {
                        result = right;
                    } else {
                        result = left;
                    }
                }
            } else if (right.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral bRight = right.to(DBSPBoolLiteral.class);
                if (!bRight.isNull) {
                    if (Objects.requireNonNull(bRight.value)) {
                        result = left;
                    } else {
                        result = right;
                    }
                }
            }
        } else if (expression.operation.equals(DBSPOpcode.OR)) {
            if (left.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral bLeft = left.to(DBSPBoolLiteral.class);
                if (!bLeft.isNull) {
                    if (Objects.requireNonNull(bLeft.value)) {
                        result = left;
                    } else {
                        result = right;
                    }
                }
            } else if (right.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral bRight = right.to(DBSPBoolLiteral.class);
                if (!bRight.isNull) {
                    if (Objects.requireNonNull(bRight.value)) {
                        result = right;
                    } else {
                        result = left;
                    }
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
                    result = right.cast(expression.getType());
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
        }
        this.map(expression, result.cast(expression.getType()));
        return VisitDecision.STOP;
    }
}
