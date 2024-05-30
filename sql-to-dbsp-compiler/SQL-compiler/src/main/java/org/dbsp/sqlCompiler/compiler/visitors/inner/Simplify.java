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
import org.apache.commons.lang3.StringUtils;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasZero;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.util.Utilities;

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
            if (type.setMayBeNull(false).sameType(litType) &&
                    !type.is(DBSPTypeString.class)) {
                // Casting to VARCHAR may change a string even if the source is the same type
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
                        LocalDate.parse(str.value, dateFormatter); // executed for exception
                        result = new DBSPDateLiteral(lit.getNode(), type, new DateString(str.value));
                    } catch (DateTimeParseException ex) {
                        this.errorReporter.reportWarning(expression.getSourcePosition(), "Not a date",
                                " String " + Utilities.singleQuote(str.value) +
                                        " cannot be interpreted as a date");
                    }
                } else if (type.is(DBSPTypeTime.class)) {
                    try {
                        TimeString ts = new TimeString(str.value);
                        result = new DBSPTimeLiteral(lit.getNode(), type, ts);
                    } catch (DateTimeParseException ex) {
                        this.errorReporter.reportWarning(expression.getSourcePosition(), "Not a number",
                                " String " + Utilities.singleQuote(str.value) +
                                        " cannot be interpreted as a time");
                    }
                } else if (type.is(DBSPTypeDecimal.class)) {
                    try {
                        DBSPTypeDecimal decType = type.to(DBSPTypeDecimal.class);
                        BigDecimal value = new BigDecimal(str.value).setScale(decType.scale, RoundingMode.HALF_EVEN);
                        result = new DBSPDecimalLiteral(type, value);
                    } catch (NumberFormatException ex) {
                        // on parse error return 0.
                        this.errorReporter.reportWarning(expression.getSourcePosition(), "Not a number",
                                " String " + Utilities.singleQuote(str.value) +
                                        " cannot be interpreted as a DECIMAL");
                    }
                } else if (type.is(DBSPTypeString.class)) {
                    DBSPTypeString typeString = type.to(DBSPTypeString.class);
                    if (typeString.precision == DBSPTypeString.UNLIMITED_PRECISION) {
                        String value = Utilities.trimRight(str.value);
                        result = new DBSPStringLiteral(value, str.charset, type.mayBeNull);
                    } else {
                        String value;
                        if (!typeString.fixed) {
                            value = Utilities.trimRight(str.value);
                        } else if (str.value.length() < typeString.precision) {
                            value = StringUtils.rightPad(str.value, typeString.precision);
                        } else {
                            value = str.value.substring(0, typeString.precision);
                        }
                        result = new DBSPStringLiteral(expression.getNode(), type, value, str.charset);
                    }
                } else
                    if (type.is(DBSPTypeInteger.class)) {
                    DBSPTypeInteger ti = type.to(DBSPTypeInteger.class);
                    switch (ti.getWidth()) {
                        case 8: {
                            byte value;
                            try {
                                value = Byte.parseByte(str.value);
                                result = new DBSPI8Literal(lit.getNode(), type, value);
                            } catch (NumberFormatException ex) {
                                // SQL semantics: parsing failures return 0
                                this.errorReporter.reportWarning(expression.getSourcePosition(), "Not a number",
                                        " String " + Utilities.singleQuote(str.value) +
                                                " cannot be interpreted as a number");
                            }
                            break;
                        }
                        case 16: {
                            short value;
                            try {
                                value = Short.parseShort(str.value);
                                result = new DBSPI16Literal(lit.getNode(), type, value);
                            } catch (NumberFormatException ex) {
                                this.errorReporter.reportWarning(expression.getSourcePosition(), "Not a number",
                                        " String " + Utilities.singleQuote(str.value) +
                                                " cannot be interpreted as a number");
                            }
                            break;
                        }
                        case 32: {
                            int value;
                            try {
                                value = Integer.parseInt(str.value);
                                result = new DBSPI32Literal(lit.getNode(), type, value);
                            } catch (NumberFormatException ex) {
                                this.errorReporter.reportWarning(expression.getSourcePosition(), "Not a number",
                                        " String " + Utilities.singleQuote(str.value) +
                                                " cannot be interpreted as a number");
                            }
                            break;
                        }
                        case 64: {
                            long value;
                            try {
                                value = Long.parseLong(str.value);
                                result = new DBSPI64Literal(lit.getNode(), type, value);
                            } catch (Exception ex) {
                                this.errorReporter.reportWarning(expression.getSourcePosition(), "Not a number",
                                        " String " + Utilities.singleQuote(str.value) +
                                                " cannot be interpreted as a number");
                            }
                            break;
                        }
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
                        this.errorReporter.reportError(expression.getSourcePosition(),
                                "Invalid DECIMAL",
                                "cannot represent " + lit + " as DECIMAL(" + decType.precision + ", " + decType.scale +
                                        "): precision of DECIMAL type too small to represent value"
                        );
                    }
                    result = new DBSPDecimalLiteral(source.getNode(), type, value);
                }
            }
        }
        assert expression.getType().mayBeNull == result.getType().mayBeNull :
                "Nullability of " + expression + " has changed " +
                " from " + expression.getType() + " to " + result.getType();
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
    public VisitDecision preorder(DBSPDerefExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = source.deref();
        if (source.is(DBSPBorrowExpression.class)) {
            result = source.to(DBSPBorrowExpression.class).expression;
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBorrowExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = source.borrow();
        if (source.is(DBSPDerefExpression.class)) {
            result = source.to(DBSPDerefExpression.class).expression;
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
                IHasZero iLeftType = leftType.as(IHasZero.class);
                if (iLeftType != null) {
                    if (iLeftType.isZero(leftLit)) {
                        result = right;
                    } else if (leftLit.isNull) {
                        // null + anything is null
                        result = left;
                    }
                }
            } else if (right.is(DBSPLiteral.class)) {
                DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                IHasZero iRightType = rightType.as(IHasZero.class);
                if (iRightType != null) {
                    if (iRightType.isZero(rightLit)) {
                        result = left;
                    } else if (rightLit.isNull) {
                        result = right;
                    }
                }
            }
        } else if (expression.operation.equals(DBSPOpcode.MUL)) {
            if (left.is(DBSPLiteral.class) && leftType.is(IsNumericType.class)) {
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
            } else if (right.is(DBSPLiteral.class) && rightType.is(IsNumericType.class)) {
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
        } else if (expression.operation.equals(DBSPOpcode.DIV)) {
            if (right.is(DBSPLiteral.class)) {
                DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                IsNumericType iRightType = rightType.to(IsNumericType.class);
                if (iRightType.isOne(rightLit)) {
                    result = left;
                } else if (iRightType.isZero(rightLit)) {
                    this.errorReporter.reportWarning(expression.getSourcePosition(), "Division by zero",
                            " Division by constant zero value.");
                } 
            }
        } else if (expression.operation.equals(DBSPOpcode.MOD)) {
            if (right.is(DBSPLiteral.class)) {
                DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                IsNumericType iRightType = rightType.to(IsNumericType.class);
                if (iRightType.isOne(rightLit)) {
                    result = iRightType.getZero();
                } else if (iRightType.isZero(rightLit)) {
                    this.errorReporter.reportWarning(expression.getSourcePosition(),
                        "Division by zero",
                        " Modulus by constant zero value as divisor."
                    );
                }
            }
        }
        this.map(expression, result.cast(expression.getType()));
        return VisitDecision.STOP;
    }
}
