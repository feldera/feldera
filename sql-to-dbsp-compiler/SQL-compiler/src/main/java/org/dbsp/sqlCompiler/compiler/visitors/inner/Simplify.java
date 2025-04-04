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
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IsIntervalLiteral;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU64Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasZero;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeISize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.Objects;

/** Visitor which does some Rust-level expression simplifications. */
public class Simplify extends ExpressionTranslator {
    public Simplify(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Starting Simplify on ")
                .appendSupplier(node::toString)
                .newline();
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        // Effectively memoize results
        if (this.translationMap.containsKey(expression))
            return VisitDecision.STOP;
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPIsNullExpression expression) {
        DBSPExpression source = this.getE(expression.expression);
        DBSPExpression result = source.is_null();
        if (!source.getType().mayBeNull)
            result = new DBSPBoolLiteral(false);
        else if (source.is(DBSPCloneExpression.class))
            result = source.to(DBSPCloneExpression.class).expression.is_null();
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPCloneExpression expression) {
        DBSPExpression source = this.getE(expression.expression);
        DBSPExpression result = source.applyCloneIfNeeded();
        if (source.is(DBSPCloneExpression.class)) {
            result = source;
        }
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        DBSPExpression source = this.getE(expression.source);
        DBSPType type = expression.getType();
        DBSPExpression result = source.cast(type, expression.safe);
        DBSPLiteral lit = source.as(DBSPLiteral.class);
        if (lit != null) {
            DBSPType litType = lit.getType();
            if (type.withMayBeNull(false).sameType(litType) &&
                    !type.is(DBSPTypeString.class)) {
                // Casting to VARCHAR may change a string even if the source is the same type
                // Cast from type to Option<type>
                result = lit.getWithNullable(type.mayBeNull);
            } else if (lit.isNull()) {
                if (type.mayBeNull) {
                    result = DBSPLiteral.none(type);
                }
                // Otherwise this expression will certainly generate
                // a runtime error if evaluated.  But it may never be evaluated.
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
                        this.compiler.reportWarning(expression.getSourcePosition(), "Not a date",
                                " String " + Utilities.singleQuote(str.value) +
                                        " cannot be interpreted as a date");
                    }
                } else if (type.is(DBSPTypeTime.class)) {
                    try {
                        TimeString ts = new TimeString(str.value);
                        result = new DBSPTimeLiteral(lit.getNode(), type, ts);
                    } catch (DateTimeParseException | IllegalArgumentException ex) {
                        this.compiler.reportWarning(expression.getSourcePosition(), "Not a number",
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
                        this.compiler.reportWarning(expression.getSourcePosition(), "Not a number",
                                " String " + Utilities.singleQuote(str.value) +
                                        " cannot be interpreted as a DECIMAL");
                    }
                } else if (type.is(DBSPTypeString.class)) {
                    DBSPTypeString typeString = type.to(DBSPTypeString.class);
                    if (typeString.precision == DBSPTypeString.UNLIMITED_PRECISION) {
                        String value = Utilities.trimRight(str.value);
                        result = new DBSPStringLiteral(value, str.charset, type);
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
                } else if (type.is(DBSPTypeInteger.class)) {
                    DBSPTypeInteger ti = type.to(DBSPTypeInteger.class);
                    switch (ti.getWidth()) {
                        case 8: {
                            byte value;
                            try {
                                value = Byte.parseByte(str.value);
                                result = new DBSPI8Literal(lit.getNode(), type, value);
                            } catch (NumberFormatException ex) {
                                // SQL semantics: parsing failures return 0
                                this.compiler.reportWarning(expression.getSourcePosition(), "Not a number",
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
                                this.compiler.reportWarning(expression.getSourcePosition(), "Not a number",
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
                                this.compiler.reportWarning(expression.getSourcePosition(), "Not a number",
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
                                this.compiler.reportWarning(expression.getSourcePosition(), "Not a number",
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
                } else if (type.is(DBSPTypeInteger.class)) {
                    if (i.isNull()) {
                        result = DBSPLiteral.none(type);
                    } else {
                        switch (type.code) {
                            case INT8:
                                if (i.value >= Byte.MIN_VALUE && i.value <= Byte.MAX_VALUE) {
                                    result = new DBSPI8Literal(source.getNode(), type, i.value.byteValue());
                                }
                                break;
                            case INT16:
                                if (i.value >= Short.MIN_VALUE && i.value <= Short.MAX_VALUE) {
                                    result = new DBSPI16Literal(source.getNode(), type, i.value.shortValue());
                                }
                                break;
                            case INT64:
                                result = new DBSPI64Literal(source.getNode(), type, i.value.longValue());
                                break;
                            case INT128:
                                result = new DBSPI128Literal(source.getNode(), type, BigInteger.valueOf(i.value));
                                break;
                            case UINT16:
                                if (i.value >= 0 && i.value <= 65536) {
                                    result = new DBSPU16Literal(source.getNode(), type, i.value);
                                }
                                break;
                            case UINT32:
                                if (i.value >= 0) {
                                    result = new DBSPU32Literal(source.getNode(), type, i.value.longValue());
                                }
                                break;
                            case UINT64:
                                if (i.value >= 0) {
                                    result = new DBSPU64Literal(source.getNode(), type, BigInteger.valueOf(i.value));
                                }
                                break;
                            case UINT128:
                                if (i.value >= 0) {
                                    result = new DBSPU128Literal(source.getNode(), type, BigInteger.valueOf(i.value));
                                }
                                break;
                        }
                    }
                } else if (type.is(DBSPTypeISize.class)) {
                    if (!i.isNull()) {
                        result = new DBSPISizeLiteral(source.getNode(), type, i.value.longValue());
                    }
                }
            } else if (lit.is(DBSPDecimalLiteral.class)) {
                DBSPDecimalLiteral dec = lit.to(DBSPDecimalLiteral.class);
                BigDecimal value = Objects.requireNonNull(dec.value);
                if (type.is(DBSPTypeDecimal.class)) {
                    // must adjust precision and scale
                    DBSPTypeDecimal decType = type.to(DBSPTypeDecimal.class);
                    value = value.setScale(decType.scale, RoundingMode.DOWN);
                    if (value.precision() > decType.precision) {
                        this.compiler.reportWarning(expression.getSourcePosition(),
                                "Invalid DECIMAL",
                                "cannot represent " + lit + " as DECIMAL(" + decType.precision + ", " + decType.scale +
                                        "): precision of DECIMAL type too small to represent value"
                        );
                    }
                    result = new DBSPDecimalLiteral(source.getNode(), type, value);
                }
            }
        }
        assert expression.getType().sameType(result.getType());
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPFieldExpression expression) {
        DBSPExpression source = this.getE(expression.expression);
        DBSPExpression result = source.field(expression.fieldNo);
        if (source.is(DBSPBaseTupleExpression.class)) {
            result = source.to(DBSPBaseTupleExpression.class).get(expression.fieldNo);
        } if (source.is(DBSPBlockExpression.class)) {
            DBSPBlockExpression block = source.to(DBSPBlockExpression.class);
            assert block.lastExpression != null;
            result = new DBSPBlockExpression(block.contents,
                    block.lastExpression.field(expression.fieldNo).simplify());
        } else if (source.is(DBSPIfExpression.class)) {
            DBSPIfExpression conditional = source.to(DBSPIfExpression.class);
            result = new DBSPIfExpression(source.getNode(), conditional.condition,
                    conditional.positive.field(expression.fieldNo),
                    conditional.negative != null ? conditional.negative.field(expression.fieldNo) : null);
        } else if (source.is(DBSPCloneExpression.class)) {
            result = new DBSPFieldExpression(expression.getNode(),
                    source.to(DBSPCloneExpression.class).expression,
                    expression.fieldNo);
        }
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPDerefExpression expression) {
        DBSPExpression source = this.getE(expression.expression);
        DBSPExpression result = source.deref();
        if (source.is(DBSPBorrowExpression.class))
            result = source.to(DBSPBorrowExpression.class).expression;
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPBorrowExpression expression) {
        DBSPExpression source = this.getE(expression.expression);
        DBSPExpression result = source.borrow(expression.mut);
        if (source.is(DBSPDerefExpression.class)) {
            result = source.to(DBSPDerefExpression.class).expression;
        }
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPIfExpression expression) {
        DBSPExpression condition = this.getE(expression.condition);
        @Nullable DBSPExpression negative = this.getEN(expression.negative);
        DBSPExpression positive = this.getE(expression.positive);
        DBSPExpression result = new DBSPIfExpression(expression.getNode(), condition, positive, negative);
        if (condition.is(DBSPBoolLiteral.class)) {
            DBSPBoolLiteral cond = condition.to(DBSPBoolLiteral.class);
            if (!cond.isNull()) {
                if (Objects.requireNonNull(cond.value)) {
                    result = positive;
                } else {
                    result = negative;
                    if (result == null)
                        result = new DBSPRawTupleExpression();
                }
            }
        } else if (negative != null &&
                positive.isCompileTimeConstant() &&
                negative.isCompileTimeConstant() &&
                positive.equivalent(negative)) {
            result = positive;
        } else if (condition != expression.condition ||
                positive != expression.positive ||
                negative != expression.negative) {
            result = new DBSPIfExpression(expression.getNode(), condition, positive, negative);
        }
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPUnaryExpression expression) {
        DBSPExpression source = this.getE(expression.source);
        DBSPExpression result = new DBSPUnaryExpression(
                expression.getNode(), expression.type, expression.opcode, source);
        if (expression.opcode == DBSPOpcode.NEG) {
            if (source.is(DBSPLiteral.class)) {
                DBSPLiteral lit = source.to(DBSPLiteral.class);
                if (lit.is(IsNumericLiteral.class)) {
                    try {
                        result = lit.to(IsNumericLiteral.class).negate().to(DBSPExpression.class);
                    } catch (ArithmeticException ex) {
                        // leave unchanged
                    }
                }
            }
        } else if (expression.opcode == DBSPOpcode.WRAP_BOOL) {
            // wrap_bool(cast_to_bn_b(expression)) => expression
            if (source.is(DBSPCastExpression.class)) {
                DBSPCastExpression cast = source.to(DBSPCastExpression.class);
                if (cast.source.getType().is(DBSPTypeBool.class) &&
                    !cast.source.getType().mayBeNull) {
                    result = cast.source;
                }
            }
        } else if (expression.opcode == DBSPOpcode.NOT) {
            if (source.is(DBSPBoolLiteral.class)) {
                DBSPBoolLiteral b = source.to(DBSPBoolLiteral.class);
                if (b.value == null)
                    result = b;
                else
                    result = new DBSPBoolLiteral(expression.getNode(), expression.getType(), !b.value);
            } else if (source.is(DBSPUnaryOperator.class)) {
                // !!e = e, true in ternary logic too
                DBSPUnaryExpression unarySource = source.to(DBSPUnaryExpression.class);
                if (unarySource.opcode == DBSPOpcode.NOT) {
                    result = unarySource.source;
                }
            }
        }
        this.map(expression, result.cast(expression.getType(), false));
    }

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        DBSPExpression left = this.getE(expression.left);
        DBSPExpression right = this.getE(expression.right);
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        boolean leftMayBeNull = leftType.mayBeNull;
        boolean rightMayBeNull = rightType.mayBeNull;
        DBSPExpression result = new DBSPBinaryExpression(
                expression.getNode(), expression.type, expression.opcode, left, right);
        DBSPOpcode opcode = expression.opcode;

        if (opcode == DBSPOpcode.MAP_INDEX ||
                opcode == DBSPOpcode.SQL_INDEX ||
                opcode == DBSPOpcode.RUST_INDEX) {
            if (expression.left.is(DBSPCloneExpression.class)) {
                result = new DBSPBinaryExpression(
                        expression.getNode(), expression.type, expression.opcode,
                        left.to(DBSPCloneExpression.class).expression, right);
            }
        }
        try {
            if (opcode == DBSPOpcode.AND) {
                if (left.is(DBSPBoolLiteral.class)) {
                    DBSPBoolLiteral bLeft = left.to(DBSPBoolLiteral.class);
                    if (!bLeft.isNull()) {
                        if (Objects.requireNonNull(bLeft.value)) {
                            result = right;
                        } else {
                            result = left;
                        }
                    }
                } else if (right.is(DBSPBoolLiteral.class)) {
                    DBSPBoolLiteral bRight = right.to(DBSPBoolLiteral.class);
                    if (!bRight.isNull()) {
                        if (Objects.requireNonNull(bRight.value)) {
                            result = left;
                        } else {
                            result = right;
                        }
                    }
                }
            } else if (opcode == DBSPOpcode.OR) {
                if (left.is(DBSPBoolLiteral.class)) {
                    DBSPBoolLiteral bLeft = left.to(DBSPBoolLiteral.class);
                    if (!bLeft.isNull()) {
                        if (Objects.requireNonNull(bLeft.value)) {
                            result = left;
                        } else {
                            result = right;
                        }
                    }
                } else if (right.is(DBSPBoolLiteral.class)) {
                    DBSPBoolLiteral bRight = right.to(DBSPBoolLiteral.class);
                    if (!bRight.isNull()) {
                        if (Objects.requireNonNull(bRight.value)) {
                            result = right;
                        } else {
                            result = left;
                        }
                    }
                }
            } else if (opcode == DBSPOpcode.ADD || opcode == DBSPOpcode.TS_ADD) {
                if (left.is(DBSPLiteral.class)) {
                    DBSPLiteral leftLit = left.to(DBSPLiteral.class);
                    IHasZero iLeftType = leftType.as(IHasZero.class);
                    if (iLeftType != null) {
                        if (iLeftType.isZero(leftLit)) {
                            result = right;
                        } else if (leftLit.isNull()) {
                            // null + anything is null
                            result = left;
                        }
                    }
                    if (left.getType().is(IsNumericType.class) &&
                            right.is(DBSPLiteral.class) &&
                            result.is(DBSPBinaryExpression.class)) {
                        result = left.getType().to(IsNumericType.class).fold(result.to(DBSPBinaryExpression.class));
                    }
                } else if (right.is(DBSPLiteral.class)) {
                    DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                    IHasZero iRightType = rightType.as(IHasZero.class);
                    if (iRightType != null) {
                        if (iRightType.isZero(rightLit)) {
                            result = left;
                        } else if (rightLit.isNull()) {
                            result = right;
                        }
                    }
                }
            } else if (opcode == DBSPOpcode.SUB || opcode == DBSPOpcode.TS_SUB) {
                if (left.is(DBSPLiteral.class)) {
                    DBSPLiteral leftLit = left.to(DBSPLiteral.class);
                    if (leftLit.isNull()) {
                        // null - anything is null
                        result = left;
                    }
                    if (left.getType().is(IsNumericType.class) &&
                            right.is(DBSPLiteral.class) &&
                            result.is(DBSPBinaryExpression.class)) {
                        result = left.getType().to(IsNumericType.class).fold(result.to(DBSPBinaryExpression.class));
                    }
                } else if (right.is(DBSPLiteral.class)) {
                    DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                    IHasZero iRightType = rightType.as(IHasZero.class);
                    if (iRightType != null) {
                        if (iRightType.isZero(rightLit)) {
                            result = left;
                        } else if (rightLit.isNull()) {
                            result = right;
                        }
                    }
                }
            }  else if (opcode == DBSPOpcode.MUL || opcode == DBSPOpcode.INTERVAL_MUL) {
                if (left.is(DBSPLiteral.class) && leftType.is(IsNumericType.class)) {
                    DBSPLiteral leftLit = left.to(DBSPLiteral.class);
                    IsNumericType iLeftType = leftType.to(IsNumericType.class);
                    if (iLeftType.isOne(leftLit)) {
                        // This works even for null
                        result = right.cast(expression.getType(), false);
                    } else if (iLeftType.isZero(leftLit) && !rightMayBeNull) {
                        // This is not true for null values
                        result = left;
                    } else if (leftLit.isNull()) {
                        result = left;
                    } else if (leftLit.is(DBSPIntLiteral.class) && right.is(IsIntervalLiteral.class)) {
                        result = right.to(IsIntervalLiteral.class)
                                .multiply(leftLit.to(DBSPIntLiteral.class).getValue())
                                .to(DBSPExpression.class);
                    }
                    if (left.getType().is(IsNumericType.class) &&
                            right.is(DBSPLiteral.class) &&
                            result.is(DBSPBinaryExpression.class)) {
                        result = left.getType().to(IsNumericType.class).fold(result.to(DBSPBinaryExpression.class));
                    }
                } else if (right.is(DBSPLiteral.class) && rightType.is(IsNumericType.class)) {
                    DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                    IsNumericType iRightType = rightType.to(IsNumericType.class);
                    if (iRightType.isOne(rightLit)) {
                        result = left;
                    } else if (iRightType.isZero(rightLit) && !leftMayBeNull) {
                        // This is not true for null values
                        result = right;
                    } else if (rightLit.isNull()) {
                        result = right;
                    } else if (rightLit.is(DBSPIntLiteral.class) && left.is(IsIntervalLiteral.class)) {
                        result = left.to(IsIntervalLiteral.class)
                                .multiply(rightLit.to(DBSPIntLiteral.class).getValue())
                                .to(DBSPExpression.class);
                    }
                }
            } else if (opcode == DBSPOpcode.DIV || opcode == DBSPOpcode.INTERVAL_DIV) {
                if (right.is(DBSPLiteral.class)) {
                    DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                    IsNumericType iRightType = rightType.to(IsNumericType.class);
                    if (iRightType.isOne(rightLit)) {
                        result = left;
                    } else if (iRightType.isZero(rightLit)) {
                        this.compiler.reportWarning(expression.getSourcePosition(), "Division by zero",
                                " Division by constant zero value.");
                    }
                    if (left.getType().is(IsNumericType.class) &&
                            right.is(DBSPLiteral.class) &&
                            result.is(DBSPBinaryExpression.class)) {
                        result = left.getType().to(IsNumericType.class).fold(result.to(DBSPBinaryExpression.class));
                    }
                }
            } else if (opcode == DBSPOpcode.MOD) {
                if (right.is(DBSPLiteral.class)) {
                    DBSPLiteral rightLit = right.to(DBSPLiteral.class);
                    IsNumericType iRightType = rightType.to(IsNumericType.class);
                    if (iRightType.isOne(rightLit)) {
                        result = iRightType.getZero();
                    } else if (iRightType.isZero(rightLit)) {
                        this.compiler.reportWarning(expression.getSourcePosition(),
                                "Division by zero",
                                " Modulus by constant zero value as divisor."
                        );
                    }
                }
            }
        } catch (ArithmeticException unused) {
            // ignore, defer to runtime
        }
        this.map(expression, result.cast(expression.getType(), false));
    }
}
