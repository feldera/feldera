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

package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ExternalFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPKeywordLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUuidLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsIntervalType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUuid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeResult;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.IsTimeRelatedType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeKeyword;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeReal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.*;

public class ExpressionCompiler extends RexVisitorImpl<DBSPExpression>
        implements IWritesLogs, ICompilerComponent {
    private final TypeCompiler typeCompiler;
    @Nullable
    public final DBSPVariablePath inputRow;
    private final RexBuilder rexBuilder;
    protected final List<RexLiteral> constants;
    private final DBSPCompiler compiler;
    /** Context in which the rex nodes are defined */
    @Nullable
    protected final RelNode context;

    public ExpressionCompiler(@Nullable RelNode context,
            @Nullable DBSPVariablePath inputRow, DBSPCompiler compiler) {
        this(context, inputRow, Linq.list(), compiler);
    }

    /**
     * Create a compiler that will translate expressions pertaining to a row.
     * @param context          Rel to which the RexNodes belong.
     * @param inputRow         Variable representing the row being compiled.
     * @param constants        Additional constants.  Expressions compiled
     *                         may use RexInputRef, which are field references
     *                         within the row.  Calcite seems to number constants
     *                         as additional fields within the row, after the end of
     *                         the input row.
     * @param compiler         Handle to the compiler.
     */
    public ExpressionCompiler(@Nullable RelNode context, @Nullable DBSPVariablePath inputRow,
                              List<RexLiteral> constants,
                              DBSPCompiler compiler) {
        super(true);
        this.context = context;
        this.inputRow = inputRow;
        this.constants = constants;
        this.rexBuilder = compiler.sqlToRelCompiler.getRexBuilder();
        this.compiler = compiler;
        this.typeCompiler = compiler.getTypeCompiler();
        if (inputRow != null &&
                !inputRow.getType().is(DBSPTypeRef.class))
            throw new InternalCompilerError("Expected a reference type for row", inputRow.getNode());
    }

    /** Expand casts to tuple types into component-wise casts */
    public static DBSPExpression expandTupleCast(CalciteObject node, DBSPExpression source, DBSPType destinationType) {
        if (destinationType.is(DBSPTypeBaseType.class)) {
            return source.cast(node, destinationType, false).applyCloneIfNeeded();
        } else switch (destinationType.code) {
            case ARRAY, MAP: return source.cast(node, destinationType, false).applyCloneIfNeeded();
            case TUPLE, RAW_TUPLE: {
                Utilities.enforce(source.getType().code == destinationType.code);
                DBSPTypeTupleBase tuple = destinationType.to(DBSPTypeTupleBase.class);
                DBSPExpression[] fields = new DBSPExpression[tuple.size()];
                DBSPExpression safeSource = source.unwrapIfNullable();
                for (int i = 0; i < tuple.size(); i++) {
                    fields[i] = expandTupleCast(node, safeSource.field(i).simplify(), tuple.getFieldType(i));
                }
                DBSPExpression convertedTuple;
                if (destinationType.code == RAW_TUPLE) {
                    convertedTuple = new DBSPRawTupleExpression(node, destinationType.to(DBSPTypeRawTuple.class), fields);
                } else {
                    convertedTuple = new DBSPTupleExpression(node, destinationType.to(DBSPTypeTuple.class), fields);
                }
                if (destinationType.mayBeNull) {
                    if (!source.getType().mayBeNull) {
                        return convertedTuple;
                    } else {
                        DBSPExpression condition = source.is_null();
                        DBSPExpression positive = destinationType.none();
                        return new DBSPIfExpression(node, condition, positive, convertedTuple);
                    }
                } else {
                    // This will panic at runtime if the source tuple is null
                    return convertedTuple;
                }
            }
            default:
                throw new InternalCompilerError("Unexpected type in cast " + destinationType);
        }
    }

    public static DBSPExpression expandTuple(CalciteObject node, DBSPExpression source) {
        return expandTupleCast(node, source, source.getType());
    }

    public DBSPExpression inputIndex(CalciteObject node, int index) {
        if (this.inputRow == null)
            throw new InternalCompilerError("Row referenced without a row context", node);
        // Unfortunately it looks like we can't trust the type coming from Calcite.
        DBSPTypeTuple type = this.inputRow.getType().deref().to(DBSPTypeTuple.class);
        if (index < type.size()) {
            DBSPExpression field = this.inputRow.deref().field(index);
            return field.applyCloneIfNeeded();
        }
        if (index - type.size() < this.constants.size())
            return this.visitLiteral(this.constants.get(index - type.size()));
        throw new InternalCompilerError("Index in row out of bounds ", node);
    }

    /** Convert an expression that refers to a field in the input row.
     * @param inputRef   index in the input row.
     * @return           the corresponding DBSP expression. */
    @SuppressWarnings("UnnecessaryLocalVariable")
    @Override
    public DBSPExpression visitInputRef(RexInputRef inputRef) {
        CalciteObject node = CalciteObject.create(this.context, inputRef);
        DBSPExpression result = this.inputIndex(node, inputRef.getIndex());
        // It is strange that the following does not hold
        // DBSPType type = this.typeCompiler.convertType(inputRef.getType(), false);
        // Utilities.enforce(type.sameType(result.getType()));
        return result;
    }

    @Override
    public DBSPExpression visitCorrelVariable(RexCorrelVariable correlVariable) {
        CalciteObject node = CalciteObject.create(this.context, correlVariable);
        if (this.inputRow == null)
            throw new InternalCompilerError("Correlation variable referenced without a row context", node);
        return this.inputRow.deref();
    }

    @Override
    public DBSPExpression visitLiteral(RexLiteral literal) {
        CalciteObject node = CalciteObject.create(this.context, literal);
        try {
            DBSPType type = this.typeCompiler.convertType(literal.getType(), true);
            if (literal.isNull())
                return DBSPLiteral.none(type);
            if (type.is(DBSPTypeInteger.class)) {
                DBSPTypeInteger intType = type.to(DBSPTypeInteger.class);
                return switch (intType.getWidth()) {
                    case 8 -> new DBSPI8Literal(Objects.requireNonNull(literal.getValueAs(Byte.class)));
                    case 16 -> new DBSPI16Literal(Objects.requireNonNull(literal.getValueAs(Short.class)));
                    case 32 -> new DBSPI32Literal(Objects.requireNonNull(literal.getValueAs(Integer.class)));
                    case 64 -> new DBSPI64Literal(Objects.requireNonNull(literal.getValueAs(Long.class)));
                    default ->
                            throw new UnsupportedOperationException("Unsupported integer width type " +
                                    intType.getWidth());
                };
            } else if (type.is(DBSPTypeDouble.class))
                return new DBSPDoubleLiteral(Objects.requireNonNull(literal.getValueAs(Double.class)));
            else if (type.is(DBSPTypeReal.class))
                return new DBSPRealLiteral(Objects.requireNonNull(literal.getValueAs(Float.class)));
            else if (type.is(DBSPTypeString.class)) {
                String str = literal.getValueAs(String.class);
                RelDataType litType = literal.getType();
                Charset charset = litType.getCharset();
                return new DBSPStringLiteral(Objects.requireNonNull(str), Objects.requireNonNull(charset), type);
            }
            else if (type.is(DBSPTypeBool.class))
                return new DBSPBoolLiteral(Objects.requireNonNull(literal.getValueAs(Boolean.class)));
            else if (type.is(DBSPTypeDecimal.class))
                return new DBSPDecimalLiteral(
                        node, type, Objects.requireNonNull(literal.getValueAs(BigDecimal.class)));
            else if (type.is(DBSPTypeKeyword.class))
                return new DBSPKeywordLiteral(node, Objects.requireNonNull(literal.getValue()).toString());
            else if (type.is(DBSPTypeMillisInterval.class)) {
                long value = Objects.requireNonNull(literal.getValueAs(BigDecimal.class)).longValue();
                return new DBSPIntervalMillisLiteral(node, type, value);
            }
            else if (type.is(DBSPTypeMonthsInterval.class)) {
                int value = Objects.requireNonNull(literal.getValueAs(Integer.class));
                return new DBSPIntervalMonthsLiteral(node, type, value);
            } else if (type.is(DBSPTypeTimestamp.class)) {
                return new DBSPTimestampLiteral(node, type,
                        Objects.requireNonNull(literal.getValueAs(TimestampString.class)));
            } else if (type.is(DBSPTypeDate.class)) {
                return new DBSPDateLiteral(node, type, Objects.requireNonNull(literal.getValueAs(DateString.class)));
            } else if (type.is(DBSPTypeGeoPoint.class)) {
                Point point = literal.getValueAs(Point.class);
                Coordinate c = Objects.requireNonNull(point).getCoordinate();
                return new DBSPGeoPointConstructor(node,
                        new DBSPDoubleLiteral(c.getOrdinate(0)),
                        new DBSPDoubleLiteral(c.getOrdinate(1)),
                        type);
            } else if (type.is(DBSPTypeTime.class)) {
                return new DBSPTimeLiteral(node, type, Objects.requireNonNull(
                        literal.getValueAs(TimeString.class)));
            } else if (type.is(DBSPTypeBinary.class)) {
                return new DBSPBinaryLiteral(node, type, literal.getValueAs(byte[].class));
            } else if (type.is(DBSPTypeUuid.class)) {
                return new DBSPUuidLiteral(node, type, literal.getValueAs(UUID.class));
            }
        } catch (BaseCompilerException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new UnimplementedException(node, ex);
        }
        throw new UnimplementedException(
                "Support for literals of type " + literal.getType() + " not yet implemented", node);
    }

    // Like makeBinaryExpression, but accepts multiple operands.
    static DBSPExpression makeBinaryExpressions(
            CalciteObject node, DBSPType type, DBSPOpcode opcode, List<DBSPExpression> operands) {
        Utilities.enforce(operands.size() >= 2, "Expected at least two operands for binary expression " + opcode);
        DBSPExpression accumulator = operands.get(0);
        for (int i = 1; i < operands.size(); i++)
            accumulator = makeBinaryExpression(node, type, opcode, accumulator, operands.get(i));
        return accumulator.cast(node, type, false);
    }

    @SuppressWarnings("unused")
    public static boolean needCommonType(DBSPOpcode opcode, DBSPType result, DBSPType left, DBSPType right) {
        if (opcode == DBSPOpcode.CONCAT) return false;
        // Dates can be mixed with other types in a binary operation
        if (left.is(IsTimeRelatedType.class)) return false;
        if (right.is(IsTimeRelatedType.class)) return false;
        // Allow arithmetic on different DECIMAL types
        if (left.is(DBSPTypeDecimal.class) && right.is(DBSPTypeDecimal.class))
            return false;
        // Allow mixing different string types in an operation
        return !left.is(DBSPTypeString.class) || !right.is(DBSPTypeString.class);
    }

    public static DBSPExpression makeBinaryExpression(
            CalciteObject node, DBSPType type, DBSPOpcode opcode, List<DBSPExpression> operands) {
        if (operands.size() != 2)
            throw new InternalCompilerError("Expected 2 operands, got " + operands.size(), node);
        DBSPExpression left = operands.get(0);
        DBSPExpression right = operands.get(1);
        Utilities.enforce(left != null && right != null, "Null operand for binary expression " + opcode + ": " + left + ", " + right);
        return makeBinaryExpression(node, type, opcode, left, right);
    }

    /** Creates a call to the INDICATOR function, which returns 0 for None and 1 for Some.
     * For tuples it returns the multiplication of the indicator for all fields.
     * This is used in the implementation of COUNT(a, b). */
    public static DBSPExpression makeIndicator(
            CalciteObject node, DBSPType resultType, DBSPExpression argument) {
        Utilities.enforce(!resultType.mayBeNull);
        Utilities.enforce(resultType.is(IsNumericType.class));
        DBSPType argType = argument.getType();
        if (argType.is(DBSPTypeTuple.class) &&
                argType.to(DBSPTypeTuple.class).originalStruct == null) {
            // This means this is a composite count: COUNT(a, b, c)
            DBSPTypeTupleBase tuple = argument.getType().to(DBSPTypeTuple.class);
            DBSPExpression result = resultType.to(IsNumericType.class).getOne();
            for (int i = 0; i < tuple.size(); i++) {
                DBSPExpression next = makeIndicator(node, resultType, argument.field(i));
                result = new DBSPBinaryExpression(node, resultType, DBSPOpcode.MUL, result, next);
            }
            return result;
        } else {
            // scalar types, but not only.  Nullable structs fall here too.
            if (!argType.mayBeNull) {
                return resultType.to(IsNumericType.class).getOne();
            } else {
                return new DBSPUnaryExpression(node, resultType, DBSPOpcode.INDICATOR, argument.borrow());
            }
        }
    }

    static DBSPOpcode timestampOperation(DBSPOpcode opcode) {
        return switch (opcode) {
            case ADD -> DBSPOpcode.TS_ADD;
            case SUB -> DBSPOpcode.TS_SUB;
            case MUL -> DBSPOpcode.INTERVAL_MUL;
            case DIV -> DBSPOpcode.INTERVAL_DIV;
            default -> throw new InternalCompilerError("Unexpected opcode " + opcode);
        };
    }

    public static DBSPExpression makeBinaryExpression(
            CalciteObject node, DBSPType type, DBSPOpcode opcode,
            DBSPExpression left, DBSPExpression right) {
        if (opcode.isStrict()) {
            if (left.is(DBSPLiteral.class) && left.to(DBSPLiteral.class).isNull())
                return type.none();
            if (right.is(DBSPLiteral.class) && right.to(DBSPLiteral.class).isNull())
                return type.none();
        }

        // Unfortunately Calcite does not insert implicit casts, so operations
        // may have different argument and result types.  So here we have to
        // potentially insert casts on either operands, and on the result.
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        boolean anyNull = leftType.mayBeNull || rightType.mayBeNull;
        DBSPType typeWithNull = type.withMayBeNull(anyNull);

        Utilities.enforce(opcode != DBSPOpcode.DIV_NULL || type.mayBeNull, "DIV_NULL should produce a nullable result");
        // Type produced by this operation; if different from 'type', a cast may be needed.
        DBSPType expressionResultType;
        if (needCommonType(opcode, type, leftType, rightType)) {
            // Need to cast both operands to a common type.  Find out what it is.
            DBSPType commonBase = TypeCompiler.reduceType(node, leftType, rightType, "", false);
            expressionResultType = commonBase.withMayBeNull(anyNull);
            if (commonBase.is(DBSPTypeDecimal.class))
                expressionResultType = DBSPTypeDecimal.getDefault().withMayBeNull(anyNull);  // no limits
            if (opcode == DBSPOpcode.BW_AND ||
                    opcode == DBSPOpcode.BW_OR || opcode == DBSPOpcode.XOR ||
                    opcode == DBSPOpcode.MAX || opcode == DBSPOpcode.MIN ||
                    opcode == DBSPOpcode.ADD ||
                    opcode == DBSPOpcode.SUB || opcode == DBSPOpcode.MUL) {
                // Use the inferred Calcite type for the output as the common type
                commonBase = typeWithNull;
                expressionResultType = commonBase;
            }
            if (opcode.isComparison()) {
                expressionResultType = DBSPTypeBool.create(anyNull);
            } else if (opcode == DBSPOpcode.IS_DISTINCT)
                // Never null
                expressionResultType = DBSPTypeBool.create(false);
            if (commonBase.is(DBSPTypeNull.class)) {
                // Result is always NULL - evaluate to the NULL literal directly
                return DBSPLiteral.none(type);
            }
            if (leftType.code == NULL || !leftType.withMayBeNull(false).sameType(commonBase))
                left = left.cast(node, commonBase.withMayBeNull(leftType.mayBeNull), false);
            if (rightType.code == NULL || !rightType.withMayBeNull(false).sameType(commonBase))
                right = right.cast(node, commonBase.withMayBeNull(rightType.mayBeNull), false);
        } else {
            // no common base.  Cases:
            // - one operand is a date/time/timestamp
            // - one operand is a string
            // - one operand is a binary
            // - both operands are decimal
            if (leftType.is(DBSPTypeNull.class) || rightType.is(DBSPTypeNull.class)) {
                // Result is always NULL - evaluate to the NULL literal directly
                return DBSPLiteral.none(type);
            }
            if (opcode == DBSPOpcode.MUL || opcode == DBSPOpcode.DIV) {
                // Multiplication between an interval and a numeric value.
                if (leftType.is(IsIntervalType.class) || rightType.is(IsIntervalType.class)) {
                    opcode = timestampOperation(opcode);
                    // swap operands so that the numeric operand is always right
                    if (opcode == DBSPOpcode.INTERVAL_MUL || opcode == DBSPOpcode.INTERVAL_DIV) {
                        if (rightType.is(IsIntervalType.class)) {
                            if (leftType.is(IsIntervalType.class)) {
                                throw new CompilationError("Operation " + opcode + " between intervals not supported", node);
                            }
                            DBSPExpression tmp = left;
                            left = right;
                            right = tmp;

                            leftType = left.getType();
                            rightType = right.getType();
                        }

                        Utilities.enforce(rightType.is(IsNumericType.class));
                        // Canonicalize the type on the right without information loss
                        if (rightType.is(DBSPTypeInteger.class)) {
                            if (leftType.is(DBSPTypeMillisInterval.class)) {
                                right = right.cast(node, DBSPTypeInteger.getType(rightType.getNode(), INT64, rightType.mayBeNull), false);
                            } else {
                                right = right.cast(node, DBSPTypeInteger.getType(rightType.getNode(), INT32, rightType.mayBeNull), false);
                            }
                        } else if (rightType.is(DBSPTypeReal.class)) {
                            right = right.cast(node, new DBSPTypeDouble(rightType.getNode(), rightType.mayBeNull), false);
                        }
                        rightType = right.getType();
                    }
                }
            }
            if (opcode == DBSPOpcode.SUB || opcode == DBSPOpcode.ADD) {
                // Addition involving a date
                if (leftType.is(IsTimeRelatedType.class) || rightType.is(IsTimeRelatedType.class)) {
                    if (rightType.is(IsNumericType.class))
                        throw new CompilationError("Cannot apply operation " + Utilities.singleQuote(opcode.toString()) +
                                " to arguments of type " + leftType.asSqlString() + " and " + rightType.asSqlString(), node);
                    opcode = timestampOperation(opcode);
                    if (leftType.is(IsIntervalType.class) && !rightType.is(IsIntervalType.class)) {
                        // Move the interval to the right when computing a date +/- interval
                        DBSPExpression tmp = left;
                        left = right;
                        right = tmp;
                        leftType = left.getType();
                        rightType = right.getType();
                    }
                }
            }
            if (leftType.is(IsTimeRelatedType.class) || rightType.is(IsTimeRelatedType.class))
                expressionResultType = typeWithNull;
            else if (leftType.is(DBSPTypeString.class) && rightType.is(DBSPTypeString.class))
                expressionResultType = typeWithNull;
            else if (leftType.is(DBSPTypeBinary.class) && rightType.is(DBSPTypeBinary.class))
                expressionResultType = typeWithNull;
            else if (leftType.is(DBSPTypeDecimal.class) && rightType.is(DBSPTypeDecimal.class))
                expressionResultType = typeWithNull;
            else
                throw new UnsupportedException("Operation " + opcode + " on " + leftType + " and " + rightType, node);
            if (opcode.isComparison()) {
                expressionResultType = DBSPTypeBool.create(anyNull);
            }
        }
        DBSPExpression call = new DBSPBinaryExpression(node, expressionResultType, opcode, left, right);
        return call.cast(node, type, false);
    }

    public static DBSPExpression makeUnaryExpression(
            CalciteObject node, DBSPType type, DBSPOpcode op, List<DBSPExpression> operands) {
        if (operands.size() != 1)
            throw new InternalCompilerError("Expected 1 operands, got " + operands.size(), node);
        DBSPExpression operand = operands.get(0);
        if (operand == null)
            throw new UnimplementedException("Found unimplemented expression in " + node, node);
        DBSPType resultType = operand.getType();
        if (op.toString().startsWith("is_"))
            // these do not produce nullable results
            resultType = resultType.withMayBeNull(false);
        DBSPExpression expr = new DBSPUnaryExpression(node, resultType, op, operand);
        return expr.cast(node, type, false);
    }

    static void validateArgCount(CalciteObject node, String name, int argCount, Integer... expectedArgCount) {
        boolean legal = false;
        for (int e: expectedArgCount) {
            if (e == argCount) {
                legal = true;
                break;
            }
        }
        if (!legal) {
            throw operandCountError(node, name, argCount);
        }
    }

    static String getCallName(RexCall call) {
        return call.op.getName().toLowerCase();
    }

    public static DBSPExpression compilePolymorphicFunction(
            boolean adjustNullability, String opName, CalciteObject node, DBSPType resultType,
            List<DBSPExpression> ops, Integer... expectedArgCount) {
        validateArgCount(node, opName, ops.size(), expectedArgCount);
        StringBuilder functionName = new StringBuilder(opName);
        DBSPExpression[] operands = ops.toArray(new DBSPExpression[0]);
        boolean resultNullable = false;
        for (DBSPExpression op: ops) {
            DBSPType type = op.getType();
            // Form the function name from the argument types
            functionName.append("_").append(type.baseTypeWithSuffix());
            if (type.mayBeNull)
                resultNullable = true;
        }
        DBSPType intermediateResultType = resultType;
        if (adjustNullability && resultType.mayBeNull && !resultNullable) {
            intermediateResultType = resultType.withMayBeNull(false);
        }
        DBSPExpression result = new DBSPApplyExpression(node, functionName.toString(), intermediateResultType, operands);
        if (resultType.sameType(intermediateResultType))
            return result;
        return result.castToNullable();
    }

    /**
     * Compile a function call into a family of Rust functions,
     * depending on the argument types.
     * @param  call Operation that is compiled.
     * @param  node CalciteObject holding the call.
     * @param  adjustNullability  If true infer nullability from arguments' nullability.  Else trust the supplied type.
     * @param  resultType Type of result produced by call.  We assume that
     *                    the typechecker is right, and this is the correct
     *                    result produced by this function, modulo nullability.  No cast needed.
     * @param  ops  Translated operands for the call.
     * @param  expectedArgCount A list containing all known possible argument counts. */
    static DBSPExpression compilePolymorphicFunction(
            boolean adjustNullability,
            RexCall call, CalciteObject node, DBSPType resultType,
            List<DBSPExpression> ops, Integer... expectedArgCount) {
        String opName = getCallName(call);
        return compilePolymorphicFunction(adjustNullability, opName, node, resultType, ops, expectedArgCount);
    }

    static String typeString(DBSPType type) {
        DBSPTypeArray vec = type.as(DBSPTypeArray.class);
        String result = "";
        if (vec != null)
            // This is the reverse of what you may expect
            result = vec.getElementType().nullableUnderlineSuffix() + "vec";
        result += type.nullableUnderlineSuffix();
        return result;
    }

    /**
     * Compile a function call into a Rust function.
     *
     * @param baseName         Base name of the called function in Rust.
     *                         To this name we append information about argument nullabilty.
     * @param node             CalciteObject holding the call.
     * @param resultType       Type of result produced by call.
     * @param ops              Translated operands for the call.
     * @param expectedArgCount A list containing all known possible argument counts.
     */
    static DBSPExpression compileFunction(String baseName, CalciteObject node,
                                          DBSPType resultType, List<DBSPExpression> ops, Integer... expectedArgCount) {
        StringBuilder builder = new StringBuilder(baseName);
        validateArgCount(node, baseName, ops.size(), expectedArgCount);
        DBSPExpression[] operands = ops.toArray(new DBSPExpression[0]);
        if (expectedArgCount.length > 1)
            // If the function can have a variable number of arguments, postfix with the argument count
            builder.append(operands.length);
        for (DBSPExpression e: ops) {
            DBSPType type = e.getType();
            builder.append(typeString(type));
        }
        return new DBSPApplyExpression(node, builder.toString(), resultType, operands);
    }

    /**
     * Compile a function call into a Rust function.
     * @param  call Call that is being compiled.
     * @param  node CalciteObject holding the call.
     * @param  resultType Type of result produced by call.
     * @param  ops  Translated operands for the call.
     * @param  expectedArgCount A list containing all known possible argument counts.
     */
    static DBSPExpression compileFunction(
            RexCall call, CalciteObject node, DBSPType resultType,
            List<DBSPExpression> ops, Integer... expectedArgCount) {
        return compileFunction(getCallName(call), node, resultType, ops, expectedArgCount);
    }

    /**
     * Compile a function call into a Rust function; if any of the arguments is NULL, the result is NULL.
     * @param  call Call that is being compiled.
     * @param  node CalciteObject holding the call.
     * @param  resultType Type of result produced by call.
     * @param  ops  Translated operands for the call.
     * @param  expectedArgCount A list containing all known possible argument counts.
     */
    static DBSPExpression compileStrictFunction(
            RexCall call, CalciteObject node, DBSPType resultType,
            List<DBSPExpression> ops, Integer... expectedArgCount) {
        for (DBSPExpression op: ops) {
            if (op.is(DBSPLiteral.class) && op.to(DBSPLiteral.class).isNull())
                return resultType.none();
        }
        return compileFunction(call, node, resultType, ops, expectedArgCount);
    }

    /**
     * Compile a function call into a Rust function.
     * One of the arguments is a keyword.
     * @param  call Call operation that is translated.
     * @param  node CalciteObject holding the call.
     * @param  functionName Name to use for function; if not specified name is derived from call.
     * @param  resultType Type of result produced by call.
     * @param  ops  Translated operands for the call.
     * @param  keywordIndex  Index in ops of the argument that is a keyword.
     * @param  expectedArgCount A list containing all known possible argument counts.
     */
    static DBSPExpression compileKeywordFunction(
            RexCall call, CalciteObject node, @Nullable String functionName,
            DBSPType resultType, List<DBSPExpression> ops,
            int keywordIndex, Integer... expectedArgCount) {
        DBSPKeywordLiteral keyword = ops.get(keywordIndex).to(DBSPKeywordLiteral.class);
        StringBuilder name = new StringBuilder();
        String baseName = functionName != null ? functionName : getCallName(call);
        validateArgCount(node, baseName, ops.size(), expectedArgCount);
        if (ops.size() <= keywordIndex)
            throw operandCountError(node, baseName,  ops.size());
        name.append(baseName)
                .append("_")
                .append(keyword);
        DBSPExpression[] operands = new DBSPExpression[ops.size() - 1];
        int index = 0;
        for (int i = 0; i < ops.size(); i++) {
            DBSPExpression op = ops.get(i);
            if (i == keywordIndex)
                continue;
            operands[index] = op;
            index++;
            name.append("_").append(op.getType().baseTypeWithSuffix());
        }
        return new DBSPApplyExpression(node, name.toString(), resultType, operands);
    }

    void ensureString(List<DBSPExpression> ops, int argument) {
        DBSPExpression arg = ops.get(argument);
        if (!arg.getType().is(DBSPTypeString.class))
            ops.set(argument, arg.cast(arg.getNode(), DBSPTypeString.varchar(arg.getType().mayBeNull), false));
    }

    void ensureDouble(CalciteObject node, List<DBSPExpression> ops, int argument) {
        DBSPExpression arg = ops.get(argument);
        if (!arg.getType().is(DBSPTypeDouble.class))
            ops.set(argument, arg.cast(node, new DBSPTypeDouble(arg.getType().getNode(), arg.getType().mayBeNull), false));
    }

    void ensureInteger(CalciteObject node, List<DBSPExpression> ops, int argument) {
        DBSPExpression arg = ops.get(argument);
        DBSPTypeInteger expected = DBSPTypeInteger.getType(arg.getType().getNode(), INT32, arg.getType().mayBeNull);
        if (!arg.getType().sameType(expected))
            ops.set(argument, arg.cast(node, expected, false));
    }

    @SuppressWarnings("SameParameterValue")
    void nullLiteralToNullArray(List<DBSPExpression> ops, int arg) {
        if (ops.get(arg).is(DBSPNullLiteral.class)) {
            ops.set(arg, new DBSPTypeArray(DBSPTypeNull.INSTANCE, true).nullValue());
        }
    }

    String getArrayOrMapCallName(RexCall call, DBSPExpression... ops) {
        String method = getCallName(call);
        StringBuilder stringBuilder = new StringBuilder(method);
        for (DBSPExpression op : ops)
            stringBuilder.append(op.getType().nullableUnderlineSuffix());
        return stringBuilder.toString();
    }

    /**
     * Creates the method name of the array function with the final character
     * being "_" (not null) or "N" (nullable) based on the nullability of the array elements
     * <br>
     * Example:
     * <br>
     * if array_max({1?, 2?, null, 3?}) => array_max_N <br>
     * if array_max({1?, 2?, null, 3?}?) => array_maxNN
     * @param call  The call that is being compiled.
     * @param ops   Translated operands for the call.
     * @return  The method name with final character considering the nullability of array elements
     */
    String getArrayCallNameWithElemNullability(RexCall call, DBSPExpression... ops) {
        String s = getArrayOrMapCallName(call, ops);
        DBSPTypeArray vec = ops[0].type.to(DBSPTypeArray.class);
        DBSPType elemType = vec.getElementType();
        s = s + elemType.nullableUnderlineSuffix();
        return s;
    }

    DBSPArrayExpression arrayConstructor(CalciteObject node, DBSPType type, List<DBSPExpression> ops) {
        DBSPTypeArray vec = type.to(DBSPTypeArray.class);
        DBSPType elemType = vec.getElementType();
        List<DBSPExpression> args = Linq.map(ops, o -> o.cast(node, elemType, false));
        return new DBSPArrayExpression(node, type, args);
    }

    /** Ensures that all the elements of array 'vec' are of the expectedType
     * @param vec the Array of elements
     * @param expectedElementType the expected type of the elements */
    DBSPExpression ensureArrayElementsOfType(CalciteObject node, DBSPExpression vec, DBSPType expectedElementType) {
        DBSPTypeArray argType = vec.getType().to(DBSPTypeArray.class);
        DBSPType argElemType = argType.getElementType();
        DBSPType expectedVecType = new DBSPTypeArray(expectedElementType, vec.type.mayBeNull);
        if (!argElemType.sameType(expectedElementType))
            vec = vec.cast(node, expectedVecType, false);
        return vec;
    }

    @Override
    public DBSPExpression visitFieldAccess(RexFieldAccess field) {
        CalciteObject node = CalciteObject.create(this.context, field);
        DBSPExpression source = field.getReferenceExpr().accept(this);
        RelDataTypeField dataField = field.getField();
        return new DBSPFieldExpression(node, source, dataField.getIndex()).applyCloneIfNeeded();
    }

    static CompilationError operandCountError(CalciteObject node, String name, int operandCount) {
        return new CompilationError(
                "Function " + Utilities.singleQuote(name) + " with " +
                        operandCount + " arguments is unknown", node);
    }

    /** Compile a string literal into a static Regex object and return it. */
    @Nullable
    DBSPExpression makeRegex(DBSPStringLiteral lit) {
        DBSPTypeUser user = new DBSPTypeUser(CalciteObject.EMPTY, USER, "Regex", true);
        // Here we lie about the type: new does not return an Regex, but a Result<Regex, Error>.
        // We lie again that ok returns an unchanged type.  These two lies cancel out.
        DBSPExpression init = user.constructor("new", lit.toStr());
        init = init.applyMethod("ok", init.getType());
        String name = DBSPStaticExpression.generateName(init, this.compiler);
        return new DBSPStaticExpression(lit.getNode(), init, name).borrow();
    }

    /** Check for polymorphic strict functions: if any operand is the NULL literal, replace with NULL */
    @Nullable
    DBSPExpression strictnessCheck(List<DBSPExpression> ops, DBSPType resultType) {
        for (DBSPExpression op: ops) {
            if (op.is(DBSPLiteral.class) && op.to(DBSPLiteral.class).isNull())
                return resultType.none();
        }
        return null;
    }

    @Override
    public DBSPExpression visitCall(RexCall call) {
        CalciteObject node = CalciteObject.create(this.context, call);
        DBSPType type = this.typeCompiler.convertType(call.getType(), false);
        // If type is NULL we can skip the call altogether...
        if (type.is(DBSPTypeNull.class))
            return DBSPNullLiteral.INSTANCE;
        Utilities.enforce(!type.is(DBSPTypeStruct.class));

        final RexCall finalCall = call;
        Logger.INSTANCE.belowLevel(this, 2)
                .appendSupplier(finalCall::toString)
                .append(" ")
                .appendSupplier(() -> finalCall.getType().toString());
        if (call.op.kind == SqlKind.SEARCH) {
            // TODO: Ideally the optimizer should do this before handing the expression to us.
            // Then the rexBuilder won't be needed.
            call = (RexCall) RexUtil.expandSearch(this.rexBuilder, null, call);
        }
        List<DBSPExpression> ops = Linq.map(call.operands, e -> e.accept(this));
        String operationName = call.op.kind.sql;
        switch (call.op.kind) {
            case CHECKED_TIMES:
            case TIMES:
                return makeBinaryExpression(node, type, DBSPOpcode.MUL, ops);
            case CHECKED_DIVIDE:
            case DIVIDE:
                return makeBinaryExpression(node, type, DBSPOpcode.DIV, ops);
            case MOD:
                return makeBinaryExpression(node, type, DBSPOpcode.MOD, ops);
            case CHECKED_PLUS:
            case PLUS:
                return makeBinaryExpressions(node, type, DBSPOpcode.ADD, ops);
            case CHECKED_MINUS:
            case MINUS:
                return makeBinaryExpression(node, type, DBSPOpcode.SUB, ops);
            case LESS_THAN:
                return makeBinaryExpression(node, type, DBSPOpcode.LT, ops);
            case GREATER_THAN:
                return makeBinaryExpression(node, type, DBSPOpcode.GT, ops);
            case LESS_THAN_OR_EQUAL:
                return makeBinaryExpression(node, type, DBSPOpcode.LTE, ops);
            case GREATER_THAN_OR_EQUAL:
                return makeBinaryExpression(node, type, DBSPOpcode.GTE, ops);
            case EQUALS:
                return makeBinaryExpression(node, type, DBSPOpcode.EQ, ops);
            case IS_DISTINCT_FROM:
                return makeBinaryExpression(node, type, DBSPOpcode.IS_DISTINCT, ops);
            case IS_NOT_DISTINCT_FROM:
                return makeBinaryExpression(node, type, DBSPOpcode.IS_DISTINCT, ops).not();
            case NOT_EQUALS:
                return makeBinaryExpression(node, type, DBSPOpcode.NEQ, ops);
            case OR:
                return makeBinaryExpressions(node, type, DBSPOpcode.OR, ops);
            case AND:
                return makeBinaryExpressions(node, type, DBSPOpcode.AND, ops);
            case NOT:
                return makeUnaryExpression(node, type, DBSPOpcode.NOT, ops);
            case IS_FALSE:
                return makeUnaryExpression(node, type, DBSPOpcode.IS_FALSE, ops);
            case IS_NOT_TRUE:
                return makeUnaryExpression(node, type, DBSPOpcode.IS_NOT_TRUE, ops);
            case IS_TRUE:
                return makeUnaryExpression(node, type, DBSPOpcode.IS_TRUE, ops);
            case IS_NOT_FALSE:
                return makeUnaryExpression(node, type, DBSPOpcode.IS_NOT_FALSE, ops);
            case PLUS_PREFIX:
                return makeUnaryExpression(node, type, DBSPOpcode.UNARY_PLUS, ops);
            case CHECKED_MINUS_PREFIX:
            case MINUS_PREFIX:
                return makeUnaryExpression(node, type, DBSPOpcode.NEG, ops);
            case BIT_AND:
                return makeBinaryExpressions(node, type, DBSPOpcode.BW_AND, ops);
            case BIT_OR:
                return makeBinaryExpressions(node, type, DBSPOpcode.BW_OR, ops);
            case BIT_XOR:
                return makeBinaryExpressions(node, type, DBSPOpcode.XOR, ops);
            case CAST:
            case SAFE_CAST:
            case REINTERPRET:
                return ops.get(0).applyCloneIfNeeded().cast(node, type, call.op.kind == SqlKind.SAFE_CAST);
            case IS_NULL:
            case IS_NOT_NULL: {
                if (!type.sameType(new DBSPTypeBool(CalciteObject.EMPTY, false)))
                    throw new InternalCompilerError("Expected expression to produce a boolean result", node);
                DBSPExpression arg = ops.get(0);
                DBSPType argType = arg.getType();
                if (argType.mayBeNull) {
                    if (call.op.kind == SqlKind.IS_NULL)
                        return ops.get(0).is_null();
                    else
                        return new DBSPUnaryExpression(node, type, DBSPOpcode.NOT, ops.get(0).is_null());
                } else {
                    // Constant-fold
                    if (call.op.kind == SqlKind.IS_NULL)
                        return new DBSPBoolLiteral(false);
                    else
                        return new DBSPBoolLiteral(true);
                }
            }
            case CASE: {
                /*
                A switched case (CASE x WHEN x1 THEN v1 ... ELSE e END)
                has an even number of arguments and odd-numbered arguments are predicates.
                A condition case (CASE WHEN p1 THEN v1 ... ELSE e END) has an odd number of
                arguments and even-numbered arguments are predicates, except for the last argument.
                */
                DBSPExpression result = ops.get(ops.size() - 1);
                if (ops.size() % 2 == 0) {
                    DBSPExpression value = ops.get(0);
                    // Compute casts if needed.
                    DBSPType finalType = result.getType();
                    for (int i = 1; i < ops.size() - 1; i += 2) {
                        if (ops.get(i + 1).getType().mayBeNull)
                            finalType = finalType.withMayBeNull(true);
                    }
                    if (!result.getType().sameType(finalType))
                        result = result.cast(node, finalType, false);
                    for (int i = 1; i < ops.size() - 1; i += 2) {
                        DBSPExpression alt = ops.get(i + 1);
                        if (!alt.getType().sameType(finalType))
                            alt = alt.cast(node, finalType, false);
                        DBSPExpression comp = makeBinaryExpression(
                                node, new DBSPTypeBool(CalciteObject.EMPTY, false), DBSPOpcode.EQ,
                                value, ops.get(i));
                        comp = comp.wrapBoolIfNeeded();
                        result = new DBSPIfExpression(node, comp, alt, result);
                    }
                } else {
                    // Compute casts if needed.
                    // Build this backwards
                    DBSPType finalType = result.getType();
                    for (int i = 0; i < ops.size() - 1; i += 2) {
                        int index = ops.size() - i - 2;
                        if (ops.get(index).getType().mayBeNull)
                            finalType = finalType.withMayBeNull(true);
                    }

                    if (!result.getType().sameType(finalType))
                        result = result.applyCloneIfNeeded().cast(node, finalType, false);
                    for (int i = 0; i < ops.size() - 1; i += 2) {
                        int index = ops.size() - i - 2;
                        DBSPExpression alt = ops.get(index);
                        if (!alt.getType().sameType(finalType))
                            alt = alt.applyCloneIfNeeded().cast(node, finalType, false);
                        DBSPExpression condition = ops.get(index - 1).wrapBoolIfNeeded();
                        result = new DBSPIfExpression(node, condition, alt, result);
                    }
                }
                return result;
            }
            case CHAR_LENGTH: {
                validateArgCount(node, operationName, ops.size(), 1);
                this.ensureString(ops, 0);
                return compileFunction(call, node, type, ops, 1);
            }
            case ST_POINT: {
                // Sometimes the Calcite type for ST_POINT is nullable
                // even if all arguments are not nullable.  So we can't
                // just use compilePolymorphicFunction.
                if (ops.size() != 2)
                    throw operandCountError(node, operationName, ops.size());
                for (int i = 0; i < 2; i++)
                    this.ensureDouble(node, ops, i);
                DBSPExpression left = ops.get(0);
                DBSPExpression right = ops.get(1);
                String functionName = "make_geopoint" +
                        left.getType().nullableUnderlineSuffix() +
                        right.getType().nullableUnderlineSuffix();
                boolean resultIsNull = left.getType().mayBeNull || right.getType().mayBeNull;
                DBSPExpression expr = this.strictnessCheck(ops, type.withMayBeNull(resultIsNull));
                if (expr != null)
                    return expr;
                return new DBSPApplyExpression(node, functionName, type.withMayBeNull(resultIsNull), left, right)
                        .cast(node, type, false);
            }
            case OTHER_FUNCTION: {
                String opName = call.op.getName().toLowerCase();
                switch (opName) {
                    case "trunc":
                    case "truncate":
                    case "round": {
                        DBSPExpression right;

                        if (call.operands.isEmpty())
                            throw operandCountError(node, operationName, call.operandCount());
                        DBSPExpression left = ops.get(0);
                        if (call.operandCount() == 1)
                            right = new DBSPI32Literal(0);
                        else
                            right = ops.get(1);

                        DBSPType leftType = left.getType();
                        DBSPType rightType = right.getType();

                        if (leftType.is(DBSPTypeInteger.class)) {
                            this.compiler.reportWarning(node.getPositionRange(),
                                    "Useless operation",
                                    Utilities.singleQuote(opName) + " applied to integer value does nothing");
                            return left;
                        }

                        if (rightType.is(DBSPTypeNull.class) ||
                                (right.is(DBSPLiteral.class) && right.to(DBSPLiteral.class).isNull())) {
                            this.compiler.reportWarning(node.getPositionRange(),
                                    "evaluates to NULL", node + ": always returns NULL");
                            return type.nullValue();
                        }

                        if (!rightType.is(DBSPTypeInteger.class))
                            throw new UnimplementedException(Utilities.singleQuote(opName) +
                                    " expects a constant second argument", node);

                        // convert to int32
                        DBSPTypeInteger rightInt = rightType.to(DBSPTypeInteger.class);
                        if (rightInt.getWidth() != 32) {
                            right = right.cast(node,
                                    DBSPTypeInteger.getType(right.getNode(), INT32, rightType.mayBeNull), false);
                        }

                        return compilePolymorphicFunction(true, call, node, type, Linq.list(left, right), 2);
                    }
                    case "abs": {
                        String opName1 = getCallName(call);
                        validateArgCount(node, opName1, ops.size(), 1);
                        StringBuilder functionName = new StringBuilder(opName1);
                        DBSPExpression[] operands = ops.toArray(new DBSPExpression[0]);
                        boolean resultNullable = false;
                        for (DBSPExpression op: ops) {
                            DBSPType type1 = op.getType();
                            if (type1.is(IsIntervalType.class)) {
                                var base = type1.to(DBSPTypeBaseType.class);
                                functionName.append("_").append(base.shortName())
                                        .append(base.nullableSuffix());
                            } else {
                                functionName.append("_").append(type1.baseTypeWithSuffix());
                            }
                            if (type1.mayBeNull)
                                resultNullable = true;
                        }
                        DBSPType intermediateResultType = type;
                        if (type.mayBeNull && !resultNullable) {
                            intermediateResultType = type.withMayBeNull(false);
                        }
                        DBSPExpression result = new DBSPApplyExpression(node, functionName.toString(), intermediateResultType, operands);
                        if (type.sameType(intermediateResultType))
                            return result;
                        return result.castToNullable();
                    }
                    case "numeric_inc", "sign", "md5": {
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        return compilePolymorphicFunction(true, call, node, type,
                                ops, 1);
                    }
                    case "st_distance": {
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        return compilePolymorphicFunction(true, call, node, type,
                                ops, 2);
                    }
                    case "log10":
                    case "ln": {
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        // Cast to Double
                        this.ensureDouble(node, ops, 0);
                        // See: https://github.com/feldera/feldera/issues/1363
                        if (!ops.get(0).type.mayBeNull) {
                            type = type.withMayBeNull(false);
                        }
                        return compilePolymorphicFunction(true, call, node, type, ops, 1);
                    }
                    case "log": {
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        // Turn the arguments into Double
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureDouble(node, ops, i);
                        return compilePolymorphicFunction(true, call, node, type, ops, 1, 2);
                    }
                    case "power": {
                        validateArgCount(node, operationName, ops.size(), 2);
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        // convert integer to double
                        DBSPExpression firstArg = ops.get(0);
                        if (firstArg.type.is(DBSPTypeInteger.class))
                            this.ensureDouble(node, ops, 0);
                        if (ops.get(1).type.is(DBSPTypeInteger.class))
                            this.ensureInteger(node, ops, 1);

                        DBSPExpression argument = ops.get(1);
                        if (argument.is(DBSPDecimalLiteral.class)) {
                            // power(a, .5) -> sqrt(a).  This is more precise.
                            // Calcite does the opposite conversion.
                            DBSPDecimalLiteral dec = argument.to(DBSPDecimalLiteral.class);
                            BigDecimal pointFive = new BigDecimal(5).movePointLeft(1);
                            if (!dec.isNull() && Objects.requireNonNull(dec.value).equals(pointFive)) {
                                ops = Linq.list(ops.get(0));
                                if (ops.get(0).getType().is(DBSPTypeNull.class)) {
                                    ops.set(0, ops.get(0).cast(node, type, false));
                                }
                                this.ensureDouble(node, ops, 0);
                                return compilePolymorphicFunction(
                                        true, "sqrt", node, type, ops, 1);
                            }
                        }

                        // Cast real or decimal to double
                        for (int i = 0; i < ops.size(); i++) {
                            if (ops.get(i).type.code == REAL || ops.get(i).type.code == DECIMAL) {
                                this.ensureDouble(node, ops, i);
                            }
                        }

                        return compilePolymorphicFunction(true, call, node, type, ops, 2);
                    }
                    case "pi": {
                        return compileFunction(call, node, type, ops, 0);
                    }
                    case "sin":
                    case "sinh":
                    case "cos":
                    case "cosh":
                    case "tan":
                    case "tanh":
                    case "atanh":
                    case "cot":
                    case "coth":
                    case "asin":
                    case "asinh":
                    case "acos":
                    case "acosh":
                    case "atan":
                    case "radians":
                    case "degrees":
                    case "cbrt":
                    case "sec":
                    case "sech":
                    case "csc":
                    case "csch":
                    case "exp": {
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        this.ensureDouble(node, ops, 0);
                        return compilePolymorphicFunction(true, call, node, type, ops, 1);
                    }
                    case "is_inf":
                    case "is_nan": {
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        // Turn the argument into Double
                        if (!ops.get(0).type.is(DBSPTypeReal.class)) {
                            this.ensureDouble(node, ops, 0);
                        }
                        return compilePolymorphicFunction(true, call, node, type, ops, 1);
                    }
                    case "atan2": {
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureDouble(node, ops, i);
                        return compilePolymorphicFunction(true, call, node, type, ops, 2);
                    }
                    case "split":
                        // Calcite should be doing this, but it doesn't.
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureString(ops, i);
                        return compileFunction(call, node, type, ops, 1, 2);
                    case "split_part": {
                        this.ensureString(ops, 0);
                        this.ensureString(ops, 1);
                        this.ensureInteger(node, ops, 2);
                        return compileFunction(call, node, type, ops, 3);
                    }
                    case "overlay": {
                        validateArgCount(node, opName, ops.size(), 3, 4);
                        String module_prefix = "";
                        if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                            module_prefix = "binary_";
                        }
                        if (ops.get(0).getType().code != ops.get(1).getType().code) {
                            throw new CompilationError(
                                    "First and second arguments to 'OVERLAY' must have the same type; the types are\n" +
                                    ops.get(0).getType().asSqlString() + " and " + ops.get(1).getType().asSqlString(),
                                    node);
                        }
                        this.ensureInteger(node, ops, 2);
                        if (ops.size() == 4)
                            this.ensureInteger(node, ops, 3);
                        return compileFunction(module_prefix + getCallName(call), node, type, ops, 3, 4);
                    }
                    case "chr": {
                        validateArgCount(node, opName, ops.size(), 1);
                        this.ensureInteger(node, ops, 0);
                        return compileStrictFunction(call, node, type, ops, 1);
                    }
                    case "ascii":
                    case "lower":
                    case "upper":
                    case "initcap":
                    case "initcap_spaces":
                        validateArgCount(node, opName, ops.size(), 1);
                        this.ensureString(ops, 0);
                        // fall through
                    case "to_hex":
                    case "octet_length": {
                        return compileStrictFunction(call, node, type, ops, 1);
                    }
                    case "cardinality": {
                        validateArgCount(node, opName, ops.size(), 1);
                        String name = opName;
                        nullLiteralToNullArray(ops, 0);
                        DBSPExpression op0 = ops.get(0);
                        DBSPType arg0Type = op0.getType();
                        if (arg0Type.is(DBSPTypeArray.class))
                            name += "Vec";
                        else if (arg0Type.is(DBSPTypeMap.class))
                            name += "Map";
                        else
                            throw new UnimplementedException("Support for operation/function " +
                                    Utilities.singleQuote(opName) + " on type " +
                                    arg0Type.asSqlString() + " not yet implemented", 1265, node);
                        if (arg0Type.mayBeNull)
                            name += "N";
                        return new DBSPApplyExpression(node, name, type, op0);
                    }
                    case "writelog":
                        return new DBSPApplyExpression(node, opName, type, ops.get(0), ops.get(1));
                    case "repeat": {
                        validateArgCount(node, opName, ops.size(), 2);
                        if (ops.get(0).getType().code == BYTES) {
                            throw new UnimplementedException("'REPEAT' with a VAR/BINARY argument not yet supported", node);
                        }
                        this.ensureInteger(node, ops, 1);
                        return compileFunction(call, node, type, ops, 2);
                    }
                    case "right":
                    case "left": {
                        this.ensureInteger(node, ops, 1);
                        return compilePolymorphicFunction(false, call, node, type, ops, 2);
                    }
                    case "format_date":
                        return compileFunction(call, node, type, ops, 2);
                    case "bround": {
                        validateArgCount(node, opName, ops.size(), 2);
                        this.ensureInteger(node, ops, 1);
                        DBSPExpression op0 = ops.get(0);
                        if (!op0.getType().is(DBSPTypeDecimal.class)) {
                            op0 = op0.cast(node, type, false);
                            ops.set(0, op0);
                        }
                        return compilePolymorphicFunction(false, call, node, type, ops, 2);
                    }
                    case "replace":
                        validateArgCount(node, opName, ops.size(), 3);
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureString(ops, i);
                        return compileStrictFunction(call, node, type, ops, 3);
                    case "division":
                        return makeBinaryExpression(node, type, DBSPOpcode.DIV, ops);
                    case "element": {
                        DBSPExpression arg = ops.get(0);
                        DBSPTypeArray arrayType = arg.getType().to(DBSPTypeArray.class);
                        String method = "element";
                        method += arrayType.nullableUnderlineSuffix();
                        method += arrayType.getElementType().nullableUnderlineSuffix();
                        return new DBSPApplyExpression(node, method, type, arg);
                    }
                    case "substring": {
                        validateArgCount(node, opName, ops.size(), 2, 3);
                        this.ensureInteger(node, ops, 1);
                        if (ops.size() == 3)
                            this.ensureInteger(node, ops, 2);
                        String module_prefix = "";
                        if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                            module_prefix = "binary_";
                        } else {
                            this.ensureString(ops, 0);
                        }
                        return compileFunction(module_prefix + opName, node, type, ops, 2, 3);
                    }
                    case "array":
                        return this.arrayConstructor(node, type, ops);
                    case "concat": {
                        // This is the concat() function, and not the "||" operator.
                        // Currently, we only support it for VARCHAR arguments.
                        // See https://issues.apache.org/jira/browse/CALCITE-7063
                        for (DBSPExpression e: ops) {
                            if (e.getType().code != STRING) {
                                throw new UnimplementedException(
                                        "Support for CONCAT() with arguments of type " + e.getType().asSqlString(), node);
                            }
                        }
                        return makeBinaryExpressions(node, type, DBSPOpcode.CONCAT, ops);
                    }
                    case "concat_ws": {
                        this.ensureString(ops, 0);
                        DBSPExpression sep = ops.get(0);
                        if (ops.size() == 1)
                            return sep.cast(node, type, false);
                        DBSPExpression accumulator = DBSPStringLiteral.none(type.withMayBeNull(true));
                        for (int i = 1; i < ops.size(); i++) {
                            this.ensureString(ops, i);
                            accumulator = compileFunction(
                                    call, node, type, Linq.list(sep, accumulator, ops.get(i)), 3);
                        }
                        return accumulator.cast(node, type, false);
                    }
                    case "now":
                    case "variantnull":
                        return compileFunction(call, node, type, ops, 0);
                    case "gunzip":
                        DBSPExpression arg = ops.get(0);
                        ops.set(0, arg.cast(node, new DBSPTypeBinary(arg.getNode(), arg.type.mayBeNull), false));
                        return compileStrictFunction(call, node, type, ops, 1);
                    case "to_int":
                    case "typeof":
                        return compileFunction(call, node, type, ops, 1);
                    case "parse_json":
                    case "to_json":
                        DBSPExpression expr = this.strictnessCheck(ops, type);
                        if (expr != null)
                            return expr;
                        return compilePolymorphicFunction(false, call, node, type, ops, 1);
                    case "sequence":
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureInteger(node, ops, i);
                        return compileFunction(call, node, type, ops, 2);
                    case "blackbox":
                        Utilities.enforce(ops.size() == 1, "expected one argument for blackbox function");
                        return new DBSPApplyExpression(node, "blackbox", ops.get(0).type, ops.toArray(new DBSPExpression[0]));
                    case "regexp_replace": {
                        validateArgCount(node, operationName, ops.size(), 2, 3);
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureString(ops, i);
                        if (ops.get(1).is(DBSPStringLiteral.class)) {
                            DBSPStringLiteral lit = ops.get(1).to(DBSPStringLiteral.class);
                            if (lit.isNull())
                                return type.nullValue();
                            ops.set(1, this.makeRegex(lit));
                            return compileFunction("regexp_replaceC", node, type, ops, 2, 3);
                        }
                        return compileStrictFunction(call, node, type, ops, 2, 3);
                    }
                    case "parse_date":
                    case "parse_time":
                    case "parse_timestamp": {
                        validateArgCount(node, operationName, ops.size(), 2);
                        ensureString(ops, 0);
                        ensureString(ops, 1);
                        return compileStrictFunction(call, node, type, ops, 2);
                    }
                    case "timestamp_trunc":
                    case "time_trunc":
                        // Like DATE_TRUNC
                        return compileKeywordFunction(call, node, null, type, ops, 1, 2);
                    case "array_insert": {
                        validateArgCount(node, operationName, ops.size(), 3);
                        Utilities.enforce(type.is(DBSPTypeArray.class));
                        // Element type must be always nullable in result
                        Utilities.enforce(type.to(DBSPTypeArray.class).getElementType().mayBeNull);
                        this.ensureInteger(node, ops, 1);
                        DBSPExpression inserted = ops.get(2);
                        inserted = inserted.cast(node, type.to(DBSPTypeArray.class).getElementType(), false);
                        String method = getArrayCallNameWithElemNullability(call, ops.get(0), ops.get(1), inserted);
                        return new DBSPApplyExpression(node, method, type, ops.get(0), ops.get(1), inserted)
                                .cast(node, type, false);
                    }
                    case "arrays_overlap": {
                        validateArgCount(node, operationName, ops.size(), 2);
                        DBSPExpression arg0 = ops.get(0);
                        DBSPExpression arg1 = ops.get(1);
                        if (arg0.getType().is(DBSPTypeNull.class) || arg1.getType().is(DBSPTypeNull.class))
                            return type.none();

                        DBSPTypeArray arg0Vec = arg0.getType().to(DBSPTypeArray.class);
                        DBSPType arg0ElemType = arg0Vec.getElementType();

                        DBSPTypeArray arg1Vec = arg1.getType().to(DBSPTypeArray.class);
                        DBSPType arg1ElemType = arg1Vec.getElementType();

                        // if the two arrays are of different types of elements
                        if (!arg0ElemType.sameType(arg1ElemType)) {
                            // they can only differ in nullability; make them both nullable
                            arg0 = this.ensureArrayElementsOfType(node, arg0, arg0ElemType.withMayBeNull(true));
                            arg1 = this.ensureArrayElementsOfType(node, arg1, arg0ElemType.withMayBeNull(true));
                            ops.set(0, arg0);
                            ops.set(1, arg1);
                        }

                        String method = "arrays_overlap" +
                                ops.get(0).getType().nullableUnderlineSuffix() +
                                ops.get(1).getType().nullableUnderlineSuffix();
                        return new DBSPApplyExpression(node, method, type, ops.get(0), ops.get(1));
                    }
                    case "array_remove": {
                        validateArgCount(node, operationName, call.operandCount(), 2);
                        DBSPExpression arg0 = ops.get(0);
                        DBSPExpression arg1 = ops.get(1);
                        DBSPTypeArray vec = arg0.getType().to(DBSPTypeArray.class);
                        DBSPType elemType = vec.getElementType();

                        // If array is null for certain, return null
                        if (arg0.type.is(DBSPTypeNull.class)) {
                            String warningMessage =
                                    node + ": always returns NULL";
                            this.compiler.reportWarning(node.getPositionRange(), "unnecessary function call", warningMessage);
                            return DBSPNullLiteral.none(type);
                        }

                        if (!elemType.sameTypeIgnoringNullability(arg1.type)) {
                            arg1 = arg1.cast(node, elemType.withMayBeNull(arg1.type.mayBeNull), false);
                        }

                        String method = getCallName(call) +
                                arg0.type.nullableUnderlineSuffix() +
                                elemType.nullableUnderlineSuffix() +
                                arg1.type.nullableUnderlineSuffix();
                        DBSPExpression exp = new DBSPApplyExpression(node, method, type, arg0, arg1);
                        return exp.cast(node, type, false);
                    }
                    case "array_exists": {
                        validateArgCount(node, operationName, ops.size(), 2);
                        DBSPClosureExpression closure = ops.get(1).to(DBSPClosureExpression.class);
                        String method = "array_exists" +
                                ops.get(0).getType().nullableUnderlineSuffix() +
                                closure.getResultType().nullableUnderlineSuffix();
                        boolean nullable = ops.get(0).getType().mayBeNull || closure.getResultType().mayBeNull;
                        return new DBSPApplyExpression(
                                node, method, type.withMayBeNull(nullable), ops.get(0), ops.get(1))
                                .cast(node, type, false);
                    }
                    case "transform": {
                        validateArgCount(node, operationName, ops.size(), 2);
                        String method = "transform" +
                                ops.get(0).getType().nullableUnderlineSuffix();
                        boolean nullable = ops.get(0).getType().mayBeNull;
                        return new DBSPApplyExpression(
                                node, method, type.withMayBeNull(nullable), ops.get(0), ops.get(1))
                                .cast(node, type, false);
                    }
                }
                return this.compileUdfOrConstructor(node, call, type, ops);
            }
            case OTHER: {
                String opName = call.op.getName().toLowerCase();
                return switch (opName) {
                    case "||" -> makeBinaryExpression(node, type, DBSPOpcode.CONCAT, ops);
                    case "epoch", "millennium", "century", "decade", "year", "quarter", "month", "day",
                         "hour", "minute", "second", "millisecond", "microsecond", "nanosecond", "isodow", "doy" ->
                            this.handleExtract(call, type, opName, ops);
                    case "dayofweek" -> this.handleExtract(call, type, "dow", ops);
                    case "dayofmonth" -> this.handleExtract(call, type, "day", ops);
                    default -> throw new UnimplementedException("Support for operation/function " +
                            Utilities.singleQuote(opName) + " not yet implemented", 1265, node);
                };
            }
            case EXTRACT: {
                // This is also hit for "date_part", which is an alias for "extract".
                return this.handleExtract(call, type, ops);
            }
            case DATE_TRUNC: {
                return compileKeywordFunction(call, node, "date_trunc", type, ops, 1, 2);
            }
            case RLIKE: {
                validateArgCount(node, operationName, ops.size(), 2);
                for (int i = 0; i < 2; i++)
                    // Calcite does not enforce the type of the arguments, why?
                    this.ensureString(ops, i);
                // if the second argument is a constant, compile it into a static
                if (ops.get(1).is(DBSPStringLiteral.class)) {
                    DBSPStringLiteral lit = ops.get(1).to(DBSPStringLiteral.class);
                    if (lit.isNull())
                        return type.nullValue();
                    ops.set(1, this.makeRegex(lit));
                    return compileFunction("rlikeC", node, type, ops, 2);
                } else {
                    return compileFunction(call, node, type, ops, 2);
                }
            }
            case POSITION: {
                validateArgCount(node, operationName, ops.size(), 2);
                String module_prefix = "";
                if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                    module_prefix = "binary_";
                } else {
                    this.ensureString(ops, 0);
                }
                return compileFunction(module_prefix + getCallName(call), node, type, ops, 2);
            }
            case ARRAY_JOIN:
            case ARRAY_TO_STRING: {
                validateArgCount(node, operationName, ops.size(), 2, 3);
                DBSPExpression op0 = ops.get(0);
                DBSPType arg0Type = op0.getType();
                if (!arg0Type.is(DBSPTypeArray.class))
                    throw new UnsupportedException("First argument must be an array" +
                            Utilities.singleQuote(operationName), node);
                DBSPTypeArray vecType = arg0Type.to(DBSPTypeArray.class);
                if (!vecType.getElementType().is(DBSPTypeString.class)) {
                    op0 = this.ensureArrayElementsOfType(node, op0, DBSPTypeString.varchar(vecType.getElementType().mayBeNull));
                    ops.set(0, op0);
                }
                this.ensureString(ops, 1);
                if (ops.size() > 2)
                    this.ensureString(ops, 2);
                return compileFunction("array_to_string", node, type, ops, 2, 3);
            }
            case LIKE:
                // ILIKE will also match LIKE in Calcite, it's just a special case for case-insensitive matching
            case SIMILAR: {
                validateArgCount(node, operationName, ops.size(), 2, 3);
                for (int i = 0; i < ops.size(); i++)
                    // Calcite does not enforce the type of the arguments, why?
                    this.ensureString(ops, i);
                return compileStrictFunction(call, node, type, ops, 2, 3);
            }
            case FLOOR:
            case CEIL: {
                DBSPExpression expr = this.strictnessCheck(ops, type);
                if (expr != null)
                    return expr;
                if (call.operands.size() == 2) {
                    DBSPKeywordLiteral keyword = ops.get(1).to(DBSPKeywordLiteral.class);
                    if (keyword.keyword.equalsIgnoreCase("dow") ||
                        keyword.keyword.equalsIgnoreCase("doy")) {
                        throw new CompilationError("Function " + Utilities.singleQuote(operationName) +
                                " not supported with unit " + Utilities.singleQuote(keyword.keyword), node);
                    }
                    return compileKeywordFunction(call, node, null, type, ops, 1, 2);
                } else if (call.operands.size() == 1) {
                    DBSPType opType = ops.get(0).getType();
                    if (opType.is(DBSPTypeInteger.class)) {
                        this.compiler.reportWarning(node.getPositionRange(), "Noop",
                                "Function " + Utilities.singleQuote(getCallName(call))
                                        + " applied to integer arguments is useless.");
                        return ops.get(0);
                    }
                    return compilePolymorphicFunction(true, call, node, type, ops, 1);
                } else {
                    throw operandCountError(node, operationName, call.operandCount());
                }
            }
            case ARRAY_VALUE_CONSTRUCTOR:
                return this.arrayConstructor(node, type, ops);
            case MAP_VALUE_CONSTRUCTOR: {
                DBSPTypeMap map = type.to(DBSPTypeMap.class);
                List<DBSPExpression> keys = DBSPMapExpression.getKeys(ops);
                keys = Linq.map(keys, o -> o.cast(node, map.getKeyType(), false));
                List<DBSPExpression> values = DBSPMapExpression.getValues(ops);
                values = Linq.map(values, o -> o.cast(node, map.getValueType(), false));
                return new DBSPMapExpression(map, keys, values);
            }
            case ITEM: {
                if (call.operands.size() != 2)
                    throw operandCountError(node, operationName, call.operandCount());
                DBSPExpression op0 = ops.get(0);
                if (op0.is(DBSPCloneExpression.class))
                    op0 = op0.to(DBSPCloneExpression.class).expression;
                DBSPType collectionType = op0.getType();
                DBSPExpression index = ops.get(1);
                DBSPOpcode opcode = DBSPOpcode.SQL_INDEX;
                String name = call.op.getName();
                if (collectionType.is(DBSPTypeMap.class)) {
                    // index into a map
                    Utilities.enforce(name.equals("ITEM"));
                    DBSPTypeMap map = collectionType.to(DBSPTypeMap.class);
                    index = index.applyCloneIfNeeded().cast(node, map.getKeyType(), false);
                    opcode = DBSPOpcode.MAP_INDEX;
                } else if (collectionType.is(DBSPTypeVariant.class)) {
                    Utilities.enforce(name.equals("ITEM"));
                    opcode = DBSPOpcode.VARIANT_INDEX;
                } else if (collectionType.is(DBSPTypeTuple.class)) {
                    Utilities.enforce(name.equals("ITEM"));
                    DBSPTypeTuple tuple = collectionType.to(DBSPTypeTuple.class);
                    Utilities.enforce(index.is(DBSPStringLiteral.class));
                    DBSPStringLiteral lit = index.to(DBSPStringLiteral.class);
                    Utilities.enforce(lit.value != null);
                    String fieldName = lit.value;
                    Utilities.enforce(tuple.originalStruct != null);
                    DBSPTypeStruct.Field field = tuple.originalStruct.getField(
                            new ProgramIdentifier(fieldName));
                    Utilities.enforce(field != null);
                    int fieldIndex = field.index;
                    return op0.field(fieldIndex).applyCloneIfNeeded();
                } else {
                    switch (name) {
                        case "ITEM": break;
                        case "SAFE_OFFSET": opcode = DBSPOpcode.SAFE_RUST_INDEX; break;
                        default: throw new UnimplementedException("Not yet implemented", node);
                    }
                    Utilities.enforce(collectionType.is(DBSPTypeArray.class));
                }
                return new DBSPBinaryExpression(node, type, opcode, op0, index);
            }
            case TRIM:
                validateArgCount(node, operationName, ops.size(), 3);
                this.ensureString(ops, 1);
                this.ensureString(ops, 2);
                // fall through
            case TIMESTAMP_DIFF:
                return compileKeywordFunction(call, node, null, type, ops, 0, 3);
            case TUMBLE: {
                validateArgCount(node, operationName, ops.size(), 2, 3);
                DBSPExpression op = ops.get(1);
                if (op.getType().is(DBSPTypeMonthsInterval.class)) {
                    throw new UnsupportedException(
                            "Tumbling window intervals must be 'short' SQL intervals (days and lower)",
                            op.getNode());
                }
                DBSPExpression[] args = new DBSPExpression[ops.size()];
                args[0] = ops.get(0);
                args[1] = ops.get(1);
                String typeName = "_" + args[1].getType().to(DBSPTypeBaseType.class).shortName();
                String functionName = "tumble_" + args[0].getType().baseTypeWithSuffix() + typeName;
                if (ops.size() == 3) {
                    args[2] = ops.get(2);
                    functionName += "_" + args[2].getType().to(DBSPTypeBaseType.class).shortName();
                }
                return new DBSPApplyExpression(node, functionName, type, args);
            }
            case ARRAY_LENGTH:
            case ARRAY_SIZE: {
                if (call.operands.size() != 1)
                    throw operandCountError(node, operationName, call.operandCount());
                String name = "cardinalityVec";
                nullLiteralToNullArray(ops, 0);
                DBSPExpression op0 = ops.get(0);
                if (op0.getType().mayBeNull)
                    name += "N";
                return new DBSPApplyExpression(node, name, type, op0);
            }
            case ARRAY_EXCEPT:
            case ARRAY_UNION:
            case ARRAY_INTERSECT:
            case ARRAY_CONCAT: {
                int operands = ops.size();
                DBSPExpression result = ops.get(0).cast(node, type, false);
                for (int i = 1; i < operands; i++) {
                    // Extra check due to https://issues.apache.org/jira/browse/CALCITE-7105
                    DBSPExpression opi = ops.get(i);
                    if (opi.getType().code != ARRAY) {
                        throw new CompilationError(
                                "Arguments of '" + operationName + "' must have ARRAY types", node);
                    }
                    DBSPExpression op = opi.cast(node, type, false);
                    String name = call.op.getName().toLowerCase();
                    name += result.type.nullableUnderlineSuffix();
                    name += op.type.nullableUnderlineSuffix();
                    result = new DBSPApplyExpression(node, name, type, result, op);
                }
                return result;
            }
            case ARRAY_PREPEND:
            case ARRAY_APPEND: {
                if (call.operands.size() != 2)
                    throw operandCountError(node, operationName, call.operandCount());

                DBSPTypeArray vec = type.to(DBSPTypeArray.class);
                DBSPType elemType = vec.getElementType();

                DBSPExpression arg0 = ops.get(0);
                DBSPType arg0type = arg0.type;
                DBSPExpression arg1 = ops.get(1).cast(node, elemType, false);

                arg0 = ensureArrayElementsOfType(node, arg0, elemType);

                String method = getCallName(call);
                if (arg0type.mayBeNull)
                    method += "N";

                return new DBSPApplyExpression(node, method, type, arg0, arg1);
            }
            case SORT_ARRAY: {
                if (ops.size() == 1) {
                    ops.add(1, new DBSPBoolLiteral(true));
                }
                DBSPExpression arg0 = ops.get(0);
                DBSPExpression arg1 = ops.get(1);

                String method = getCallName(call);
                if (arg0.type.mayBeNull)
                    method += "N";

                return new DBSPApplyExpression(node, method, type, arg0, arg1);
            }
            case ARRAY_MAX:
            case ARRAY_MIN: {
                if (call.operands.size() != 1)
                    throw operandCountError(node, operationName, call.operandCount());

                DBSPExpression arg0 = ops.get(0);
                String method = getArrayCallNameWithElemNullability(call, arg0);
                return new DBSPApplyExpression(node, method, type, arg0);
            }
            case ARRAY_CONTAINS:
            case ARRAY_POSITION: {
                validateArgCount(node, operationName, call.operandCount(), 2);
                DBSPExpression arg0 = ops.get(0);
                DBSPExpression arg1 = ops.get(1);
                DBSPTypeArray vec = arg0.getType().to(DBSPTypeArray.class);
                DBSPType elemType = vec.getElementType();

                // If array is null for certain, return null
                if (arg0.type.is(DBSPTypeNull.class)) {
                    String warningMessage =
                            node + ": always returns NULL";
                    this.compiler.reportWarning(node.getPositionRange(), "unnecessary function call", warningMessage);
                    return DBSPNullLiteral.none(type);
                }

                if (elemType.mayBeNull && !arg1.type.mayBeNull) {
                    arg1 = arg1.some();
                }

                if (!elemType.sameType(arg1.type)) {
                    DBSPType commonType = TypeCompiler.reduceType(node, elemType, arg1.type,
                            "Cannot find common type between value and array", true);
                    arg1 = arg1.cast(node, commonType, false);
                    arg0 = arg0.cast(node, new DBSPTypeArray(commonType, arg0.type.mayBeNull), false);
                }

                // Calcite's type inference returns nullable results if arg1 is nullable.
                // We need to correct for that.
                DBSPType resultType = type;
                if (!arg0.getType().mayBeNull)
                    resultType = resultType.withMayBeNull(false);

                String method = getCallName(call) + arg0.type.nullableUnderlineSuffix();
                DBSPExpression exp = new DBSPApplyExpression(node, method, resultType, arg0, arg1);
                return exp.cast(node, type, false);
            }
            case MAP_CONTAINS_KEY: {
                validateArgCount(node, operationName, call.operandCount(), 2);
                DBSPExpression arg0 = ops.get(0);
                DBSPExpression arg1 = ops.get(1);
                DBSPTypeMap map = arg0.getType().to(DBSPTypeMap.class);
                DBSPType keyType = map.getKeyType();

                // If argument is null for certain, return null
                if (arg1.type.is(DBSPTypeNull.class)) {
                    String warningMessage =
                            node + ": always returns NULL";
                    this.compiler.reportWarning(node.getPositionRange(), "unnecessary function call", warningMessage);
                    return DBSPNullLiteral.none(type);
                }

                String method = getArrayOrMapCallName(call, arg0, arg1);
                // the rust code has signature: (Vec<T>, T)
                // so if elements of the vector are nullable and T is an Option type
                // therefore we need to wrap arg1 in Some
                if (keyType.mayBeNull) {
                    arg1 = new DBSPApplyExpression(arg1.getNode(), "Some", arg1.type.withMayBeNull(true), arg1);
                }
                return new DBSPApplyExpression(node, method, type, arg0, arg1);
            }
            case ARRAY_DISTINCT: {
                DBSPExpression arg0 = ops.get(0);
                String method = getCallName(call);
                if (arg0.type.mayBeNull)
                    method += "N";
                return new DBSPApplyExpression(node, method, type, arg0);
            }
            case ARRAY_REVERSE: {
                if (call.operands.size() != 1)
                    throw operandCountError(node, operationName, call.operandCount());

                DBSPExpression arg0 = ops.get(0);
                String method = getArrayOrMapCallName(call, arg0);

                return new DBSPApplyExpression(node, method, type, arg0);
            }
            case ARRAY_COMPACT: {
                if (call.operands.size() != 1)
                    throw operandCountError(node, operationName, call.operandCount());

                DBSPExpression arg0 = ops.get(0);
                DBSPTypeArray vecType = arg0.getType().to(DBSPTypeArray.class);
                DBSPType elemType = vecType.getElementType();
                if (!elemType.mayBeNull) {
                    // The element type is not nullable.
                    String warningMessage =
                            node + ": no null elements in the array";
                    this.compiler.reportWarning(node.getPositionRange(), "unnecessary function call", warningMessage);
                    return arg0;
                }
                if (elemType.sameType(DBSPTypeNull.getDefault())) {
                    String warningMessage =
                            node + ": all elements are null; result is always an empty array";
                    this.compiler.reportWarning(node.getPositionRange(), "unnecessary function call", warningMessage);
                    return new DBSPArrayExpression(arg0.getNode(), arg0.getType(), Linq.list());
                }

                String method = getArrayOrMapCallName(call, arg0);
                return new DBSPApplyExpression(node, method, type, arg0);
            }
            case ARRAY_REPEAT: {
                validateArgCount(node, operationName, ops.size(), 2);
                this.ensureInteger(node, ops, 1);
                String method = getArrayOrMapCallName(call, ops.get(0), ops.get(1));
                return new DBSPApplyExpression(node, method, type, ops.get(0), ops.get(1)).cast(node, type, false);
            }
            case HOP:
                throw new UnimplementedException("Please use the TABLE function HOP", node);
            case ROW:
                return new DBSPTupleExpression(node, ops);
            case COALESCE: {
                if (ops.isEmpty()) {
                    throw new CompilationError(
                            "Function " + Utilities.singleQuote(operationName) +
                                    " with 0 arguments is unknown", node);
                }
                ops = Linq.where(ops, op -> !op.is(DBSPNullLiteral.class));
                if (ops.isEmpty()) {
                    if (!type.mayBeNull)
                        throw new InternalCompilerError(
                                "COALESCE of all-NULL values returning non-nullable result", node);
                    return type.none();
                }
                DBSPExpression last = ops.get(ops.size() - 1).cast(node, type, false);
                for (int i = 1; i < ops.size(); i++) {
                    int index = ops.size() - i - 1;
                    DBSPExpression op = ops.get(index);
                    last = new DBSPIfExpression(
                            node, op.is_null(), last, op.cast(node, type, false));
                }
                return last;
            }
            case TIMESTAMP_ADD:
                throw new UnimplementedException("Function " + Utilities.singleQuote(call.getOperator().toString())
                        + " not yet implemented", 1265,
                        "Perhaps you can use DATE_ADD or addition between a date and an interval?", node);
            case NULLIF: {
                // The return type is the type of arg0, but the comparison may introduce casts
                validateArgCount(node, operationName, ops.size(), 2);
                boolean anyNull = Linq.any(ops, o -> o.getType().mayBeNull);
                var cmp = makeBinaryExpression(node, DBSPTypeBool.create(anyNull), DBSPOpcode.EQ, ops.get(0), ops.get(1));
                return new DBSPIfExpression(node, cmp.wrapBoolIfNeeded(), type.none(), ops.get(0).cast(node, type, false));
            }
            case GREATEST_PG:
            case LEAST_PG: {
                if (ops.isEmpty()) {
                    throw new CompilationError(
                            "Function " + Utilities.singleQuote(operationName) +
                                    " with 0 arguments is unknown", node);
                }
                ops = Linq.where(ops, op -> !op.is(DBSPNullLiteral.class));
                DBSPOpcode code = call.getKind() == SqlKind.GREATEST_PG ?
                        DBSPOpcode.MAX_IGNORE_NULLS : DBSPOpcode.MIN_IGNORE_NULLS;
                DBSPExpression next = ops.get(0).cast(node, type, false);
                for (int i = 1; i < ops.size(); i++) {
                    DBSPExpression op = ops.get(i);
                    next = new DBSPBinaryExpression(node, type, code, next, op.cast(node, type, false));
                }
                return next;
            }
            case DOT:
            default:
                throw new UnimplementedException("Function " + Utilities.singleQuote(call.getOperator().toString())
                        + " not yet implemented", 1265, node);
        }
    }

    @Override
    public DBSPExpression visitLambdaRef(RexLambdaRef variable) {
        DBSPType type = this.typeCompiler.convertType(variable.getType(), false);
        return new DBSPVariablePath(variable.getName(), type.ref()).deref().applyCloneIfNeeded();
    }

    @Override
    public DBSPExpression visitLambda(RexLambda lambda) {
        var params = lambda.getParameters();
        DBSPParameter[] converted = new DBSPParameter[params.size()];
        int index = 0;
        for (var param: params) {
            DBSPType type = this.typeCompiler.convertType(param.getType(), false);
            DBSPParameter var = new DBSPParameter(param.getName(), type.ref());
            converted[index++] = var;
        }
        DBSPExpression body = lambda.getExpression().accept(this);
        return body.closure(converted);
    }

    DBSPExpression handleExtract(RexCall call, DBSPType type, String keyword, List<DBSPExpression> args) {
        CalciteObject node = CalciteObject.create(this.context, call);
        List<DBSPExpression> newArgs = new ArrayList<>(args.size() + 1);
        newArgs.add(new DBSPKeywordLiteral(node, keyword));
        newArgs.addAll(args);
        return this.handleExtract(call, type, newArgs);
    }

    DBSPExpression handleExtract(RexCall call, DBSPType type, List<DBSPExpression> ops) {
        CalciteObject node = CalciteObject.create(this.context, call);
        String baseName = "extract";
        validateArgCount(node, baseName, ops.size(), 2);
        DBSPKeywordLiteral keyword = ops.get(0).to(DBSPKeywordLiteral.class);
        StringBuilder name = new StringBuilder();
        name.append(baseName)
                .append("_")
                .append(keyword);
        DBSPExpression[] operands = new DBSPExpression[ops.size() - 1];
        int index = 0;
        for (int i = 0; i < ops.size(); i++) {
            DBSPExpression op = ops.get(i);
            if (i == 0)
                continue;
            operands[index] = op;
            index++;
            name.append("_");
            DBSPType operandType = op.getType();
            if (operandType.is(IsIntervalType.class))
                name.append(operandType.to(DBSPTypeBaseType.class).shortName())
                        .append(operandType.nullableSuffix());
            else
                name.append(op.getType().baseTypeWithSuffix());
        }
        return new DBSPApplyExpression(node, name.toString(), type, operands);
    }

    static DBSPExpression toPosition(SourcePosition pos) {
        return new DBSPConstructorExpression(
                new DBSPPath("SourcePosition", "new").toExpression(),
                DBSPTypeAny.getDefault(),
                new DBSPU32Literal(pos.line), new DBSPU32Literal(pos.column));
    }

    // We expect this will be used at some point for error handling
    @SuppressWarnings("unused")
    static DBSPExpression toPosition(SourcePositionRange range) {
        return new DBSPConstructorExpression(
                new DBSPPath("SourcePositionRange", "new").toExpression(),
                DBSPTypeAny.getDefault(),
                toPosition(range.start), toPosition(range.end));
    }

    private DBSPExpression compileUdfOrConstructor(CalciteObject node, RexCall call, DBSPType type, List<DBSPExpression> ops) {
        ProgramIdentifier function = new ProgramIdentifier(call.op.getName());  // no lowercase applied
        boolean isConstructor = this.compiler.isStructConstructor(function);
        if (isConstructor) {
            DBSPTypeStruct struct = this.compiler.getStructByName(function);
            DBSPType structTuple = Objects.requireNonNull(struct).toTupleDeep();
            Utilities.enforce(structTuple.sameType(type), "Expected the same type " + structTuple + " and " + type);
            DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
            for (int i = 0; i < ops.size(); i++) {
                DBSPExpression opi = ops.get(i);
                opi = opi.applyCloneIfNeeded().cast(node, tuple.getFieldType(i), false);
                ops.set(i, opi);
            }
            return new DBSPTupleExpression(node, type.to(DBSPTypeTuple.class), ops);
        }

        ExternalFunction ef = this.compiler.getCustomFunctions().getUDF(function);
        if (ef == null)
            throw new CompilationError("Function " + function.singleQuote() + " is unknown", node);
        List<DBSPType> operandTypes = Linq.map(ef.parameterList,
                p -> this.typeCompiler.convertType(p.getType(), false));
        // Give warnings if we have to insert casts that cast away nullability,
        // these can cause runtime crashes.
        for (int i = 0; i < ops.size(); i++) {
            if (ops.get(i).getType().mayBeNull && !operandTypes.get(i).mayBeNull) {
                this.compiler.reportWarning(new SourcePositionRange(call.getParserPosition()),
                        "Nullable argument",
                        "Argument " + i + " is nullable, while " +
                        function.singleQuote() + " expects a not nullable value; this may cause a runtime crash");
            }
        }

        List<DBSPExpression> converted = Linq.zip(ops, operandTypes, (e, t) -> e.cast(node, t, false));
        DBSPExpression[] arguments = new DBSPExpression[converted.size()];
        for (int i = 0; i < converted.size(); i++)
            arguments[i] = converted.get(i);
        // The convention is that all such functions return Result.
        // This is not true for functions that we implement internally.
        // Currently, we need to unwrap.
        boolean unwrap = !ef.generated;
        if (unwrap)
            type = new DBSPTypeResult(type);
        DBSPTypeFunction functionType = new DBSPTypeFunction(type, operandTypes.toArray(new DBSPType[0]));
        DBSPPathExpression func = new DBSPPathExpression(functionType, new DBSPPath(function.name()));
        DBSPExpression result = new DBSPApplyExpression(func, type, arguments);
        if (unwrap)
            result = result.resultUnwrap();
        return result;
    }

    public DBSPExpression compile(RexNode expression) {
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Compiling ")
                .appendSupplier(expression::toString)
                .newline();
        DBSPExpression result = expression.accept(this);
        if (result == null)
            throw new InternalCompilerError("Unexpected Calcite expression " + expression,
                    CalciteObject.create(this.context, expression));
        return result;
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }
}

