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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustSqlRuntimeLibrary;
import org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.util.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

public class ExpressionCompiler extends RexVisitorImpl<DBSPExpression> implements IWritesLogs, ICompilerComponent {
    private final TypeCompiler typeCompiler;
    @Nullable
    public final DBSPVariablePath inputRow;
    private final RexBuilder rexBuilder;
    private final List<RexLiteral> constants;
    private final DBSPCompiler compiler;

    public ExpressionCompiler(@Nullable DBSPVariablePath inputRow, DBSPCompiler compiler) {
        this(inputRow, Linq.list(), compiler);
    }

    /**
     * Create a compiler that will translate expressions pertaining to a row.
     * @param inputRow         Variable representing the row being compiled.
     * @param constants        Additional constants.  Expressions compiled
     *                         may use RexInputRef, which are field references
     *                         within the row.  Calcite seems to number constants
     *                         as additional fields within the row, after the end of
     *                         the input row.
     * @param compiler         Handle to the compiler.
     */
    public ExpressionCompiler(@Nullable DBSPVariablePath inputRow,
                              List<RexLiteral> constants,
                              DBSPCompiler compiler) {
        super(true);
        this.inputRow = inputRow;
        this.constants = constants;
        this.rexBuilder = compiler.frontend.getRexBuilder();
        this.compiler = compiler;
        this.typeCompiler = compiler.getTypeCompiler();
        if (inputRow != null &&
                !inputRow.getType().is(DBSPTypeRef.class))
            throw new InternalCompilerError("Expected a reference type for row", inputRow.getNode());
    }

    /**
     * Convert an expression that refers to a field in the input row.
     * @param inputRef   index in the input row.
     * @return           the corresponding DBSP expression.
     */
    @Override
    public DBSPExpression visitInputRef(RexInputRef inputRef) {
        CalciteObject node = new CalciteObject(inputRef);
        if (this.inputRow == null)
            throw new InternalCompilerError("Row referenced without a row context", node);
        // Unfortunately it looks like we can't trust the type coming from Calcite.
        DBSPTypeTuple type = this.inputRow.getType().deref().to(DBSPTypeTuple.class);
        int index = inputRef.getIndex();
        if (index < type.size()) {
            return new DBSPFieldExpression(
                    node, this.inputRow,
                    inputRef.getIndex()).applyCloneIfNeeded();
        }
        if (index - type.size() < this.constants.size())
            return this.visitLiteral(this.constants.get(index - type.size()));
        throw new InternalCompilerError("Index in row out of bounds ", node);
    }

    @Override
    public DBSPExpression visitLiteral(RexLiteral literal) {
        CalciteObject node = new CalciteObject(literal);
        try {
            DBSPType type = this.typeCompiler.convertType(literal.getType());
            if (literal.isNull())
                return DBSPLiteral.none(type);
            if (type.is(DBSPTypeInteger.class)) {
                DBSPTypeInteger intType = type.to(DBSPTypeInteger.class);
                switch (intType.getWidth()) {
                    case 16:
                        return new DBSPI32Literal(Objects.requireNonNull(literal.getValueAs(Short.class)));
                    case 32:
                        return new DBSPI32Literal(Objects.requireNonNull(literal.getValueAs(Integer.class)));
                    case 64:
                        return new DBSPI64Literal(Objects.requireNonNull(literal.getValueAs(Long.class)));
                    default:
                        throw new UnsupportedOperationException("Unsupported integer width type " + intType.getWidth());
                }
            } else if (type.is(DBSPTypeDouble.class))
                return new DBSPDoubleLiteral(Objects.requireNonNull(literal.getValueAs(Double.class)));
            else if (type.is(DBSPTypeFloat.class))
                return new DBSPFloatLiteral(Objects.requireNonNull(literal.getValueAs(Float.class)));
            else if (type.is(DBSPTypeString.class)) {
                String str = literal.getValueAs(String.class);
                RelDataType litType = literal.getType();
                Charset charset = litType.getCharset();
                return new DBSPStringLiteral(Objects.requireNonNull(str), Objects.requireNonNull(charset));
            }
            else if (type.is(DBSPTypeBool.class))
                return new DBSPBoolLiteral(Objects.requireNonNull(literal.getValueAs(Boolean.class)));
            else if (type.is(DBSPTypeDecimal.class))
                return new DBSPDecimalLiteral(
                        node, type, Objects.requireNonNull(literal.getValueAs(BigDecimal.class)));
            else if (type.is(DBSPTypeKeyword.class))
                return new DBSPKeywordLiteral(node, Objects.requireNonNull(literal.getValue()).toString());
            else if (type.is(DBSPTypeMillisInterval.class))
                return new DBSPIntervalMillisLiteral(node, type, Objects.requireNonNull(
                        literal.getValueAs(BigDecimal.class)).longValue());
            else if (type.is(DBSPTypeTimestamp.class)) {
                return new DBSPTimestampLiteral(node, type,
                        Objects.requireNonNull(literal.getValueAs(TimestampString.class)));
            } else if (type.is(DBSPTypeDate.class)) {
                return new DBSPDateLiteral(node, type, Objects.requireNonNull(literal.getValueAs(DateString.class)));
            } else if (type.is(DBSPTypeGeoPoint.class)) {
                Point point = literal.getValueAs(Point.class);
                Coordinate c = Objects.requireNonNull(point).getCoordinate();
                return new DBSPGeoPointLiteral(node,
                        new DBSPDoubleLiteral(c.getOrdinate(0)),
                        new DBSPDoubleLiteral(c.getOrdinate(1)));
            }
        } catch (BaseCompilerException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new UnimplementedException(node, ex);
        }
        throw new UnimplementedException(node);
    }

    /**
     * Given operands for "operation" with left and right types,
     * compute the type that both operands must be cast to.
     * Note: this ignores nullability of types.
     * @param left       Left operand type.
     * @param right      Right operand type.
     * @return           Common type operands must be cast to.
     */
    public static DBSPType reduceType(DBSPType left, DBSPType right) {
        if (left.is(DBSPTypeNull.class))
            return right.setMayBeNull(true);
        if (right.is(DBSPTypeNull.class))
            return left.setMayBeNull(true);
        left = left.setMayBeNull(false);
        right = right.setMayBeNull(false);
        if (left.sameType(right))
            return left;

        DBSPTypeInteger li = left.as(DBSPTypeInteger.class);
        DBSPTypeInteger ri = right.as(DBSPTypeInteger.class);
        DBSPTypeDecimal ld = left.as(DBSPTypeDecimal.class);
        DBSPTypeDecimal rd = right.as(DBSPTypeDecimal.class);
        DBSPTypeFP lf = left.as(DBSPTypeFP.class);
        DBSPTypeFP rf = right.as(DBSPTypeFP.class);
        if (li != null) {
            if (ri != null)
                return new DBSPTypeInteger(left.getNode(), Math.max(li.getWidth(), ri.getWidth()), true,false);
            if (rf != null || rd != null)
                return right.setMayBeNull(false);
        }
        if (lf != null) {
            if (ri != null || rd != null)
                return left.setMayBeNull(false);
            if (rf != null) {
                if (lf.getWidth() < rf.getWidth())
                    return right.setMayBeNull(false);
                else
                    return left.setMayBeNull(false);
            }
        }
        if (ld != null) {
            if (ri != null)
                return left.setMayBeNull(false);
            if (rf != null)
                return right.setMayBeNull(false);
        }
        throw new UnimplementedException("Cast from " + right + " to " + left);
    }

    public static DBSPExpression aggregateOperation(
            CalciteObject node, DBSPOpcode op,
            DBSPType type, DBSPExpression left, DBSPExpression right) {
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        DBSPType commonBase = reduceType(leftType, rightType);
        if (commonBase.is(DBSPTypeNull.class)) {
            return DBSPLiteral.none(type);
        }
        DBSPType resultType = commonBase.setMayBeNull(leftType.mayBeNull || rightType.mayBeNull);
        DBSPExpression binOp = new DBSPBinaryExpression(node, resultType, op, left, right);
        return binOp.cast(type);
    }

    // Like makeBinaryExpression, but accepts multiple operands.
    private static DBSPExpression makeBinaryExpressions(
            CalciteObject node, DBSPType type, DBSPOpcode opcode, List<DBSPExpression> operands) {
        if (operands.size() < 2)
            throw new UnimplementedException(node);
        DBSPExpression accumulator = operands.get(0);
        for (int i = 1; i < operands.size(); i++)
            accumulator = makeBinaryExpression(node, type, opcode, Linq.list(accumulator, operands.get(i)));
        return accumulator.cast(type);
    }

    @SuppressWarnings("unused")
    public static boolean needCommonType(DBSPType result, DBSPType left, DBSPType right) {
        // Dates can be mixed with other types in a binary operation
        if (left.is(IsDateType.class)) return false;
        if (right.is(IsDateType.class)) return false;
        // Allow mixing different string types in an operation
        if (left.is(DBSPTypeString.class) && right.is(DBSPTypeString.class))
            return false;
        return true;
    }

    public static DBSPExpression makeBinaryExpression(
            CalciteObject node, DBSPType type, DBSPOpcode opcode, List<DBSPExpression> operands) {
        // Why doesn't Calcite do this?
        if (operands.size() != 2)
            throw new InternalCompilerError("Expected 2 operands, got " + operands.size(), node);
        DBSPExpression left = operands.get(0);
        DBSPExpression right = operands.get(1);
        if (left == null || right == null)
            throw new UnimplementedException(node);
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();

        if (needCommonType(type, leftType, rightType)) {
            DBSPType commonBase = reduceType(leftType, rightType);
            if (commonBase.is(DBSPTypeNull.class)) {
                // Result is always NULL.  Perhaps we should give a warning?
                return DBSPLiteral.none(type);
            }
            if (!leftType.setMayBeNull(false).sameType(commonBase))
                left = left.cast(commonBase.setMayBeNull(leftType.mayBeNull));
            if (!rightType.setMayBeNull(false).sameType(commonBase))
                right = right.cast(commonBase.setMayBeNull(rightType.mayBeNull));
        }
        // TODO: we don't need the whole function here, just the result type.
        RustSqlRuntimeLibrary.FunctionDescription function = RustSqlRuntimeLibrary.INSTANCE.getImplementation(
                opcode, type, left.getType(), right.getType());
        DBSPExpression call = new DBSPBinaryExpression(node, function.returnType, opcode, left, right);
        return call.cast(type);
    }

    public static DBSPExpression makeUnaryExpression(
            CalciteObject node, DBSPType type, DBSPOpcode op, List<DBSPExpression> operands) {
        if (operands.size() != 1)
            throw new InternalCompilerError("Expected 1 operands, got " + operands.size(), node);
        DBSPExpression operand = operands.get(0);
        if (operand == null)
            throw new UnimplementedException("Found unimplemented expression in " + node);
        DBSPType resultType = operand.getType();
        if (op.toString().startsWith("is_"))
            // these do not produce nullable results
            resultType = resultType.setMayBeNull(false);
        DBSPExpression expr = new DBSPUnaryExpression(node, resultType, op, operand);
        return expr.cast(type);
    }

    public static DBSPExpression wrapBoolIfNeeded(DBSPExpression expression) {
        DBSPType type = expression.getType();
        if (type.mayBeNull) {
            return new DBSPUnaryExpression(
                    expression.getNode(), type.setMayBeNull(false),
                    DBSPOpcode.WRAP_BOOL, expression);
        }
        return expression;
    }

    @Override
    public DBSPExpression visitCall(RexCall call) {
        CalciteObject node = new CalciteObject(call);
        Logger.INSTANCE.belowLevel(this, 2)
                .append(call.toString())
                .append(" ")
                .append(call.getType().toString());
        if (call.op.kind == SqlKind.SEARCH) {
            // TODO: Ideally the optimizer should do this before handing the expression to us.
            // Then the rexBuilder won't be needed.
            call = (RexCall)RexUtil.expandSearch(this.rexBuilder, null, call);
        }
        List<DBSPExpression> ops = Linq.map(call.operands, e -> e.accept(this));
        boolean anyNull = Linq.any(ops, o -> o.getType().mayBeNull);
        DBSPType type = this.typeCompiler.convertType(call.getType());
        switch (call.op.kind) {
            case TIMES:
                return makeBinaryExpression(node, type, DBSPOpcode.MUL, ops);
            case DIVIDE:
                // We enforce that the type of the result of division is always nullable
                type = type.setMayBeNull(true);
                return makeBinaryExpression(node, type, DBSPOpcode.DIV, ops);
            case MOD:
                return makeBinaryExpression(node, type, DBSPOpcode.MOD, ops);
            case PLUS:
                return makeBinaryExpressions(node, type, DBSPOpcode.ADD, ops);
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
            case IS_NOT_DISTINCT_FROM: {
                DBSPExpression op = makeBinaryExpression(node, type, DBSPOpcode.IS_DISTINCT, ops);
                return makeUnaryExpression(node, DBSPTypeBool.INSTANCE, DBSPOpcode.NOT, Linq.list(op));
            }
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
            case MINUS_PREFIX:
                return makeUnaryExpression(node, type, DBSPOpcode.NEG, ops);
            case BIT_AND:
                return makeBinaryExpressions(node, type, DBSPOpcode.BW_AND, ops);
            case BIT_OR:
                return makeBinaryExpressions(node, type, DBSPOpcode.BW_OR, ops);
            case BIT_XOR:
                return makeBinaryExpressions(node, type, DBSPOpcode.XOR, ops);
            case CAST:
            case REINTERPRET:
                return ops.get(0).cast(type);
            case IS_NULL:
            case IS_NOT_NULL: {
                if (!type.sameType(DBSPTypeBool.INSTANCE))
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
                            finalType = finalType.setMayBeNull(true);
                    }
                    if (!result.getType().sameType(finalType))
                        result = result.cast(finalType);
                    for (int i = 1; i < ops.size() - 1; i += 2) {
                        DBSPExpression alt = ops.get(i + 1);
                        if (!alt.getType().sameType(finalType))
                            alt = alt.cast(finalType);
                        DBSPExpression comp = makeBinaryExpression(
                                node, DBSPTypeBool.INSTANCE, DBSPOpcode.EQ,
                                Linq.list(value, ops.get(i)));
                        comp = wrapBoolIfNeeded(comp);
                        result = new DBSPIfExpression(node, comp, alt, result);
                    }
                } else {
                    // Compute casts if needed.
                    // Build this backwards
                    DBSPType finalType = result.getType();
                    for (int i = 0; i < ops.size() - 1; i += 2) {
                        int index = ops.size() - i - 2;
                        if (ops.get(index).getType().mayBeNull)
                            finalType = finalType.setMayBeNull(true);
                    }

                    if (!result.getType().sameType(finalType))
                        result = result.cast(finalType);
                    for (int i = 0; i < ops.size() - 1; i += 2) {
                        int index = ops.size() - i - 2;
                        DBSPExpression alt = ops.get(index);
                        if (!alt.getType().sameType(finalType))
                            alt = alt.cast(finalType);
                        DBSPExpression condition = wrapBoolIfNeeded(ops.get(index - 1));
                        result = new DBSPIfExpression(node, condition, alt, result);
                    }
                }
                return result;
            }
            case ST_POINT: {
                if (ops.size() != 2)
                    throw new UnimplementedException("Expected only 2 operands", node);
                DBSPExpression left = ops.get(0);
                DBSPExpression right = ops.get(1);
                String functionName = "make_geopoint" + type.nullableSuffix() +
                        "_d" + left.getType().nullableSuffix() +
                        "_d" + right.getType().nullableSuffix();
                return new DBSPApplyExpression(node, functionName, type, left, right);
            }
            case OTHER_FUNCTION: {
                String opName = call.op.getName().toLowerCase();
                switch (opName) {
                    case "truncate":
                    case "round": {
                        DBSPExpression right;
                        if (call.operands.size() < 1)
                            throw new UnimplementedException(node);
                        DBSPExpression left = ops.get(0);
                        if (call.operands.size() == 1)
                            right = new DBSPI32Literal(0);
                        else
                            right = ops.get(1);
                        DBSPType leftType = left.getType();
                        DBSPType rightType = right.getType();
                        if (!rightType.is(DBSPTypeInteger.class))
                            throw new UnimplementedException("ROUND expects a constant second argument", node);
                        String function = opName + "_" +
                                leftType.baseTypeWithSuffix();
                        return new DBSPApplyExpression(node, function, type, left, right);
                    }
                    case "numeric_inc":
                    case "sign":
                    case "log10":
                    case "ln":
                    case "abs": {
                        if (call.operands.size() != 1)
                            throw new UnimplementedException(node);
                        DBSPExpression arg = ops.get(0);
                        DBSPType argType = arg.getType();
                        String function = opName + "_" + argType.baseTypeWithSuffix();
                        return new DBSPApplyExpression(node, function, type, arg);
                    }
                    case "st_distance": {
                        if (call.operands.size() != 2)
                            throw new UnimplementedException(node);
                        DBSPExpression left = ops.get(0);
                        DBSPExpression right = ops.get(1);
                        String function = opName + "_" + left.getType().nullableSuffix() +
                                "_" + right.getType().nullableSuffix();
                        return new DBSPApplyExpression(node, function, DBSPTypeDouble.INSTANCE.setMayBeNull(anyNull), left, right);
                    }
                    case "division":
                        return makeBinaryExpression(node, type, DBSPOpcode.DIV, ops);
                    case "cardinality": {
                        if (call.operands.size() != 1)
                            throw new UnimplementedException(node);
                        DBSPExpression arg = ops.get(0);
                        DBSPExpression len = new DBSPApplyMethodExpression(node, "len", DBSPTypeUSize.INSTANCE, arg);
                        return len.cast(type);
                    }
                    case "element": {
                        type = type.setMayBeNull(true);  // Why isn't this always nullable?
                        DBSPExpression arg = ops.get(0);
                        DBSPTypeVec arrayType = arg.getType().to(DBSPTypeVec.class);
                        String method = "element";
                        if (arrayType.getElementType().mayBeNull)
                            method += "N";
                        return new DBSPApplyExpression(node, method, type, arg);
                    }
                    case "power": {
                        if (call.operands.size() != 2)
                            throw new UnimplementedException(node);
                        DBSPType leftType = ops.get(0).getType();
                        DBSPType rightType = ops.get(1).getType();
                        String functionName = opName + "_" + leftType.baseTypeWithSuffix() +
                                "_" + rightType.baseTypeWithSuffix();
                        return new DBSPApplyExpression(node, functionName, type, ops.get(0), ops.get(1));
                    }
                    case "substring": {
                        DBSPType baseType = ops.get(0).getType();
                        String functionName = opName + baseType.nullableSuffix();
                        if (call.operands.size() == 3) {
                            functionName += "3";
                            return new DBSPApplyExpression(node, functionName, type, ops.get(0), ops.get(1), ops.get(2));
                        } else if (call.operands.size() == 2) {
                            functionName += "2";
                            return new DBSPApplyExpression(node, functionName, type, ops.get(0), ops.get(1));
                        } else {
                            throw new UnimplementedException(node);
                        }
                    }
                }
                throw new UnimplementedException(node);
            }
            case OTHER:
                String opName = call.op.getName().toLowerCase();
                //noinspection SwitchStatementWithTooFewBranches
                switch (opName) {
                    case "||":
                        return makeBinaryExpression(node, type, DBSPOpcode.CONCAT, ops);
                    default:
                        break;
                }
                throw new UnimplementedException(node);
            case EXTRACT: {
                if (call.operands.size() != 2)
                    throw new UnimplementedException(node);
                DBSPKeywordLiteral keyword = ops.get(0).to(DBSPKeywordLiteral.class);
                DBSPType type1 = ops.get(1).getType();
                String functionName = "extract_" + type1.to(IsNumericType.class).getRustString() +
                        "_" + keyword + type1.nullableSuffix();
                return new DBSPApplyExpression(node, functionName, type, ops.get(1));
            }
            case LIKE:
            case SIMILAR: {
                String functionName = call.getKind().toString().toLowerCase() + ops.size();
                if (call.operands.size() == 2) {
                    return new DBSPApplyExpression(node, functionName, type, ops.get(0), ops.get(1));
                } else if (call.operands.size() == 3) {
                    return new DBSPApplyExpression(node, functionName, type, ops.get(0), ops.get(1), ops.get(2));
                } else {
                    throw new UnimplementedException(node);
                }
            }
            case FLOOR:
            case CEIL: {
                if (call.operands.size() == 2) {
                    DBSPKeywordLiteral keyword = ops.get(1).to(DBSPKeywordLiteral.class);
                    String functionName = call.getKind().toString().toLowerCase() + "_" +
                            type.to(DBSPTypeBaseType.class).shortName() + "_" + keyword + type.nullableSuffix();
                    return new DBSPApplyExpression(node, functionName, type, ops.get(0));
                } else if (call.operands.size() == 1) {
                    String functionName = call.getKind().toString().toLowerCase() + "_" +
                            type.to(DBSPTypeBaseType.class).shortName() + type.nullableSuffix();
                    return new DBSPApplyExpression(node, functionName, type, ops.get(0));
                } else {
                    throw new UnimplementedException(node);
                }
            }
            case ARRAY_VALUE_CONSTRUCTOR: {
                DBSPTypeVec vec = type.to(DBSPTypeVec.class);
                DBSPType elemType = vec.getElementType();
                List<DBSPExpression> args = Linq.map(ops, o -> o.cast(elemType));
                return new DBSPApplyExpression(node, "vec!", type, args.toArray(new DBSPExpression[0]));
            }
            case ITEM: {
                if (call.operands.size() != 2)
                    throw new UnimplementedException(node);
                return new DBSPIndexExpression(node, ops.get(0), ops.get(1).cast(DBSPTypeUSize.INSTANCE), true);
            }
            case TRIM: {
                if (call.operands.size() != 3)
                    throw new UnsupportedException(node);
                DBSPKeywordLiteral keyword = ops.get(0).to(DBSPKeywordLiteral.class);
                String functionName = call.getKind().toString().toLowerCase() + "_" +
                        keyword + type.nullableSuffix();
                return new DBSPApplyExpression(node, functionName, type, ops.get(1), ops.get(2));
            }
            case DOT:
            default:
                throw new UnimplementedException(node);
        }
    }

    DBSPExpression compile(RexNode expression) {
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Compiling ")
                .append(expression.toString())
                .newline();
        DBSPExpression result = expression.accept(this);
        if (result == null)
            throw new UnimplementedException(new CalciteObject(expression));
        return result;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
