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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
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
import org.dbsp.sqlCompiler.compiler.backend.rust.RustSqlRuntimeLibrary;
import org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ExternalFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPGeoPointLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPKeywordLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeResult;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeFP;
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
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.NULL;

public class ExpressionCompiler extends RexVisitorImpl<DBSPExpression>
        implements IWritesLogs, ICompilerComponent {
    private final TypeCompiler typeCompiler;
    @Nullable
    public final DBSPVariablePath inputRow;
    private final RexBuilder rexBuilder;
    private final List<RexLiteral> constants;
    private final DBSPCompiler compiler;

    public ExpressionCompiler(
            @Nullable DBSPVariablePath inputRow, DBSPCompiler compiler) {
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

    /** Convert an expression that refers to a field in the input row.
     * @param inputRef   index in the input row.
     * @return           the corresponding DBSP expression. */
    @Override
    public DBSPExpression visitInputRef(RexInputRef inputRef) {
        CalciteObject node = CalciteObject.create(inputRef);
        if (this.inputRow == null)
            throw new InternalCompilerError("Row referenced without a row context", node);
        // Unfortunately it looks like we can't trust the type coming from Calcite.
        DBSPTypeTuple type = this.inputRow.getType().deref().to(DBSPTypeTuple.class);
        int index = inputRef.getIndex();
        if (index < type.size()) {
            DBSPExpression field = this.inputRow.deepCopy().deref().field(index);
            return field.applyCloneIfNeeded();
        }
        if (index - type.size() < this.constants.size())
            return this.visitLiteral(this.constants.get(index - type.size()));
        throw new InternalCompilerError("Index in row out of bounds ", node);
    }

    @Override
    public DBSPExpression visitCorrelVariable(RexCorrelVariable correlVariable) {
        CalciteObject node = CalciteObject.create(correlVariable);
        if (this.inputRow == null)
            throw new InternalCompilerError("Correlation variable referenced without a row context", node);
        return this.inputRow.deref().deepCopy();
    }

    @Override
    public DBSPExpression visitLiteral(RexLiteral literal) {
        CalciteObject node = CalciteObject.create(literal);
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
            else if (type.is(DBSPTypeMonthsInterval.class))
                return new DBSPIntervalMonthsLiteral(node, type, Objects.requireNonNull(
                        literal.getValueAs(Integer.class)));
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
                        new DBSPDoubleLiteral(c.getOrdinate(1)),
                        type.mayBeNull);
            } else if (type.is(DBSPTypeTime.class)) {
                return new DBSPTimeLiteral(node, type, Objects.requireNonNull(
                        literal.getValueAs(TimeString.class)));
            } else if (type.is(DBSPTypeBinary.class)) {
                return new DBSPBinaryLiteral(node, type, literal.getValueAs(byte[].class));
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
            if (ri != null) {
                int width = Math.max(li.getWidth(), ri.getWidth());
                return new DBSPTypeInteger(left.getNode(), width, true, false);
            }
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
            if (rd != null)
                return left.setMayBeNull(false);
        }
        throw new UnimplementedException("Cast from " + right + " to " + left);
    }

    // Like makeBinaryExpression, but accepts multiple operands.
    private static DBSPExpression makeBinaryExpressions(
            CalciteObject node, DBSPType type, DBSPOpcode opcode, List<DBSPExpression> operands) {
        if (operands.size() < 2)
            throw new UnimplementedException(node);
        DBSPExpression accumulator = operands.get(0);
        for (int i = 1; i < operands.size(); i++)
            accumulator = makeBinaryExpression(node, type, opcode, accumulator, operands.get(i));
        return accumulator.cast(type);
    }

    @SuppressWarnings("unused")
    public static boolean needCommonType(DBSPOpcode opcode, DBSPType result, DBSPType left, DBSPType right) {
        // Dates can be mixed with other types in a binary operation
        if (left.is(IsDateType.class)) return false;
        if (right.is(IsDateType.class)) return false;
        // Allow mixing different string types in an operation
        return !left.is(DBSPTypeString.class) || !right.is(DBSPTypeString.class);
    }

    public static DBSPExpression makeBinaryExpression(
            CalciteObject node, DBSPType type, DBSPOpcode opcode, List<DBSPExpression> operands) {
        if (operands.size() != 2)
            throw new InternalCompilerError("Expected 2 operands, got " + operands.size(), node);
        DBSPExpression left = operands.get(0);
        DBSPExpression right = operands.get(1);
        if (left == null || right == null)
            throw new UnimplementedException(node);
        return makeBinaryExpression(node, type, opcode, left, right);
    }

    /** Creates a call to the INDICATOR function,
     * which returns 0 for None and 1 for Some.
     * For tuples it returns the multiplication of the indicator for all fields */
    public static DBSPExpression makeIndicator(
            CalciteObject node, DBSPType resultType, DBSPExpression argument) {
        assert !resultType.mayBeNull;
        assert resultType.is(IsNumericType.class);
        DBSPType argType = argument.getType();
        if (argType.is(DBSPTypeTuple.class)) {
            DBSPTypeTupleBase tuple = argument.getType().to(DBSPTypeTuple.class);
            DBSPExpression result = resultType.to(IsNumericType.class).getOne();
            for (int i = 0; i < tuple.size(); i++) {
                DBSPExpression next = makeIndicator(node, resultType, argument.field(i));
                result = new DBSPBinaryExpression(node, resultType, DBSPOpcode.MUL, result, next);
            }
            return result;
        } else  {
            // scalar types, but not only.  Nullable structs fall here too.
            if (!argType.mayBeNull) {
                return resultType.to(IsNumericType.class).getOne();
            } else {
                return new DBSPUnaryExpression(node, resultType, DBSPOpcode.INDICATOR, argument.borrow());
            }
        }
    }

    public static DBSPExpression makeBinaryExpression(
            CalciteObject node, DBSPType type, DBSPOpcode opcode, DBSPExpression left, DBSPExpression right) {
        // Why doesn't Calcite do this?
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();

        if (needCommonType(opcode, type, leftType, rightType)) {
            DBSPType commonBase = reduceType(leftType, rightType);
            if (opcode == DBSPOpcode.ADD || opcode == DBSPOpcode.SUB ||
                    opcode == DBSPOpcode.MUL || opcode == DBSPOpcode.DIV ||
                    // not MOD, Calcite is too smart and it uses the right type for mod
                    opcode == DBSPOpcode.BW_AND ||
                    opcode == DBSPOpcode.BW_OR || opcode == DBSPOpcode.XOR ||
                    opcode == DBSPOpcode.MAX || opcode == DBSPOpcode.MIN) {
                // Use the inferred Calcite type for the output
                commonBase = type.setMayBeNull(false);
            }
            if (commonBase.is(DBSPTypeNull.class)) {
                // Result is always NULL.
                return DBSPLiteral.none(type);
            }
            if (leftType.code == NULL || !leftType.setMayBeNull(false).sameType(commonBase))
                left = left.cast(commonBase.setMayBeNull(leftType.mayBeNull));
            if (rightType.code == NULL || !rightType.setMayBeNull(false).sameType(commonBase))
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

    static void validateArgCount(CalciteObject node, int argCount, Integer... expectedArgCount) {
        boolean legal = false;
        for (int e: expectedArgCount) {
            if (e == argCount) {
                legal = true;
                break;
            }
        }
        if (!legal)
            throw new UnimplementedException(node);
    }

    static String getCallName(RexCall call) {
        return call.op.getName().toLowerCase();
    }

    static DBSPExpression compilePolymorphicFunction(String opName, CalciteObject node, DBSPType resultType,
                                              List<DBSPExpression> ops, Integer... expectedArgCount) {
        validateArgCount(node, ops.size(), expectedArgCount);
        StringBuilder functionName = new StringBuilder(opName);
        DBSPExpression[] operands = ops.toArray(new DBSPExpression[0]);
        for (DBSPExpression op: ops) {
            DBSPType type = op.getType();
            // Form the function name from the argument types
            functionName.append("_").append(type.baseTypeWithSuffix());
        }
        return new DBSPApplyExpression(node, functionName.toString(), resultType, operands);
    }

    /**
     * Compile a function call into a family of Rust functions,
     * depending on the argument types.
     * @param  call Operation that is compiled.
     * @param  node CalciteObject holding the call.
     * @param  resultType Type of result produced by call.  We assume that
     *                    the typechecker is right, and this is the correct
     *                    result produced by this function.  No cast needed.
     * @param  ops  Translated operands for the call.
     * @param  expectedArgCount A list containing all known possible argument counts.
     */
    static DBSPExpression compilePolymorphicFunction(RexCall call, CalciteObject node, DBSPType resultType,
                                    List<DBSPExpression> ops, Integer... expectedArgCount) {
        String opName = getCallName(call);
        return compilePolymorphicFunction(opName, node, resultType, ops, expectedArgCount);
    }

    static String typeString(DBSPType type) {
        DBSPTypeVec vec = type.as(DBSPTypeVec.class);
        String result = "";
        if (vec != null)
            // This is the reverse of what you may expect
            result = typeString(vec.getElementType()) + "vec";
        result += type.mayBeNull ? "N" : "_";
        return result;
    }

    /**
     * Compile a function call into a Rust function.
     *
     * @param baseName         Base name of the called function in Rust.
     *                         To this name we append information about argument
     *                         nullabilty.
     * @param node             CalciteObject holding the call.
     * @param resultType       Type of result produced by call.
     * @param ops              Translated operands for the call.
     * @param expectedArgCount A list containing all known possible argument counts.
     */
    static DBSPExpression compileFunction(String baseName, CalciteObject node,
                                   DBSPType resultType, List<DBSPExpression> ops, Integer... expectedArgCount) {
        StringBuilder builder = new StringBuilder(baseName);
        validateArgCount(node, ops.size(), expectedArgCount);
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
        validateArgCount(node, ops.size(), expectedArgCount);
        if (ops.size() <= keywordIndex)
            throw new UnimplementedException(node);
        DBSPKeywordLiteral keyword = ops.get(keywordIndex).to(DBSPKeywordLiteral.class);
        StringBuilder name = new StringBuilder();
        String baseName = functionName != null ? functionName : getCallName(call);
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
            ops.set(argument, arg.cast(DBSPTypeString.varchar(arg.getType().mayBeNull)));
    }

    void ensureDouble(List<DBSPExpression> ops, int argument) {
        DBSPExpression arg = ops.get(argument);
        if (!arg.getType().is(DBSPTypeDouble.class))
            ops.set(argument, arg.cast(new DBSPTypeDouble(arg.getType().getNode(), arg.getType().mayBeNull)));
    }

    void ensureInteger(List<DBSPExpression> ops, int argument, int length) {
        DBSPExpression arg = ops.get(argument);
        DBSPTypeInteger expected = new DBSPTypeInteger(arg.getType().getNode(), length, true, arg.getType().mayBeNull);

        if (!arg.getType().sameType(expected))
            ops.set(argument, arg.cast(expected));
    }

    void nullLiteralToNullArray(List<DBSPExpression> ops, int arg) {
        if (ops.get(arg).is(DBSPNullLiteral.class)) {
            ops.set(arg, new DBSPTypeVec(new DBSPTypeNull(CalciteObject.EMPTY), true).nullValue());
        }
    }

    String getArrayCallName(RexCall call, DBSPExpression... ops) {
        String method = getCallName(call);
        StringBuilder stringBuilder = new StringBuilder(method);

        for (DBSPExpression op : ops) {
            if (op.getType().mayBeNull) {
                stringBuilder.append("N");
            } else {
                stringBuilder.append("_");
            }
        }

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
        String s = getArrayCallName(call, ops);

        DBSPTypeVec vec = ops[0].type.to(DBSPTypeVec.class);
        DBSPType elemType = vec.getElementType();

        if (elemType.mayBeNull) {
            s = s + "N";
        } else {
            s = s + "_";
        }

        return s;
    }

    /** Ensures that all the elements of this array are of the expectedType
     *  Casts to the expected type if necessary
     * @param arg the Array of elements
     * @param expectedType the expected type of the elements
     */
    DBSPExpression ensureArrayElementsOfType(DBSPExpression arg, DBSPType expectedType) {
        DBSPTypeVec argType = arg.getType().to(DBSPTypeVec.class);
        DBSPType argElemType = argType.getElementType();
        DBSPType expectedVecType = new DBSPTypeVec(expectedType, arg.type.mayBeNull);

        if (!argElemType.sameType(expectedType)) {
            // Apply a cast to every element of the vector
            DBSPVariablePath var = new DBSPVariablePath("v", argElemType.ref());
            DBSPExpression cast = var.deref().cast(expectedType).closure(var.asParameter());
            arg = new DBSPApplyExpression("map", expectedVecType, arg.borrow(), cast);
        }

        return arg;
    }

    @Override
    public DBSPExpression visitFieldAccess(RexFieldAccess field) {
        CalciteObject node = CalciteObject.create(field);
        DBSPExpression source = field.getReferenceExpr().accept(this);
        RelDataTypeField dataField = field.getField();
        return new DBSPFieldExpression(node, source, dataField.getIndex());
    }

    @Override
    public DBSPExpression visitCall(RexCall call) {
        CalciteObject node = CalciteObject.create(call);
        DBSPType type = this.typeCompiler.convertType(call.getType(), false);
        // If type is NULL we can skip the call altogether...
        if (type.is(DBSPTypeNull.class))
            return new DBSPNullLiteral();

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
        switch (call.op.kind) {
            case TIMES:
                return makeBinaryExpression(node, type, DBSPOpcode.MUL, ops);
            case DIVIDE:
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
                return makeUnaryExpression(node, new DBSPTypeBool(CalciteObject.EMPTY, false), DBSPOpcode.NOT, Linq.list(op));
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
                            finalType = finalType.setMayBeNull(true);
                    }
                    if (!result.getType().sameType(finalType))
                        result = result.cast(finalType);
                    for (int i = 1; i < ops.size() - 1; i += 2) {
                        DBSPExpression alt = ops.get(i + 1);
                        if (!alt.getType().sameType(finalType))
                            alt = alt.cast(finalType);
                        DBSPExpression comp = makeBinaryExpression(
                                node, new DBSPTypeBool(CalciteObject.EMPTY, false), DBSPOpcode.EQ,
                                value, ops.get(i));
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
            case CHAR_LENGTH: {
                return compileFunction(call, node, type, ops, 1);
            }
            case ST_POINT: {
                // Sometimes the Calcite type for ST_POINT is nullable
                // even if all arguments are not nullable.  So we can't
                // just use compilePolymorphicFunction.
                if (ops.size() != 2)
                    throw new UnimplementedException("Expected only 2 operands", node);
                DBSPExpression left = ops.get(0);
                DBSPExpression right = ops.get(1);
                String functionName = "make_geopoint" + type.nullableSuffix() +
                        "_" + left.getType().baseTypeWithSuffix() +
                        "_" + right.getType().baseTypeWithSuffix();
                return new DBSPApplyExpression(node, functionName, type, left, right);
            }
            case OTHER_FUNCTION: {
                String opName = call.op.getName().toLowerCase();
                switch (opName) {
                    case "truncate":
                    case "round": {
                        DBSPExpression right;

                        if (call.operands.isEmpty())
                            throw new UnimplementedException(node);

                        DBSPExpression left = ops.get(0);

                        if (call.operands.size() == 1)
                            right = new DBSPI32Literal(0);
                        else
                            right = ops.get(1);

                        DBSPType leftType = left.getType();
                        DBSPType rightType = right.getType();

                        if (rightType.is(DBSPTypeNull.class) ||
                                (right.is(DBSPLiteral.class) && right.to(DBSPLiteral.class).isNull)) {
                            this.compiler.reportWarning(node.getPositionRange(),
                                    "evaluates to NULL", node + ": always returns NULL");
                            return type.nullValue();
                        }

                        if (!rightType.is(DBSPTypeInteger.class))
                            throw new UnimplementedException("ROUND expects a constant second argument", node);

                        // convert to int32
                        DBSPTypeInteger rightInt = rightType.to(DBSPTypeInteger.class);
                        if (rightInt.getWidth() != 32) {
                            right = right.cast(new DBSPTypeInteger(right.getNode(), 32, true, rightType.mayBeNull));
                        }

                        String function = opName + "_" +
                                leftType.baseTypeWithSuffix();
                        return new DBSPApplyExpression(node, function, type, left, right);
                    }
                    case "numeric_inc":
                    case "sign":
                    case "abs": {
                        return compilePolymorphicFunction(call, node, type,
                                ops, 1);
                    }
                    case "st_distance": {
                        return compilePolymorphicFunction(call, node, type,
                                ops, 2);
                    }
                    case "log10":
                    case "ln":
                    {
                        // Cast to Double
                        this.ensureDouble(ops, 0);
                        // See: https://github.com/feldera/feldera/issues/1363
                        if (!ops.get(0).type.mayBeNull) {
                            type = type.setMayBeNull(false);
                        }
                        return compilePolymorphicFunction(call, node, type, ops, 1);
                    }
                    case "log":
                    {
                        // Turn the arguments into Double
                        for (int i = 0; i < ops.size(); i++) {
                            this.ensureDouble(ops, i);
                        }
                        return compilePolymorphicFunction(call, node, type, ops, 1, 2);
                    }
                    case "power": {
                        // power(a, .5) -> sqrt(a).  This is more precise.
                        // Calcite does the opposite conversion.
                        assert ops.size() == 2: "Expected two arguments for power function";

                        // convert integer to double
                        DBSPExpression firstArg = ops.get(0);
                        if (firstArg.type.is(DBSPTypeInteger.class)) {
                            this.ensureDouble(ops, 0);
                        }

                        DBSPExpression argument = ops.get(1);
                        if (argument.is(DBSPDecimalLiteral.class)) {
                            DBSPDecimalLiteral dec = argument.to(DBSPDecimalLiteral.class);
                            BigDecimal pointFive = new BigDecimal(5).movePointLeft(1);
                            if (!dec.isNull && Objects.requireNonNull(dec.value).equals(pointFive)) {
                                ops = Linq.list(ops.get(0));
                                if (ops.get(0).getType().is(DBSPTypeNull.class)) {
                                    ops.set(0, ops.get(0).cast(type));
                                }
                                return compilePolymorphicFunction(
                                        "sqrt", node, type, ops, 1);
                            }
                        }

                        // Cast real to double
                        for (int i = 0; i < ops.size(); i++) {
                            if (ops.get(i).type.is(DBSPTypeReal.class)) {
                                this.ensureDouble(ops, i);
                            }
                        }

                        return compilePolymorphicFunction(call, node, type,
                                ops, 2);
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
                    case "exp":
                    {
                        this.ensureDouble(ops, 0);
                        return compilePolymorphicFunction(call, node, type, ops, 1);
                    }
                    case "is_inf":
                    case "is_nan":
                    {
                        // Turn the argument into Double
                        if (!ops.get(0).type.is(DBSPTypeReal.class)) {
                            this.ensureDouble(ops, 0);
                        }
                        return compilePolymorphicFunction(call, node, type, ops, 1);
                    }
                    case "atan2":
                    {
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureDouble(ops, i);
                        return compilePolymorphicFunction(call, node, type, ops, 2);
                    }
                    case "split":
                        // Calcite should be doing this, but it doesn't.
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureString(ops, i);
                        return compileFunction(call, node, type, ops, 1, 2);
                    case "overlay": {
                        // case "regexp_replace":
                        String module_prefix;
                        if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                            module_prefix = "binary::";
                        } else {
                            module_prefix = "string::";
                        }
                        return compileFunction(module_prefix + getCallName(call), node, type, ops, 3, 4);
                    }
                    case "ascii":
                    case "chr":
                    case "lower":
                    case "upper":
                    case "to_hex":
                    case "octet_length":
                    case "initcap": {
                        return compileFunction(call, node, type, ops, 1);
                    }
                    case "cardinality": {
                        validateArgCount(node, ops.size(), 1);
                        String name = "cardinality";

                        nullLiteralToNullArray(ops, 0);

                        if (ops.get(0).getType().mayBeNull)
                            name += "N";
                        return new DBSPApplyExpression(node, name, type, ops.get(0));
                    }
                    case "writelog":
                        return new DBSPApplyExpression(node, opName, type, ops.get(0), ops.get(1));
                    case "repeat":
                    case "left":
                    case "format_date":
                        return compileFunction(call, node, type, ops, 2);
                    case "replace":
                        return compileFunction(call, node, type, ops, 3);
                    case "division":
                        return makeBinaryExpression(node, type, DBSPOpcode.DIV, ops);
                    case "element": {
                        DBSPExpression arg = ops.get(0);
                        DBSPTypeVec arrayType = arg.getType().to(DBSPTypeVec.class);
                        String method = "element";
                        if (arrayType.mayBeNull)
                            method += "N";
                        return new DBSPApplyExpression(node, method, type, arg);
                    }
                    case "substring": {
                        if (ops.isEmpty())
                            throw new UnimplementedException(node);
                        String module_prefix;
                        if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                            module_prefix = "binary::";
                        } else {
                            module_prefix = "string::";
                        }
                        return compileFunction(module_prefix + opName, node, type, ops, 2, 3);
                    }
                    case "concat":
                        return makeBinaryExpressions(node, type, DBSPOpcode.CONCAT, ops);
                    case "array":
                        return compileFunction(call, node, type, ops, 0);
                    case "gunzip":
                        DBSPExpression arg = ops.get(0);
                        ops.set(0, arg.cast(new DBSPTypeBinary(arg.getNode(), arg.type.mayBeNull)));

                        return compileFunction(call, node, type, ops, 1);
                    case "sequence":
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureInteger(ops, i, 32);

                        return compileFunction(call, node, type, ops, 2);
                }
                return this.compileUDF(node, call, type, ops);
            }
            case ARRAYS_OVERLAP: {
                if (ops.size() != 2)
                    throw new UnimplementedException(node);

                DBSPExpression arg0 = ops.get(0);
                DBSPTypeVec arg0Vec = arg0.getType().to(DBSPTypeVec.class);
                DBSPType arg0ElemType = arg0Vec.getElementType();

                DBSPExpression arg1 = ops.get(1);
                DBSPTypeVec arg1Vec = arg1.getType().to(DBSPTypeVec.class);
                DBSPType arg1ElemType = arg1Vec.getElementType();

                // if the two arrays are of different types of elements
                if (!arg0ElemType.sameType(arg1ElemType)) {
                    // check if the nullability of arg0 needs to change to match arg1
                    if (arg1ElemType.mayBeNull && !arg0ElemType.mayBeNull) {
                        if (arg0Vec.mayBeNull && arg0.equals(arg0Vec.nullValue())) {
                            arg0 = new DBSPTypeVec(arg1ElemType, true).nullValue();
                            ops.set(0, arg0);
                        } else {
                            arg0 = this.ensureArrayElementsOfType(arg0, arg1ElemType);
                            ops.set(0, arg0);
                        }
                    } else if (arg0ElemType.mayBeNull && !arg1ElemType.mayBeNull) {
                        if (arg1Vec.mayBeNull && arg1.equals(arg1Vec.nullValue())) {
                            arg1 = new DBSPTypeVec(arg0ElemType, true).nullValue();
                            ops.set(1, arg1);
                        }
                        arg1 = this.ensureArrayElementsOfType(arg1, arg0ElemType);
                        ops.set(1, arg1);
                    }
                    // if the nullability is not the problem, return an unimplemented error as types are different
                    else {
                        this.compiler.reportError(node.getPositionRange(), "Array elements are of different types",
                                "Fist array is of type: " + arg0ElemType + " and second array is of type: " + arg1ElemType);
                    }
                }

                return compileFunction(call, node, type, ops, 2);
            }
            case OTHER:
                String opName = call.op.getName().toLowerCase();
                //noinspection SwitchStatementWithTooFewBranches
                return switch (opName) {
                    case "||" -> makeBinaryExpression(node, type, DBSPOpcode.CONCAT, ops);
                    default -> throw new UnimplementedException(node);
                };
            case EXTRACT: {
                // This is also hit for "date_part", which is an alias for "extract".
                return compileKeywordFunction(call, node, "extract", type, ops, 0, 2);
            }
            case RLIKE: {
                // Calcite does not enforce the type of the arguments, why?
                for (int i = 0; i < 2; i++)
                    this.ensureString(ops, i);
                return compileFunction(call, node, type, ops, 2);
            }
            case POSITION: {
                String module_prefix;
                if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                    module_prefix = "binary::";
                } else {
                    module_prefix = "string::";
                }
                return compileFunction(module_prefix + getCallName(call), node, type, ops, 2);
            }
            case ARRAY_TO_STRING: {
                // Calcite does not enforce the type of the arguments, why?
                this.ensureString(ops, 1);
                if (ops.size() > 2)
                    this.ensureString(ops, 2);
                return compileFunction(call, node, type, ops, 2, 3);
            }
            case LIKE:
            case SIMILAR: {
                return compileFunction(call, node, type, ops, 2, 3);
            }
            case FLOOR:
            case CEIL: {
                if (call.operands.size() == 2) {
                    return compileKeywordFunction(call, node, null, type, ops, 1, 2);
                } else if (call.operands.size() == 1) {
                    DBSPType opType = ops.get(0).getType();
                    if (opType.is(DBSPTypeInteger.class)) {
                        this.compiler.reportWarning(node.getPositionRange(), "Noop",
                                "Function " + Utilities.singleQuote(getCallName(call))
                                        + " applied to integer arguments is useless.");
                        return ops.get(0);
                    }
                    return compilePolymorphicFunction(call, node, type, ops, 1);
                } else {
                    throw new UnimplementedException(node);
                }
            }
            case ARRAY_VALUE_CONSTRUCTOR: {
                DBSPTypeVec vec = type.to(DBSPTypeVec.class);
                DBSPType elemType = vec.getElementType();
                List<DBSPExpression> args = Linq.map(ops, o -> o.cast(elemType));
                return new DBSPVecLiteral(node, type, args);
            }
            case ITEM: {
                if (call.operands.size() != 2)
                    throw new UnimplementedException(node);
                return new DBSPBinaryExpression(node, type, DBSPOpcode.SQL_INDEX,
                        ops.get(0), ops.get(1).cast(new DBSPTypeUSize(CalciteObject.EMPTY, false)));
            }
            case TIMESTAMP_DIFF:
            case TRIM: {
                return compileKeywordFunction(call, node, null, type, ops, 0, 3);
            }
            case TUMBLE:
                return compilePolymorphicFunction(
                        "tumble", node, type, ops, 2, 3);
            case ARRAY_LENGTH:
            case ARRAY_SIZE: {
                if (call.operands.size() != 1)
                    throw new UnimplementedException(node);

                // same as "cardinality"
                String name = "cardinality";

                nullLiteralToNullArray(ops, 0);

                if (ops.get(0).getType().mayBeNull)
                    name += "N";

                return new DBSPApplyExpression(node, name, type, ops.get(0));
            }
            case ARRAY_PREPEND:
            case ARRAY_APPEND: {
                if (call.operands.size() != 2)
                    throw new UnimplementedException(node);

                DBSPTypeVec vec = type.to(DBSPTypeVec.class);
                DBSPType elemType = vec.getElementType();

                DBSPExpression arg0 = ops.get(0);
                DBSPType arg0type = arg0.type;
                DBSPExpression arg1 = ops.get(1).cast(elemType);

                arg0 = ensureArrayElementsOfType(arg0, elemType);

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
            case ARRAY_MIN:
            {
                if (call.operands.size() != 1)
                    throw new UnimplementedException(node);

                DBSPExpression arg0 = ops.get(0);
                String method = getArrayCallNameWithElemNullability(call, arg0);

                return new DBSPApplyExpression(node, method, type, arg0);
            }
            case ARRAY_CONTAINS:
            case ARRAY_REMOVE:
            case ARRAY_POSITION:
            {
                if (call.operands.size() != 2)
                    throw new UnimplementedException(node);

                DBSPExpression arg0 = ops.get(0);
                DBSPExpression arg1 = ops.get(1);
                DBSPTypeVec vec = arg0.getType().to(DBSPTypeVec.class);
                DBSPType elemType = vec.getElementType();

                // If argument is null for certain, return null
                if (arg1.type.is(DBSPTypeNull.class)) {
                    String warningMessage =
                            node + ": always returns NULL";
                    this.compiler.reportWarning(node.getPositionRange(), "unnecessary function call", warningMessage);
                    return DBSPNullLiteral.none(type);
                }

                String method = getArrayCallName(call, arg0, arg1);

                // the rust code has signature: (Vec<T>, T)
                // so if elements of the vector are nullable and T is an Option type
                // therefore we need to wrap arg1 in Some
                if (elemType.mayBeNull) {
                    arg1 = new DBSPApplyExpression(arg1.getNode(), "Some", arg1.type.setMayBeNull(true), arg1);
                }

                if (!elemType.sameType(arg1.type)) {
                    // edge case for elements of type null (like ARRAY [null])
                    if (elemType.is(DBSPTypeNull.class)) {
                        // cast to type of arg1
                        arg0 = ensureArrayElementsOfType(arg0, arg1.type.setMayBeNull(true));
                    }
                    // if apart from the null case, the types are different
                    else if (!elemType.sameType(arg1.type.setMayBeNull(elemType.mayBeNull))) {
                        this.compiler.reportError(node.getPositionRange(), "array elements and argument of different types",
                                "Fist argument is an array of type: " + elemType +
                                        " and second argument is of type: " + arg1.type
                        );
                    }
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
            case ARRAY_REVERSE:
            {
                if (call.operands.size() != 1)
                    throw new UnimplementedException(node);

                DBSPExpression arg0 = ops.get(0);
                String method = getArrayCallName(call, arg0);

                return new DBSPApplyExpression(node, method, type, arg0);
            }
            case ARRAY_COMPACT: {
                if (call.operands.size() != 1)
                    throw new UnimplementedException(node);

                DBSPExpression arg0 = ops.get(0);
                DBSPTypeVec vecType = arg0.getType().to(DBSPTypeVec.class);
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
                    return new DBSPVecLiteral(arg0.getNode(), arg0.getType(), Linq.list());
                }

                String method = getArrayCallName(call, arg0);
                return new DBSPApplyExpression(node, method, type, arg0);
            }
            case ARRAY_REPEAT: {
                if (call.operands.size() != 2)
                    throw new UnimplementedException(node);
                String method = getArrayCallName(call);
                // Calcite thinks this is nullable, but it probably shouldn't be
                DBSPType nonNull = type.setMayBeNull(false);
                return new DBSPApplyExpression(node, method, nonNull, ops.get(0), ops.get(1)).cast(type);
            }
            case DOT:
            default:
                throw new UnimplementedException(node);
        }
    }

    DBSPExpression toPosition(SourcePosition pos) {
        return new DBSPConstructorExpression(
                new DBSPPath("SourcePosition", "new").toExpression(),
                DBSPTypeAny.getDefault(),
                new DBSPU32Literal(pos.line), new DBSPU32Literal(pos.column));
    }

    DBSPExpression toPosition(SourcePositionRange range) {
        return new DBSPConstructorExpression(
                new DBSPPath("SourcePositionRange", "new").toExpression(),
                DBSPTypeAny.getDefault(),
                toPosition(range.start), toPosition(range.end));
    }

    private DBSPExpression compileUDF(CalciteObject node, RexCall call, DBSPType type, List<DBSPExpression> ops) {
        String function = call.op.getName();  // no lowercase applied
        boolean isConstructor = this.compiler.isStructConstructor(function);
        if (isConstructor) {
            DBSPTypeStruct struct = this.compiler.getStructByName(function);
            assert struct.toTuple().sameType(type);
            DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
            for (int i = 0; i < ops.size(); i++) {
                DBSPExpression opi = ops.get(i);
                opi = opi.applyCloneIfNeeded().cast(tuple.getFieldType(i));
                ops.set(i, opi);
            }
            return new DBSPTupleExpression(node, ops);
        }

        ExternalFunction ef = this.compiler.getCustomFunctions()
                .getSignature(function);
        if (ef == null)
            throw new CompilationError("Function " + Utilities.singleQuote(function) + " is unknown", node);
        List<DBSPType> operandTypes = Linq.map(ef.parameterList,
                p -> this.typeCompiler.convertType(p.getType(), false));
        List<DBSPExpression> converted = Linq.zip(ops, operandTypes, DBSPExpression::cast);
        SourcePositionRange pos = node.getPositionRange();
        DBSPExpression[] arguments = new DBSPExpression[converted.size() + 1];
        arguments[0] = this.toPosition(pos).borrow();
        for (int i = 0; i < converted.size(); i++)
            arguments[i+1] = converted.get(i);
        // The convention is that all such functions return Result.
        // This is not true for functions that we implement internally.
        // Currently, we need to unwrap.
        boolean unwrap = !ef.generated;
        if (unwrap)
            return new DBSPApplyExpression(function, new DBSPTypeResult(type), arguments).resultUnwrap();
        else
            return new DBSPApplyExpression(function, type, arguments);
    }

    public DBSPExpression compile(RexNode expression) {
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Compiling ")
                .append(expression.toString())
                .newline();
        DBSPExpression result = expression.accept(this);
        if (result == null)
            throw new UnimplementedException(CalciteObject.create(expression));
        return result;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
