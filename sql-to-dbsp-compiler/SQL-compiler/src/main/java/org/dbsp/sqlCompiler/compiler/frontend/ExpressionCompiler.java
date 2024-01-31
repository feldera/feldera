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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
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
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CustomFunctions;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
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
import org.dbsp.sqlCompiler.ir.type.DBSPTypeResult;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
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

public class ExpressionCompiler extends RexVisitorImpl<DBSPExpression> implements IWritesLogs, ICompilerComponent {
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

    /**
     * Convert an expression that refers to a field in the input row.
     * @param inputRef   index in the input row.
     * @return           the corresponding DBSP expression.
     */
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
            accumulator = makeBinaryExpression(node, type, opcode, Linq.list(accumulator, operands.get(i)));
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
        // Why doesn't Calcite do this?
        if (operands.size() != 2)
            throw new InternalCompilerError("Expected 2 operands, got " + operands.size(), node);
        DBSPExpression left = operands.get(0);
        DBSPExpression right = operands.get(1);
        if (left == null || right == null)
            throw new UnimplementedException(node);
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();

        if (needCommonType(opcode, type, leftType, rightType)) {
            DBSPType commonBase = reduceType(leftType, rightType);
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

    void validateArgCount(CalciteObject node, int argCount, Integer... expectedArgCount) {
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

    String getCallName(RexCall call) {
        return call.op.getName().toLowerCase();
    }

    DBSPExpression compilePolymorphicFunction(String opName, CalciteObject node, DBSPType resultType,
                                              List<DBSPExpression> ops, Integer... expectedArgCount) {
        this.validateArgCount(node, ops.size(), expectedArgCount);
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
    DBSPExpression compilePolymorphicFunction(RexCall call, CalciteObject node, DBSPType resultType,
                                    List<DBSPExpression> ops, Integer... expectedArgCount) {
        String opName = this.getCallName(call);
        return this.compilePolymorphicFunction(opName, node, resultType, ops, expectedArgCount);
    }

    String typeString(DBSPType type) {
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
    DBSPExpression compileFunction(String baseName, CalciteObject node,
                                   DBSPType resultType, List<DBSPExpression> ops, Integer... expectedArgCount) {
        StringBuilder builder = new StringBuilder(baseName);
        this.validateArgCount(node, ops.size(), expectedArgCount);
        DBSPExpression[] operands = ops.toArray(new DBSPExpression[0]);
        if (expectedArgCount.length > 1)
            // If the function can have a variable number of arguments, postfix with the argument count
            builder.append(operands.length);
        for (DBSPExpression e: ops) {
            DBSPType type = e.getType();
            builder.append(this.typeString(type));
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
    DBSPExpression compileFunction(
            RexCall call, CalciteObject node, DBSPType resultType,
            List<DBSPExpression> ops, Integer... expectedArgCount) {
        return this.compileFunction(this.getCallName(call), node, resultType, ops, expectedArgCount);
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
    DBSPExpression compileKeywordFunction(
            RexCall call, CalciteObject node, @Nullable String functionName,
            DBSPType resultType, List<DBSPExpression> ops,
            int keywordIndex, Integer... expectedArgCount) {
        this.validateArgCount(node, ops.size(), expectedArgCount);
        if (ops.size() <= keywordIndex)
            throw new UnimplementedException(node);
        DBSPKeywordLiteral keyword = ops.get(keywordIndex).to(DBSPKeywordLiteral.class);
        StringBuilder name = new StringBuilder();
        String baseName = functionName != null ? functionName : this.getCallName(call);
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

                        if (left.type.is(DBSPTypeDouble.class))
                            return this.compilePolymorphicFunction(call, node, type, ops, 1);

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
                    case "abs": {
                        return this.compilePolymorphicFunction(call, node, type,
                                ops, 1);
                    }
                    case "st_distance": {
                        return this.compilePolymorphicFunction(call, node, type,
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
                        return this.compilePolymorphicFunction(call, node, type, ops, 1);
                    }
                    case "log":
                    {
                        // Turn the arguments into Double
                        for (int i = 0; i < ops.size(); i++) {
                            this.ensureDouble(ops, i);
                        }
                        return this.compilePolymorphicFunction(call, node, type, ops, 1, 2);
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
                                return this.compilePolymorphicFunction(
                                        "sqrt", node, type, ops, 1);
                            }
                        }

                        // Cast real to double
                        for (int i = 0; i < ops.size(); i++) {
                            if (ops.get(i).type.is(DBSPTypeReal.class)) {
                                this.ensureDouble(ops, i);
                            }
                        }

                        return this.compilePolymorphicFunction(call, node, type,
                                ops, 2);
                    }
                    case "pi": {
                        return this.compileFunction(call, node, type, ops, 0);
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
                    {
                        this.ensureDouble(ops, 0);
                        return this.compilePolymorphicFunction(call, node, type, ops, 1);
                    }
                    case "is_inf":
                    case "is_nan":
                    {
                        // Turn the argument into Double
                        if (!ops.get(0).type.is(DBSPTypeReal.class)) {
                            this.ensureDouble(ops, 0);
                        }
                        return this.compilePolymorphicFunction(call, node, type, ops, 1);
                    }
                    case "atan2":
                    {
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureDouble(ops, i);
                        return this.compilePolymorphicFunction(call, node, type, ops, 2);
                    }
                    case "split":
                        // Calcite should be doing this, but it doesn't.
                        for (int i = 0; i < ops.size(); i++)
                            this.ensureString(ops, i);
                        return this.compileFunction(call, node, type, ops, 1, 2);
                    case "overlay": {
                        // case "regexp_replace":
                        String module_prefix = "";
                        if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                            module_prefix = "binary::";
                        } else {
                            module_prefix = "string::";
                        }
                        return this.compileFunction(module_prefix + getCallName(call), node, type, ops, 3, 4);
                    }
                    case "char_length":
                    case "ascii":
                    case "chr":
                    case "lower":
                    case "upper":
                    case "to_hex":
                    case "octet_length":
                    case "initcap": {
                        return this.compileFunction(call, node, type, ops, 1);
                    }
                    case "cardinality": {
                        this.validateArgCount(node, ops.size(), 1);
                        String name = "cardinality";
                        if (ops.get(0).getType().mayBeNull)
                            name += "N";
                        return new DBSPApplyExpression(node, name, type, ops.get(0));
                    }
                    case "writelog":
                        return new DBSPApplyExpression(node, opName, type, ops.get(0), ops.get(1));
                    case "repeat":
                    case "left":
                        return this.compileFunction(call, node, type, ops, 2);
                    case "replace":
                        return this.compileFunction(call, node, type, ops, 3);
                    case "division":
                        return makeBinaryExpression(node, type, DBSPOpcode.DIV, ops);
                    case "element": {
                        // https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6228
                        type = type.setMayBeNull(true);
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
                        DBSPType baseType = ops.get(0).getType();
                        String functionName = opName + baseType.nullableSuffix();
                        String module_prefix = "";
                        if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                            module_prefix = "binary::";
                        } else {
                            module_prefix = "string::";
                        }
                        return this.compileFunction(module_prefix + functionName, node, type, ops, 2, 3);
                    }
                    case "concat":
                        return makeBinaryExpressions(node, type, DBSPOpcode.CONCAT, ops);
                    case "array":
                        return this.compileFunction(call, node, type, ops, 0);
                }
                return this.compileUDF(node, call, type, ops);
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
                // This is also hit for "date_part", which is an alias for "extract".
                return this.compileKeywordFunction(call, node, "extract", type, ops, 0, 2);
            }
            case RLIKE: {
                // Calcite does not enforce the type of the arguments, why?
                for (int i = 0; i < 2; i++)
                    this.ensureString(ops, i);
                return this.compileFunction(call, node, type, ops, 2);
            }
            case POSITION: {
                String module_prefix = "";
                if (ops.get(0).type.is(DBSPTypeBinary.class)) {
                    module_prefix = "binary::";
                } else {
                    module_prefix = "string::";
                }
                return this.compileFunction(module_prefix + getCallName(call), node, type, ops, 2);
            }
            case ARRAY_TO_STRING: {
                // Calcite does not enforce the type of the arguments, why?
                this.ensureString(ops, 1);
                if (ops.size() > 2)
                    this.ensureString(ops, 2);
                return this.compileFunction(call, node, type, ops, 2, 3);
            }
            case LIKE:
            case SIMILAR: {
                return this.compileFunction(call, node, type, ops, 2, 3);
            }
            case FLOOR:
            case CEIL: {
                if (call.operands.size() == 2) {
                    return this.compileKeywordFunction(call, node, null, type, ops, 1, 2);
                } else if (call.operands.size() == 1) {
                    return this.compilePolymorphicFunction(call, node, type, ops, 1);
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
                return this.compileKeywordFunction(call, node, null, type, ops, 0, 3);
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
        CustomFunctions.ExternalFunction ef = this.compiler.getCustomFunctions()
                .getImplementation(function);
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
        return new DBSPApplyExpression(function, new DBSPTypeResult(type), arguments).unwrap();
    }

    private DBSPExpression castTo(DBSPExpression expression, DBSPType type) {
        DBSPType originalType = expression.type;
        return expression.cast(type.setMayBeNull(originalType.mayBeNull));
    }

    DBSPExpression compile(RexNode expression) {
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
