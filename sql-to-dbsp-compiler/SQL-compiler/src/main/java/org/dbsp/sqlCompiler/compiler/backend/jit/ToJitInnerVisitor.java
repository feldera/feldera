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

package org.dbsp.sqlCompiler.compiler.backend.jit;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITParameter;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITParameterMapping;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBlock;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBlockDestination;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBranchTerminator;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITJumpTerminator;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITReturnTerminator;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITBinaryInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITCastInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITConstantInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITCopyInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITFunctionCall;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstructionPair;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstructionRef;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITIsNullInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITLiteral;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITLoadInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITMuxInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITSetNullInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITStoreInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITUnaryInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JitUninitInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITBoolType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITI64Type;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITScalarType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITType;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.util.IModule;
import org.dbsp.util.Logger;
import org.dbsp.util.Unimplemented;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Generate code for the JIT compiler.
 * Handles InnerNodes - i.e., expressions, closures, statements.
 */
public class ToJitInnerVisitor extends InnerVisitor implements IModule {
    /**
     * Contexts keep track of variables defined.
     * Variable names can be redefined, as in Rust, and a context
     * will always return the one in the current scope.
     */
    class Context {
        /**
         * Maps variable names to expression ids.
         */
        final Map<String, JITInstructionPair> variables;

        public Context() {
            this.variables = new HashMap<>();
        }

        @Nullable
        JITInstructionPair lookup(String varName) {
            if (this.variables.containsKey(varName))
                return this.variables.get(varName);
            return null;
        }

        /**
         * Add a new variable to the current context.
         * @param varName   Variable name.
         * @param needsNull True if the variable is a scalar nullable variable.
         *                  Then we allocate an extra value to hold its
         *                  nullability.
         */
        JITInstructionPair addVariable(String varName, boolean needsNull) {
            if (this.variables.containsKey(varName)) {
                throw new RuntimeException("Duplicate declaration " + varName);
            }
            JITInstructionRef value = ToJitInnerVisitor.this.nextId();
            JITInstructionRef isNull = new JITInstructionRef();
            if (needsNull) {
                isNull = ToJitInnerVisitor.this.nextId();
            }
            JITInstructionPair result = new JITInstructionPair(value, isNull);
            this.variables.put(varName, result);
            return result;
        }

        /**
         * Add a variable whose definition is known.
         * @param varName  Name of variable.
         * @param pair     Expression that computed the value of the variable.
         */
        void addVariable(String varName, JITInstructionPair pair) {
            this.variables.put(varName, pair);
        }
    }

    /**
     * Used to allocate ids for expressions and blocks.
     */
    private long nextInstrId = 1;
    /**
     * Write here the blocks generated.
     */
    final List<JITBlock> blocks;
    /**
     * Maps each expression to the values that it produces.
     */
    final Map<DBSPExpression, JITInstructionPair> expressionToValues;
    /**
     * A context for each block expression.
     */
    public final List<Context> declarations;

    @Nullable
    JITBlock currentBlock;
    /**
     * The type catalog shared with the ToJitVisitor.
     */
    public final TypeCatalog typeCatalog;
    /**
     * The names of the variables currently being assigned.
     * This is a stack, because we can have nested blocks:
     * let var1 = { let v2 = 1 + 2; v2 + 1 }
     */
    final List<String> variableAssigned;
    /**
     * A description how closure parameters are mapped to JIT parameters.
     */
    final JITParameterMapping mapping;
    final ToJitVisitor jitVisitor;

    public ToJitInnerVisitor(DBSPCompiler compiler, List<JITBlock> blocks,
                             TypeCatalog typeCatalog, ToJitVisitor parent,
                             JITParameterMapping mapping) {
        super(compiler, true);
        this.blocks = blocks;
        this.jitVisitor = parent;
        this.typeCatalog = typeCatalog;
        this.expressionToValues = new HashMap<>();
        this.declarations = new ArrayList<>();
        this.currentBlock = null;
        this.mapping = mapping;
        this.variableAssigned = new ArrayList<>();
    }

    long nextInstructionId() {
        long result = this.nextInstrId;
        this.nextInstrId++;
        return result;
    }

    JITInstructionRef nextId() {
        long id = this.nextInstructionId();
        return new JITInstructionRef(id);
    }

    JITInstructionPair resolve(String varName) {
        // Look in the contexts in backwards order
        for (int i = 0; i < this.declarations.size(); i++) {
            int index = this.declarations.size() - 1 - i;
            Context current = this.declarations.get(index);
            JITInstructionPair ids = current.lookup(varName);
            if (ids != null)
                return ids;
        }
        throw new RuntimeException("Could not resolve " + varName);
    }

    void map(DBSPExpression expression, JITInstructionPair pair) {
        Logger.INSTANCE.from(this, 2)
                .append("Compiled ")
                .append(expression.toString())
                .append(" to ")
                .append(pair.toString())
                .newline();
        Utilities.putNew(this.expressionToValues, expression, pair);
    }

    JITInstructionPair getExpressionValues(DBSPExpression expression) {
        return Utilities.getExists(this.expressionToValues, expression);
    }
    
    JITInstructionPair accept(DBSPExpression expression) {
        expression.accept(this);
        return this.getExpressionValues(expression);
    }

    public JITInstructionRef constantBool(boolean value) {
        return this.accept(new DBSPBoolLiteral(value)).value;
    }

    public JITScalarType convertScalarType(DBSPExpression expression) {
        return this.jitVisitor.scalarType(expression.getNonVoidType());
    }

    JITInstruction add(JITInstruction instruction) {
        this.getCurrentBlock().add(instruction);
        return instruction;
    }

    void newContext() {
        this.declarations.add(new Context());
    }

    Context getCurrentContext() {
        return this.declarations.get(this.declarations.size() - 1);
    }

    void popContext() {
        Utilities.removeLast(this.declarations);
    }

    JITInstructionPair declare(String var, boolean needsNull) {
        return this.getCurrentContext().addVariable(var, needsNull);
    }

    /**
     * True if this type needs to store an "is_null" value in a separate place.
     * Tuples don't.  Only scalar nullable types may.
     */
    static boolean needsNull(DBSPType type) {
        if (type.is(DBSPTypeTuple.class))
            return false;
        return type.mayBeNull;
    }

    static boolean needsNull(DBSPExpression expression) {
        return needsNull(expression.getNonVoidType());
    }

    public JITType convertType(DBSPType type) {
        if (this.jitVisitor.isScalarType(type)) {
            return this.jitVisitor.scalarType(type);
        } else {
            return this.typeCatalog.convertTupleType(type, this.jitVisitor);
        }
    }

    public JITBlock getCurrentBlock() {
        return Objects.requireNonNull(this.currentBlock);
    }

    void createFunctionCall(String name,
                            DBSPExpression expression,  // usually an ApplyExpression, but not always
                            DBSPExpression... arguments) {
        // This assumes that the function is called only if no argument is nullable,
        // and that if any argument IS nullable the result is NULL.
        List<JITInstructionRef> nullableArgs = new ArrayList<>();
        List<JITType> argumentTypes = new ArrayList<>();
        List<JITInstructionRef> args = new ArrayList<>();
        for (DBSPExpression arg: arguments) {
            JITInstructionPair argValues = this.accept(arg);
            DBSPType argType = arg.getNonVoidType();
            args.add(argValues.value);
            argumentTypes.add(this.convertType(argType));
            if (argValues.hasNull())
                nullableArgs.add(argValues.isNull);
        }

        // Is any arg nullable?
        JITInstructionRef isNull = new JITInstructionRef();
        for (JITInstructionRef arg: nullableArgs) {
            if (!isNull.isValid())
                isNull = arg;
            else  {
                isNull = this.insertBinary(JITBinaryInstruction.Operation.OR,
                        isNull, arg, JITBoolType.INSTANCE, "");
            }
        }

        // If any operand is null we create a branch and only call the function
        // conditionally.
        // if (anyNull) { result = default; } else { result = funcCall(args); }
        JITBlock onNullBlock = null;
        JITBlock onNonNullBlock = null;
        JITBlock nextBlock = this.getCurrentBlock();
        if (isNull.isValid()) {
            onNullBlock = this.newBlock();
            onNonNullBlock = this.newBlock();
            nextBlock = this.newBlock();
            JITBranchTerminator branch = new JITBranchTerminator(
                    isNull, onNullBlock.createDestination(),
                    onNonNullBlock.createDestination());
            this.getCurrentBlock().terminate(branch);
            this.setCurrentBlock(onNonNullBlock);
        }

        JITScalarType resultType = convertScalarType(expression);
        long id = this.nextInstructionId();
        JITInstruction call = this.add(new JITFunctionCall(id, name, args, argumentTypes, resultType));
        JITInstructionPair result;

        if (isNull.isValid()) {
            Objects.requireNonNull(nextBlock);
            Objects.requireNonNull(onNonNullBlock);
            Objects.requireNonNull(onNullBlock);
            JITInstructionRef param = this.addParameter(nextBlock, resultType);

            JITBlockDestination next = nextBlock.createDestination();
            next.addArgument(call.getInstructionReference());
            JITJumpTerminator terminator = new JITJumpTerminator(next);
            onNonNullBlock.terminate(terminator);

            next = nextBlock.createDestination();
            this.setCurrentBlock(onNullBlock);
            DBSPLiteral defaultValue = expression.getNonVoidType()
                    .setMayBeNull(false)
                    .to(DBSPTypeBaseType.class)
                    .defaultValue();
            JITInstructionPair defJitValue = this.accept(defaultValue);
            next.addArgument(defJitValue.value);
            terminator = new JITJumpTerminator(next);
            onNullBlock.terminate(terminator);

            this.setCurrentBlock(nextBlock);
            result = new JITInstructionPair(param, isNull);
        } else {
            result = new JITInstructionPair(call);
        }
        this.map(expression, result);
    }

    /////////////////////////// Code generation

    @Override
    public boolean preorder(DBSPExpression expression) {
        throw new Unimplemented(expression);
    }

    @Override
    public boolean preorder(DBSPSomeExpression expression) {
        JITInstructionPair source = this.accept(expression.expression);
        JITInstructionRef False = this.constantBool(false);
        JITInstructionPair result = new JITInstructionPair(source.value, False);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPApplyExpression expression) {
        DBSPPathExpression path = expression.function.as(DBSPPathExpression.class);
        if (path != null) {
            String jitFunction = null;
            String function = path.path.toString();
            switch (function) {
                case "extract_Timestamp_second":
                    jitFunction = "dbsp.timestamp.second";
                    break;
                case "extract_Timestamp_minute":
                    jitFunction = "dbsp.timestamp.minute";
                    break;
                case "extract_Timestamp_hour":
                    jitFunction = "dbsp.timestamp.hour";
                    break;
                case "extract_Timestamp_day":
                    jitFunction = "dbsp.timestamp.day";
                    break;
                case "extract_Timestamp_dow":
                    jitFunction = "dbsp.timestamp.day_of_week";
                    break;
                case "extract_Timestamp_doy":
                    jitFunction = "dbsp.timestamp.day_of_year";
                    break;
                case "extract_Timestamp_isodow":
                    jitFunction = "dbsp.timestamp.iso_day_of_week";
                    break;
                case "extract_Timestamp_week":
                    jitFunction = "dbsp.timestamp.week";
                    break;
                case "extract_Timestamp_month":
                    jitFunction = "dbsp.timestamp.month";
                    break;
                case "extract_Timestamp_year":
                    jitFunction = "dbsp.timestamp.year";
                    break;
                case "extract_Timestamp_isoyear":
                    jitFunction = "dbsp.timestamp.isoyear";
                    break;
                case "extract_Timestamp_quarter":
                    jitFunction = "dbsp.timestamp.quarter";
                    break;
                case "extract_Timestamp_decade":
                    jitFunction = "dbsp.timestamp.decade";
                    break;
                case "extract_Timestamp_century":
                    jitFunction = "dbsp.timestamp.century";
                    break;
                case "extract_Timestamp_millennium":
                    jitFunction = "dbsp.timestamp.millennium";
                    break;
                case "extract_Timestamp_epoch":
                    jitFunction = "dbsp.timestamp.epoch";
                    break;
                default:
                    break;
            }
            if (jitFunction != null) {
                this.createFunctionCall(jitFunction, expression, expression.arguments);
                return false;
            }
        }
        throw new Unimplemented(expression);
    }

    @Override
    public boolean preorder(DBSPLiteral expression) {
        JITScalarType type = convertScalarType(expression);
        JITLiteral literal = new JITLiteral(expression, type);
        boolean mayBeNull = expression.getNonVoidType().mayBeNull;
        JITConstantInstruction value = new JITConstantInstruction(
                this.nextInstructionId(), type, literal, true);
        this.add(value);

        JITInstructionRef isNull = new JITInstructionRef();
        if (mayBeNull) {
            isNull = this.constantBool(expression.isNull);
        }
        JITInstructionPair pair = new JITInstructionPair(value.getInstructionReference(), isNull);
        this.map(expression, pair);
        return false;
    }

    public boolean preorder(DBSPBorrowExpression expression) {
        JITInstructionPair sourceId = this.accept(expression.expression);
        Utilities.putNew(this.expressionToValues, expression, sourceId);
        return false;
    }

    @Override
    public boolean preorder(DBSPCastExpression expression) {
        JITInstructionPair sourceId = this.accept(expression.source);
        JITScalarType sourceType = convertScalarType(expression.source);
        JITScalarType destinationType = convertScalarType(expression);
        JITInstructionRef cast = this.insertCast(sourceId.value, sourceType, destinationType, expression.toString());
        JITInstructionRef isNull = new JITInstructionRef();
        if (needsNull(expression)) {
            if (needsNull(expression.source)) {
                isNull = sourceId.isNull;
            } else {
                isNull = this.constantBool(false);
            }
        } else {
            if (needsNull(expression.source)) {
                isNull = new JITInstructionRef();
                // TODO: if source is nullable and is null must panic at runtime
                // this.createFunctionCall("dbsp.error.abort", expression);
            }
            // else nothing to do for null field
        }
        this.map(expression, new JITInstructionPair(cast, isNull));
        return false;
    }

    static final Map<DBSPOpcode, JITBinaryInstruction.Operation> opNames = new HashMap<>();

    static {
        opNames.put(DBSPOpcode.ADD, JITBinaryInstruction.Operation.ADD);
        opNames.put(DBSPOpcode.AGG_ADD, JITBinaryInstruction.Operation.ADD);
        opNames.put(DBSPOpcode.AGG_MAX, JITBinaryInstruction.Operation.MAX);
        opNames.put(DBSPOpcode.AGG_MIN, JITBinaryInstruction.Operation.MIN);
        opNames.put(DBSPOpcode.SUB, JITBinaryInstruction.Operation.SUB);
        opNames.put(DBSPOpcode.MUL, JITBinaryInstruction.Operation.MUL);
        opNames.put(DBSPOpcode.DIV, JITBinaryInstruction.Operation.DIV);
        opNames.put(DBSPOpcode.EQ,JITBinaryInstruction.Operation.EQ);
        opNames.put(DBSPOpcode.NEQ, JITBinaryInstruction.Operation.NEQ);
        opNames.put(DBSPOpcode.LT, JITBinaryInstruction.Operation.LT);
        opNames.put(DBSPOpcode.GT, JITBinaryInstruction.Operation.GT);
        opNames.put(DBSPOpcode.LTE, JITBinaryInstruction.Operation.LTE);
        opNames.put(DBSPOpcode.GTE, JITBinaryInstruction.Operation.GTE);
        opNames.put(DBSPOpcode.BW_AND, JITBinaryInstruction.Operation.AND);
        opNames.put(DBSPOpcode.AND, JITBinaryInstruction.Operation.AND);
        opNames.put(DBSPOpcode.BW_OR, JITBinaryInstruction.Operation.OR);
        opNames.put(DBSPOpcode.OR, JITBinaryInstruction.Operation.OR);
        opNames.put(DBSPOpcode.XOR, JITBinaryInstruction.Operation.XOR);
        opNames.put(DBSPOpcode.MAX, JITBinaryInstruction.Operation.MAX);
        opNames.put(DBSPOpcode.MIN, JITBinaryInstruction.Operation.MIN);
    }

    /**
     * Insert an instruction which computes left.isNull OR right.isNull
     * but which works when either is not nullable.  Return a reference to this instruction.
     */
    JITInstructionRef eitherNull(JITInstructionPair left, JITInstructionPair right) {
        if (left.hasNull()) {
            if (right.hasNull()) {
                return this.insertBinary(JITBinaryInstruction.Operation.OR,
                        left.isNull, right.isNull, JITBoolType.INSTANCE, "");
            } else {
                return left.isNull;
            }
        } else {
            if (right.hasNull()) {
                return right.isNull;
            } else {
                return this.constantBool(false);
            }
        }
    }

    JITInstructionRef insertCast(JITInstructionRef source,
                                 JITScalarType sourceType, JITScalarType destType, String comment) {
        if (sourceType.equals(destType))
            // This can happen for casts that only change nullability
            return source;
        JITInstruction cast = this.add(new JITCastInstruction(
                this.nextInstructionId(), source, sourceType, destType, comment));
        return cast.getInstructionReference();
    }

    /**
     * Insert a mux operation in the current block.
     * @param condition  Reference to instruction computing mux condition.
     * @param False      Reference to the constant 'false'.  May be invalid.
     * @param True       Reference to the constant 'true'.  May be invalid.
     * @param left       Reference to the left operand.
     * @param right      Reference to the right operand.
     * @return           A reference to the inserted mux.  If the mux condition
     *                   can be determined to be constant only one of the
     *                   operands may be returned.
     */
    JITInstructionRef insertMux(
            JITInstructionRef condition,
            JITInstructionRef False, JITInstructionRef True,
            JITInstructionRef left, JITInstructionRef right) {
        condition.mustBeValid();
        left.mustBeValid();
        right.mustBeValid();
        if (condition.equals(False))
            return right;
        if (condition.equals(True))
            return left;
        JITInstruction mux = this.add(new JITMuxInstruction(this.nextInstructionId(),
                condition, left, right));
        return mux.getInstructionReference();
    }

    JITInstructionRef insertUnary(JITUnaryInstruction.Operation opcode,
                               JITInstructionRef operand, JITType type) {
        operand.mustBeValid();
        JITInstruction result = this.add(new JITUnaryInstruction(this.nextInstructionId(), opcode, operand, type));
        return result.getInstructionReference();
    }

    JITInstructionRef insertBinary(JITBinaryInstruction.Operation opcode,
                                JITInstructionRef left, JITInstructionRef right,
                                JITType type, String comment) {
        left.mustBeValid();
        right.mustBeValid();
        JITInstruction result = this.add(new JITBinaryInstruction(
                this.nextInstructionId(), opcode, left, right, type, comment));
        return result.getInstructionReference();
    }

    JITInstructionRef addParameter(JITBlock block, JITType type) {
        return block.addParameter(new JITInstructionRef(this.nextInstructionId()), type);
    }

    @Override
    public boolean preorder(DBSPBinaryExpression expression) {
        // a || b for strings is concatenation.
        if (expression.operation.equals(DBSPOpcode.CONCAT)) {
            this.createFunctionCall("dbsp.str.concat_clone", expression,
                    expression.left, expression.right);
            return false;
        }
        if (expression.operation.equals(DBSPOpcode.DIV) &&
                expression.left.getNonVoidType().is(DBSPTypeInteger.class)) {
            // left / right
            JITInstructionPair left = this.accept(expression.left);
            JITInstructionPair right = this.accept(expression.right);

            // division by 0 returns null.
            IsNumericType numeric = expression.left.getNonVoidType().to(IsNumericType.class);
            JITScalarType type = convertScalarType(expression.left);
            // TODO: replace with uninit
            DBSPLiteral numericZero = numeric.getZero();
            JITLiteral jitZero = new JITLiteral(numericZero, type);
            JITInstruction zero = this.add(
                    new JITConstantInstruction(this.nextInstructionId(), type, jitZero, true));
            // (right == 0)
            JITInstructionRef compare = this.insertBinary(JITBinaryInstruction.Operation.EQ,
                    zero.getInstructionReference(), right.value, type, "");
            JITBlock isZero = this.newBlock();
            JITBlock isNotZero = this.newBlock();
            JITBlock next = this.newBlock();
            JITBlockDestination isZeroDestination = isZero.createDestination();
            JITBlockDestination isNotZeroDestination = isNotZero.createDestination();
            JITBranchTerminator branch = new JITBranchTerminator(
                    compare, isZeroDestination, isNotZeroDestination);
            this.getCurrentBlock().terminate(branch);

            // if (right == 0)
            this.setCurrentBlock(isZero);
            JITInstructionRef True = this.constantBool(true);
            // isNull = true
            JITBlockDestination zeroToNext = next.createDestination();
            zeroToNext.addArgument(True);
            // result can be 0
            zeroToNext.addArgument(zero.getInstructionReference());
            JITJumpTerminator jump = new JITJumpTerminator(zeroToNext);
            isZero.terminate(jump);

            // else
            this.setCurrentBlock(isNotZero);
            // isNull = left.isNull || right.isNull
            JITInstructionRef isNull = this.eitherNull(left, right);
            // result = left / right (even if either is null this is hopefully fine).
            JITInstructionRef div = this.insertBinary(JITBinaryInstruction.Operation.DIV,
                            left.value, right.value, type, expression.toString());
            JITBlockDestination isNotZeroToNext = next.createDestination();
            isNotZeroToNext.addArgument(isNull);
            isNotZeroToNext.addArgument(div);
            jump = new JITJumpTerminator(isNotZeroToNext);
            isNotZero.terminate(jump);

            // join point
            this.setCurrentBlock(next);
            JITInstructionRef resultIsNull = this.addParameter(next, JITBoolType.INSTANCE);
            JITInstructionRef value = this.addParameter(next, type);
            JITInstructionPair result = new JITInstructionPair(value, resultIsNull);
            this.map(expression, result);
            return false;
        }

        JITInstructionPair leftId = this.accept(expression.left);
        JITInstructionPair rightId = this.accept(expression.right);
        boolean special = expression.operation.equals(DBSPOpcode.AND) ||
                (expression.operation.equals(DBSPOpcode.OR));
        if (needsNull(expression) && special) {
            JITInstructionRef False = this.constantBool(false);
            JITInstructionRef True = this.constantBool(true);

            JITInstructionRef leftNullId;
            if (leftId.hasNull())
                leftNullId = leftId.isNull;
            else
                // Not nullable: use false.
                leftNullId = False;
            JITInstructionRef rightNullId;
            if (rightId.hasNull())
                rightNullId = rightId.isNull;
            else
                rightNullId = False;

            if (expression.operation.equals(DBSPOpcode.AND)) {
                // Nullable bit computation
                // (a && b).is_null = a.is_null ? (b.is_null ? true    : b.value)
                //                              : (b.is_null ? a.value : false)

                // cond1 = (b.is_null ? true : b.value)
                this.insertMux(rightNullId, False, True, True, rightId.value);
                JITInstructionRef cond1 = this.insertMux(rightNullId, False, True, True, rightId.value);
                // cond2 = (b.is_null ? !a.value   : false)
                JITInstructionRef cond2 = this.insertMux(rightNullId, False, True, leftId.value, False);

                // (a && b).value = a.is_null ? b.value
                //                            : (b.is_null ? a.value : a.value && b.value)
                // (The value for a.is_null & b.is_null does not matter, so we can choose it to be b.value)
                // a.value && b.value
                JITInstructionRef and = this.insertBinary(
                        JITBinaryInstruction.Operation.AND,
                        leftId.value, rightId.value, convertScalarType(expression.left), "");
                // (b.is_null ? a.value : a.value && b.value)
                JITInstructionRef secondBranch = this.insertMux(rightNullId, False, True, leftId.value, and);
                // Final Mux
                JITInstructionRef value = this.insertMux(
                        leftNullId, False, True, rightId.value, secondBranch);
                JITInstructionRef isNull = this.insertMux(leftNullId, False, True, cond1, cond2);
                this.map(expression, new JITInstructionPair(value, isNull));
            } else { // Boolean ||
                // Nullable bit computation
                // (a || b).is_null = a.is_null ? (b.is_null ? true : !b.value)
                //                              : (b.is_null ? !a.value : false)
                // true
                // cond1 = (b.is_null ? true : !b.value)
                // !b.value
                JITInstructionRef notB = this.insertUnary(
                        JITUnaryInstruction.Operation.NOT, rightId.value, JITBoolType.INSTANCE);
                JITInstructionRef cond1 = this.insertMux(
                        rightNullId, False, True, True, notB);
                // cond2 = (b.is_null ? !a.value : false)
                // !a
                JITInstructionRef notA = this.insertUnary(
                        JITUnaryInstruction.Operation.NOT, leftId.value, JITBoolType.INSTANCE);
                JITInstructionRef cond2 = this.insertMux(rightNullId, False, True, notA, False);

                // (a || b).value = a.is_null ? b.value
                //                            : a.value || b.value
                // a.value || b.value
                JITInstructionRef or = this.insertBinary(
                                JITBinaryInstruction.Operation.OR,
                                leftId.value, rightId.value, convertScalarType(expression.left), "");
                // Result
                JITInstructionRef value = this.insertMux(leftNullId, False, True, cond1, cond2);
                JITInstructionRef isNull = this.insertMux(leftNullId, False, True, rightId.value, or);
                this.map(expression, new JITInstructionPair(value, isNull));
            }
            return false;
        } else if (expression.operation.isAggregate) {
            JITBinaryInstruction.Operation op = Utilities.getExists(opNames, expression.operation);
            // If either operand is null, the result is the other operand.
            if (leftId.hasNull()) {
                JITBlock ifNull = this.newBlock();
                JITBlock ifNotNull = this.newBlock();
                JITBlock next = this.newBlock();
                JITBranchTerminator branch = new JITBranchTerminator(
                        leftId.isNull, ifNull.createDestination(), ifNotNull.createDestination());
                this.getCurrentBlock().terminate(branch);

                this.setCurrentBlock(ifNull);
                JITBlockDestination nextDestination = next.createDestination();
                nextDestination.addArgument(rightId.value);
                nextDestination.addArgument(rightId.hasNull() ? rightId.isNull : this.constantBool(false));
                JITJumpTerminator jump = new JITJumpTerminator(nextDestination);
                this.getCurrentBlock().terminate(jump);

                this.setCurrentBlock(ifNotNull);
                if (rightId.hasNull()) {
                    JITBlock ifRightNull = this.newBlock();
                    JITBlock ifRightNotNull = this.newBlock();
                    branch = new JITBranchTerminator(
                            rightId.isNull, ifRightNull.createDestination(), ifRightNotNull.createDestination());
                    this.getCurrentBlock().terminate(branch);

                    this.setCurrentBlock(ifRightNull);
                    nextDestination = next.createDestination();
                    nextDestination.addArgument(leftId.value);
                    nextDestination.addArgument(leftId.hasNull() ? leftId.isNull : this.constantBool(false));
                    jump = new JITJumpTerminator(nextDestination);
                    this.getCurrentBlock().terminate(jump);

                    this.setCurrentBlock(ifRightNotNull);
                    JITInstructionRef value = this.insertBinary(op, leftId.value, rightId.value,
                            convertScalarType(expression.left), expression.toString());
                    nextDestination = next.createDestination();
                    nextDestination.addArgument(value);
                    nextDestination.addArgument(this.constantBool(false));
                } else {
                    JITInstructionRef value = this.insertBinary(op, leftId.value, rightId.value,
                            convertScalarType(expression.left), expression.toString());
                    nextDestination = next.createDestination();
                    nextDestination.addArgument(value);
                    nextDestination.addArgument(this.constantBool(false));
                }
                jump = new JITJumpTerminator(nextDestination);
                this.getCurrentBlock().terminate(jump);

                this.setCurrentBlock(next);
                JITInstructionRef resultValue = this.addParameter(next, convertScalarType(expression));
                JITInstructionRef resultIsNull = this.addParameter(next, JITBoolType.INSTANCE);
                JITInstructionPair result = new JITInstructionPair(resultValue, resultIsNull);
                this.map(expression, result);
            } else {
                if (rightId.hasNull()) {
                    JITBlock ifNull = this.newBlock();
                    JITBlock ifNotNull = this.newBlock();
                    JITBlock next = this.newBlock();
                    JITBranchTerminator branch = new JITBranchTerminator(
                            rightId.isNull, ifNull.createDestination(), ifNotNull.createDestination());
                    this.getCurrentBlock().terminate(branch);

                    this.setCurrentBlock(ifNull);
                    JITBlockDestination nextDestination = next.createDestination();
                    nextDestination.addArgument(leftId.value);
                    nextDestination.addArgument(leftId.hasNull() ? leftId.isNull : this.constantBool(false));
                    JITJumpTerminator jump = new JITJumpTerminator(nextDestination);
                    this.getCurrentBlock().terminate(jump);

                    this.setCurrentBlock(ifNotNull);
                    JITInstructionRef value = this.insertBinary(op, leftId.value, rightId.value,
                            convertScalarType(expression.left), expression.toString());
                    nextDestination = next.createDestination();
                    nextDestination.addArgument(value);
                    nextDestination.addArgument(this.constantBool(false));
                    jump = new JITJumpTerminator(nextDestination);
                    this.getCurrentBlock().terminate(jump);

                    this.setCurrentBlock(next);
                    JITInstructionRef resultValue = this.addParameter(next, convertScalarType(expression));
                    JITInstructionRef resultIsNull = this.addParameter(next, JITBoolType.INSTANCE);
                    JITInstructionPair result = new JITInstructionPair(resultValue, resultIsNull);
                    this.map(expression, result);
                } else {
                    JITInstructionRef value = this.insertBinary(op, leftId.value, rightId.value,
                            convertScalarType(expression.left), expression.toString());
                    this.map(expression, new JITInstructionPair(value));
                }
            }
            return false;
        } else if (expression.operation.equals(DBSPOpcode.MUL_WEIGHT)) {
            // (a * w).value = (a.value * (type_of_a)w)
            // (a * w).is_null = a.is_null
            JITInstructionRef right;
            JITScalarType rightType = convertScalarType(expression.right);
            JITScalarType leftType = convertScalarType(expression.left);
            // Have to convert the weight to the correct type
            right = this.insertCast(rightId.value, rightType, leftType, "");
            JITInstructionRef value = this.insertBinary(
                JITBinaryInstruction.Operation.MUL, leftId.value, right, leftType, expression.toString());
            JITInstructionRef isNull = new JITInstructionRef();
            if (needsNull(expression))
                isNull = leftId.isNull;
            this.map(expression, new JITInstructionPair(value, isNull));
            return false;
        }

        JITInstructionRef value = this.insertBinary(
                Utilities.getExists(opNames, expression.operation), leftId.value, rightId.value,
                convertScalarType(expression.left), expression.toString());
        JITInstructionRef isNull = new JITInstructionRef();
        if (needsNull(expression))
            // The result is null if either operand is null.
            isNull = this.eitherNull(leftId, rightId);
        this.map(expression, new JITInstructionPair(value, isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPUnaryExpression expression) {
        JITInstructionPair source = this.accept(expression.source);
        boolean isWrapBool = expression.operation.equals(DBSPOpcode.WRAP_BOOL);
        JITInstructionRef False = new JITInstructionRef();
        if (isWrapBool || expression.operation.equals(DBSPOpcode.IS_FALSE)
                || expression.operation.equals(DBSPOpcode.IS_TRUE))
            False = this.constantBool(false);
        JITUnaryInstruction.Operation kind;
        JITInstructionRef None = new JITInstructionRef();
        switch (expression.operation) {
            case NEG:
                kind = JITUnaryInstruction.Operation.NEG;
                break;
            case NOT:
                kind = JITUnaryInstruction.Operation.NOT;
                break;
            case WRAP_BOOL: {
                JITInstructionRef value = this.insertMux(
                        source.isNull, False, None, False, source.value);
                this.map(expression, new JITInstructionPair(value));
                return false;
            }
            case IS_FALSE: {
                if (source.hasNull()) {
                    // result = left.is_null ? false : !left.value
                    // ! left.value
                    JITInstructionRef ni = this.insertUnary(
                        JITUnaryInstruction.Operation.NOT, source.value, convertScalarType(expression.source));
                    // result
                    JITInstructionRef value = this.insertMux(source.isNull, False, None, False, ni);
                    this.map(expression, new JITInstructionPair(value));
                    return false;
                } else {
                    kind = JITUnaryInstruction.Operation.NOT;
                }
                break;
            }
            case IS_TRUE: {
                if (source.hasNull()) {
                    // result = left.is_null ? false : left.value
                    // result
                    JITInstructionRef value = this.insertMux(
                        source.isNull, False, None, False, source.value);
                    this.map(expression, new JITInstructionPair(value));
                } else {
                    this.map(expression, new JITInstructionPair(source.value));
                }
                return false;
            }
            case IS_NOT_TRUE: {
                if (source.hasNull()) {
                    // result = left.is_null ? true : !left.value
                    // ! left.value
                    JITInstructionRef ni = this.insertUnary(
                        JITUnaryInstruction.Operation.NOT, source.value, convertScalarType(expression.source));
                    JITInstructionRef True = this.constantBool(true);
                    // result
                    JITInstructionRef value = this.insertMux(source.isNull, False, True, True, ni);
                    this.map(expression, new JITInstructionPair(value));
                    return false;
                } else {
                    kind = JITUnaryInstruction.Operation.NOT;
                }
                break;
            }
            case IS_NOT_FALSE: {
                if (source.hasNull()) {
                    // result = left.is_null ? true : left.value
                    JITInstructionRef True = this.constantBool(true);
                    // result
                    JITInstructionRef value = this.insertMux(
                        source.isNull, False, True, True, source.value);
                    this.map(expression, new JITInstructionPair(value));
                } else {
                    this.map(expression, new JITInstructionPair(source.value));
                }
                return false;
            }
            case INDICATOR: {
                if (!source.hasNull())
                    throw new RuntimeException("indicator called on non-nullable expression" + expression);
                JITInstructionRef value = this.insertCast(
                    source.isNull, JITBoolType.INSTANCE, JITI64Type.INSTANCE, "");
                this.map(expression, new JITInstructionPair(value));
                return false;
            }
            default:
                throw new Unimplemented(expression);
        }
        JITInstructionRef value = this.insertUnary(kind, source.value, convertScalarType(expression.source));
        JITInstructionRef isNull = new JITInstructionRef();
        if (source.hasNull())
            isNull = source.isNull;
        this.map(expression, new JITInstructionPair(value, isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPVariablePath expression) {
        JITInstructionPair pair = this.resolve(expression.variable);
        this.expressionToValues.put(expression, pair);
        // may already be there, but this may be a new variable with the same name,
        // and then we overwrite with the new definition.
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression closure) {
        for (JITParameter param: this.mapping.parameters) {
            this.declare(param.originalName, param.mayBeNull);
            if (param.direction != JITParameter.Direction.IN) {
                String varName = param.originalName;
                this.variableAssigned.add(varName);
            }
        }

        closure.body.accept(this);

        for (JITParameter param: this.mapping.parameters) {
            if (param.direction != JITParameter.Direction.IN)
                Utilities.removeLast(this.variableAssigned);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPLetStatement statement) {
        boolean isTuple = statement.type.is(DBSPTypeTuple.class);
        this.variableAssigned.add(statement.variable);
        if (isTuple) {
            JITInstructionPair ids = this.declare(statement.variable, needsNull(statement.type));
            JITType type = this.convertType(statement.type);
            this.add(new JitUninitInstruction(ids.value.getId(), type.to(JITRowType.class), statement.variable));
        }
        if (statement.initializer != null) {
            statement.initializer.accept(this);
            if (!isTuple) {
                // Only if the expression produces a scalar value will there be an init.
                JITInstructionPair init = this.getExpressionValues(statement.initializer);
                this.getCurrentContext().addVariable(statement.variable, init);
            }
        }
        Utilities.removeLast(this.variableAssigned);
        return false;
    }

    @Override
    public boolean preorder(DBSPFieldExpression expression) {
        JITInstruction isNull = null;
        JITInstructionPair sourceId = this.accept(expression.expression);
        JITRowType sourceType = this.typeCatalog.convertTupleType(
                expression.expression.getNonVoidType(), this.jitVisitor);
        JITInstruction load = this.add(new JITLoadInstruction(
                this.nextInstructionId(), sourceId.value, sourceType,
                expression.fieldNo, convertScalarType(expression), expression.toString()));
        if (needsNull(expression)) {
            isNull = this.add(new JITIsNullInstruction(this.nextInstructionId(), sourceId.value,
                sourceType, expression.fieldNo));
        }
        this.map(expression, new JITInstructionPair(load, isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPIsNullExpression expression) {
        JITInstructionPair sourceId = this.accept(expression.expression);
        this.map(expression, new JITInstructionPair(sourceId.isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPCloneExpression expression) {
        JITInstructionPair source = this.accept(expression.expression);
        if (expression.getNonVoidType().hasCopy()) {
            this.map(expression, source);
            return false;
        }

        JITType type = this.convertType(expression.getNonVoidType());
        if (type.isScalarType()) {
            JITScalarType scalarType = type.to(JITScalarType.class);
            if (!needsNull(expression)) {
                JITInstruction copy = this.add(new JITCopyInstruction(this.nextInstructionId(), source.value, scalarType));
                this.map(expression, new JITInstructionPair(copy));
                return false;
            } else {
                JITBlock isNull = this.newBlock();
                JITBlock isNotNull = this.newBlock();
                JITBlock next = this.newBlock();
                JITBranchTerminator branch = new JITBranchTerminator(
                        source.isNull, isNull.createDestination(), isNotNull.createDestination());
                this.getCurrentBlock().terminate(branch);
                this.setCurrentBlock(isNull);

                JITInstruction uninit = this.add(new JitUninitInstruction(
                        this.nextInstructionId(), scalarType, "if (" + expression + ").is_null"));
                JITBlockDestination nextDestination = next.createDestination();
                nextDestination.addArgument(uninit.getInstructionReference());
                JITJumpTerminator jump = new JITJumpTerminator(nextDestination);
                this.getCurrentBlock().terminate(jump);

                this.setCurrentBlock(isNotNull);
                JITInstruction copy = this.add(new JITCopyInstruction(this.nextInstructionId(), source.value, scalarType));
                nextDestination = next.createDestination();
                nextDestination.addArgument(copy.getInstructionReference());
                jump = new JITJumpTerminator(nextDestination);
                this.getCurrentBlock().terminate(jump);

                this.setCurrentBlock(next);
                JITInstructionRef param = this.addParameter(next, scalarType);
                JITInstructionPair result = new JITInstructionPair(param, source.isNull);
                this.map(expression, result);
            }
        } else {
            throw new Unimplemented("Clone with non-scalar type ", expression);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPIfExpression expression) {
        JITInstructionPair cond = this.accept(expression.condition);
        JITBlock ifTrue = this.newBlock();
        JITBlock ifFalse = this.newBlock();
        boolean nullable = needsNull(expression);

        JITBlock next = this.newBlock();
        JITBranchTerminator branch = new JITBranchTerminator(
                cond.value, ifTrue.createDestination(), ifFalse.createDestination());
        this.getCurrentBlock().terminate(branch);

        this.setCurrentBlock(ifTrue);
        JITInstructionPair positive = this.accept(expression.positive);
        JITBlockDestination nextDest = next.createDestination();
        nextDest.addArgument(positive.value);
        if (nullable)
            nextDest.addArgument(positive.isNull);
        JITJumpTerminator jump = new JITJumpTerminator(nextDest);
        this.getCurrentBlock().terminate(jump);

        this.setCurrentBlock(ifFalse);
        JITInstructionPair negative = this.accept(expression.negative);
        nextDest = next.createDestination();
        nextDest.addArgument(negative.value);
        if (nullable)
            nextDest.addArgument(negative.isNull);
        jump = new JITJumpTerminator(nextDest);
        this.getCurrentBlock().terminate(jump);

        this.setCurrentBlock(next);
        JITType type = this.convertType(expression.getNonVoidType());
        JITInstructionRef paramValue = new JITInstructionRef(this.nextInstructionId());
        JITInstructionRef isNull = new JITInstructionRef();
        this.addParameter(next, type);
        if (nullable) {
            this.addParameter(next, JITBoolType.INSTANCE);
        }
        this.setCurrentBlock(next);
        this.map(expression, new JITInstructionPair(paramValue, isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPRawTupleExpression expression) {
        // Each field is assigned to a different variable.
        // Remove the last n variables
        List<String> tail = new ArrayList<>();
        for (DBSPExpression ignored: expression.fields)
            tail.add(Utilities.removeLast(this.variableAssigned));
        for (DBSPExpression field: expression.fields) {
            // Add each variable and process the corresponding field.
            this.variableAssigned.add(Utilities.removeLast(tail));
            // Convert RawTuples inside RawTuples to regular Tuples
            if (field.is(DBSPRawTupleExpression.class))
                field = new DBSPTupleExpression(field.to(DBSPRawTupleExpression.class).fields);
            field.accept(this);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleExpression expression) {
        // Compile this as an assignment to the currently assigned variable
        String variableAssigned = this.variableAssigned.get(this.variableAssigned.size() - 1);
        JITInstructionPair retValId = this.resolve(variableAssigned);
        JITRowType tupleTypeId = this.typeCatalog.convertTupleType(expression.getNonVoidType(), this.jitVisitor);
        int index = 0;
        for (DBSPExpression field: expression.fields) {
            // Generates 1 or 2 instructions for each field (depending on nullability)
            JITInstructionPair fieldId = this.accept(field);
            this.add(new JITStoreInstruction(this.nextInstructionId(),
                    retValId.value, tupleTypeId, index, fieldId.value,
                    this.jitVisitor.scalarType(field.getNonVoidType()),
                    "into " + expression + "." + index));
            if (fieldId.hasNull()) {
                this.add(new JITSetNullInstruction(this.nextInstructionId(),
                        retValId.value, tupleTypeId, index, fieldId.isNull));
            }
            index++;
        }
        this.map(expression, retValId);
        return false;
    }

    JITBlock newBlock() {
        int blockId = this.blocks.size() + 1;
        JITBlock result = new JITBlock(blockId);
        this.blocks.add(result);
        return result;
    }

    void setCurrentBlock(@Nullable JITBlock block) {
        this.currentBlock = block;
    }

    @Override
    public boolean preorder(DBSPBlockExpression expression) {
        this.newContext();
        JITBlock saveBlock = this.currentBlock;
        JITBlock newBlock = this.newBlock();
        if (this.currentBlock != null) {
            JITJumpTerminator terminator = new JITJumpTerminator(newBlock.createDestination());
            this.currentBlock.terminate(terminator);
        }
        this.setCurrentBlock(newBlock);
        for (DBSPStatement stat: expression.contents)
            stat.accept(this);

        // TODO: handle nullability
        JITInstructionPair resultId = new JITInstructionPair(new JITInstructionRef());
        if (expression.lastExpression != null) {
            if (this.jitVisitor.isScalarType(expression.lastExpression.getType())) {
                resultId = this.accept(expression.lastExpression);
                // Otherwise the result of the block
                // is the result computed by the last expression
                JITInstructionPair id = this.getExpressionValues(expression.lastExpression);
                this.map(expression, id);
            } else {
                // If the result is a tuple, this will store the result in the return value
                // and this block will return Unit.
                expression.lastExpression.accept(this);
            }
        }

        this.popContext();
        if (saveBlock == null) {
            // No outer block, we are terminating a closure, return.
            JITReturnTerminator ret = new JITReturnTerminator(resultId.value);
            this.getCurrentBlock().terminate(ret);
        } else {
            // Create a continuation block and jump to it.
            newBlock = newBlock();
            JITJumpTerminator terminator = new JITJumpTerminator(newBlock.createDestination());
            this.getCurrentBlock().terminate(terminator);
            this.setCurrentBlock(newBlock);
        }
        return false;
    }

    /**
     * Convert the body of a closure expression to JIT representation.
     * @param expression  Expression to generate code for.
     * @param parent      The JIT visitor which invokes this visitor.
     * @param parameterMapping  Mapping that describes the function parameters.
     * @param catalog     The catalog of Tuple types.
     */
    static List<JITBlock> convertClosure(
            DBSPCompiler compiler, ToJitVisitor parent, JITParameterMapping parameterMapping,
            DBSPClosureExpression expression, TypeCatalog catalog) {
        List<JITBlock> blocks = new ArrayList<>();
        ToJitInnerVisitor visitor = new ToJitInnerVisitor(compiler, blocks, catalog, parent, parameterMapping);
        visitor.newContext();
        expression.accept(visitor);
        visitor.popContext();
        return blocks;
    }
}
