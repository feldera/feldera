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

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.*;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.visitors.inner.IRTransform;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBlock;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITTupleLiteral;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITZSetLiteral;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators.*;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.*;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EliminateMulWeight;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpandClone;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveWeightType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerPasses;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Generates an encoding of the circuit as a JITProgram representation.
 */
public class ToJitVisitor extends CircuitVisitor implements IWritesLogs {
    final JITProgram program;

    public ToJitVisitor(IErrorReporter reporter) {
        super(reporter);
        this.program = new JITProgram();
    }

    public TypeCatalog getTypeCatalog() {
        return this.program.typeCatalog;
    }

    class OperatorConversion {
        final @Nullable JITFunction function;
        final List<JITOperatorReference> inputs;
        final JITRowType type;

        public OperatorConversion(DBSPOperator operator, ToJitVisitor jitVisitor) {
            this.type = ToJitVisitor.this.getTypeCatalog().convertTupleType(
                    operator.getOutputZSetElementType(), jitVisitor);
            if (operator.function != null) {
                DBSPExpression func = operator.getFunction();
                this.function = ToJitVisitor.this.convertFunction(func.to(DBSPClosureExpression.class), true);
            } else {
                this.function = null;
            }
            this.inputs = Linq.map(operator.inputs, i -> new JITOperatorReference(i.id));
        }

        public JITFunction getFunction() { return Objects.requireNonNull(this.function); }
    }

    public IRTransform normalizer(boolean simpleParameters) {
        InnerPasses passes = new InnerPasses();
        passes.add(new ExpandClone(this.errorReporter));
        passes.add(new BetaReduction(this.errorReporter));
        passes.add(new Simplify(this.errorReporter));
        if (simpleParameters)
            passes.add(new SimpleClosureParameters(this.errorReporter));
        return passes;
    }

    JITFunction convertFunction(DBSPClosureExpression function, boolean simpleParameters) {
        Logger.INSTANCE.belowLevel(this, 4)
                .append("Canonicalizing")
                .newline()
                .append(function.toString())
                .newline();

        IRTransform normalizer = this.normalizer(simpleParameters);
        function = normalizer.apply(function).to(DBSPClosureExpression.class);
        function = this.tupleEachParameter(function);

        Logger.INSTANCE.belowLevel(this, 4)
                .append("Converting function to JIT")
                .newline()
                .append(function.toString())
                .newline();
        DBSPType resultType = function.getResultType();
        JITParameterMapping mapping = new JITParameterMapping(this.getTypeCatalog());

        for (DBSPParameter param: function.parameters)
            mapping.addParameter(param, JITParameter.Direction.IN, this);

        // If the result type is a scalar, it is marked as a result type.
        // Otherwise, we have to create a new parameter that is returned by reference.
        JITScalarType returnType = mapping.addReturn(resultType, this);

        List<JITBlock> blocks = ToJitInnerVisitor.convertClosure(
                this.errorReporter, this, mapping, function, this.getTypeCatalog());
        JITFunction result = new JITFunction(mapping.parameters, blocks, returnType);
        Logger.INSTANCE.belowLevel(this, 4)
                .append(result.toAssembly())
                .newline();
        return result;
    }

    /**
     * Slightly different version of 'convertFunction', which handles
     * only step functions for the aggregate operation.  We need to
     * handle these specially since they have an 'inout' parameter,
     * which cannot be deduced from the signature we use for the function.
     */
    JITFunction convertStepFunction(DBSPClosureExpression function) {
        Logger.INSTANCE.belowLevel(this, 4)
                .append("Canonicalizing")
                .newline()
                .appendSupplier(function::toString)
                .newline();

        IRTransform normalizer = this.normalizer(false);
        IDBSPInnerNode func = normalizer.apply(function);
        function = this.tupleEachParameter(func.to(DBSPClosureExpression.class));

        Logger.INSTANCE.belowLevel(this, 4)
                .append("Converting step function to JIT")
                .newline()
                .appendSupplier(function::toString)
                .newline();
        JITParameterMapping mapping = new JITParameterMapping(this.getTypeCatalog());
        int index = 0;
        for (DBSPParameter param: function.parameters) {
            mapping.addParameter(param,
                    index == 0 ? JITParameter.Direction.INOUT : JITParameter.Direction.IN,
                    this);
            index++;
        }

        if (index != 3)
            throw new InternalCompilerError(
                    "Expected 3 parameters for step function, got " + index, function);

        List<JITBlock> blocks = ToJitInnerVisitor.convertClosure(
                this.errorReporter, this, mapping, function, this.getTypeCatalog());
        JITFunction result = new JITFunction(mapping.parameters, blocks, JITUnitType.INSTANCE);
        Logger.INSTANCE.belowLevel(this, 4)
                .append(result.toAssembly())
                .newline();
        return result;
    }

    public JITScalarType scalarType(DBSPType type) {
        if (type.sameType(new DBSPTypeTuple()) ||
                type.sameType(new DBSPTypeRawTuple()))
            return JITUnitType.INSTANCE;
        DBSPTypeRef ref = type.as(DBSPTypeRef.class);
        if (ref != null)
            type = ref.type;
        DBSPTypeBaseType base = type.as(DBSPTypeBaseType.class);
        if (base == null)
            throw new InternalCompilerError("Expected a base type " + type, type);
        switch (base.code) {
            case BOOL:
                return JITBoolType.INSTANCE;
            case INT16:
                return JITI16Type.INSTANCE;
            case INT32:
                return JITI32Type.INSTANCE;
            case INT64:
                return JITI64Type.INSTANCE;
            case DOUBLE:
                return JITF64Type.INSTANCE;
            case FLOAT:
                return JITF32Type.INSTANCE;
            case STRING:
                return JITStringType.INSTANCE;
            case DATE:
                return JITDateType.INSTANCE;
            case TIME:
                return JITTimeType.INSTANCE;
            case TIMESTAMP:
                return JITTimestampType.INSTANCE;
            case USIZE:
                return JITUSizeType.INSTANCE;
            case ISIZE:
                return JITISizeType.INSTANCE;
            case DECIMAL:
                return JITDecimalType.INSTANCE;
            case VOID:
                return JITUnitType.INSTANCE;
            default:
                break;
        }
        throw new UnimplementedException(type);
    }

    IJitKvOrRowType convertType(DBSPType type) {
        if (type.is(DBSPTypeZSet.class)) {
            DBSPTypeZSet t = type.to(DBSPTypeZSet.class);
            DBSPType elementType = t.getElementType();
            return this.getTypeCatalog().convertTupleType(elementType, this);
        } else {
            DBSPTypeIndexedZSet t = type.to(DBSPTypeIndexedZSet.class);
            JITRowType keyType = this.getTypeCatalog().convertTupleType(t.keyType, this);
            JITRowType valueType = this.getTypeCatalog().convertTupleType(t.elementType, this);
            return new JITKVType(keyType, valueType);
        }
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        JITRowType type = this.getTypeCatalog().convertTupleType(operator.getOutputZSetElementType(), this);
        JITSourceOperator source = new JITSourceOperator(operator.id, type, operator.outputName);
        this.program.add(source);
        return VisitDecision.STOP;
    }

    public boolean isScalarType(@Nullable DBSPType type) {
        if (type == null)
            return true;
        if (type.is(DBSPTypeBaseType.class))
            return true;
        return type.is(DBSPTypeTupleBase.class) && type.to(DBSPTypeTupleBase.class).size() == 0;
    }

    @Override
    public VisitDecision preorder(DBSPFilterOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITFilterOperator result = new JITFilterOperator(operator.id, conversion.type,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMapIndexOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITRowType keyType = this.getTypeCatalog().convertTupleType(operator.keyType, this);
        JITRowType valueType = this.getTypeCatalog().convertTupleType(operator.valueType, this);
        JITOperator result = new JITMapIndexOperator(operator.id,
                new JITKVType(keyType, valueType), conversion.type,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMapOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        DBSPType inputType = operator.input().outputType;
        IJitKvOrRowType jitInputType = this.convertType(inputType);
        JITOperator result = new JITMapOperator(operator.id, conversion.type, jitInputType,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFlatMapOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITFlatMapOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSumOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITSumOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIncrementalDistinctOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITDistinctOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDistinctOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITStreamDistinctOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSubtractOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITSubtractOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntegralOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITIntegrateOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDifferentialOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITDifferentiateOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIndexOperator operator) {
        DBSPExpression func = operator.getFunction();
        JITFunction function = ToJitVisitor.this.convertFunction(func.to(DBSPClosureExpression.class), true);
        List<JITOperatorReference> inputs = Linq.map(operator.inputs, i -> new JITOperatorReference(i.id));

        JITRowType keyType = this.getTypeCatalog().convertTupleType(operator.keyType, this);
        JITRowType valueType = this.getTypeCatalog().convertTupleType(operator.elementType, this);
        JITOperator result = new JITIndexWithOperator(operator.id,
                keyType, valueType, inputs, function);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIncrementalJoinOperator operator) {
        JITRowType keyType = this.getTypeCatalog().convertTupleType(operator.elementResultType, this);
        JITRowType valueType = this.getTypeCatalog().convertTupleType(new DBSPTypeTuple(new DBSPTypeTuple()), this);
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITJoinOperator(operator.id, keyType, valueType, conversion.type,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPJoinOperator operator) {
        this.errorReporter.reportError(operator.getSourcePosition(), true,
                "No JIT implementation",
                "JIT joins not supported, using incremental join");
        JITRowType keyType = this.getTypeCatalog().convertTupleType(operator.elementResultType, this);
        JITRowType valueType = this.getTypeCatalog().convertTupleType(new DBSPTypeTuple(new DBSPTypeTuple()), this);
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITJoinOperator(operator.id, keyType, valueType, conversion.type,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return VisitDecision.STOP;
    }

    /**
     * Given a closure expression, convert each parameter with a scalar type
     * into a parameter with a 1-dimensional tuple type.  E.g.
     * (t: Tuple1<i32>, u: i32) { body }
     * is converted to
     * (t: Tuple1<i32>, u: Tuple1<i32>) { let u: i32 = u.0; body }
     * Pre-condition: the closure body is a BlockExpression.
     * The JIT representation only supports tuples for closure parameters.
     */
    DBSPClosureExpression tupleEachParameter(DBSPClosureExpression closure) {
        List<DBSPStatement> statements = new ArrayList<>();
        DBSPParameter[] newParams = new DBSPParameter[closure.parameters.length];
        int index = 0;
        for (DBSPParameter param: closure.parameters) {
            if (isScalarType(param.type)) {
                DBSPParameter tuple = new DBSPParameter(param.name, new DBSPTypeTuple(param.type));
                statements.add(new DBSPLetStatement(
                        tuple.asVariableReference().variable,
                        tuple.asVariableReference().field(0)));
                newParams[index] = tuple;
            } else {
                newParams[index] = param;
            }
            index++;
        }
        if (statements.isEmpty())
            return closure;
        DBSPBlockExpression block = closure.body.to(DBSPBlockExpression.class);
        statements.addAll(block.contents);
        DBSPBlockExpression newBlock = new DBSPBlockExpression(
                statements,
                block.lastExpression);
        return newBlock.closure(newParams);
    }

    /**
     * Take a tuple expression and 'flatmap' all tuples inside.
     */
    void flatten(DBSPExpression expression, List<DBSPExpression> fields) {
        DBSPBaseTupleExpression source = expression.as(DBSPBaseTupleExpression.class);
        if (source == null) {
            fields.add(expression);
            return;
        }
        for (DBSPExpression field: source.fields) {
            this.flatten(field, fields);
        }
    }

    @Override
    public VisitDecision preorder(DBSPIncrementalAggregateOperator operator) {
        if (operator.function != null)
            throw new InternalCompilerError("Didn't expect the Aggregate to have a function", operator);

        List<JITOperatorReference> inputs = Linq.map(
                operator.inputs, i -> new JITOperatorReference(i.id));
        JITRowType outputType = this.getTypeCatalog().convertTupleType(operator.outputElementType, this);

        DBSPAggregate aggregate = operator.getAggregate();
        DBSPExpression initial = aggregate.getZero();
        List<DBSPExpression> fields = new ArrayList<>();
        flatten(initial, fields);
        DBSPTupleExpression zeroValue = new DBSPTupleExpression(initial.getNode(), fields);
        JITTupleLiteral init = new JITTupleLiteral(zeroValue, this);

        DBSPClosureExpression closure = aggregate.getIncrement();
        JITFunction stepFn = this.convertStepFunction(closure);

        closure = aggregate.getPostprocessing();
        if (closure.parameters.length != 1)
            throw new InternalCompilerError("Expected function to have exactly 1 parameter, not " +
                    closure.parameters.length, operator);
        JITFunction finishFn = this.convertFunction(closure, false);

        JITRowType accLayout = this.getTypeCatalog().convertTupleType(zeroValue.type, this);
        JITRowType stepLayout = this.getTypeCatalog().convertTupleType(
                aggregate.getIncrement().parameters[1].getType(), this);
        JITOperator result = new JITAggregateOperator(
                operator.id, accLayout, stepLayout, outputType,
                inputs, init, stepFn, finishFn);
        this.program.add(result);

        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregateOperator operator) {
        this.errorReporter.reportError(operator.getSourcePosition(), true,
                "No JIT implementation",
                "JIT aggregates not supported, using incremental aggregate");
        if (operator.function != null)
            throw new InternalCompilerError("Didn't expect the Aggregate to have a function", operator);

        List<JITOperatorReference> inputs = Linq.map(
                operator.inputs, i -> new JITOperatorReference(i.id));
        JITRowType outputType = this.getTypeCatalog().convertTupleType(operator.outputElementType, this);

        DBSPAggregate aggregate = operator.getAggregate();
        DBSPExpression initial = aggregate.getZero();
        List<DBSPExpression> fields = new ArrayList<>();
        flatten(initial, fields);
        DBSPTupleExpression zeroValue = new DBSPTupleExpression(initial.getNode(), fields);
        JITTupleLiteral init = new JITTupleLiteral(zeroValue, this);

        DBSPClosureExpression closure = aggregate.getIncrement();
        JITFunction stepFn = this.convertStepFunction(closure);

        closure = aggregate.getPostprocessing();
        if (closure.parameters.length != 1)
            throw new InternalCompilerError("Expected function to have exactly 1 parameter, not " +
                    closure.parameters.length, operator);
        JITFunction finishFn = this.convertFunction(closure, false);

        JITRowType accLayout = this.getTypeCatalog().convertTupleType(zeroValue.type, this);
        JITRowType stepLayout = this.getTypeCatalog().convertTupleType(
                aggregate.getIncrement().parameters[1].getType(), this);
        JITOperator result = new JITAggregateOperator(
                operator.id, accLayout, stepLayout, outputType,
                inputs, init, stepFn, finishFn);
        this.program.add(result);

        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNegateOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITNegOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator, this);
        JITOperator result = new JITSinkOperator(operator.id, conversion.type, conversion.inputs,
                operator.outputName, operator.query);
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstantOperator operator) {
        JITRowType type = this.getTypeCatalog().convertTupleType(operator.getOutputZSetElementType(), this);
        DBSPZSetLiteral setValue = Objects.requireNonNull(operator.function)
                .to(DBSPZSetLiteral.class);
        JITZSetLiteral setLiteral = new JITZSetLiteral(setValue, type, this);
        JITOperator result = new JITConstantOperator(
                operator.id, type, setLiteral, operator.getFunction().toString());
        this.program.add(result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        throw new UnimplementedException(operator);
    }

    public static JITProgram circuitToJIT(DBSPCompiler compiler, DBSPCircuit circuit) {
        Passes rewriter = new Passes(compiler);
        rewriter.add(new ExpandWritelog(compiler));
        rewriter.add(new BlockClosures(compiler));
        rewriter.add(new ResolveWeightType(compiler, compiler.getWeightTypeImplementation()));
        rewriter.add(new EliminateMulWeight(compiler));
        rewriter.add(new Simplify(compiler));

        circuit = rewriter.apply(circuit);
        Logger.INSTANCE.belowLevel("ToJitVisitor", 2)
                .append("Converting circuit to JIT")
                .newline()
                .appendSupplier(circuit::toString);
        ToJitVisitor visitor = new ToJitVisitor(compiler);
        visitor.apply(circuit);
        return visitor.program;
    }
}
