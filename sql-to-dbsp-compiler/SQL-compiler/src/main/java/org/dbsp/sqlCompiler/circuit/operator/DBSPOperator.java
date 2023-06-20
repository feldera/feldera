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

package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.util.IHasName;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.NameGen;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A DBSP operator that applies a function to the inputs and produces an output.
 */
public abstract class DBSPOperator extends DBSPNode implements IHasName, IHasType, IDBSPOuterNode {
    public final List<DBSPOperator> inputs;
    /**
     * Operation that is invoked on inputs; corresponds to a DBSP operator name, e.g., join.
     */
    public final String operation;
    /**
     * Computation invoked by the operator, usually a closure.
     */
    @Nullable
    public final DBSPExpression function;
    /**
     * Output assigned to this variable.
     */
    public final String outputName;
    /**
     * Type of output produced.
     */
    public final DBSPType outputType;
    /**
     * True if the output of the operator is a multiset.
     */
    public final boolean isMultiset;
    @Nullable
    public final String comment;

    protected DBSPOperator(CalciteObject node, String operation,
                           @Nullable DBSPExpression function, DBSPType outputType,
                           boolean isMultiset, @Nullable String comment, String outputName) {
        super(node);
        this.inputs = new ArrayList<>();
        this.operation = operation;
        this.function = function;
        this.outputName = outputName;
        this.outputType = outputType;
        this.isMultiset = isMultiset;
        this.comment = comment;
    }

    public DBSPOperator(CalciteObject node, String operation,
                        @Nullable DBSPExpression function,
                        DBSPType outputType, boolean isMultiset) {
        this(node, operation, function, outputType, isMultiset, null,
                new NameGen("stream").nextName());
    }

    /**
     * Check that the result type of function is the same as expected.
     * @param function  An expression with a function type.
     * @param expected  Type expected to be returned by the function.
     */
    public void checkResultType(DBSPExpression function, DBSPType expected) {
        if (function.getType().is(DBSPTypeAny.class))
            return;
        DBSPType type = function.getType().to(DBSPTypeFunction.class).resultType;
        if (!expected.sameType(type))
            throw new RuntimeException(this + ": Expected function to return " + expected +
                    " but it returns " + type);
    }

    /**
     * True if this operator is parameterized by a function.
     * This is not always the same as this.function == null,
     * e.g., AggregateOperator always returns 'true', although
     * the function can be represented as an Aggregate.
     * This is also true for ConstantOperator, although for these
     * the function may not be a closure, but rather a constant.
     */
    public boolean hasFunction() {
        return this.function != null;
    }

    /**
     * Return a version of this operator with the function replaced.
     */
    public abstract DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType);

    public DBSPTypeTuple getOutputZSetElementType() {
        return this.outputType.to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
    }

    /**
     * Check that the specified source operator produces a ZSet/IndexedZSet with element types that can be fed
     * to the specified function.
     * @param function Function with multiple arguments
     * @param source   Source operator producing the arg input to function.
     * @param arg      Argument number of the function supplied from source operator.
     */
    protected void checkArgumentFunctionType(DBSPExpression function,
                                             @SuppressWarnings("SameParameterValue") int arg, DBSPOperator source) {
        if (function.getType().is(DBSPTypeAny.class))
            return;
        DBSPType sourceElementType;
        DBSPTypeZSet zSet = source.outputType.as(DBSPTypeZSet.class);
        DBSPTypeIndexedZSet iZSet = source.outputType.as(DBSPTypeIndexedZSet.class);
        if (zSet != null) {
            sourceElementType = zSet.elementType.ref();
        } else if (iZSet != null) {
            sourceElementType = new DBSPTypeRawTuple(
                    iZSet.keyType.ref(),
                    iZSet.elementType.ref());
        } else {
            throw new RuntimeException("Source " + source + " does not produce an (Indexed)ZSet, but "
                    + source.outputType);
        }
        DBSPTypeFunction funcType = function.getType().to(DBSPTypeFunction.class);
        DBSPType argType = funcType.argumentTypes[arg];
        if (argType.is(DBSPTypeAny.class))
            return;
        if (!sourceElementType.sameType(argType))
            throw new RuntimeException(this + ": Expected function to accept " + sourceElementType +
                    " as argument " + arg + " but it expects " + funcType.argumentTypes[arg]);
    }

    protected void addInput(DBSPOperator node) {
        //noinspection ConstantConditions
        if (node == null)
            throw new RuntimeException("Null input to operator");
        this.inputs.add(node);
    }

    @Override
    public String getName() {
        return this.outputName;
    }

    @Override
    public DBSPType getType() {
        return this.outputType;
    }

    public DBSPExpression getFunction() {
        return Objects.requireNonNull(this.function);
    }

    /**
     * Return a version of this operator with the inputs replaced.
     * @param newInputs  Inputs to use instead of the old ones.
     * @param force      If true always return a new operator.
     *                   If false and the inputs are the same this may return this.
     */
    public abstract DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force);

    /**
     * Return true if any of the inputs in `newInputs` is different from one of the inputs
     * of this operator.
     * @param newInputs  List of alternative inputs.
     */
    public boolean inputsDiffer(List<DBSPOperator> newInputs) {
        if (this.inputs.size() != newInputs.size())
            throw new RuntimeException("Trying to replace " + this.inputs.size() +
                    " with " + newInputs.size() + " inputs");
        for (boolean b: Linq.zip(this.inputs, newInputs, (l, r) -> l != r)) {
            if (b)
                return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return this.getClass()
                .getSimpleName()
                .replace("DBSP", "")
                .replace("Operator", "")
                + " " + this.id;
    }

    public SourcePositionRange getSourcePosition() {
        return this.getNode().getPositionRange();
    }

    IIndentStream writeComments(IIndentStream builder, @Nullable String comment) {
        if (comment == null)
            return builder;
        String[] parts = comment.split("\n");
        parts = Linq.map(parts, p -> "// " + p, String.class);
        return builder.intercalate("\n", parts);
    }
    
    IIndentStream writeComments(IIndentStream builder) {
        return this.writeComments(builder, 
                this.getClass().getSimpleName() + " " + this.id +
                (this.comment != null ? "\n" + this.comment : ""));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        DBSPType streamType = new DBSPTypeStream(this.outputType);
        this.writeComments(builder)
                .append("let ")
                .append(this.getName())
                .append(": ")
                .append(streamType)
                .append(" = ");
        if (!this.inputs.isEmpty())
            builder.append(this.inputs.get(0).getName())
                    .append(".");
        builder.append(this.operation)
                .append("(");
        for (int i = 1; i < this.inputs.size(); i++) {
            if (i > 1)
                builder.append(",");
            builder.append("&")
                    .append(this.inputs.get(i).getName());
        }
        if (this.function != null) {
            if (this.inputs.size() > 1)
                builder.append(", ");
            builder.append(this.function);
        }
        return builder.append(");");
    }
}
