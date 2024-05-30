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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A DBSP operator that applies a function to the inputs and produces an output.
 * On the naming of the operator classes:
 * Each operator has an "operation" field. This one corresponds to the
 * Rust Stream method that is invoked to implement the operator.
 * Some operators have "Stream" in their name.  These usually correspond
 * to a method starting with "stream_*".
 * Some operators compute correctly both over deltas and aver whole sets, e.g. Map.
 */
public abstract class DBSPOperator extends DBSPNode implements IHasType, IDBSPOuterNode {
    public final List<DBSPOperator> inputs;
    /** Operation that is invoked on inputs; corresponds to a DBSP operator name, e.g., join. */
    public final String operation;
    /** Computation invoked by the operator, usually a closure. */
    @Nullable
    public final DBSPExpression function;
    /** Type of output produced. */
    public final DBSPType outputType;
    /** True if the output of the operator is a multiset. */
    public final boolean isMultiset;
    @Nullable
    public final String comment;
    public final DBSPType outputStreamType;
    /** id of the operator this one is derived from.  -1 for "new" operators */
    public long derivedFrom;

    protected DBSPOperator(CalciteObject node, String operation,
                           @Nullable DBSPExpression function, DBSPType outputType,
                           boolean isMultiset, @Nullable String comment) {
        super(node);
        this.inputs = new ArrayList<>();
        this.operation = operation;
        this.function = function;
        this.outputType = outputType;
        this.isMultiset = isMultiset;
        this.comment = comment;
        this.outputStreamType = new DBSPTypeStream(this.outputType);
        this.derivedFrom = -1;
        if (!operation.startsWith("waterline") &&
                !operation.startsWith("apply") &&
                !operation.startsWith("delay") &&
                !outputType.is(DBSPTypeZSet.class) &&
                !outputType.is(DBSPTypeIndexedZSet.class))
            throw new InternalCompilerError("Operator output type is unexpected " + outputType);
    }

    public String getOutputName() {
        return "stream" + this.getId();
    }

    public String getDerivedFrom() {
        if (this.derivedFrom >= 0)
            return Long.toString(this.derivedFrom);
        return Long.toString(this.id);
    }

    public String getIdString() {
        String result = Long.toString(this.id);
        if (this.derivedFrom >= 0)
            result += "(" + this.derivedFrom + ")";
        return result;
    }

    public DBSPOperator(CalciteObject node, String operation,
                        @Nullable DBSPExpression function,
                        DBSPType outputType, boolean isMultiset) {
        this(node, operation, function, outputType, isMultiset, null);
    }

    public void setDerivedFrom(long id) {
        if (id != this.id)
            this.derivedFrom = id;
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
            throw new InternalCompilerError(this + ": Expected function to return " + expected +
                    " but it returns " + type, this);
    }

    /**
     * True if this operator is parameterized by a function.
     * This is not always the same as this.function == null,
     * e.g., AggregateOperator always returns 'true', although
     * the function can be represented as an Aggregate.
     * This is also true for ConstantOperator, although for these
     * the function may not be a closure, but rather a constant. */
    public boolean hasFunction() {
        return this.function != null;
    }

    /** Return a version of this operator with the function replaced. */
    public abstract DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType);

    public DBSPTypeZSet getOutputZSetType() { return this.outputType.to(DBSPTypeZSet.class); }

    public DBSPTypeIndexedZSet getOutputIndexedZSetType() {
        return this.outputType.to(DBSPTypeIndexedZSet.class);
    }

    public DBSPType getOutputZSetElementType() {
        return this.getOutputZSetType().elementType;
    }

    /** If the output is a ZSet it returns the element type.
     * If the output is an IndexedZSet it returns the tuple (keyType, elementType).
     * If the output is something else, it returns its type.
     * (The last can happen for apply nodes, after insertion of limiters). */
    public DBSPType getOutputRowType() {
        if (this.outputType.is(DBSPTypeZSet.class))
            return this.getOutputZSetElementType();
        if (this.outputType.is(DBSPTypeIndexedZSet.class))
            return this.getOutputIndexedZSetType().getKVType();
        return this.outputType;
    }

    /**
     * Check that the specified source operator produces a ZSet/IndexedZSet with element types that can be fed
     * to the specified function.
     * @param function Function with multiple arguments
     * @param source   Source operator producing the arg input to function.
     * @param arg      Argument number of the function supplied from source operator. */
    protected void checkArgumentFunctionType(
            DBSPExpression function, @SuppressWarnings("SameParameterValue") int arg, DBSPOperator source) {
        if (function.getType().is(DBSPTypeAny.class))
            return;
        DBSPType sourceElementType;
        DBSPTypeZSet zSet = source.outputType.as(DBSPTypeZSet.class);
        DBSPTypeIndexedZSet iZSet = source.outputType.as(DBSPTypeIndexedZSet.class);
        if (zSet != null) {
            sourceElementType = zSet.elementType.ref();
        } else if (iZSet != null) {
            sourceElementType = iZSet.getKVRefType();
        } else {
            throw new InternalCompilerError(
                    "Source " + source + " does not produce an (Indexed)ZSet, but "
                    + source.outputType, this);
        }
        DBSPTypeFunction funcType = function.getType().to(DBSPTypeFunction.class);
        DBSPType argType = funcType.argumentTypes[arg];
        if (argType.is(DBSPTypeAny.class))
            return;
        if (!sourceElementType.sameType(argType))
            throw new InternalCompilerError("Expected function to accept " + sourceElementType +
                    " as argument " + arg + " but it expects " + funcType.argumentTypes[arg], this);
    }

    protected void addInput(DBSPOperator node) {
        //noinspection ConstantConditions
        if (node == null)
            throw new InternalCompilerError("Null input to operator", node);
        this.inputs.add(node);
    }

    @Override
    public DBSPType getType() {
        return this.outputType;
    }

    public DBSPExpression getFunction() {
        return Objects.requireNonNull(this.function);
    }

    /** Return a version of this operator with the inputs replaced.
     * @param newInputs  Inputs to use instead of the old ones.
     * @param force      If true always return a new operator.
     *                   If false and the inputs are the same this may return this.
     */
    public abstract DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force);

    /**
     * Return true if any of the inputs in `newInputs` is different from one of the inputs
     * of this operator.
     * @param newInputs  List of alternative inputs.
     * @param sameSizeRequired  If true and the sizes don't match, throw.
     */
    public boolean inputsDiffer(List<DBSPOperator> newInputs, boolean sameSizeRequired) {
        if (this.inputs.size() != newInputs.size()) {
            if (sameSizeRequired)
                throw new InternalCompilerError("Comparing opeartor with " + this.inputs.size() +
                        " inputs with a list of " + newInputs.size() + " inputs", this);
            else
                return false;
        }
        for (boolean b: Linq.zip(this.inputs, newInputs, (l, r) -> l != r)) {
            if (b)
                return true;
        }
        return false;
    }

    public boolean sameInputs(DBSPOperator other) {
        return !this.inputsDiffer(other.inputs, false);
    }

    public boolean inputsDiffer(List<DBSPOperator> newInputs) {
        return this.inputsDiffer(newInputs, true);
    }

    @Override
    public String toString() {
        return this.getClass()
                .getSimpleName()
                .replace("DBSP", "")
                .replace("Operator", "")
                + " " + this.getIdString()
                + (this.comment != null ? this.comment : "");
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
                this.getClass().getSimpleName() + " " + this.getIdString() +
                (this.comment != null ? "\n" + this.comment : ""));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        this.writeComments(builder)
                .append("let ")
                .append(this.getOutputName())
                .append(": ")
                .append(this.outputStreamType)
                .append(" = ");
        if (!this.inputs.isEmpty())
            builder.append(this.inputs.get(0).getOutputName())
                    .append(".");
        builder.append(this.operation)
                .append("(");
        for (int i = 1; i < this.inputs.size(); i++) {
            if (i > 1)
                builder.append(",");
            builder.append("&")
                    .append(this.inputs.get(i).getOutputName());
        }
        if (this.function != null) {
            if (this.inputs.size() > 1)
                builder.append(", ");
            builder.append(this.function);
        }
        return builder.append(");");
    }

    /** True if this is equivalent with the other operator,
     * which means that common-subexpression elimination can replace this with 'other'.
     * This implies that all inputs are the same, and the computed functions are the same. */
    public boolean equivalent(DBSPOperator other) {
        // Default implementation
        if (!this.operation.equals(other.operation))
            return false;
        if (!this.sameInputs(other))
            return false;
        return EquivalenceContext.equiv(this.function, other.function);
    }
}
