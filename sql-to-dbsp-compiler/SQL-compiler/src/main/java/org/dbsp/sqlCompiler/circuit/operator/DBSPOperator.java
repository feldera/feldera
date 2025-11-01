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

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Annotation;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.circuit.annotation.CompactName;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.util.HashString;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/** Base class for DBSPOperators */
public abstract class DBSPOperator extends DBSPNode implements IDBSPOuterNode {
    public final List<OutputPort> inputs;
    public final Annotations annotations;
    /** id of the operator this one is derived from. */
    public long derivedFrom;
    @Nullable
    public final String comment;

    protected DBSPOperator(CalciteRelNode node, @Nullable String comment) {
        super(node);
        this.inputs = new ArrayList<>();
        this.annotations = new Annotations();
        this.derivedFrom = this.id;
        this.comment = comment;
    }

    protected DBSPOperator(CalciteRelNode node) {
        this(node, null);
    }

    /** True if the operator has a port with such an output number.
     * Most useful for {@link DBSPNestedOperator}, where some ports
     * may exist but have no internal operator attached (because they
     * have been deleted by optimizations).
     * @param outputNumber port number. */
    public abstract boolean hasOutput(int outputNumber);

    static DBSPClosureExpression toClosure(@Nullable DBSPExpression expression) {
        return Objects.requireNonNull(expression).to(DBSPClosureExpression.class);
    }

    public DBSPOperator copyAnnotations(DBSPOperator source) {
        if (source != this)
            this.annotations.replace(source.annotations);
        return this;
    }

    public long getDerivedFrom() {
        return this.derivedFrom;
    }

    /** Explicitly set the origin of this operator. */
    public void setDerivedFrom(long id) {
        if (id != this.id && this.derivedFrom > id) {
            this.derivedFrom = id;
            Utilities.enforce(id < this.id);
        }
    }

    public void setDerivedFrom(DBSPOperator operator) {
        this.setDerivedFrom(operator.getDerivedFrom());
    }

    protected void addInput(OutputPort port) {
        this.inputs.add(port);
    }

    @SuppressWarnings("UnusedReturnValue")
    public <T extends DBSPOperator> T addAnnotation(Annotation annotation, Class<T> clazz) {
        this.annotations.add(annotation);
        return this.to(clazz);
    }

    public <T extends DBSPOperator> T addAnnotations(Annotations annotations, Class<T> clazz) {
        this.annotations.add(annotations);
        return this.to(clazz);
    }

    public boolean hasAnnotation(Predicate<Annotation> test) {
        return this.annotations.contains(test);
    }

    public <T extends Annotation> boolean hasAnnotation(Class<T> clazz) {
        return this.hasAnnotation(clazz::isInstance);
    }

    /**
     * Return true if any of the inputs in `newInputs` is different from one of the inputs
     * of this operator.
     * @param newInputs  List of alternative inputs.
     * @param sameSizeRequired  If true and the sizes don't match, throw.
     */
    public boolean inputsDiffer(List<OutputPort> newInputs, boolean sameSizeRequired) {
        if (this.inputs.size() != newInputs.size()) {
            if (sameSizeRequired)
                throw new InternalCompilerError("Comparing operator with " + this.inputs.size() +
                        " inputs with a list of " + newInputs.size() + " inputs", this);
            else
                return true;
        }
        for (boolean b: Linq.zip(this.inputs, newInputs, (l, r) -> !l.equals(r))) {
            if (b)
                return true;
        }
        return false;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean sameInputs(DBSPOperator other) {
        return !this.inputsDiffer(other.inputs, false);
    }

    public boolean inputsDiffer(List<OutputPort> newInputs) {
        return this.inputsDiffer(newInputs, true);
    }

    public String getIdString() {
        String name = CompactName.getCompactName(this);
        if (name != null)
            return name;
        String result = Long.toString(this.id);
        if (this.derivedFrom >= 0 && this.derivedFrom != this.id)
            result += "(" + this.derivedFrom + ")";
        return result;
    }

    /** True if this is equivalent with the other operator,
     * which means that common-subexpression elimination can replace this with 'other'.
     * This implies that all inputs are the same, and the computed functions are the same. */
    public abstract boolean equivalent(DBSPOperator other);

    /** Get the type of the specified output */
    public abstract DBSPType outputType(int outputNo);

    /** True if the specified output is a multiset */
    public abstract boolean isMultiset(int outputNumber);

    public String getNodeName(boolean preferHash) {
        if (preferHash) {
            HashString name = OperatorHash.getHash(this, false);
            if (name != null)
                return name.makeIdentifier("operator");
        }
        String name = CompactName.getCompactName(this);
        if (name == null)
            name = "stream" + this.getId();
        return name;
    }

    public abstract int outputCount();

    public String getCompactName() {
        String name = CompactName.getCompactName(this);
        if (name == null)
            name = Long.toString(this.getId());
        return name;
    }

    /** Return a version of this operator with the inputs replaced.
     * @param newInputs  Inputs to use instead of the old ones.
     * @param force      If true always return a new operator.
     *                   If false and the inputs are the same this may return this.
     */
    public abstract DBSPOperator withInputs(List<OutputPort> newInputs, boolean force);

    public DBSPType outputStreamType(int outputNo, boolean outerCircuit) {
        return new DBSPTypeStream(this.outputType(outputNo), outerCircuit);
    }

    public OutputPort getOutput(int outputNo) {
        return new OutputPort(this, outputNo);
    }

    public CalciteRelNode getRelNode() {
        return this.node.to(CalciteRelNode.class);
    }
}
