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

package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Core representation of a dataflow graph (aka a query plan).
 */
public class DBSPCircuit extends DBSPNode implements IDBSPOuterNode {
    /**
     * All the operators are in fact in the partial circuit.
     */
    public final DBSPPartialCircuit circuit;
    public final String name;

    public DBSPCircuit(CalciteObject object, DBSPPartialCircuit circuit, String name) {
        super(object);
        this.circuit = circuit;
        this.name = name;
    }

    @Override
    public CalciteObject getNode() {
        return this.circuit.getNode();
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop()) {
            this.circuit.accept(visitor);
            visitor.postorder(this);
        }
        visitor.pop(this);
    }

    @Nullable
    public DBSPOperator getOperator(String tableOrView) {
        return this.circuit.getOperator(tableOrView);
    }

    /**
     * @return The number of outputs of the circuit (SinkOperators).
     */
    public int getOutputCount() {
        return this.circuit.getOutputCount();
    }

    /**
     * The output type of the i-th output.
     * Outputs are the views, numbered in the order of creation in the
     * input SQL program.
     * @param i Output number.
     */
    public DBSPType getOutputType(int i) {
        return this.circuit.getOutputType(i);
    }

    /**
     * The output type of the i-th input.
     * Inputs are the tables, numbered in the order of creation in the
     * input SQL program.
     * @param i Input number.
     */
    public DBSPType getInputType(int i) {
        return this.circuit.getInputType(i);
    }

    /**
     * The list of all input tables of the circuit.
     */
    public List<String> getInputTables() {
        return this.circuit.getInputTables();
    }

    /**
     * Create an identical circuit to this one but with a different name.
     * @param name Name of the new circuit.
     */
    public DBSPCircuit rename(String name) {
        return new DBSPCircuit(this.getNode(), this.circuit, name);
    }

    /**
     * Return true if this circuit and other are identical (have the exact same operators).
     */
    public boolean sameCircuit(DBSPCircuit other) {
        return this.circuit.sameCircuit(other.circuit);
    }

    /**
     * Number of operators in the circuit.
     */
    public int size() {
        return this.circuit.size();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("Circuit ")
                .append(this.name)
                .append(" {")
                .increase()
                .append(this.circuit)
                .decrease()
                .append("}")
                .newline();
    }
}
