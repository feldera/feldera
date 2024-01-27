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

package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;

/**
 * A partial circuit is a circuit under construction.
 * A complete circuit can be obtained by calling the "seal" method.
 */
public class DBSPPartialCircuit extends DBSPNode implements IDBSPOuterNode, IWritesLogs {
    public final List<DBSPSourceBaseOperator> inputOperators = new ArrayList<>();
    public final List<DBSPSinkOperator> outputOperators = new ArrayList<>();
    public final List<DBSPOperator> allOperators = new ArrayList<>();
    public final Map<String, DBSPOperator> operatorDeclarations = new HashMap<>();
    /** Maps indexed z-set table names to the corresponding deindex operator */
    public final IErrorReporter errorReporter;
    public final ProgramMetadata metadata;

    public DBSPPartialCircuit(IErrorReporter errorReporter, ProgramMetadata metadata) {
        super(CalciteObject.EMPTY);
        this.errorReporter = errorReporter;
        this.metadata = metadata;
    }

    /**
     * @return the names of the input tables.
     * The order of the tables corresponds to the inputs of the generated circuit.
     */
    public List<String> getInputTables() {
        return Linq.map(this.inputOperators, DBSPOperator::getName);
    }

    public int getOutputCount() {
        return this.outputOperators.size();
    }

    public DBSPType getOutputType(int outputNo) {
        return this.outputOperators.get(outputNo).getType();
    }

    public DBSPType getInputType(int inputNo) {
        return this.inputOperators.get(inputNo).getType();
    }

    public void addOperator(DBSPOperator operator) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Adding ")
                .append(operator.toString())
                .newline();
        if (this.operatorDeclarations.containsKey(operator.outputName)) {
            DBSPOperator previous = this.operatorDeclarations.get(operator.outputName);
            this.errorReporter.reportError(operator.getSourcePosition(), "Duplicate definition",
                    "Stream " + operator.outputName + " already defined");
            this.errorReporter.reportError(previous.getSourcePosition(), "Duplicate definition",
                    "This is the previous definition");
            return;
        }
        Utilities.putNew(this.operatorDeclarations, operator.outputName, operator);
        if (operator.is(DBSPSourceBaseOperator.class))
            this.inputOperators.add(operator.to(DBSPSourceBaseOperator.class));
        else if (operator.is(DBSPSinkOperator.class))
            this.outputOperators.add(operator.to(DBSPSinkOperator.class));
        this.allOperators.add(operator);
    }

    public Iterable<DBSPOperator> getAllOperators() { return this.allOperators; }

    @Nullable
    public DBSPOperator getOperator(String tableOrView) {
        return this.operatorDeclarations.get(tableOrView);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop()) {
            for (DBSPOperator op : this.allOperators)
                op.accept(visitor);
            visitor.postorder(this);
        }
        visitor.pop(this);
    }

    /**
     * Return true if this circuit and other are identical (have the exact same operators).
     */
    public boolean sameCircuit(DBSPPartialCircuit other) {
        if (this == other)
            return true;
        return Linq.same(this.allOperators, other.allOperators);
    }

    /**
     * No more changes are expected to the circuit.
     */
    public DBSPCircuit seal(String name) {
        return new DBSPCircuit(this.getNode(), this, name);
    }

    public int getInputCount() {
        return this.inputOperators.size();
    }

    @Override
    public String toString() {
        return "PartialCircuit" + this.id;
    }

    /**
     * Number of operators in the circuit.
     */
    public int size() {
        return this.allOperators.size();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.intercalateI(System.lineSeparator(), this.allOperators);
    }
}
