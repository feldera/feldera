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

import org.dbsp.sqlCompiler.circuit.operator.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/** A partial circuit is a circuit under construction.
 * A complete circuit can be obtained by calling the "seal" method. */
public final class DBSPPartialCircuit extends DBSPNode implements IDBSPOuterNode, IWritesLogs {
    public final List<DBSPDeclaration> declarations = new ArrayList<>();
    public final LinkedHashMap<String, DBSPSourceBaseOperator> sourceOperators = new LinkedHashMap<>();
    public final LinkedHashMap<String, DBSPViewOperator> viewOperators = new LinkedHashMap<>();
    public final LinkedHashMap<String, DBSPSinkOperator> sinkOperators = new LinkedHashMap<>();
    public final List<DBSPOperator> allOperators = new ArrayList<>();
    /** Maps indexed z-set table names to the corresponding deindex operator */
    public final IErrorReporter errorReporter;
    public final ProgramMetadata metadata;
    final Set<DBSPOperator> operators = new HashSet<>();

    public DBSPPartialCircuit(IErrorReporter errorReporter, ProgramMetadata metadata) {
        super(CalciteObject.EMPTY);
        this.errorReporter = errorReporter;
        this.metadata = metadata;
    }

    /** @return the names of the input tables.
     * The order of the tables corresponds to the inputs of the generated circuit. */
    public Set<String> getInputTables() {
        return this.sourceOperators.keySet();
    }

    public int getOutputCount() {
        return this.sinkOperators.size();
    }

    public DBSPType getSingleOutputType() {
        assert this.sinkOperators.size() == 1: "Expected a single output, got " + this.sinkOperators.size();
        return this.sinkOperators.values().iterator().next().getType();
    }

    public void addDeclaration(DBSPDeclaration decl) {
        this.declarations.add(decl);
    }

    public void addOperator(DBSPOperator operator) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Adding ")
                .append(operator.toString())
                .newline();
        assert !this.operators.contains(operator): "Operator " + operator + " already inserted";
        this.operators.add(operator);
        DBSPSourceBaseOperator source = operator.as(DBSPSourceBaseOperator.class);
        if (source != null)
            Utilities.putNew(this.sourceOperators, source.tableName, source);
        DBSPViewOperator view = operator.as(DBSPViewOperator.class);
        if (view != null)
            Utilities.putNew(this.viewOperators, view.viewName, view);
        DBSPSinkOperator sink = operator.as(DBSPSinkOperator.class);
        if (sink != null)
            Utilities.putNew(this.sinkOperators, sink.viewName, sink);
        this.allOperators.add(operator);
    }

    public Iterable<DBSPOperator> getAllOperators() { return this.allOperators; }

    @Nullable
    public DBSPSourceBaseOperator getInput(String tableName) {
        return this.sourceOperators.get(tableName);
    }

    @Nullable
    public DBSPViewOperator getView(String viewName) {
        return this.viewOperators.get(viewName);
    }

    @Nullable
    public DBSPSinkOperator getSink(String viewName) {
        return this.sinkOperators.get(viewName);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop()) {
            for (DBSPDeclaration decl: this.declarations)
                decl.accept(visitor);
            for (DBSPOperator op : this.allOperators)
                op.accept(visitor);
            visitor.postorder(this);
        }
        visitor.pop(this);
    }

    /** Return true if this circuit and other are identical (have the exact same operators). */
    public boolean sameCircuit(DBSPPartialCircuit other) {
        if (this == other)
            return true;
        return Linq.same(this.allOperators, other.allOperators);
    }

    /** No more changes are expected to the circuit. */
    public DBSPCircuit seal(String name) {
        return new DBSPCircuit(this.getNode(), this, name);
    }

    @Override
    public String toString() {
        return "PartialCircuit" + this.id;
    }

    /** Number of operators in the circuit. */
    public int size() {
        return this.allOperators.size();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.intercalateI(System.lineSeparator(), this.allOperators);
    }
}
