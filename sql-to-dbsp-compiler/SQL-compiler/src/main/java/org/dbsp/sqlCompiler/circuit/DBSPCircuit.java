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

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.DiGraph;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/** A DBSP circuit. */
public final class DBSPCircuit extends DBSPNode
        implements IDBSPOuterNode, IWritesLogs, ICircuit, DiGraph<DBSPOperator> {
    public final List<DBSPDeclaration> declarations = new ArrayList<>();
    public final LinkedHashMap<ProgramIdentifier, DBSPSourceTableOperator> sourceOperators = new LinkedHashMap<>();
    public final LinkedHashMap<ProgramIdentifier, DBSPViewOperator> viewOperators = new LinkedHashMap<>();
    public final LinkedHashMap<ProgramIdentifier, DBSPSinkOperator> sinkOperators = new LinkedHashMap<>();
    // Should always be in topological order
    public final List<DBSPOperator> allOperators = new ArrayList<>();
    public final ProgramMetadata metadata;
    // Used to detect duplicate insertions (always a bug).
    final Set<DBSPOperator> operators = new HashSet<>();
    public String name = "circuit";

    public DBSPCircuit(ProgramMetadata metadata) {
        super(CalciteObject.EMPTY);
        this.metadata = metadata;
    }

    /** Use a stable sort to resort the operators in the circuit.
     * The sort must produce a legal topological order. */
    public void sortOperators(Comparator<DBSPOperator> comparator) {
        this.allOperators.sort(comparator);
    }

    /** @return the names of the input tables.
     * The order of the tables corresponds to the inputs of the generated circuit. */
    public Set<ProgramIdentifier> getInputTables() {
        return this.sourceOperators.keySet();
    }

    /** @return the names of the output views.
     * The order of the tables corresponds to the outputs of the generated circuit. */
    public Set<ProgramIdentifier> getOutputViews() {
        return this.sinkOperators.keySet();
    }

    public int getOutputCount() {
        return this.sinkOperators.size();
    }

    /** The circuit is expected to have a single non-system view.  Return its type */
    public DBSPType getSingleOutputType() {
        List<DBSPSinkOperator> sinks = Linq.where(
                Linq.list(this.sinkOperators.values()), o -> !o.metadata.system);
        assert sinks.size() == 1: "Expected a single output, got " + sinks.size();
        return sinks.get(0).getType();
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void addDeclaration(DBSPDeclaration decl) {
        this.declarations.add(decl);
    }

    public void addOperator(DBSPOperator operator) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Adding ")
                .appendSupplier(operator::toString)
                .newline();
        assert !this.operators.contains(operator):
                "Operator " + operator + " already inserted";
        this.operators.add(operator);
        DBSPSourceTableOperator source = operator.as(DBSPSourceTableOperator.class);
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

    @Override
    public Iterable<DBSPOperator> getNodes() {
        return this.getAllOperators();
    }

    /** Returns the *predecessors* of a node.
     * The graph represented by {@link DBSPCircuit} is actually the reverse graph.
     * Use {@link CircuitGraph} to get the actual graph. */
    @Override
    public List<Port<DBSPOperator>> getSuccessors(DBSPOperator operator) {
        if (operator.is(DBSPViewDeclarationOperator.class)) {
            DBSPViewDeclarationOperator vd = operator.to(DBSPViewDeclarationOperator.class);
            DBSPViewOperator view = vd.getCorrespondingView(this);
            if (view != null)
                // Can happen if the view is declared by not defined.
                return Linq.list(new Port<>(view, 0));
            return Linq.list();
        }
        return Linq.map(operator.inputs, i -> new Port<>(i.node(), 0));
    }

    /** Get the table with the specified name.
     * @param tableName must use the proper casing */
    @Nullable
    public DBSPSourceTableOperator getInput(ProgramIdentifier tableName) {
        return this.sourceOperators.get(tableName);
    }

    /** Get the local view with the specified name.
     * @param viewName must use the proper casing */
    @Nullable
    public DBSPViewOperator getView(ProgramIdentifier viewName) {
        return this.viewOperators.get(viewName);
    }

    /** Get the external view with the specified name.
     * @param viewName must use the proper casing */
    @Nullable
    public DBSPSinkOperator getSink(ProgramIdentifier viewName) {
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
    public boolean sameCircuit(ICircuit other) {
        if (this == other)
            return true;
        if (!other.is(DBSPCircuit.class))
            return false;
        return Linq.same(this.allOperators, other.to(DBSPCircuit.class).allOperators);
    }

    @Override
    public boolean contains(DBSPOperator node) {
        return this.operators.contains(node);
    }

    /** Number of operators in the circuit. */
    public int size() {
        return this.allOperators.size();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("Circuit ")
                .append(this.getName())
                .append(" {")
                .increase()
                .intercalateI(System.lineSeparator(), this.allOperators)
                .decrease()
                .append("}")
                .newline();
    }

    public boolean isEmpty() {
        return this.allOperators.isEmpty();
    }

    public ProgramMetadata getMetadata() {
        return this.metadata;
    }
}
