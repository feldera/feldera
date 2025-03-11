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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A DBSP circuit. */
public final class DBSPCircuit extends DBSPNode
        implements IDBSPOuterNode, IWritesLogs, ICircuit, DiGraph<DBSPOperator> {
    public final List<DBSPDeclaration> declarations;
    public final Map<String, DBSPDeclaration> declarationMap = new HashMap<>();
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
        this.declarations = new ArrayList<>();
        this.metadata = metadata;
    }

    private DBSPCircuit(ProgramMetadata metadata, List<DBSPDeclaration> declarations, List<DBSPOperator> allOperators) {
        this(metadata);
        for (DBSPDeclaration decl: declarations)
            this.addDeclaration(decl);
        for (DBSPOperator op: allOperators) {
            this.addOperator(op);
        }
    }

    /** Use a stable sort to resort the operators in the circuit.
     * The sort must produce a legal topological order. */
    public void sortOperators(Comparator<DBSPOperator> comparator) {
        this.allOperators.sort(comparator);
    }

    /** Sort the nodes to be compatible with a topological order
     * on the specified graph.
     * @param graph Topological order to enforce.
     *              Mutated by the method. */
    public void resort(CircuitGraph graph) {
        this.allOperators.clear();
        DBSPOperator previous = null;
        for (DBSPSourceTableOperator source: this.sourceOperators.values()) {
            if (previous != null) {
                // Preserve the input order
                graph.addEdge(previous, source, 0);
            }
            previous = source;
        }

        previous = null;
        for (DBSPSinkOperator sink: this.sinkOperators.values()) {
            if (previous != null) {
                // Preserve the output order
                graph.addEdge(previous, sink, 0);
            }
            previous = sink;
        }
        for (DBSPOperator op: graph.sort())
            this.allOperators.add(op);
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

    public void replaceDeclaration(DBSPDeclaration decl) {
        DBSPDeclaration existing = Utilities.getExists(this.declarationMap, decl.getName());
        this.declarations.remove(existing);
        this.declarations.add(decl);
        this.declarationMap.put(decl.getName(), decl);
    }

    @Nullable
    public DBSPDeclaration getDeclaration(String name) {
        return this.declarationMap.get(name);
    }

    public void addDeclaration(DBSPDeclaration decl) {
        this.declarations.add(decl);
        if (this.declarationMap.containsKey(decl.getName())) {
            DBSPDeclaration prev = Utilities.getExists(this.declarationMap, decl.getName());
            throw new CompilationError(
                    "Declaration " + decl + " has the same name as an existing declaration " + prev,
                    decl.item.getNode());
        }
        Utilities.putNew(this.declarationMap, decl.getName(), decl);
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
    public void accept(InnerVisitor visitor) {}

    @Override
    public long getDerivedFrom() {
        return this.id;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop()) {
            visitor.startArrayProperty("declarations");
            int index = 0;
            for (DBSPDeclaration decl: this.declarations) {
                visitor.propertyIndex(index);
                index++;
                decl.accept(visitor);
            }
            visitor.endArrayProperty("declarations");
            index = 0;
            visitor.startArrayProperty("allOperators");
            for (DBSPOperator op : this.allOperators) {
                visitor.propertyIndex(index);
                index++;
                op.accept(visitor);
            }
            visitor.endArrayProperty("allOperators");
            visitor.postorder(this);
        }
        visitor.pop(this);
    }

    /** Return true if this circuit and other are identical (have the exact same operators and declarations). */
    public boolean sameCircuit(ICircuit other) {
        if (this == other)
            return true;
        if (!other.is(DBSPCircuit.class))
            return false;
        if (!Linq.same(this.allOperators, other.to(DBSPCircuit.class).allOperators))
            return false;
        return Linq.same(this.declarations, other.to(DBSPCircuit.class).declarations);
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

    @SuppressWarnings("unused")
    public static DBSPCircuit fromJson(JsonNode node, JsonDecoder decoder) {
        List<DBSPDeclaration> declarations =
                fromJsonOuterList(node, "declarations", decoder, DBSPDeclaration.class);
        List<DBSPOperator> operators =
                fromJsonOuterList(node, "allOperators", decoder, DBSPOperator.class);
        ProgramMetadata metadata = ProgramMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder.typeFactory);
        return new DBSPCircuit(metadata, declarations, operators);
    }
}
