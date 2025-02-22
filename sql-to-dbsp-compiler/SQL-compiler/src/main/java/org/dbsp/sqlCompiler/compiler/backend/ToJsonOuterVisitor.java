package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Serializes an outer node as a JSON string */
public class ToJsonOuterVisitor extends CircuitVisitor {
    final JsonStream stream;
    final int verbosity;
    final Set<Long> serialized;
    final ToJsonInnerVisitor innerVisitor;

    public ToJsonOuterVisitor(DBSPCompiler compiler, int verbosity) {
        super(compiler);
        IndentStream stream = new IndentStream(new StringBuilder());
        stream.setIndentAmount(verbosity);
        this.stream = new JsonStream(stream);
        this.verbosity = verbosity;
        this.serialized = new HashSet<>();
        this.innerVisitor = new ToJsonInnerVisitor(compiler, this.stream, verbosity);
    }

    List<Integer> propertyIndexStack = new ArrayList<>();

    int propertyIndex = 0;

    @Override
    public void push(IDBSPOuterNode node) {
        this.propertyIndexStack.add(this.propertyIndex);
        this.propertyIndex = 0;
        super.push(node);
    }

    @Override
    public void pop(IDBSPOuterNode node) {
        this.propertyIndex = Utilities.removeLast(this.propertyIndexStack);
        super.pop(node);
    }

    @Override
    public void endArrayProperty(String property) {
        this.stream.endArray();
    }

    @Override
    public void startArrayProperty(String property) {
        this.property(property);
        this.stream.beginArray();
    }

    @Override
    public void property(String name) {
        this.stream.label(name);
    }

    JsonStream open() {
        return this.stream.beginObject();
    }

    void close() {
        this.stream.endObject();
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        this.open().label("id").append(operator.id);
        this.stream.label("class");
        this.stream.append(operator.getClass().getSimpleName());
        operator.accept(this.innerVisitor);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPOperator operator) {
        this.close();
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        this.open();
        this.property("circuit");
        this.stream.append(circuit.id);
        return super.preorder(circuit);
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        this.close();
    }

    public String getJsonString() {
        return this.stream.toString();
    }
}
