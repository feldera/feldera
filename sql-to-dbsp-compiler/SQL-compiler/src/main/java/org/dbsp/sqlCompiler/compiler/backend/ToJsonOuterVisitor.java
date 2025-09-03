package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.IJson;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Serializes an outer node as a JSON string */
public class ToJsonOuterVisitor extends CircuitVisitor {
    public final JsonStream stream;
    final int verbosity;
    final Set<Long> serialized;
    final ToJsonInnerVisitor innerVisitor;

    public ToJsonOuterVisitor(DBSPCompiler compiler, int verbosity, ToJsonInnerVisitor innerVisitor) {
        super(compiler);
        this.stream = innerVisitor.stream;
        this.verbosity = verbosity;
        this.serialized = new HashSet<>();
        this.innerVisitor = innerVisitor;
    }

    public static ToJsonOuterVisitor create(DBSPCompiler compiler, int verbosity) {
        IndentStream stream = new IndentStream(new StringBuilder());
        stream.setIndentAmount(1);
        JsonStream json = new JsonStream(stream);
        ToJsonInnerVisitor inner = new ToJsonInnerVisitor(compiler, json, 1);
        return new ToJsonOuterVisitor(compiler, verbosity, inner);
    }

    final List<Integer> propertyIndexStack = new ArrayList<>();

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
        this.label(property);
        this.stream.beginArray();
    }

    @Override
    public void label(String name) {
        this.property(name);
    }

    public void property(String name) {
        this.stream.label(name);
    }
    
    @SuppressWarnings("SameParameterValue")
    boolean checkDone(IDBSPOuterNode node, boolean silent) {
        if (this.serialized.contains(node.getId())) {
            if (silent)
                return true;
            this.stream.beginObject()
                    .label("node")
                    .append(node.getId())
                    .endObject();
            return true;
        }
        return false;
    }

    @Override
    public VisitDecision preorder(IDBSPOuterNode node) {
        if (!this.checkDone(node, false)) {
            this.stream.beginObject().appendClass(node);
            this.property("id");
            this.stream.append(node.getId());
            this.serialized.add(node.getId());
            return VisitDecision.CONTINUE;
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        if (this.preorder(operator.to(IDBSPOuterNode.class)).stop())
            return VisitDecision.STOP;
        this.label("annotations");
        operator.annotations.asJson(this.stream);
        this.label("inputs");
        this.stream.beginArray();
        for (OutputPort port: operator.inputs) {
            port.asJson(this);
        }
        this.stream.endArray();
        operator.accept(this.innerVisitor);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPNestedOperator operator) {
        this.label("internalOutputs");
        this.stream.beginArray();
        int index = 0;
        for (OutputPort port: operator.internalOutputs) {
            this.propertyIndex(index);
            index++;
            if (port == null)
                this.stream.appendNull();
            else
                port.asJson(this);
        }
        this.stream.endArray();

        this.label("outputViews");
        this.stream.beginArray();
        index = 0;
        for (ProgramIdentifier view: operator.outputViews) {
            this.propertyIndex(index);
            index++;
            this.asJsonInner(view);
        }
        this.stream.endArray();
        super.postorder(operator);
    }

    @Override
    public VisitDecision preorder(DBSPIndexedTopKOperator operator) {
        VisitDecision decision = this.preorder(operator.to(DBSPUnaryOperator.class));
        if (decision.stop())
            return VisitDecision.STOP;
        this.label("numbering");
        this.stream.append(operator.numbering.name());
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator operator) {
        VisitDecision decision = this.preorder(operator.to(DBSPOperator.class));
        if (decision.stop())
            return VisitDecision.STOP;
        this.property("isMultiset");
        this.stream.append(operator.isMultiset);
        return VisitDecision.CONTINUE;
    }

    void asJsonInner(IJson value) {
        value.asJson(this.innerVisitor);
    }

    @Override
    public VisitDecision preorder(DBSPViewBaseOperator operator) {
        if (this.preorder(operator.to(DBSPUnaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("viewName");
        this.asJsonInner(operator.viewName);
        /*
        ignore deliberately
        this.property("query");
        this.stream.append(operator.query);
         */
        this.property("metadata");
        this.asJsonInner(operator.metadata);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPSourceBaseOperator operator) {
        if (this.preorder(operator.to(DBSPSimpleOperator.class)).stop())
            return VisitDecision.STOP;
        this.label("tableName");
        this.asJsonInner(operator.tableName);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPSourceTableOperator operator) {
        if (this.preorder(operator.to(DBSPSourceBaseOperator.class)).stop())
            return VisitDecision.STOP;
        this.label("metadata");
        this.asJsonInner(operator.metadata);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        return this.preorder(operator.to(DBSPSourceTableOperator.class));
    }

    @Override
    public VisitDecision preorder(DBSPSourceMapOperator operator) {
        if (this.preorder(operator.to(DBSPSourceTableOperator.class)).stop())
            return VisitDecision.STOP;
        this.label("keyFields");
        this.stream.beginArray();
        int index = 0;
        for (int i: operator.keyFields) {
            this.propertyIndex(index);
            index++;
            this.stream.append(i);
        }
        this.stream.endArray();
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPLagOperator operator) {
        if (this.preorder(operator.to(DBSPUnaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("offset");
        this.stream.append(operator.offset);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPOperatorWithError operator) {
        if (this.preorder(operator.to(DBSPOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("isMultiset");
        // This is not really used, but makes reading it back simpler
        this.stream.append(false);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPConstantOperator operator) {
        VisitDecision decision = this.preorder(operator.to(DBSPSimpleOperator.class));
        if (decision.stop())
            return VisitDecision.STOP;
        this.property("incremental");
        this.stream.append(operator.incremental);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPConcreteAsofJoinOperator operator) {
        if (this.preorder(operator.to(DBSPJoinBaseOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("isLeft");
        this.stream.append(operator.isLeft);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPAsofJoinOperator operator) {
        if (this.preorder(operator.to(DBSPJoinBaseOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("isLeft");
        this.stream.append(operator.isLeft);
        this.property("leftTimestampIndex");
        this.stream.append(operator.leftTimestampIndex);
        this.property("rightTimestampIndex");
        this.stream.append(operator.rightTimestampIndex);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPWindowOperator operator) {
        if (this.preorder(operator.to(DBSPBinaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("lowerInclusive");
        this.stream.append(operator.lowerInclusive);
        this.property("upperInclusive");
        this.stream.append(operator.upperInclusive);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPOperator operator) {
        this.stream.endObject();
    }

    @Override
    public VisitDecision preorder(DBSPDeclaration declaration) {
        if (this.preorder(declaration.to(IDBSPOuterNode.class)).stop())
            return VisitDecision.STOP;
        declaration.accept(this.innerVisitor);
        this.stream.endObject();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        super.preorder(circuit);
        this.preorder(circuit.to(IDBSPOuterNode.class));
        this.property("metadata");
        this.asJsonInner(circuit.metadata);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        super.postorder(circuit);
        this.stream.endObject();
    }

    public String getJsonString() {
        return this.stream.toString();
    }
}
