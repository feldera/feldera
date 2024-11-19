package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;

import java.util.List;

/** Helper class displaying a circuit as a string */
public class CircuitToString {
    final IIndentStream builder;

    CircuitToString(IIndentStream builder) {
        this.builder = builder;
    }

    void nested(DBSPNestedOperator operator) {
        this.builder.append(operator.id);
        list(operator.inputs);
        this.builder.append(" {").increase();
        for (var child: operator.getAllOperators()) {
            toString(child);
        }
        int index = 0;
        for (var o: operator.outputs) {
            this.builder.append(operator.id)
                    .append(":")
                    .append(index++)
                    .append(" = ");
            port(o);
            this.builder.newline();
        }
        builder.decrease().append("}").newline();
    }

    void port(OutputPort port) {
        if (port.node().is(DBSPSimpleOperator.class)) {
            this.builder.append(port.node().getId());
        } else {
            this.builder.append(port.node().getId())
                    .append(":")
                    .append(port.port());
        }
    }

    void list(List<OutputPort> ports) {
        this.builder.append("(");
        boolean first = true;
        for (var port: ports) {
            if (!first)
                this.builder.append(", ");
            first = false;
            this.port(port);
        }
        this.builder.append(")");
    }

    void operator(DBSPSimpleOperator operator) {
        this.builder.append(operator.getId())
                .append(" = ")
                .append(operator.operation);
        if (operator.is(DBSPConstantOperator.class))
            this.builder.append(operator.getFunction().toString());
        else
            list(operator.inputs);
        builder.newline();
    }

    void toString(DBSPOperator operator) {
        if (operator.is(DBSPSimpleOperator.class)) {
            this.operator(operator.to(DBSPSimpleOperator.class));
        } else {
            this.nested(operator.to(DBSPNestedOperator.class));
        }
    }

    String convert(DBSPCircuit circuit) {
        this.builder.append("{").increase();
        for (DBSPOperator op: circuit.getAllOperators()) {
            this.toString(op);
        }
        this.builder.decrease().append("}").newline();
        return this.builder.toString();
    }

    public static String toString(IDBSPOuterNode node) {
        DBSPCircuit circuit = node.to(DBSPCircuit.class);
        StringBuilder builder1 = new StringBuilder();
        IIndentStream stream = new IndentStream(builder1);
        CircuitToString cts = new CircuitToString(stream);
        return cts.convert(circuit);
    }
}
