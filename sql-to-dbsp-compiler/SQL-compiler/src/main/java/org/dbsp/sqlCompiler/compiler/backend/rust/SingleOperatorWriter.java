package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.CompactName;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.IndentStream;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Writes the implementation of a single operator as a function */
public class SingleOperatorWriter extends BaseCodeGenerator {
    @Nullable
    DBSPOperator operator = null;
    final DBSPCircuit circuit;
    final List<String> dependencies;

    public SingleOperatorWriter(DBSPCircuit circuit) {
        this.circuit = circuit;
        this.dependencies = new ArrayList<>();
    }

    @Override
    public void add(IDBSPNode node) {
        this.operator = node.to(DBSPOperator.class);
    }

    @Override
    public void write(DBSPCompiler compiler) throws IOException {
        assert this.outputStream != null;
        assert this.operator != null;
        this.outputStream.append(RustWriter.COMMON_PREAMBLE);
        this.outputStream.append(RustWriter.STANDARD_PREAMBLE);
        IndentStream builder = new IndentStream(this.outputStream);
        ToRustVisitor visitor = new ToRustVisitor(compiler, builder, circuit.metadata);
        final String name = CompactName.getCompactName(this.operator);
        final String nl = System.lineSeparator();
        this.outputStream.append(nl);
        for (String dep: this.dependencies)
            this.outputStream.println("use " + dep + "::*;");

        this.outputStream.append("pub fn ")
                .append(name)
                .append("(circuit: RootCircuit, ");
        for (var port: operator.inputs) {
            String n = CompactName.getCompactName(port.operator);
            this.outputStream.append(n);
            this.outputStream.append(": ");
            port.operator.streamType(port.outputNumber).accept(visitor.innerVisitor);
            this.outputStream.append(", ");
        }
        this.outputStream.append(") -> ");
        if (operator.outputCount() == 1) {
            operator.streamType(0).accept(visitor.innerVisitor);
        } else {
            this.outputStream.append("(");
            for (int i = 0; i < operator.outputCount(); i++) {
                operator.streamType(i).accept(visitor.innerVisitor);
                this.outputStream.append(",");
            }
            this.outputStream.append(")");
        }
        this.outputStream.append(nl).append("{").append(nl);
        this.operator.accept(visitor);
        this.outputStream.append(nl);
        this.outputStream.append("return ").append(name).append(";").append(nl);
        this.outputStream.append("}");
    }
}
