package org.dbsp.sqlCompiler.compiler.backend.rust.multi;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.BaseRustCodeGenerator;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;

import java.io.IOException;

/** Writes the implementation of a single operator as a function that instantiates
 * the operator in circuit. */
public final class SingleOperatorWriter extends BaseRustCodeGenerator {
    final DBSPOperator operator;
    final DBSPCircuit circuit;
    final boolean topLevel;

    /* Example output:
     * ... preamble ...
     * pub fn create_s9(circuit: &RootCircuit, catalog: &mut Catalog,
     *                  s2: &Stream<RootCircuit, WSet<Tup1<Option<i32>>>>,
     *                  s8: &Stream<RootCircuit, WSet<Tup1<Option<i32>>>>,
     *                  s6: &Stream<RootCircuit, WSet<Tup1<Option<i32>>>>, ) ->
     *                      Stream<RootCircuit, WSet<Tup1<Option<i32>>>> {
     *    let s9 = s2.sum([s8, s6]);
     *    return s9;
     * } */
    public SingleOperatorWriter(DBSPOperator operator, DBSPCircuit circuit, boolean topLevel) {
        this.circuit = circuit;
        this.operator = operator;
        this.topLevel = topLevel;
    }

    @Override
    public void write(DBSPCompiler compiler) throws IOException {
        this.getOutputStream()
                .append(RustWriter.COMMON_PREAMBLE)
                .append(RustWriter.STANDARD_PREAMBLE);
        ToRustVisitor visitor = new ToRustVisitor(compiler, this.getOutputStream(), this.circuit.metadata);
        final String name = this.operator.getNodeName(USE_HASH_NAMES);
        this.getOutputStream().newline();
        for (String dep: this.dependencies)
            this.getOutputStream().append("use ").append(dep).append("::*;");

        this.getOutputStream().append("pub fn create_")
                .append(name)
                .append("(circuit: &RootCircuit, catalog: &mut Catalog,")
                .increase();
        for (var port: operator.inputs) {
            String n = port.operator.getNodeName(USE_HASH_NAMES);
            this.getOutputStream().append(n);
            this.getOutputStream().append(": &");
            DBSPTypeStream streamType = new DBSPTypeStream(port.operator.outputType(port.outputNumber), true);
            streamType.accept(visitor.innerVisitor);
            this.getOutputStream().append(",").newline();
        }
        this.getOutputStream().append(")").newline();
        if (!operator.is(DBSPViewBaseOperator.class)) {
            this.getOutputStream().append(" -> ");
            if (operator.outputCount() == 1) {
                DBSPTypeStream streamType = new DBSPTypeStream(operator.outputType(0), this.topLevel);
                streamType.accept(visitor.innerVisitor);
            } else {
                this.getOutputStream().append("(");
                for (int i = 0; i < operator.outputCount(); i++) {
                    DBSPTypeStream streamType = new DBSPTypeStream(operator.outputType(i), this.topLevel);
                    streamType.accept(visitor.innerVisitor);
                    this.getOutputStream().append(",");
                }
                this.getOutputStream().append(")");
            }
        }
        this.getOutputStream().decrease().newline().append("{").increase();
        visitor.push(circuit);
        if (this.operator.is(DBSPSumOperator.class)) {
            this.getOutputStream()
                    .append("let ")
                    .append(operator.to(DBSPSumOperator.class).getNodeName(USE_HASH_NAMES))
                    .append(" = ")
                    .append(operator.inputs.get(0).getNodeName(USE_HASH_NAMES))
                    .append(".sum([");
            for (int i = 1; i < operator.inputs.size(); i++) {
                if (i > 1)
                    this.getOutputStream().append(", ");
                this.getOutputStream().append(operator.inputs.get(i).getNodeName(USE_HASH_NAMES));
            }
            this.getOutputStream().append("])").append(";");
        } else {
            this.operator.accept(visitor);
        }
        visitor.pop(circuit);
        this.getOutputStream().newline();
        if (!operator.is(DBSPViewBaseOperator.class)) {
            this.getOutputStream().append("return ").append(name).append(";").newline();
        }
        this.getOutputStream().append("}").decrease();
    }
}
