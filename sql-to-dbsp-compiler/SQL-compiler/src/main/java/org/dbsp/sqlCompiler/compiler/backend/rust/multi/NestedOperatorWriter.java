package org.dbsp.sqlCompiler.compiler.backend.rust.multi;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.BaseRustCodeGenerator;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.util.HashString;
import org.dbsp.util.Utilities;

/** Writes the implementation of a {@link DBSPNestedOperator} as a function that instantiates
 * the operator in circuit. */
public final class NestedOperatorWriter extends BaseRustCodeGenerator {
    final DBSPNestedOperator operator;
    final DBSPCircuit circuit;

    public NestedOperatorWriter(DBSPNestedOperator operator, DBSPCircuit circuit) {
        this.circuit = circuit;
        this.operator = operator;
    }

    private void processChild(DBSPOperator node) {
        DBSPOperator op = node.to(DBSPOperator.class);
        String name = op.getNodeName(false);
        String hash = op.getNodeName(true);
        HashString merkle = OperatorHash.getHash(node, true);
        if (!node.is(DBSPViewBaseOperator.class)) {
            this.builder().append("let ");
            if (node.is(DBSPSimpleOperator.class)) {
                this.builder().append(name);
            } else {
                this.builder().append("(");
                for (int i = 0; i < node.outputCount(); i++) {
                    String portName = node.getOutput(i).getName(false);
                    this.builder().append(portName).append(",");
                }
                this.builder().append(")");
            }
            this.builder().append(" = ");
        }
        this.builder().append("create_")
                .append(hash)
                .append("(&circuit, ");
        if (merkle != null) {
            this.builder().append("Some(\"")
                    .append(merkle.toString())
                    .append("\"), ");
        } else {
            this.builder().append("None, ");
        }
        this.builder().append(CircuitWriter.SOURCE_MAP_VARIABLE_NAME)
                .append(", ");
        for (var input: op.inputs) {
            name = "";
            int index = 0;
            for (var delta: this.operator.deltaInputs) {
                if (op == delta) {
                    // in this case op.inputs.size() == 1
                    name = this.inputName(index);
                    break;
                }
                index++;
            }
            if (name.isEmpty())
                name = "&" + input.getName(false);
            this.builder()
                    .append(name)
                    .append(",");
        }
        this.builder().append(");").newline();
    }

    String inputName(int inputNo) {
        return "i" + inputNo;
    }

    @Override
    public void write(DBSPCompiler compiler) {
        boolean useHandles = compiler.options.ioOptions.emitHandles;
        this.builder()
                .append(RustWriter.COMMON_PREAMBLE)
                .append(RustWriter.STANDARD_PREAMBLE);
        ToRustVisitor visitor = new ToRustVisitor(
                compiler, this.builder(), this.circuit.metadata, new ProjectDeclarations())
                .withPreferHash(true);
        final String hash = this.operator.getNodeName(true);
        this.builder().newline();
        for (String dep : this.dependencies)
            this.builder().append("use ").append(dep).append("::*;").newline();

        this.builder().append("pub fn create_")
                .append(hash)
                .append("(circuit: &")
                .append(this.dbspCircuit(true))
                .append(", ")
                .append(CircuitWriter.SOURCE_MAP_VARIABLE_NAME)
                .append(": &'static SourceMap, ");
        if (!useHandles)
            this.builder().append("catalog: &mut Catalog,");
        this.builder().increase();
        int input = 0;
        for (var port : this.operator.inputs) {
            String n = this.inputName(input);
            input++;
            this.builder().append(n);
            this.builder().append(": &");
            DBSPType streamType = port.streamType(true);
            streamType.accept(visitor.innerVisitor);
            this.builder().append(",").newline();
        }
        this.builder().decrease().append(")");
        this.builder().append(" -> ");
        this.builder().append("(");
        for (int i = 0; i < this.operator.outputCount(); i++) {
            if (this.operator.hasOutput(i)) {
                DBSPType streamType = this.operator.outputStreamType(i, true);
                streamType.accept(visitor.innerVisitor);
                this.builder().append(",");
            }
        }
        this.builder().append(")");
        this.builder().append("{").increase();

        this.builder().append("let (");
        for (int i = 0; i < operator.outputCount(); i++) {
            OutputPort port = operator.internalOutputs.get(i);
            if (port == null)
                this.builder().append("_, ");
            else
                this.builder().append(port.getName(false)).append(", ");
        }
        this.builder().append(") = ")
                .append("circuit.recursive(|circuit, (");
        for (int i = 0; i < operator.outputCount(); i++) {
            ProgramIdentifier view = operator.outputViews.get(i);
            DBSPViewDeclarationOperator decl = operator.declarationByName.get(view);
            if (decl != null) {
                this.builder().append(decl.getNodeName(false)).append(", ");
            } else {
                // view is not really recursive
                this.builder().append("_").append(", ");
            }
        }
        this.builder().append("): (");
        for (int i = 0; i < operator.outputCount(); i++) {
            if (operator.internalOutputs.get(i) == null) {
                this.builder().append("()").append(", ");
            } else {
                DBSPType streamType = new DBSPTypeStream(operator.outputType(i), false);
                streamType.accept(visitor.innerVisitor);
                this.builder().append(", ");
            }
        }
        this.builder().append(")| {").increase();

        for (DBSPOperator node : this.operator.getAllOperators())
            if (!node.is(DBSPViewDeclarationOperator.class))
                this.processChild(node);

        this.builder().append("Ok((");
        for (int i = 0; i < this.operator.outputCount(); i++) {
            OutputPort port = this.operator.internalOutputs.get(i);
            if (port != null)
                this.builder().append(port.getName(false));
            else
                this.builder().append("()");
            this.builder().append(", ");
        }
        this.builder().append("))").newline();
        this.builder().decrease().append("}).unwrap();").newline();

        for (int i = 0; i < operator.outputCount(); i++) {
            OutputPort port = operator.internalOutputs.get(i);
            if (port != null) {
                this.builder().append("let hash = ");
                HashString hash1 = OperatorHash.getHash(port.operator, true);
                if (hash1 == null) {
                    this.builder().append("None;").newline();
                } else {
                    this.builder().append("Some(")
                            .append(Utilities.doubleQuote(hash1.toString(), false))
                            .append(");")
                            .newline();
                }
                this.builder().append(port.getName(false))
                        .append(".set_persistent_id(hash);")
                        .newline();
            }
        }

        this.builder().append("return (");
        for (int i = 0; i < this.operator.outputCount(); i++) {
            OutputPort port = this.operator.internalOutputs.get(i);
            if (port != null) {
                this.builder().append(port.getName(false));
                this.builder().append(", ");
            }
        }
        this.builder().append(");").newline();
        this.builder().decrease().append("}");
    }
}
