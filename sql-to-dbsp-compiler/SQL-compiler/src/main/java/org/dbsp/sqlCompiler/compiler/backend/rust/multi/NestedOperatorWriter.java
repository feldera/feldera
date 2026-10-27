package org.dbsp.sqlCompiler.compiler.backend.rust.multi;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.annotation.RegionAnnotation;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.BaseRustCodeGenerator;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitPostfix;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.HashString;
import org.dbsp.util.Utilities;

import java.util.HashSet;
import java.util.Set;

/** Writes the implementation of a {@link DBSPNestedOperator} as a function that instantiates
 * the operator in circuit. */
public final class NestedOperatorWriter extends BaseRustCodeGenerator {
    final DBSPNestedOperator operator;
    final DBSPCircuit circuit;
    final CircuitPostfix materializations;
    /** Names of the region variables that the generated function declares */
    final Set<String> regionsDeclared;

    public NestedOperatorWriter(DBSPNestedOperator operator, DBSPCircuit circuit,
                                CircuitPostfix materializations) {
        this.circuit = circuit;
        this.operator = operator;
        this.materializations = materializations;
        this.regionsDeclared = new HashSet<>();
    }

    private void processChild(DBSPOperator node) {
        DBSPOperator op = node.to(DBSPOperator.class);
        String name = op.getNodeName(false);
        String hash = op.getNodeName(true);
        HashString merkle = OperatorHash.getHash(node, true);
        RegionAnnotation region = node.annotations.first(RegionAnnotation.class);
        if (region != null) {
            // The generated function is in a crate of its own, so it declares each region it uses
            if (this.regionsDeclared.add(region.asVarName())) {
                this.builder().append("let ")
                        .append(region.asVarName())
                        .append(": Option<RegionName> = Some(circuit.create_region_name(")
                        .append(Utilities.doubleQuote(region.getTag(), true))
                        .append(", ")
                        .append(region.getId())
                        .append("));")
                        .newline();
            }
        }

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

        if (region != null) {
            this.builder().append("&")
                    .append(region.asVarName())
                    .append(", ");
        } else {
            this.builder().append("&None, ");
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
                .append(RustWriter.ALLOW_DEPRECATED_PREAMBLE)
                .append(RustWriter.STANDARD_PREAMBLE);
        if (!useHandles)
            this.builder().append(RustWriter.CATALOG_PREAMBLE);
        ToRustVisitor visitor = new ToRustVisitor(
                compiler, this.builder(), this.circuit.metadata, new ProjectDeclarations(), this.materializations)
                .withPreferHash(true);
        final String hash = this.operator.getNodeName(true);
        this.builder().newline();
        for (String dep : this.dependencies)
            this.builder().append("use ").append(dep).append("::*;").newline();

        this.builder().append("pub fn create_")
                .append(hash)
                .append("(circuit: &")
                .append(this.dbspCircuit(true))
                .append(", region: &Option<RegionName>, ")
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
            DBSPType streamType = port.streamType(0);
            streamType.accept(visitor.innerVisitor);
            this.builder().append(",").newline();
        }
        this.builder().decrease().append(")");
        this.builder().append(" -> ");
        this.builder().append("(");
        for (int i : this.operator.distinctOutputs()) {
            // The outputs of the nested operator are streams of the root circuit
            DBSPType streamType = this.operator.outputStreamType(i, 0);
            streamType.accept(visitor.innerVisitor);
            this.builder().append(",");
        }
        this.builder().append(")");
        this.builder().append("{").increase();

        this.builder().append("if let Some(region) = region { circuit.open_region(region.clone()) };").newline();
        visitor.getRecursiveComponentsGenerator(this.operator, this::processChild).emit();

        this.builder().append("if let Some(region) = region { circuit.close_region(region.clone()) };").newline();
        this.builder().append("return (");
        for (int i : this.operator.distinctOutputs()) {
            this.builder().append(this.operator.internalOutputs.get(i).getName(false));
            this.builder().append(", ");
        }
        this.builder().append(");").newline();
        this.builder().decrease().append("}");
    }
}
