package org.dbsp.sqlCompiler.compiler.backend.rust.multi;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.GCOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.BaseRustCodeGenerator;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/** Writes the implementation of a single operator as a function that instantiates
 * the operator in circuit. */
public final class SingleOperatorWriter extends BaseRustCodeGenerator {
    final DBSPOperator operator;
    final DBSPCircuit circuit;
    final ICircuit parent;
    final boolean topLevel;

    /* Example output:
     * ... preamble ...
     * pub fn create_xxx(circuit: &RootCircuit, catalog: &mut Catalog,
     *                  i0: &Stream<RootCircuit, WSet<Tup1<Option<i32>>>>,
     *                  i1: &Stream<RootCircuit, WSet<Tup1<Option<i32>>>>,
     *                  i2: &Stream<RootCircuit, WSet<Tup1<Option<i32>>>>, ) ->
     *                      Stream<RootCircuit, WSet<Tup1<Option<i32>>>> {
     *    let xxx = i0.sum([i1, i2]);
     *    return xxx;
     * } */
    public SingleOperatorWriter(DBSPOperator operator, DBSPCircuit circuit, ICircuit parent) {
        this.circuit = circuit;
        this.parent = parent;
        this.operator = operator;
        this.topLevel = circuit == parent;
    }

    /** Collects all {@link DBSPStaticExpression}s that appear in an expression */
    static class FindStatics extends InnerVisitor {
        final Map<String, DBSPStaticExpression> found = new HashMap<>();

        public FindStatics(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPStaticExpression expression) {
            this.found.put(expression.getName(), expression);
        }
    }

    String inputName(int inputNo) {
        return "i" + inputNo;
    }

    @Override
    public void write(DBSPCompiler compiler) {
        this.builder()
                .append(RustWriter.COMMON_PREAMBLE)
                .append(RustWriter.STANDARD_PREAMBLE);
        ToRustVisitor visitor = new ToRustVisitor(
                compiler, this.builder(), this.circuit.metadata, new HashSet<>())
                .withPreferHash(true);
        final String name = this.operator.getNodeName(true);
        this.builder().newline();
        for (String dep: this.dependencies)
            this.builder().append("use ").append(dep).append("::*;").newline();

        boolean hasOutput = !operator.is(DBSPViewBaseOperator.class) && !operator.is(GCOperator.class);
        this.builder().append("pub fn create_")
                .append(name)
                .append("(circuit: &")
                .append(this.dbspCircuit(this.topLevel))
                .append(", hash: Option<&str>, ");
        if (this.topLevel)
            this.builder().append("catalog: &mut Catalog,");
        this.builder().increase();
        int input = 0;
        for (var port: operator.inputs) {
            String n = this.inputName(input);
            input++;
            this.builder().append(n);
            this.builder().append(": &");
            boolean inputTopLevel = this.operator.is(DBSPDeltaOperator.class) || this.topLevel;
            DBSPType streamType = port.streamType(inputTopLevel);
            streamType.accept(visitor.innerVisitor);
            this.builder().append(",").newline();
        }
        this.builder().decrease().append(")");
        if (hasOutput) {
            this.builder().append(" -> ");
            if (operator.is(DBSPSimpleOperator.class)) {
                DBSPType streamType = operator.outputStreamType(0, this.topLevel);
                streamType.accept(visitor.innerVisitor);
            } else {
                this.builder().append("(");
                for (int i = 0; i < operator.outputCount(); i++) {
                    DBSPType streamType = operator.outputStreamType(i, this.topLevel);
                    streamType.accept(visitor.innerVisitor);
                    this.builder().append(",");
                }
                this.builder().append(")");
            }
        }
        this.builder().append("{").increase();

        FindStatics staticsFinder = new FindStatics(compiler);
        this.operator.accept(staticsFinder);
        for (DBSPStaticExpression expression: staticsFinder.found.values()) {
            DBSPStaticItem item = new DBSPStaticItem(expression);
            item.accept(visitor.innerVisitor);
        }

        visitor.push(this.circuit);
        if (this.parent != this.circuit)
            visitor.push(this.parent);
        if (this.operator.is(DBSPSumOperator.class)) {
            // Special case for sum, which normally takes references.
           this.builder()
                    .append("let ")
                    .append(name)
                    .append(" = ")
                    .append(this.inputName(0))
                    .append(".sum([");
            for (int i = 1; i < operator.inputs.size(); i++) {
                if (i > 1)
                    this.builder().append(", ");
                this.builder().append(this.inputName(i));
            }
            this.builder().append("])").append(";").newline();
            this.builder().newline()
                    .append(operator.getNodeName(true))
                    .append(".set_persistent_id(hash);");
        } else {
            visitor.generateOperator(this.operator);
        }
        if (this.parent != this.circuit)
            visitor.pop(this.parent);
        visitor.pop(circuit);
        if (hasOutput) {
            this.builder().newline().append("return ");
            if (operator.is(DBSPNestedOperator.class)) {
                DBSPNestedOperator nested = operator.to(DBSPNestedOperator.class);
                this.builder().append("(");
                for (int i = 0; i < nested.outputCount(); i++) {
                    OutputPort port = nested.internalOutputs.get(i);
                    if (port != null)
                        this.builder().append(port.getName(true));
                    else
                        this.builder().append("()");
                    this.builder().append(", ");
                }
                this.builder().append(")");
            } else if (operator.outputCount() > 1) {
                this.builder().append("(");
                for (int i = 0; i < operator.outputCount(); i++) {
                    OutputPort port = operator.getOutput(i);
                    this.builder().append(port.getName(true));
                    this.builder().append(", ");
                }
                this.builder().append(")");
            } else {
                this.builder().append(name);
            }
            this.builder().append(";").newline();
        }
        this.builder().decrease().append("}");
    }
}
