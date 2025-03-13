package org.dbsp.sqlCompiler.compiler.backend.rust.multi;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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

    @Override
    public void write(DBSPCompiler compiler) throws IOException {
        this.getOutputStream()
                .append(RustWriter.COMMON_PREAMBLE)
                .append(RustWriter.STANDARD_PREAMBLE);
        ToRustVisitor visitor = new ToRustVisitor(compiler, this.getOutputStream(), this.circuit.metadata, new HashSet<>())
                .withPreferHash(USE_HASH_NAMES);
        final String name = this.operator.getNodeName(USE_HASH_NAMES);
        this.getOutputStream().newline();
        for (String dep: this.dependencies)
            this.getOutputStream().append("use ").append(dep).append("::*;").newline();

        boolean hasOutput = !operator.is(DBSPViewBaseOperator.class) && !operator.is(GCOperator.class);
        this.getOutputStream().append("pub fn create_")
                .append(name)
                .append("(circuit: &RootCircuit, catalog: &mut Catalog,")
                .increase();
        for (var port: operator.inputs) {
            String n = port.getName(USE_HASH_NAMES);
            this.getOutputStream().append(n);
            this.getOutputStream().append(": &");
            DBSPType streamType = port.streamType(true);
            streamType.accept(visitor.innerVisitor);
            this.getOutputStream().append(",").newline();
        }
        this.getOutputStream().decrease().append(")");
        if (hasOutput) {
            this.getOutputStream().append(" -> ");
            if (operator.is(DBSPSimpleOperator.class)) {
                DBSPType streamType = operator.outputStreamType(0, this.topLevel);
                streamType.accept(visitor.innerVisitor);
            } else {
                this.getOutputStream().append("(");
                for (int i = 0; i < operator.outputCount(); i++) {
                    DBSPType streamType = operator.outputStreamType(i, this.topLevel);
                    streamType.accept(visitor.innerVisitor);
                    this.getOutputStream().append(",");
                }
                this.getOutputStream().append(")");
            }
        }
        this.getOutputStream().append("{").increase();

        FindStatics staticsFinder = new FindStatics(compiler);
        this.operator.accept(staticsFinder);
        for (DBSPStaticExpression expression: staticsFinder.found.values()) {
            DBSPStaticItem item = new DBSPStaticItem(expression);
            item.accept(visitor.innerVisitor);
        }

        visitor.push(circuit);
        if (this.operator.is(DBSPSumOperator.class)) {
            // Special case for sum, which normally takes references.
            this.getOutputStream()
                    .append("let ")
                    .append(name)
                    .append(" = ")
                    .append(operator.inputs.get(0).getName(USE_HASH_NAMES))
                    .append(".sum([");
            for (int i = 1; i < operator.inputs.size(); i++) {
                if (i > 1)
                    this.getOutputStream().append(", ");
                this.getOutputStream().append(operator.inputs.get(i).getName(USE_HASH_NAMES));
            }
            this.getOutputStream().append("])").append(";").newline();
        } else {
            visitor.generateOperator(this.operator);
        }
        visitor.pop(circuit);
        if (hasOutput) {
            this.getOutputStream().newline().append("return ");
            if (operator.is(DBSPNestedOperator.class)) {
                DBSPNestedOperator nested = operator.to(DBSPNestedOperator.class);
                this.getOutputStream().append("(");
                for (int i = 0; i < nested.outputCount(); i++) {
                    OutputPort port = nested.internalOutputs.get(i);
                    if (port != null)
                        this.getOutputStream().append(port.getName(USE_HASH_NAMES));
                    else
                        this.getOutputStream().append("()");
                    this.getOutputStream().append(", ");
                }
                this.getOutputStream().append(")");
            } else if (operator.outputCount() > 1) {
                this.getOutputStream().append("(");
                for (int i = 0; i < operator.outputCount(); i++) {
                    OutputPort port = operator.getOutput(i);
                    this.getOutputStream().append(port.getName(USE_HASH_NAMES));
                    this.getOutputStream().append(", ");
                }
                this.getOutputStream().append(")");
            } else {
                this.getOutputStream().append(name);
            }
            this.getOutputStream().append(";").newline();
        }
        this.getOutputStream().decrease().append("}");
    }
}
