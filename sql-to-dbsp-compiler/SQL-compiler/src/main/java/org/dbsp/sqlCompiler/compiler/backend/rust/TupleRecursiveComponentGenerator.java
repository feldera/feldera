package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.util.IIndentStream;

import java.util.function.Consumer;

/** Emits code for implementing a recursive component using the DBSP function {@code recursive}.
 * This writer may be deprecated soon, it is here for backwards-compatibility. */
// Code generated for
//     DECLARE RECURSIVE VIEW V(v INT);
//     CREATE VIEW V AS SELECT v FROM V UNION SELECT 1;
//
// let (s5, ) = circuit.recursive(|circuit, (s2, ): (Stream<NestedCircuit, WSet<Tup1<Option<i32>>>>, )| {
//     let hash = Some("ddd8...");
//     s2.set_persistent_id(hash);
//     // ... the operators of the component, computing s5 from s2 ...
//     Ok((s5, ))
// }).unwrap();
// let hash = Some(concat!("123c...", ".export"));
// s5.set_persistent_id(hash);
public final class TupleRecursiveComponentGenerator extends RecursiveComponentGenerator {
    /** Maximum number of mutually recursive streams that this implementation supports. */
    private static final int MAX_STREAMS = 14;

    TupleRecursiveComponentGenerator(IIndentStream builder, ToRustInnerVisitor innerVisitor,
                                  DBSPNestedOperator operator, Consumer<DBSPOperator> emitChild) {
        super(builder, innerVisitor, operator, emitChild);
    }

    @Override
    void bindStream(int stream, int index, String name) { }

    @Override
    void bindExport(int stream, int index, String name) { }

    @Override
    void emitResult() {
        this.builder.append("Ok((");
        for (int index : this.outputs)
            this.builder.append(this.exportName(index)).append(", ");
        this.builder.append("))").newline();
    }

    @Override
    void emitHeader() {
        if (this.outputs.size() > MAX_STREAMS)
            throw new UnsupportedException("Recursive computation with " + this.outputs.size() +
                    " mutually recursive views; at most " +
                    MAX_STREAMS + " mutually-recursive views are supported.  See " +
                    "https://github.com/feldera/feldera/issues/5193",
                    this.operator.getRelNode());

        this.builder.append("let (");
        for (int index : this.outputs)
            this.builder.append(this.exportName(index)).append(", ");
        this.builder.append(") = circuit.recursive(|circuit, (");
        for (int index : this.outputs)
            this.builder.append(this.streamName(this.namedOutput(index))).append(", ");
        this.builder.append("): (");
        for (int index : this.outputs) {
            // The streams produced inside the nested circuit
            this.operator.internalOutputs.get(index).streamType(1).accept(this.innerVisitor);
            this.builder.append(", ");
        }
        this.builder.append(")| {");
    }
}
