package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.IIndentStream;

import java.util.function.Consumer;

/** Emits code for implementing a recursive component by using the DBSP function 
 * {@code dyn_recursive}. */
// Code generated for
//     DECLARE RECURSIVE VIEW V(v INT);
//     CREATE VIEW V AS SELECT v FROM V UNION SELECT 1;
//
// let recursive_factories = vec![
//     DistinctFactories::new::<Tup1<Option<i32>>, ()>(),
// ];
// let recursive_outputs = circuit.dyn_recursive(&recursive_factories,
//         |circuit, recursive_streams: Vec<Stream<_, _>>| {
//     let hash = Some("ddd8...");
//     let s2 = recursive_streams[0].typed::<WSet<Tup1<Option<i32>>>>();
//     s2.set_persistent_id(hash);
//     // ... the operators of the component, computing s5 from s2 ...
//     Ok(vec![s5.inner(), ])
// }).unwrap();
// let hash = Some(concat!("123c...", ".export"));
// let s5 = recursive_outputs[0].typed::<WSet<Tup1<Option<i32>>>>();
// s5.set_persistent_id(hash);
public final class VectorRecursiveComponentGenerator extends RecursiveComponentGenerator {
    /** Rust name of the vector holding one distinct factory per recursive stream */
    private static final String FACTORIES = "recursive_factories";
    /** Rust name of the closure parameter holding the erased recursive streams */
    private static final String STREAMS = "recursive_streams";
    /** Rust name of the vector holding the streams the component exports */
    private static final String OUTPUTS = "recursive_outputs";

    VectorRecursiveComponentGenerator(IIndentStream builder, ToRustInnerVisitor innerVisitor,
                                   DBSPNestedOperator operator, Consumer<DBSPOperator> emitChild) {
        super(builder, innerVisitor, operator, emitChild);
    }

    /** Emit {@code let name = source[stream].typed::<W>();}, which recovers from an erased
     * stream the row type W that the component computes.
     *
     * @param name      Rust name to bind.
     * @param source    Rust name of the vector of erased streams to index.
     * @param stream    Position of the stream in that vector.
     * @param outputNo  Output number of the component whose row type the stream has. */
    private void emitTypedStream(String name, String source, int stream, int outputNo) {
        this.builder.append("let ")
                .append(name)
                .append(" = ")
                .append(source)
                .append("[")
                .append(stream)
                .append("].typed::<");
        this.operator.outputType(outputNo).accept(this.innerVisitor);
        this.builder.append(">();").newline();
    }

    @Override
    void bindStream(int stream, int outputNo, String name) {
        this.emitTypedStream(name, STREAMS, stream, outputNo);
    }

    @Override
    void bindExport(int stream, int outputNo, String name) {
        this.emitTypedStream(name, OUTPUTS, stream, outputNo);
    }

    @Override
    void emitResult() {
        this.builder.append("Ok(vec![");
        for (int index : this.outputs)
            this.builder.append(this.exportName(index)).append(".inner(), ");
        this.builder.append("])").newline();
    }

    /** Emit the factories that supply the row formats of the recursive streams.
     * {@code dyn_recursive} reads the number of mutually recursive streams from the
     * length of this vector. */
    void emitFactories() {
        this.builder.append("let ").append(FACTORIES).append(" = vec![").increase();
        for (int index : this.outputs) {
            DBSPTypeZSet zset = this.operator.outputType(index).to(DBSPTypeZSet.class);
            this.builder.append("DistinctFactories::new::<");
            zset.elementType.accept(this.innerVisitor);
            this.builder.append(", ()>(),").newline();
        }
        this.builder.decrease().append("];").newline();
    }

    @Override
    void emitHeader() {
        this.emitFactories();
        this.builder.append("let ")
                .append(OUTPUTS)
                .append(" = circuit.dyn_recursive(&")
                .append(FACTORIES)
                .append(", |circuit, ")
                .append(STREAMS)
                .append(": Vec<Stream<_, _>>| {");
    }
}
