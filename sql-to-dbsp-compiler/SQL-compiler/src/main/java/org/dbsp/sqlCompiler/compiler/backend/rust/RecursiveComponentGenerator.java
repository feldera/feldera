package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.annotation.Recursive;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.HashString;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Base class for the Rust code generators that implement a {@link DBSPNestedOperator}. */
public abstract class RecursiveComponentGenerator {
    final IIndentStream builder;
    final ToRustInnerVisitor innerVisitor;
    final DBSPNestedOperator operator;
    final Consumer<DBSPOperator> emitChild;
    /** One output number of the component per distinct stream */
    final List<Integer> outputs;

    protected RecursiveComponentGenerator(IIndentStream builder, ToRustInnerVisitor innerVisitor,
                                           DBSPNestedOperator operator, Consumer<DBSPOperator> emitChild) {
        this.builder = builder;
        this.innerVisitor = innerVisitor;
        this.operator = operator;
        this.emitChild = emitChild;
        this.outputs = operator.distinctOutputs();
    }

    /** Declaration that stands for the view of output {@code index} inside the component,
     * or null if the component declares no such view */
    @Nullable
    DBSPViewDeclarationOperator declaration(int index) {
        ProgramIdentifier view = this.operator.outputViews.get(index);
        return this.operator.declarationByName.get(view);
    }

    /** Outputs of the component that carry the same stream as output {@code index} */
    List<Integer> sharing(int index) {
        OutputPort stream = this.operator.internalOutputs.get(index);
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < this.operator.outputCount(); i++)
            if (stream.equals(this.operator.internalOutputs.get(i)))
                result.add(i);
        return result;
    }

    /** Rust name of the stream that output {@code index} exports out of the component */
    String exportName(int index) {
        return this.operator.internalOutputs.get(index).getName(false);
    }

    /** Rust name of the stream that the recursion feeds back to output {@code index} */
    String streamName(int index) {
        DBSPViewDeclarationOperator declaration = this.declaration(index);
        if (declaration != null)
            return declaration.getNodeName(false);
        return "unused_" + index;
    }

    /** Output whose name the recursion binds to the stream that output {@code index}
     * carries.  The names of the other outputs that share the stream alias that one. */
    int namedOutput(int index) {
        for (int shared : this.sharing(index))
            if (this.declaration(shared) != null)
                return shared;
        return index;
    }

    /** Emit {@code let hash = ...;}, the persistent id that DBSP gives the next stream */
    void emitHash(DBSPOperator operator, @Nullable String suffix) {
        HashString hash = OperatorHash.getHash(operator, true);
        if (hash == null) {
            this.builder.append("let hash = None;").newline();
            return;
        }
        this.builder.append("let hash = Some(");
        if (suffix == null) {
            this.builder.append(hash.toQuotedString());
        } else {
            this.builder.append("concat!(")
                    .append(hash.toQuotedString())
                    .append(", \"")
                    .append(suffix)
                    .append("\")");
        }
        this.builder.append(");").newline();
    }

    /** Emit {@code name.set_persistent_id(hash);} */
    void emitPersistentId(String name) {
        this.builder.append(name).append(".set_persistent_id(hash);").newline();
    }

    /** Bind the Rust name of a stream that the recursion feeds back into the component.
     *
     * @param stream  Position of the stream among the recursive streams.
     * @param index   Output number of the component that the stream carries.
     * @param name    Rust name to bind. */
    abstract void bindStream(int stream, int index, String name);

    /** Bind the Rust name of a stream that the recursion exports out of the component.
     *
     * @param stream  Position of the stream among the recursive streams.
     * @param index   Output number of the component that the stream carries.
     * @param name    Rust name to bind. */
    abstract void bindExport(int stream, int index, String name);

    /** Emit the streams the component computed, in the order the recursion expects them */
    abstract void emitResult();

    /** Emit the recursion up to and including the brace that opens the closure which
     * computes the streams */
    abstract void emitHeader();

    /** Emit the code that closes the call that {@link #emitHeader} opened */
    void emitTrailer() {
        this.builder.append("}).unwrap();").newline();
    }

    /** Emit the code that gives each recursive stream its Rust name and its persistent id */
    void emitStreamNames() {
        for (int stream = 0; stream < this.outputs.size(); stream++) {
            int index = this.outputs.get(stream);
            int named = this.namedOutput(index);
            DBSPViewDeclarationOperator declaration = this.declaration(named);
            if (declaration != null)
                this.emitHash(declaration, null);
            else
                this.emitHash(this.operator.internalOutputs.get(index).operator, ".delay");
            String name = this.streamName(named);
            this.bindStream(stream, index, name);
            this.emitPersistentId(name);
            // The views that share this stream compute the same relation, so the
            // declarations that the other views read name the same stream.
            for (int shared : this.sharing(index))
                if (shared != named && this.declaration(shared) != null)
                    this.builder.append("let ")
                            .append(this.streamName(shared))
                            .append(" = ")
                            .append(name)
                            .append(".clone();")
                            .newline();
        }
    }

    /** Emit the operators of the component */
    void emitChildren() {
        for (DBSPOperator node : this.operator.getAllOperators())
            if (!node.is(DBSPViewDeclarationOperator.class))
                this.emitChild.accept(node);
    }

    /** Give each stream that leaves the component its persistent id. */
    void emitExports() {
        for (int stream = 0; stream < this.outputs.size(); stream++) {
            int index = this.outputs.get(stream);
            String name = this.exportName(index);
            this.emitHash(this.operator.internalOutputs.get(index).operator, ".export");
            this.bindExport(stream, index, name);
            this.emitPersistentId(name);
        }
    }

    /** Emit the code for the component. */
    public void emit() {
        Utilities.enforce(this.operator.hasAnnotation(a -> a.is(Recursive.class)),
                () -> "NestedOperator not recursive");
        if (this.outputs.isEmpty())
            // Every view of the component was deleted
            return;

        this.emitHeader();
        this.builder.increase();
        this.emitStreamNames();
        this.emitChildren();
        this.emitResult();
        this.builder.decrease();
        this.emitTrailer();
        this.emitExports();
    }
}
