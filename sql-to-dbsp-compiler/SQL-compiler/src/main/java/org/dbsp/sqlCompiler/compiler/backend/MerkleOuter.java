package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.HashString;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Computes a Merkle hash for each operator.
 * The hash combines the hashes of the inputs (if includeInputs is true), and the hashes of all
 * the code that the operator invokes (e.g., function).  For operators
 * involved in input/output, it also includes metadata (e.g., column names).
 *
 * <p>This visitor is declared read-only, but it actually modifies the annotations on operators. */
public class MerkleOuter extends CircuitVisitor {
    /** Version of the format in which the runtime stores the state of a recursive circuit.
     *
     * <p>Mixed into the persistent id of every stream that leaves a recursive circuit, and
     * hence into the ids of all operators downstream of the recursion.  A pipeline resumed
     * from a checkpoint written by a runtime that used a different format does not find the
     * state of these operators, so it recomputes the recursive views from their inputs
     * instead of restoring state that it cannot interpret.  The pipeline manager sees the
     * same ids, so it reports the recursive views as modified and bootstraps them.
     *
     * <p>Bump this string whenever the runtime changes the way it stores that state. */
    public static final String RECURSIVE_STATE_VERSION = "recursive-state-v1";

    public final Map<Long, HashString> operatorHash;
    public final boolean includeInputs;
    /** Value mixed into the hash of the streams that leave a recursive circuit;
     * {@link #RECURSIVE_STATE_VERSION} in production, other values in tests. */
    final String recursiveStateVersion;
    /** Ids of the operators that produce the outputs of a recursive circuit. */
    final Set<Long> recursiveOutputs;

    public MerkleOuter(DBSPCompiler compiler, boolean includeInputs) {
        this(compiler, includeInputs, RECURSIVE_STATE_VERSION);
    }

    public MerkleOuter(DBSPCompiler compiler, boolean includeInputs, String recursiveStateVersion) {
        super(compiler);
        this.includeInputs = includeInputs;
        this.recursiveStateVersion = recursiveStateVersion;
        this.operatorHash = new HashMap<>();
        this.recursiveOutputs = new HashSet<>();
    }

    void setHashedString(DBSPOperator operator, String string) {
        // A stream that leaves a recursive circuit gets an id that depends on the format
        // in which its state is stored.  Only the global hash becomes a persistent id;
        // the local hash just names the generated code.
        if (this.includeInputs && this.recursiveOutputs.contains(operator.getId()))
            string += this.recursiveStateVersion;
        HashString hash = MerkleInner.hash(string);
        Logger.INSTANCE.belowLevel(this, 1)
                .append(this.includeInputs ? "Global " : "")
                .append("Merkle hash of ")
                .append(operator.id);
        Logger.INSTANCE.belowLevel(this, 2)
                .append(" from").newline()
                .append(string).newline();
        Logger.INSTANCE.belowLevel(this, 1)
                .append(" is ")
                .append(hash.toString())
                .newline();
        Utilities.putNew(this.operatorHash, operator.id, hash);
        operator.annotations.add(new OperatorHash(hash, this.includeInputs));
    }

    final List<ModifiedJsonOuter> generator = new ArrayList<>();

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        ModifiedJsonOuter outer = new ModifiedJsonOuter(this.compiler);
        this.generator.add(outer);
        for (IDBSPOuterNode parent: this.context)
            if (parent != operator)
                outer.push(parent);
        operator.accept(outer);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPOperator operator) {
        var outer = Utilities.removeLast(this.generator);
        String json = outer.getJsonString();
        this.setHashedString(operator, json);
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator nested) {
        this.push(nested);
        // Mark the streams that leave the recursive circuit before hashing the operators
        // that produce them.
        for (OutputPort output : nested.internalOutputs)
            if (output != null)
                this.recursiveOutputs.add(output.node().getId());
        int repeats = 1 + (this.includeInputs ? 1 : 0);
        for (int i = 0; i < repeats; i++) {
            this.startArrayProperty("allOperators");
            int index = 0;
            for (DBSPOperator op : nested.getAllOperators()) {
                this.propertyIndex(index++);
                // Pretend we haven't done it yet
                this.operatorHash.remove(op.id);
                op.accept(this);
            }
            this.endArrayProperty("allOperators");
        }

        this.pop(nested);
        StringBuilder builder = new StringBuilder();
        // The hash is obtained from the hash of all operators inside
        for (var operator : nested.getAllOperators()) {
            HashString hash = Utilities.getExists(
                    MerkleOuter.this.operatorHash, operator.getId());
            builder.append(operator.getCompactName())
                    .append(" = ")
                    .append(hash)
                    .append("(")
                    .append(OperatorHash.getHash(operator, true))
                    .append(");")
                    .append("\n");
        }

        String string = builder.toString();
        MerkleOuter.this.setHashedString(nested, string);
        return VisitDecision.STOP;
    }

    class ModifiedJsonOuter extends ToJsonOuterVisitor {
        ModifiedJsonOuter(DBSPCompiler compiler) {
            super(compiler, 0, new MerkleInner(
                    compiler,
                    new JsonStream(new IndentStreamBuilder().setIndentAmount(1))));
        }

        @Override
        public VisitDecision preorder(DBSPViewBaseOperator operator) {
            if (this.preorder(operator.to(DBSPUnaryOperator.class)).stop())
                return VisitDecision.STOP;
            this.property("viewName");
            this.asJsonInner(operator.viewName);
            this.property("metadata");
            operator.metadata.asJson(this.innerVisitor, true);
            return VisitDecision.CONTINUE;
        }

        @Override
        public VisitDecision preorder(DBSPOperator operator) {
            this.stream.beginObject();
            if (MerkleOuter.this.operatorHash.containsKey(operator.getId())) {
                this.stream.label("node");
                HashString hash = Utilities.getExists(
                        MerkleOuter.this.operatorHash, operator.getId());
                this.stream.append(hash.toString());
                this.stream.endObject();
                return VisitDecision.STOP;
            }

            operator.accept(this.innerVisitor);
            this.stream.appendClass(operator);
            // Identical operators at different depths are NOT equivalent
            this.label("depth");
            this.stream.append(this.context.size());
            this.label("inputs");
            this.stream.beginArray();

            List<OutputPort> inputs = operator.inputs;
            if (operator.is(DBSPViewDeclarationOperator.class)) {
                DBSPViewDeclarationOperator decl = operator.to(DBSPViewDeclarationOperator.class);
                DBSPNestedOperator parent = this.getParent().to(DBSPNestedOperator.class);
                inputs = new ArrayList<>();
                inputs.add(parent.outputForDeclaration(decl));
            }
            for (OutputPort port : inputs) {
                if (port != null) {
                    // Can be null for some outputs of a nested operator
                    if (MerkleOuter.this.includeInputs) {
                        this.stream.beginObject()
                                .label("outputNumber").append(port.outputNumber)
                                .label("operator");
                        HashString hashString = MerkleOuter.this.operatorHash.get(port.operator.id);
                        String str = "";
                        if (hashString != null)
                            str = hashString.toString();
                        this.stream.append(str);
                        this.stream.endObject();
                    } else {
                        port.outputType().accept(this.innerVisitor);
                    }
                }
            }
            this.stream.endArray();
            return VisitDecision.CONTINUE;
        }
    }
}
