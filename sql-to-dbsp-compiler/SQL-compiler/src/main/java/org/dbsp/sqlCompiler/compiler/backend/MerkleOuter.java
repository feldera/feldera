package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.MerkleHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Computes a Merkle hash for each operator.
 * The hash combines the hashes of the inputs, and the hashes of all
 * the code that the operator invokes (e.g., function).  For operators
 * involved in input/output, it also includes metadata (e.g., column names).
 *
 * <p>This visitor is declared read-only, but it actually modifies the annotations on operators. */
public class MerkleOuter extends ToJsonOuterVisitor {
    public final Map<Long, String> operatorHash;

    MerkleOuter(DBSPCompiler compiler, ToJsonInnerVisitor inner, Map<Long, String> operatorHash) {
        super(compiler, 0, inner);
        this.operatorHash = operatorHash;
    }

    public MerkleOuter(DBSPCompiler compiler) {
        this(compiler, new MerkleInner(compiler, new JsonStream(new IndentStreamBuilder().setIndentAmount(1))), new HashMap<>());
    }

    MerkleOuter(MerkleOuter other) {
        this(other.compiler, new MerkleInner(
                other.compiler, new JsonStream(new IndentStreamBuilder())), other.operatorHash);
    }

    void setHash(DBSPOperator operator, String hash) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Merkle hash ")
                .append(operator.getId())
                .append(" is ")
                .append(hash)
                .newline();
        Utilities.putNew(this.operatorHash, operator.id, hash);
        operator.annotations.add(new MerkleHash(hash));
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        this.stream.beginObject();
        if (this.operatorHash.containsKey(operator.getId())) {
            this.stream.label("node");
            String hash = Utilities.getExists(this.operatorHash, operator.getId());
            this.stream.append(hash);
            this.stream.endObject();
            return VisitDecision.STOP;
        }

        // Use a new visitor, to generate a JSON object per Operator.
        // Then we hash that JSON object.
        MerkleOuter perOp = new MerkleOuter(this);
        if (operator.is(DBSPNestedOperator.class)) {
            return VisitDecision.CONTINUE;
        }

        operator.accept(this.innerVisitor);
        this.stream.appendClass(operator);
        this.label("inputs");
        this.stream.beginArray();
        for (OutputPort port: operator.inputs) {
            port.asJson(perOp);
        }
        this.stream.endArray();
        String string = perOp.getJsonString();
        String hash = MerkleInner.hash(string);
        this.setHash(operator, hash);
        this.stream.endObject();
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPNestedOperator nested) {
        this.stream.endObject();
    }
}
