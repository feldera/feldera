package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.HashString;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Computes a Merkle hash for each operator.
 * The hash combines the hashes of the inputs (if includeInputs is true), and the hashes of all
 * the code that the operator invokes (e.g., function).  For operators
 * involved in input/output, it also includes metadata (e.g., column names).
 *
 * <p>This visitor is declared read-only, but it actually modifies the annotations on operators. */
public class MerkleOuter extends ToJsonOuterVisitor {
    public final Map<Long, HashString> operatorHash;
    public final boolean includeInputs;

    MerkleOuter(DBSPCompiler compiler, ToJsonInnerVisitor inner, boolean includeInputs, 
                Map<Long, HashString> operatorHash) {
        super(compiler, 0, inner);
        this.operatorHash = operatorHash;
        this.includeInputs = includeInputs;
    }

    public MerkleOuter(DBSPCompiler compiler, boolean includeInputs) {
        this(compiler, new MerkleInner(compiler, new JsonStream(new IndentStreamBuilder().setIndentAmount(1))), 
                includeInputs, new HashMap<>());
    }

    MerkleOuter(MerkleOuter other) {
        this(other.compiler, new MerkleInner(
                other.compiler, new JsonStream(new IndentStreamBuilder())), 
                other.includeInputs, other.operatorHash);
    }

    void setHash(DBSPOperator operator, HashString hash) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Merkle hash ")
                .append(operator.getId())
                .append(" is ")
                .append(hash.toString())
                .newline();
        Utilities.putNew(this.operatorHash, operator.id, hash);
        operator.annotations.add(new OperatorHash(hash, this.includeInputs));
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator operator) {
        this.stream.beginObject();
        if (this.operatorHash.containsKey(operator.getId())) {
            this.stream.label("node");
            HashString hash = Utilities.getExists(this.operatorHash, operator.getId());
            this.stream.append(hash.toString());
            this.stream.endObject();
            return VisitDecision.STOP;
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPNestedOperator nested) {
        StringBuilder builder = new StringBuilder();
        // The hash is obtained from the hash of all operators inside
        for (var operator: nested.getAllOperators()) {
            HashString hash = Utilities.getExists(this.operatorHash, operator.getId());
            builder.append(hash);
            builder.append(",");
        }

        String string = builder.toString();
        HashString hash = MerkleInner.hash(string);
        this.setHash(nested, hash);
        this.stream.endObject();
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        this.stream.beginObject();
        if (this.operatorHash.containsKey(operator.getId())) {
            this.stream.label("node");
            HashString hash = Utilities.getExists(this.operatorHash, operator.getId());
            this.stream.append(hash.toString());
            this.stream.endObject();
            return VisitDecision.STOP;
        }

        // Use a new visitor, to generate a JSON object only for this operator;
        // then, hash that JSON object.
        MerkleOuter perOp = new MerkleOuter(this);
        perOp.stream.beginObject();
        operator.accept(perOp.innerVisitor);
        perOp.stream.appendClass(operator);
        if (operator.is(DBSPSourceTableOperator.class)) {
            perOp.label("metadata");
            operator.to(DBSPSourceTableOperator.class).metadata.asJson(perOp.innerVisitor);
        }
        // Identical operators at different depths are NOT equivalent
        perOp.label("depth");
        perOp.stream.append(this.current.size());
        if (this.includeInputs) {
            perOp.label("inputs");
            perOp.stream.beginArray();
            for (OutputPort port : operator.inputs) {
                port.asJson(perOp);
            }
            perOp.stream.endArray();
        }
        perOp.stream.endObject();

        String string = perOp.getJsonString();
        HashString hash = MerkleInner.hash(string);
        this.setHash(operator, hash);
        this.stream.endObject();
        return VisitDecision.STOP;
    }
}
