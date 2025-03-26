package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.HashString;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
                other.compiler, new JsonStream(new IndentStreamBuilder().setIndentAmount(1))),
                other.includeInputs, other.operatorHash);
    }

    void setHash(DBSPOperator operator, String string) {
        HashString hash = MerkleInner.hash(string);
        Logger.INSTANCE.belowLevel(this, 1)
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

    @Override
    public VisitDecision preorder(DBSPNestedOperator nested) {
        this.stream.beginObject();
        this.push(nested);
        // Traverse TWICE
        for (int i = 0; i < 2; i++) {
            this.startArrayProperty("allOperators");
            int index = 0;
            for (DBSPOperator op : nested.getAllOperators()) {
                this.propertyIndex(index++);
                // Pretend we haven't done it yet
                this.serialized.remove(op.id);
                this.operatorHash.remove(op.id);
                op.accept(this);
            }
            this.endArrayProperty("allOperators");
        }

        this.pop(nested);
        StringBuilder builder = new StringBuilder();
        // The hash is obtained from the hash of all operators inside
        for (var operator: nested.getAllOperators()) {
            HashString hash = Utilities.getExists(this.operatorHash, operator.getId());
            builder.append(hash);
            builder.append(",");
        }

        String string = builder.toString();
        this.setHash(nested, string);
        this.stream.endObject();
        return VisitDecision.STOP;
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
        for (var ctxt: this.context)
            perOp.push(ctxt);

        perOp.stream.beginObject();
        operator.accept(perOp.innerVisitor);
        perOp.stream.appendClass(operator);
        if (operator.is(DBSPSourceTableOperator.class)) {
            perOp.label("metadata");
            operator.to(DBSPSourceTableOperator.class).metadata.asJson(perOp.innerVisitor);
        }
        // Identical operators at different depths are NOT equivalent
        perOp.label("depth");
        perOp.stream.append(this.context.size());
        perOp.label("inputs");
        perOp.stream.beginArray();

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
                if (this.includeInputs) {
                    perOp.stream.beginObject()
                            .label("outputNumber").append(port.outputNumber)
                            .label("operator");
                    HashString hashString = this.operatorHash.get(port.operator.id);
                    String str = "";
                    if (hashString != null)
                        str = hashString.toString();
                    perOp.stream.append(str);
                    perOp.stream.endObject();
                } else {
                    port.outputType().accept(perOp.innerVisitor);
                }
            }
        }
        perOp.stream.endArray();
        perOp.stream.endObject();

        String string = perOp.getJsonString();
        this.setHash(operator, string);
        this.stream.endObject();
        return VisitDecision.STOP;
    }
}
