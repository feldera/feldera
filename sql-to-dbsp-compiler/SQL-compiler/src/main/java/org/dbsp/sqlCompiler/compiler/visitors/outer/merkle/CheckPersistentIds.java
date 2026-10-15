package org.dbsp.sqlCompiler.compiler.visitors.outer.merkle;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.HashString;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Check that the persistent ids of the operators are not empty and are unique.
 *
 * <p>The persistent id is the global Merkle hash of an operator.  The runtime stores the
 * checkpointed state of an operator under its persistent id.
 *
 * <p>Also warns about views that always have the same contents. */
public class CheckPersistentIds extends CircuitVisitor {
    /** Maps a persistent id to the first operator that has it. */
    final Map<HashString, DBSPOperator> operatorById;
    /** Maps the persistent id and output number of a sink's input to the first sink that reads it. */
    final Map<String, DBSPSinkOperator> sinkByInput;

    public CheckPersistentIds(DBSPCompiler compiler) {
        super(compiler);
        this.operatorById = new HashMap<>();
        this.sinkByInput = new HashMap<>();
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.operatorById.clear();
        this.sinkByInput.clear();
        return super.startVisit(node);
    }

    @Override
    public void postorder(DBSPOperator operator) {
        HashString id = Objects.requireNonNull(OperatorHash.getHash(operator, true));
        if (id.toString().isEmpty())
            throw new InternalCompilerError("Operator " + operator + " has an empty persistent id", operator);
        DBSPOperator previous = this.operatorById.putIfAbsent(id, operator);
        if (previous != null)
            throw new InternalCompilerError("Operators " + previous + " and " + operator +
                    " have the same persistent id " + id, operator);
    }

    @Override
    public void postorder(DBSPSinkOperator sink) {
        super.postorder(sink);
        if (sink.metadata.system)
            return;
        OutputPort input = sink.input();
        HashString inputId = Objects.requireNonNull(OperatorHash.getHash(input.operator, true));
        DBSPSinkOperator previous = this.sinkByInput.putIfAbsent(inputId + ":" + input.outputNumber, sink);
        if (previous != null)
            this.compiler.reportWarning(sink.getSourcePosition(), "Identical views",
                    "View " + sink.viewName.singleQuote() + " always has the same contents as view " +
                            previous.viewName.singleQuote());
    }
}
