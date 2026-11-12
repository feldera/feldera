package org.dbsp.sqlCompiler.compiler.visitors.outer.merkle;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.ISplitInput;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.HashString;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Check that the persistent ids of the operators are not empty and are unique.
 *
 * <p>The persistent id is the global Merkle hash of an operator.  The runtime stores the
 * checkpointed state of an operator under its persistent id.
 *
 * <p>The two halves of a split operator are one node of the runtime: they must have the same id.
 *
 * <p>Also warns about views that always have the same contents. */
public class CheckPersistentIds extends CircuitVisitor {
    /** Maps a persistent id to the first operator that has it. */
    final Map<HashString, DBSPOperator> operatorById;
    /** Maps the persistent id and output number of a sink's input to the first sink that reads it. */
    final Map<String, DBSPSinkOperator> sinkByInput;
    /** False when an earlier check has already warned about identical views. */
    final boolean warnIdenticalViews;

    public CheckPersistentIds(DBSPCompiler compiler) {
        this(compiler, true);
    }

    public CheckPersistentIds(DBSPCompiler compiler, boolean warnIdenticalViews) {
        super(compiler);
        this.operatorById = new HashMap<>();
        this.sinkByInput = new HashMap<>();
        this.warnIdenticalViews = warnIdenticalViews;
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
        if (operator.is(ISplitInput.class)) {
            DBSPOperator output = operator.to(ISplitInput.class).splitOutput().outputPort().node();
            if (!id.equals(OperatorHash.getHash(output, true)))
                throw new InternalCompilerError("The halves " + output + " and " + operator +
                        " of a split operator have different persistent ids", operator);
            return;
        }
        DBSPOperator previous = this.operatorById.putIfAbsent(id, operator);
        if (previous != null)
            throw new InternalCompilerError("Operators " + previous + " and " + operator +
                    " have the same persistent id " + id, operator);
    }

    @Override
    public void postorder(DBSPNestedOperator operator) {
        // An empty nested circuit hashes to the hash of the empty string
        Utilities.enforce(operator.size() > 0,
                () -> "Recursive circuit for views " + operator.outputViews + " has no operators");
        // Outputs that are the same stream would give two feedback streams one persistent id
        Set<OutputPort> outputs = new HashSet<>();
        for (OutputPort port : operator.internalOutputs)
            Utilities.enforce(port == null || outputs.add(port),
                    () -> "Recursive circuit for views " + operator.outputViews + " has two outputs that are the same stream " + port);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSinkOperator sink) {
        super.postorder(sink);
        if (sink.metadata.system || !this.warnIdenticalViews)
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
