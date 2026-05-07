package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.JoinStrategy;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Check that there are no conflicting hints. */
public class CheckHints extends CircuitVisitor {
    final Map<OutputPort, List<JoinStrategy>> portHints;

    void addHint(OutputPort port, JoinStrategy hint) {
        if (!this.portHints.containsKey(port)) {
            this.portHints.put(port, new ArrayList<>());
        } else {
            for (var existing: this.portHints.get(port)) {
                if (!existing.compatible(hint)) {
                    this.compiler.reportError(hint.getPosition(),
                            "Incompatible hints",
                            "Found two conflicting hints for the same collection: " + hint);
                    this.compiler.reportError(existing.getPosition(),
                            "Incompatible hints",
                            "Previous hint: " + existing,
                            true);
                }
            }
        }
        this.portHints.get(port).add(hint);
    }

    public CheckHints(DBSPCompiler compiler) {
        super(compiler);
        this.portHints = new HashMap<>();
    }

    @Override
    public void postorder(DBSPJoinBaseOperator join) {
        var hints = join.annotations.get(JoinStrategy.class);
        int inputNo = 0;
        for (var input: join.inputs) {
            for (var hint: hints) {
                if (hint.input == inputNo) {
                    this.addHint(input, hint);
                }
            }
            inputNo++;
        }
    }
}
