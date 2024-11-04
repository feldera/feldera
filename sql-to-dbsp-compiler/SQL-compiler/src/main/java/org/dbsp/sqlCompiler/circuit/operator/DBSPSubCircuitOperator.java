package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** An operator which contains multiple other operators. */
public class DBSPSubCircuitOperator extends DBSPOperator {
    Set<DBSPOperator> operators;
    List<DBSPOperator> allOperators;

    protected DBSPSubCircuitOperator(CalciteObject node) {
        super(node);
        this.allOperators = new ArrayList<>();
        this.operators = new HashSet<>();
    }

    void addOperator(DBSPOperator operator) {
        assert !this.operators.contains(operator);
        this.allOperators.add(operator);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop()) {
            for (DBSPOperator op : this.allOperators)
                op.accept(visitor);
            visitor.postorder(this);
        }
        visitor.pop(this);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (this == other)
            return true;
        if (other instanceof DBSPSubCircuitOperator sub) {
            return Linq.all(Linq.zipSameLength(this.allOperators, sub.allOperators, DBSPOperator::equivalent));
        }
        return false;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("{").increase();
        for (DBSPOperator op: this.allOperators)
            builder.append(op);
        return builder.append("}").newline().decrease();
    }
}
