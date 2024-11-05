package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An operator which contains multiple other operators.
 * It has no inputs or outputs, it is just a container for other operators. */
public class DBSPNestedOperator extends DBSPOperator implements ICircuit {
    final List<DBSPOperator> allOperators;
    public final Map<String, DBSPViewOperator> views;

    public DBSPNestedOperator(CalciteObject node) {
        super(node);
        this.allOperators = new ArrayList<>();
        this.views = new HashMap<>();
    }

    public void addOperator(DBSPOperator operator) {
        this.allOperators.add(operator);
        if (operator.is(DBSPViewOperator.class)) {
            DBSPViewOperator view = operator.to(DBSPViewOperator.class);
            Utilities.putNew(this.views, view.viewName, view);
        }
    }

    @Override
    public void addDeclaration(DBSPDeclaration declaration) {
        throw new InternalCompilerError("Adding declaration to CircuitOperator");
    }

    @Override
    public DBSPViewOperator getView(String name) {
        return Utilities.getExists(this.views, name);
    }

    @Override
    public Iterable<DBSPOperator> getAllOperators() {
        return this.allOperators;
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
        if (other instanceof DBSPNestedOperator sub) {
            return Linq.all(Linq.zipSameLength(this.allOperators, sub.allOperators, DBSPOperator::equivalent));
        }
        return false;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("/* subcircuit")
                .append(this.id)
                .append("*/ {")
                .increase();
        for (DBSPOperator op: this.allOperators)
            builder.append(op);
        return builder.newline().decrease().append("}");
    }

    @Override
    public boolean sameCircuit(ICircuit other) {
        if (this == other)
            return true;
        if (other.is(DBSPNestedOperator.class)) {
            return Linq.same(this.allOperators, other.to(DBSPNestedOperator.class).allOperators);
        }
        return false;
    }
}
