package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;

import javax.annotation.CheckReturnValue;
import java.util.List;

/** The inverse of the map_index operator.  This operator simply drops the index
 * from an indexed z-set and keeps all the values.  It can be implemented by
 * a DBSP map operator, but it is worthwhile to have a separate operation. */
public final class DBSPDeindexOperator extends DBSPUnaryOperator {
    static DBSPType outputType(DBSPTypeIndexedZSet ix) {
        return TypeCompiler.makeZSet(ix.elementType);
    }

    static DBSPExpression function(DBSPType inputType) {
        DBSPTypeIndexedZSet ix = inputType.to(DBSPTypeIndexedZSet.class);
        DBSPVariablePath t = new DBSPVariablePath("t", ix.getKVRefType());
        return t.field(1).deref().applyClone().closure(t.asParameter());
    }

    public DBSPDeindexOperator(CalciteObject node, DBSPOperator input) {
        super(node, "map", function(input.outputType),
                outputType(input.getOutputIndexedZSetType()), true, input);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @CheckReturnValue
    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDeindexOperator(this.getNode(), newInputs.get(0));
        return this;
    }
}
