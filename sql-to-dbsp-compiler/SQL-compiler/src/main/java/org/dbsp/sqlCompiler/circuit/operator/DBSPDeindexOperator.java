package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.List;

/**
 * The inverse of the map_index operator.  This operator simply drops the index
 * from an indexed z-set and keeps all the values.
 */
public class DBSPDeindexOperator extends DBSPUnaryOperator {
    static DBSPType outputType(DBSPType inputType) {
        DBSPTypeIndexedZSet ix = inputType.to(DBSPTypeIndexedZSet.class);
        return TypeCompiler.makeZSet(ix.elementType, ix.weightType);
    }

    static DBSPExpression function(DBSPType inputType) {
        DBSPTypeIndexedZSet ix = inputType.to(DBSPTypeIndexedZSet.class);
        DBSPVariablePath t = new DBSPVariablePath("t",
                new DBSPTypeRawTuple(ix.keyType.ref(), ix.elementType.ref()));
        return t.field(1).applyClone().closure(t.asParameter());
    }

    public DBSPDeindexOperator(CalciteObject node, DBSPOperator input) {
        super(node, "map", function(input.outputType), outputType(input.outputType), true, input);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.postorder(this);
    }

    @CheckReturnValue
    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDeindexOperator(this.getNode(), newInputs.get(0));
        return this;
    }
}
