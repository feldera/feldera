package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

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
        DBSPVariablePath t = new DBSPVariablePath(ix.getKVRefType());
        return t.field(1).deref().applyClone().closure(t);
    }

    public DBSPDeindexOperator(CalciteObject node, OutputPort input) {
        super(node, "map", function(input.outputType()),
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
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDeindexOperator(this.getNode(), newInputs.get(0))
                    .copyAnnotations(this);
        return this;
    }

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPDeindexOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPDeindexOperator(CalciteObject.EMPTY, info.getInput(0));
    }
}
