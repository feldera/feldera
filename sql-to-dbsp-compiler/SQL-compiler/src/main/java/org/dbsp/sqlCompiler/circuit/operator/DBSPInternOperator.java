package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** An operator whose input contains all interned strings.
 * Behaves like a sink which is always live.
 * Does not correspond to a DBSP operator.
 * This operator is purely incremental; in non-incremental circuits
 * it must be preceded by a differentiator. */
public class DBSPInternOperator extends DBSPUnaryOperator {
    public DBSPInternOperator(OutputPort source) {
        super(CalciteEmptyRel.INSTANCE, "interned_strings", null, source.outputType(), false, source);
    }

    @Override
    public DBSPSimpleOperator with(@Nullable DBSPExpression function, DBSPType outputType, List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(function == null);
            Utilities.enforce(newInputs.size() == 1);
            Utilities.enforce(outputType.sameType(newInputs.get(0).outputType()));
            return new DBSPInternOperator(newInputs.get(0));
        }
        return this;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @SuppressWarnings("unused")
    public static DBSPInternOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPInternOperator(info.getInput(0))
                .addAnnotations(info.annotations(), DBSPInternOperator.class);
    }
}
