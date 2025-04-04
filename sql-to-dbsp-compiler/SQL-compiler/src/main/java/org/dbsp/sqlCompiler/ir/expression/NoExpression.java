package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

/** This expression cannot appear in the IR tree representation
 * of a program.  It is only used as a value in dataflow analyses. */
@NonCoreIR
public final class NoExpression extends DBSPExpression {
    public NoExpression(DBSPType type) {
        super(CalciteObject.EMPTY, type);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (!(other instanceof NoExpression noexp))
            return false;
        return this.type == noexp.type;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new NoExpression(this.type);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        return other.is(NoExpression.class);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("[NO]:").append(this.type);
    }

    @SuppressWarnings("unused")
    public static NoExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = getJsonType(node, decoder);
        return new NoExpression(type);
    }
}
