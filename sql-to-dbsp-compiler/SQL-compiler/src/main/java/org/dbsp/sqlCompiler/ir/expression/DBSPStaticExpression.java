package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.MerkleInner;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;

/** Represents an expression that is compiled into a
 * Lazy declaration and a reference to the lazy lock value.
 * The name of the cell is not represented explicitly. */
public class DBSPStaticExpression extends DBSPExpression {
    public final DBSPExpression initializer;
    final String name;

    public static final String RUST_IMPLEMENTATION = "StaticLazy";

    public DBSPStaticExpression(CalciteObject node, DBSPExpression initializer) {
        super(node, initializer.getType());
        String str = this.type + ":" + initializer;
        this.name = MerkleInner.hash(str).makeIdentifier("STATIC");
        this.initializer = initializer;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPStaticExpression(this.getNode(), this.initializer.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        return this == other;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("initializer");
        this.initializer.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPStaticExpression o = other.as(DBSPStaticExpression.class);
        if (o == null)
            return false;
        return this.initializer == o.initializer;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return this.initializer.toString(builder);
    }

    public String getName() {
        return this.name;
    }

    @SuppressWarnings("unused")
    public static DBSPStaticExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression initializer = fromJsonInner(node, "initializer", decoder, DBSPExpression.class);
        return new DBSPStaticExpression(CalciteObject.EMPTY, initializer);
    }
}
