package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.MerkleInner;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.Utilities;

/** Represents an expression that is compiled into a
 * Lazy declaration and a reference to the lazy lock value.
 * The name of the cell is not represented explicitly. */
public class DBSPStaticExpression extends DBSPExpression {
    public final DBSPExpression initializer;
    public final String name;

    public static final String RUST_IMPLEMENTATION = "StaticLazy";

    public DBSPStaticExpression(CalciteObject node, DBSPExpression initializer, String name) {
        super(node, initializer.getType());
        this.initializer = initializer;
        this.name = name;
    }

    public static String generateName(DBSPExpression initializer, DBSPCompiler compiler) {
        IndentStream stream = new IndentStreamBuilder();
        ToRustInnerVisitor toRust = new ToRustInnerVisitor(compiler, stream, null, false);
        initializer.accept(toRust);
        stream.append(":");
        initializer.type.accept(toRust);
        String str = stream.toString();
        return MerkleInner.hash(str).makeIdentifier("STATIC");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPStaticExpression(this.getNode(), this.initializer.deepCopy(), this.name);
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
        String name = Utilities.getStringProperty(node, "name");
        return new DBSPStaticExpression(CalciteObject.EMPTY, initializer, name);
    }
}
