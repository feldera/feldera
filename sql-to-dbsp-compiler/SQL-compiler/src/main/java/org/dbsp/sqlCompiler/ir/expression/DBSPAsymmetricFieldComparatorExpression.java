package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;

/** A comparator that looks at the fields of two different tuples.
 * Slightly similar to {@link DBSPFieldComparatorExpression}, but used in more constrained ways */
public final class DBSPAsymmetricFieldComparatorExpression extends DBSPExpression {
    public record Collation(int leftField, int rightField, boolean ascending, boolean nullsFirst) {
        public String toString() {
            return "compare(l." + this.leftField + ", r. " + this.rightField + ", " + ascending + ", " + nullsFirst + ")";
        }

        public void asJson(JsonStream stream) {
            stream.beginObject();
            stream.label("leftField");
            stream.append(this.leftField);
            stream.label("rightField");
            stream.append(this.rightField);
            stream.label("ascending");
            stream.append(this.ascending);
            stream.label("nullsFirst");
            stream.append(this.nullsFirst);
            stream.endObject();
        }

        public static Collation fromJson(JsonNode node) {
            int leftField = Utilities.getIntProperty(node, "leftField");
            int rightField = Utilities.getIntProperty(node, "rightField");
            boolean ascending = Utilities.getBooleanProperty(node, "ascending");
            boolean nullsFirst = Utilities.getBooleanProperty(node, "nullsFirst");
            return new Collation(leftField, rightField, ascending, nullsFirst);
        }
    }

    public final List<Collation> comparisons;
    public final DBSPType leftType;
    public final DBSPType rightType;

    public DBSPAsymmetricFieldComparatorExpression(
            CalciteObject node, DBSPType leftType, DBSPType rightType, List<Collation> comparisons) {
        super(node, DBSPTypeVoid.INSTANCE);
        this.comparisons = comparisons;
        this.leftType = leftType;
        this.rightType = rightType;
        // Fields must have the same types
        for (Collation c: comparisons) {
            DBSPType left = leftType.to(DBSPTypeTupleBase.class).getFieldType(c.leftField);
            DBSPType right = rightType.to(DBSPTypeTupleBase.class).getFieldType(c.rightField);
            Utilities.enforce(left.sameType(right));
        }
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("leftType");
        this.leftType.accept(visitor);
        visitor.property("rightType");
        this.rightType.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPAsymmetricFieldComparatorExpression o = other.as(DBSPAsymmetricFieldComparatorExpression.class);
        if (o == null)
            return false;
        return this.leftType.sameType(o.leftType) &&
                this.rightType.sameType(o.rightType) &&
                this.comparisons.equals(o.comparisons);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.comparisons.toString());
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPAsymmetricFieldComparatorExpression(this.node, this.leftType, this.rightType, this.comparisons);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPAsymmetricFieldComparatorExpression otherExpression = other.as(DBSPAsymmetricFieldComparatorExpression.class);
        if (otherExpression == null)
            return false;
        return this.leftType.sameType(otherExpression.leftType) &&
                this.rightType.sameType(otherExpression.rightType) &&
                this.comparisons.equals(otherExpression.comparisons);
    }

    @SuppressWarnings("unused")
    public static DBSPAsymmetricFieldComparatorExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType leftType = fromJsonInner(node, "leftType", decoder, DBSPType.class);
        DBSPType rightType = fromJsonInner(node, "rightType", decoder, DBSPType.class);
        Utilities.enforce(node.isArray(), () -> "Node is not an array " + Utilities.toDepth(node, 1));
        var comparisons = Linq.list(Linq.map(node.elements(), Collation::fromJson));
        return new DBSPAsymmetricFieldComparatorExpression(CalciteObject.EMPTY, leftType, rightType, comparisons);
    }
}
