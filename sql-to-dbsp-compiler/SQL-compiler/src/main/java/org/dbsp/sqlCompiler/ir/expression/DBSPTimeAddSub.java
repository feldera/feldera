package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeShortInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeLongInterval;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** Represents a wide class of addition or subtraction operations on time-related values.
 * Time-related values can have one of 5 main types
 * - DATE
 * - TIME
 * - TIMESTAMP
 * - SHORT INTERVAL
 * - LONG INTERVAL
 * Operation can be addition or subtraction. */
public class DBSPTimeAddSub extends DBSPExpression {
    public final DBSPOpcode opcode;
    public final DBSPExpression left;
    public final DBSPExpression right;
    // The following two fields are redundant with the result type information, and can be derived from it.
    // However, by keeping them here explicitly we ensure that equivalence checks do not forget to check the result type.
    // The result type for all other operations is a deterministic function of the input types and the operation fields.
    // By using these fields, we ensure that this operation follows the same pattern.
    @Nullable
    public final DBSPTypeShortInterval.Units shortUnits;
    @Nullable
    public final DBSPTypeLongInterval.Units longUnits;

    public DBSPTimeAddSub(CalciteObject node, DBSPType type, DBSPOpcode opcode, DBSPExpression left, DBSPExpression right) {
        super(node, type);
        this.opcode = opcode;
        Utilities.enforce(opcode == DBSPOpcode.ADD || opcode == DBSPOpcode.SUB);
        this.left = left;
        this.right = right;
        if (this.type.is(DBSPTypeShortInterval.class)) {
            this.shortUnits = this.type.to(DBSPTypeShortInterval.class).units;
            this.longUnits = null;
        } else if (this.type.is(DBSPTypeLongInterval.class)) {
            this.longUnits = this.type.to(DBSPTypeLongInterval.class).units;
            this.shortUnits = null;
        } else {
            this.shortUnits = null;
            this.longUnits = null;
        }
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPTimeAddSub(this.node, this.type, this.opcode, left.deepCopy(), right.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPTimeAddSub otherExpression = other.as(DBSPTimeAddSub.class);
        if (otherExpression == null)
            return false;
        return this.opcode == otherExpression.opcode &&
                this.shortUnits == otherExpression.shortUnits &&
                this.longUnits == otherExpression.longUnits &&
                context.equivalent(this.left, otherExpression.left) &&
                context.equivalent(this.right, otherExpression.right);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.property("left");
        this.left.accept(visitor);
        visitor.property("right");
        this.right.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPTimeAddSub o = other.as(DBSPTimeAddSub.class);
        if (o == null)
            return false;
        return this.left == o.left &&
                this.right == o.right &&
                this.opcode == o.opcode &&
                this.longUnits == o.longUnits &&
                this.shortUnits == o.shortUnits;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(")
                .append(this.left)
                .append(" ")
                .append(this.left.type.mayBeNull ? "?" : "")
                .append(this.opcode.toString())
                .append(this.right.type.mayBeNull ? "?" : "")
                .append(" ")
                .append(this.right)
                .append(")");
    }

    @SuppressWarnings("unused")
    public static DBSPTimeAddSub fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = getJsonType(node, decoder);
        DBSPExpression left = fromJsonInner(node, "left", decoder, DBSPExpression.class);
        DBSPExpression right = fromJsonInner(node, "right", decoder, DBSPExpression.class);
        DBSPOpcode opcode = DBSPOpcode.fromJson(node);
        return new DBSPTimeAddSub(CalciteObject.EMPTY, type, opcode, left, right);
    }
}
