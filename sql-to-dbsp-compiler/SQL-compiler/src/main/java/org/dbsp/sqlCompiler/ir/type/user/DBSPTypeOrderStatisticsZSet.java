package org.dbsp.sqlCompiler.ir.type.user;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import java.util.List;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/**
 * Represents the OrderStatisticsZSet type used for PERCENTILE_CONT/PERCENTILE_DISC aggregates.
 *
 * OrderStatisticsZSet supports:
 * - O(log n) insertion with positive/negative weights for incremental computation
 * - O(log n) deletion via negative weights
 * - O(log n) k-th element selection
 * - Proper multiset semantics (duplicate values with counts)
 */
public class DBSPTypeOrderStatisticsZSet extends DBSPTypeUser {
    public DBSPTypeOrderStatisticsZSet(DBSPType elementType, boolean mayBeNull) {
        super(elementType.getNode(), USER, "OrderStatisticsZSet", mayBeNull, elementType);
    }

    public DBSPType getElementType() {
        return this.getTypeArg(0);
    }

    /** OrderStatisticsZSet::new() or Some(OrderStatisticsZSet::new()), depending on nullability */
    public DBSPExpression emptyTree() {
        DBSPExpression tree = new DBSPApplyExpression("OrderStatisticsZSet::new", this.withMayBeNull(false));
        if (!this.mayBeNull)
            return tree;
        return tree.some();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.startArrayProperty("typeArgs");
        int index = 0;
        for (DBSPType type : this.typeArgs) {
            visitor.propertyIndex(index);
            index++;
            type.accept(visitor);
        }
        visitor.endArrayProperty("typeArgs");
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeOrderStatisticsZSet(this.getElementType(), mayBeNull);
    }

    // sameType and hashCode inherited from TypeUser.

    @SuppressWarnings("unused")
    public static DBSPTypeOrderStatisticsZSet fromJson(JsonNode node, JsonDecoder decoder) {
        List<DBSPType> typeArgs = fromJsonInnerList(node, "typeArgs", decoder, DBSPType.class);
        Utilities.enforce(typeArgs.size() == 1);
        boolean mayBeNull = fromJsonMayBeNull(node);
        return new DBSPTypeOrderStatisticsZSet(typeArgs.get(0), mayBeNull);
    }
}
