package org.dbsp.sqlCompiler.ir.type.user;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.MerkleInner;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.Utilities;

/** The type of a comparator.  In Rust, it's just a struct,
 * but which implements a CmpFun trait. */
public class DBSPComparatorType extends DBSPTypeUser {
    DBSPComparatorType(String name) {
        super(CalciteObject.EMPTY, DBSPTypeCode.COMPARATOR, name, false);
    }

    /** The name of a comparator that compares no fields of a tuple.
     * This is a comparator for a
     * {@link org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression}.  */
    static String generateName(DBSPType tuple) {
        return MerkleInner.hash(tuple.toString()).makeIdentifier("CMP");
    }

    public static DBSPComparatorType
    generateType(DBSPType tuple) {
        return new DBSPComparatorType(generateName(tuple));
    }

    /** The name of a comparator that compares an extra field in addition to
     * another comparator.  This is the name of a
     * {@link org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression}. */
    static String generateName(
            DBSPComparatorType comparator,
            int field, boolean ascending, boolean nullsFirst) {
        return MerkleInner.hash(comparator.name + "." + field + " " + ascending + " " + nullsFirst)
                .makeIdentifier("CMP");
    }

    public static DBSPComparatorType
    generateType(DBSPComparatorType type, int field, boolean ascending, boolean nullsFirst) {
        return new DBSPComparatorType(generateName(type, field, ascending, nullsFirst));
    }

    /** The name of a comparator that compares an extra field in addition to
     * another comparator.  This is the name of a
     * {@link org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression}. */
    static String generateName(
            DBSPComparatorType comparator,
            boolean ascending) {
        return MerkleInner.hash(comparator.name + " " + ascending)
                .makeIdentifier("CMP");
    }

    // Do not forget to override this function in subclasses, even
    // if the implementation is identical.
    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.postorder(this);
    }

    public static DBSPComparatorType
    generateType(DBSPComparatorType type, boolean ascending) {
        return new DBSPComparatorType(generateName(type, ascending));
    }

    @SuppressWarnings("unused")
    public static DBSPComparatorType fromJson(JsonNode node, JsonDecoder decoder) {
        String name = Utilities.getStringProperty(node, "name");
        DBSPTypeCode code = DBSPTypeCode.valueOf(Utilities.getStringProperty(node, "code"));
        return new DBSPComparatorType(name);
    }
}
