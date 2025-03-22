package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;
import java.util.Objects;

/** Represents the type of a user-defined struct */
public class RelStruct extends RelRecordType {
    /** Name of the struct type */
    public final SqlIdentifier typeName;
    /** The Calcite type system uses a static cache to store the
     * canonical version of a type.  This means that it cannot represent
     * simultaneously two structs with the same name, even if they are
     * in different compiled programs.  There is no way to invalidate the cache.
     * So we decorate each struct with an additional id, which comes from the
     * type-system. */
    public final int id;

    /** Do not call this directly, invoke instead typeFactory.createRelStruct */
    public RelStruct(int id, SqlIdentifier typeName, List<RelDataTypeField> fields, boolean nullable) {
        super(StructKind.FULLY_QUALIFIED, fields, nullable);
        this.typeName = typeName;
        this.id = id;
        this.computeDigest();
    }

    public String getFullTypeString() {
        return Objects.requireNonNull(this.digest);
    }

    @Override
    public String toString() {
        return this.typeName.toString();
    }

    protected void computeDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.typeName);
        sb.append(this.id);
        if (!this.isNullable()) {
            sb.append(" NOT NULL");
        }

        this.digest = sb.toString();
    }
}
