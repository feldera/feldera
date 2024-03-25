package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Represents the type of a user-defined struct */
public class RelStruct extends RelRecordType {
    /** Name of the struct type */
    public final SqlIdentifier typeName;

    public RelStruct(SqlIdentifier typeName, List<RelDataTypeField> fields, boolean nullable) {
        super(StructKind.FULLY_QUALIFIED, fields, nullable);
        this.typeName = typeName;
    }

    @Override
    protected void generateTypeString(
            StringBuilder sb,
            boolean withDetail) {
        sb.append(this.typeName);
        sb.append("(");
        for (Ord<RelDataTypeField> ord : Ord.zip(requireNonNull(fieldList, "fieldList"))) {
            if (ord.i > 0) {
                sb.append(", ");
            }
            RelDataTypeField field = ord.e;
            if (withDetail) {
                sb.append(field.getType().getFullTypeString());
            } else {
                sb.append(field.getType().toString());
            }
            sb.append(" ");
            sb.append(field.getName());
        }
        sb.append(")");
    }
}
