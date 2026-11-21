package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

import java.util.HashMap;

/** Keeps implicit casts consistent with Calcite's cast rules and expression nullability. */
public final class FelderaTypeCoercion extends TypeCoercionImpl {
    private static final SqlTypeCoercionRule IMPLICIT_CASTS = implicitCasts();

    public FelderaTypeCoercion(RelDataTypeFactory factory, SqlValidator validator) {
        super(factory, validator);
    }

    private static SqlTypeCoercionRule implicitCasts() {
        // booleanEquality casts Boolean operands to the other operand's numeric type.
        // Calcite's lenient mapping lists Boolean only for integer targets, despite
        // supporting the other numeric casts when assertions are disabled. Complete
        // that mapping so assertion-enabled builds preserve production behavior.
        var mappings = new HashMap<>(SqlTypeCoercionRule.lenientInstance().getTypeMapping());
        for (SqlTypeName type: new SqlTypeName[] {
                SqlTypeName.DECIMAL, SqlTypeName.FLOAT, SqlTypeName.REAL, SqlTypeName.DOUBLE}) {
            mappings.put(type, ImmutableSet.<SqlTypeName>builder()
                    .addAll(mappings.get(type)).add(SqlTypeName.BOOLEAN).build());
        }
        return SqlTypeCoercionRule.instance(mappings);
    }

    @Override
    protected boolean needToCast(SqlValidatorScope scope, SqlNode node,
                                 RelDataType targetType, SqlTypeMappingRule mapping) {
        // Only operand coercion uses the lenient mapping. Explicit CAST validation
        // and set-operation column coercion retain their existing mapping.
        return super.needToCast(scope, node, targetType,
                mapping == SqlTypeCoercionRule.lenientInstance() ? IMPLICIT_CASTS : mapping);
    }

    @Override
    protected void updateInferredColumnType(SqlValidatorScope scope, SqlNode query,
                                           int columnIndex, RelDataType desiredType) {
        // coerceColumnType preserves the source expression's nullability on the CAST.
        // The enclosing query must report the same type. Using the common target's
        // nullability here makes INTERSECT/EXCEPT validation disagree with their RelNodes.
        RelDataType sourceType = this.validator.deriveType(scope, query)
                .getFieldList().get(columnIndex).getType();
        RelDataType targetType = this.factory.createTypeWithNullability(desiredType, sourceType.isNullable());
        super.updateInferredColumnType(scope, query, columnIndex, targetType);
    }
}
