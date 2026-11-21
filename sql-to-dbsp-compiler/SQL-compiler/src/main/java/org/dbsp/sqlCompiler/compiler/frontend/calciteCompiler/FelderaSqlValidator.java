package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static org.apache.calcite.util.Static.RESOURCE;

/** Located diagnostics for invalid operands that Calcite otherwise dereferences. */
public final class FelderaSqlValidator extends SqlValidatorImpl {
    public FelderaSqlValidator(SqlOperatorTable operators, SqlValidatorCatalogReader catalog,
                               RelDataTypeFactory types, Config config) {
        super(operators, catalog, types, config);
    }

    @Override
    public RelDataType deriveType(SqlValidatorScope scope, SqlNode expression) {
        if (expression instanceof SqlCall call && call.getKind() == SqlKind.ITEM) {
            RelDataType collection = this.deriveType(scope, call.operand(0));
            if (SqlTypeUtil.isRow(collection)) {
                RelDataType index = this.deriveType(scope, call.operand(1));
                if (SqlTypeUtil.isString(index)
                        && new SqlCallBinding(this, scope, call)
                        .getOperandLiteralValue(1, String.class) == null) {
                    // SqlItemOperator already diagnoses nonliteral integer indices,
                    // but dereferences string indices before reporting that error.
                    throw this.newValidationError(call.operand(1), RESOURCE.illegalRowIndex());
                }
            }
        }
        return super.deriveType(scope, expression);
    }
}
