package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;

import static java.util.Objects.requireNonNull;

/**
 * A variant of OperandTypes.TypeNameChecker which does not extend the interface
 * ImplicitCastOperandTypeChecker.  We do not want to allow implicit casts for these operands.
 */
record ExactTypeNameChecker(SqlTypeName typeName) implements SqlSingleOperandTypeChecker {
    ExactTypeNameChecker(SqlTypeName typeName) {
        this.typeName = requireNonNull(typeName, "typeName");
    }

    @Override
    public boolean checkSingleOperandType(SqlCallBinding callBinding,
                                          SqlNode operand, int iFormalOperand, boolean throwOnFailure) {
        final RelDataType operandType =
                callBinding.getValidator().getValidatedNodeType(operand);
        return operandType.getSqlTypeName() == typeName;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        return opName + "(" + typeName.getSpaceName() + ")";
    }
}
