package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.FunctionSqlType;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/** Calcite-level implementation of the VARIANT_FILTER and VARIANT_DEEP_FILTER functions.
 * Both take (variant, (label, value) -&gt; predicate) and return a VARIANT with
 * the items of the input variant for which the predicate is true. */
class VariantFilterFunction extends CustomFunctions.NonOptimizedFunction {
    private VariantFilterFunction(String name, SqlTypeName labelTypeName, String documentation) {
        super(name,
                FILTER_INFERENCE,
                makeChecker(name, labelTypeName),
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                documentation, FunctionDocumentation.NO_FILE);
    }

    /** Always nullable: a dropped non-map variant produces NULL */
    static final SqlReturnTypeInference FILTER_INFERENCE = opBinding -> {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARIANT), true);
    };

    /** Checks (VARIANT, (labelTypeName, VARIANT) -&gt; BOOLEAN) operands */
    static SqlOperandTypeChecker makeChecker(String name, SqlTypeName labelTypeName) {
        String signature = name + "(<VARIANT>, <FUNCTION(" +
                labelTypeName + ", VARIANT)-><BOOLEAN>>)";
        return new SqlOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
                if (!OperandTypes.VARIANT.checkSingleOperandType(
                        callBinding, callBinding.operand(0), 0, throwOnFailure))
                    return false;

                RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                // The label is NULL when the variant does not hold a map
                RelDataType labelType = typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(labelTypeName), true);
                RelDataType valueType = typeFactory.createSqlType(SqlTypeName.VARIANT);
                GenericLambdaTypeChecker lambdaChecker =
                        new GenericLambdaTypeChecker(signature, labelType, valueType);
                if (!lambdaChecker.checkSingleOperandType(
                        callBinding, callBinding.operand(1), 1, throwOnFailure))
                    return false;

                RelDataType functionType = SqlTypeUtil.deriveType(callBinding, callBinding.operand(1));
                if (!(functionType instanceof FunctionSqlType fType)
                        || fType.getReturnType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
                    if (throwOnFailure)
                        throw callBinding.newValidationSignatureError();
                    return false;
                }
                return true;
            }

            @Override
            public SqlOperandCountRange getOperandCountRange() {
                return SqlOperandCountRanges.of(2);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
                return signature;
            }
        };
    }

    static final VariantFilterFunction INSTANCE = new VariantFilterFunction(
            "VARIANT_FILTER", SqlTypeName.VARIANT, "json#variant_filter");
    static final VariantFilterFunction DEEP = new VariantFilterFunction(
            "VARIANT_DEEP_FILTER", SqlTypeName.VARCHAR, "json#variant_deep_filter");
}
