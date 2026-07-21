package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

/** Calcite-level implementation of the VARIANT_MAP and VARIANT_DEEP_MAP functions.
 * Both take (variant, (label, value) -&gt; expression) and build a result
 * isomorphic to the input, with values replaced by the lambda's result. */
class VariantMapFunction extends CustomFunctions.NonOptimizedFunction {
    private VariantMapFunction(String name, SqlTypeName labelTypeName, String documentation) {
        super(name,
                MAP_INFERENCE,
                makeChecker(name, labelTypeName),
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                documentation, FunctionDocumentation.NO_FILE);
    }

    /** Always nullable: mapping a non-map variant can produce NULL */
    static final SqlReturnTypeInference MAP_INFERENCE = opBinding -> {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARIANT), true);
    };

    /** Checks (VARIANT, (labelTypeName, VARIANT) -&gt; any type) operands */
    static SqlOperandTypeChecker makeChecker(String name, SqlTypeName labelTypeName) {
        String signature = name + "(<VARIANT>, <FUNCTION(" +
                labelTypeName + ", VARIANT)-><ANY>>)";
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
                // The lambda may return any type; the result is converted to VARIANT
                return lambdaChecker.checkSingleOperandType(
                        callBinding, callBinding.operand(1), 1, throwOnFailure);
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

    static final VariantMapFunction INSTANCE = new VariantMapFunction(
            "VARIANT_MAP", SqlTypeName.VARIANT, "json#variant_map");
    static final VariantMapFunction DEEP = new VariantMapFunction(
            "VARIANT_DEEP_MAP", SqlTypeName.VARCHAR, "json#variant_deep_map");
}
