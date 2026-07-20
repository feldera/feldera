package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.FunctionSqlType;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.dbsp.util.Utilities;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.type.OperandTypes.ARRAY;

/** Calcite-level implementation of the TRANSFORM array function */
class ArrayTransformFunction extends CustomFunctions.NonOptimizedFunction {
    private ArrayTransformFunction() {
        super("TRANSFORM",
                TRANSFORM_INFERENCE,
                TRANSFORM_CHECKER,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                "array#transform", FunctionDocumentation.NO_FILE);
    }

    static final SqlReturnTypeInference TRANSFORM_INFERENCE = new SqlReturnTypeInference() {
        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable RelDataType inferReturnType(
                SqlOperatorBinding opBinding) {
            RelDataType arrayType = opBinding.getOperandType(0);
            RelDataType functionType = opBinding.getOperandType(1);
            Utilities.enforce(functionType instanceof FunctionSqlType);
            FunctionSqlType fType = (FunctionSqlType) functionType;
            RelDataType returnType = fType.getReturnType();
            return new ArraySqlType(returnType, arrayType.isNullable());
        }
    };

    static final SqlOperandTypeChecker TRANSFORM_CHECKER = new SqlOperandTypeChecker() {
        @Override
        public boolean checkOperandTypes(
                SqlCallBinding callBinding,
                boolean throwOnFailure) {
            // The first operand must be an array type
            ARRAY.checkSingleOperandType(callBinding, callBinding.operand(0), 0, throwOnFailure);
            final RelDataType arrayType =
                    SqlTypeUtil.deriveType(callBinding, callBinding.operand(0));
            final RelDataType componentType =
                    requireNonNull(arrayType.getComponentType(), "componentType");

            // The second operand is a function(array_element_type) -> returnType type
            GenericLambdaTypeChecker lambdaChecker =
                    new GenericLambdaTypeChecker("<T> -> <S>", componentType);
            return lambdaChecker.checkSingleOperandType(callBinding, callBinding.operand(1), 1, throwOnFailure);
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(2);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return "TRANSFORM(<ARRAY>, <FUNCTION(ARRAY_ELEMENT_TYPE)->RESULT_TYPE>)";
        }
    };

    // Must follow TRANSFORM_INFERENCE and TRANSFORM_CHECKER: static initializers run in textual order
    static final ArrayTransformFunction INSTANCE = new ArrayTransformFunction();
}
