package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.FunctionSqlType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.dbsp.util.Utilities;

/** Calcite-level implementation of the TRANSFORM array function */
class ArrayTransformFunction extends CustomFunctions.NonOptimizedFunction {
    private ArrayTransformFunction() {
        super("TRANSFORM",
                TRANSFORM_INFERENCE,
                new ArrayLambdaOperandTypeChecker(
                        "TRANSFORM(<ARRAY>, <FUNCTION(ARRAY_ELEMENT_TYPE)->RESULT_TYPE>)"),
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                "array#transform", FunctionDocumentation.NO_FILE);
    }

    /** An array of the lambda result type, nullable when the input array is */
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

    // Must follow TRANSFORM_INFERENCE: static initializers run in textual order
    static final ArrayTransformFunction INSTANCE = new ArrayTransformFunction();
}
