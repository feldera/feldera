package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import static org.apache.calcite.sql.type.OperandTypes.ARRAY;

/** Calcite-level implementation of the ARRAY_FLATTEN function, which concatenates
 * the arrays of an array of arrays, removing one level of nesting */
class ArrayFlattenFunction extends CustomFunctions.NonOptimizedFunction {
    private ArrayFlattenFunction() {
        super("ARRAY_FLATTEN",
                FLATTEN_INFERENCE,
                FLATTEN_CHECKER,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                "array#array_flatten", FunctionDocumentation.NO_FILE);
    }

    /** True when the element type of {@code type} is unknown. */
    private static boolean elementTypeUnknown(RelDataType type) {
        RelDataType elementType = type.getComponentType();
        return type.getSqlTypeName() == SqlTypeName.ANY
                || elementType == null
                || elementType.getSqlTypeName() == SqlTypeName.ANY;
    }

    /** An array of the inner arrays' element type.  The result is NULL when the
     * outer array is NULL and when an inner array is NULL, so it is nullable if
     * either of them is. */
    static final SqlReturnTypeInference FLATTEN_INFERENCE = opBinding -> {
        RelDataType arrayType = opBinding.getOperandType(0);
        if (elementTypeUnknown(arrayType))
            // Keep the result an array, so that it still type-checks in the
            // enclosing expression
            return new ArraySqlType(arrayType, arrayType.isNullable());
        RelDataType innerType = arrayType.getComponentType();
        if (elementTypeUnknown(innerType))
            return new ArraySqlType(innerType, arrayType.isNullable());
        return new ArraySqlType(innerType.getComponentType(),
                arrayType.isNullable() || innerType.isNullable());
    };

    /** Checks a single ARRAY operand whose elements are also arrays */
    static final SqlOperandTypeChecker FLATTEN_CHECKER = new SqlOperandTypeChecker() {
        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!ARRAY.checkSingleOperandType(callBinding, callBinding.operand(0), 0, throwOnFailure))
                return false;
            RelDataType arrayType = SqlTypeUtil.deriveType(callBinding, callBinding.operand(0));
            if (elementTypeUnknown(arrayType))
                // An untyped NULL literal, whose call returns NULL, or a lambda
                // parameter that the first validation pass types as ANY; the second
                // pass validates the lambda body again with the concrete element type
                return true;
            RelDataType innerType = arrayType.getComponentType();
            if (innerType.getSqlTypeName() == SqlTypeName.ARRAY)
                return true;
            if (throwOnFailure)
                throw callBinding.newValidationSignatureError();
            return false;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(1);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return "ARRAY_FLATTEN(<ANY ARRAY ARRAY>)";
        }
    };

    // Must follow FLATTEN_INFERENCE and FLATTEN_CHECKER: static initializers run in textual order
    static final ArrayFlattenFunction INSTANCE = new ArrayFlattenFunction();
}
