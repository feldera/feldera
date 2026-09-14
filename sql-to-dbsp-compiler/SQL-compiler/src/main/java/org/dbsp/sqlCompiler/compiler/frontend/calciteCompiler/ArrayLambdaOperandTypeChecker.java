package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.FunctionSqlType;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

import static org.apache.calcite.sql.type.OperandTypes.ARRAY;

/** Checks the operands of a function with signature (array, element -&gt; result),
 * such as TRANSFORM and ARRAY_FILTER.  The lambda parameter receives the array
 * element type; the lambda body must have the required result type, when one is given. */
class ArrayLambdaOperandTypeChecker implements SqlOperandTypeChecker {
    private final String signature;
    /** Type the lambda body must have; null accepts any type */
    @Nullable
    private final SqlTypeName resultTypeName;

    /** Checker accepting a lambda with any result type */
    ArrayLambdaOperandTypeChecker(String signature) {
        this(signature, null);
    }

    ArrayLambdaOperandTypeChecker(String signature, @Nullable SqlTypeName resultTypeName) {
        this.signature = signature;
        this.resultTypeName = resultTypeName;
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        // The first operand must be an array type
        if (!ARRAY.checkSingleOperandType(callBinding, callBinding.operand(0), 0, throwOnFailure))
            return false;
        final RelDataType arrayType =
                SqlTypeUtil.deriveType(callBinding, callBinding.operand(0));
        RelDataType componentType = arrayType.getComponentType();
        if (componentType == null) {
            // The ARRAY family check above accepts operands of type ANY and
            // untyped NULL literals, which have no component type.
            if (arrayType.getSqlTypeName() == SqlTypeName.ANY) {
                // Most often a parameter of an enclosing lambda, whose type is not yet
                // inferred (SqlLambdaScope defaults parameters to ANY).  Accept: the
                // enclosing function's checker re-validates the lambda body with
                // concrete parameter types, which runs this checker again.
                return true;
            }
            // Untyped NULL literal: an unknown array whose elements are also NULL;
            // the call returns NULL
            componentType = callBinding.getTypeFactory().createSqlType(SqlTypeName.NULL);
        }

        // The second operand is a function(array_element_type) -> result type
        GenericLambdaTypeChecker lambdaChecker =
                new GenericLambdaTypeChecker(this.signature, componentType);
        if (!lambdaChecker.checkSingleOperandType(callBinding, callBinding.operand(1), 1, throwOnFailure))
            return false;
        return this.checkResultType(callBinding, throwOnFailure);
    }

    private boolean checkResultType(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (this.resultTypeName == null)
            return true;
        RelDataType functionType = SqlTypeUtil.deriveType(callBinding, callBinding.operand(1));
        Utilities.enforce(functionType instanceof FunctionSqlType);
        SqlTypeName actual = ((FunctionSqlType) functionType).getReturnType().getSqlTypeName();
        // ANY: the body refers to a parameter of an enclosing lambda whose type is not
        // yet inferred; the enclosing function's checker re-validates with concrete types
        if (actual == this.resultTypeName || actual == SqlTypeName.ANY)
            return true;
        if (throwOnFailure)
            throw callBinding.newValidationSignatureError();
        return false;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        return this.signature;
    }
}
