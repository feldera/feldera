package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlValidator;

import static org.apache.calcite.util.Static.RESOURCE;

/** Typecheck a generic Lambda when the argument types are known */
public class GenericLambdaTypeChecker implements SqlSingleOperandTypeChecker {
    private final String signatures;
    private final RelDataType[] argumentTypes;

    GenericLambdaTypeChecker(String signatures, RelDataType... argumentTypes) {
        this.signatures = signatures;
        this.argumentTypes = argumentTypes;
    }

    @Override
    public boolean checkSingleOperandType(
            SqlCallBinding callBinding, SqlNode operand,
            int iFormalOperand, boolean throwOnFailure) {
        // Code copied from LambdaRelOperandTypeChecker
        if (!(operand instanceof SqlLambda lambdaExpr)
                || lambdaExpr.getParameters().size() != this.argumentTypes.length) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }

        if (SqlUtil.isNullLiteral(lambdaExpr.getExpression(), false)) {
            throw callBinding.getValidator().newValidationError(lambdaExpr.getExpression(),
                    RESOURCE.nullIllegal());
        }

        // Replace the parameter types in the lambda expression.
        final SqlValidator validator = callBinding.getValidator();
        final SqlLambdaScope scope = (SqlLambdaScope) validator.getLambdaScope(lambdaExpr);
        for (int i = 0; i < this.argumentTypes.length; i++) {
            final SqlNode param = lambdaExpr.getParameters().get(i);
            final RelDataType type = this.argumentTypes[i];
            if (type != null) {
                scope.getParameterTypes().put(param.toString(), type);
            }
        }

        lambdaExpr.accept(new TypeRemover(validator));
        // Given the new relDataType, re-validate the lambda expression.
        validator.validateLambda(lambdaExpr);
        return true;
    }

    // This class is unreachable in Calcite, so I made a copy
    protected static class TypeRemover extends SqlBasicVisitor<Void> {
        private final SqlValidator validator;

        protected TypeRemover(SqlValidator validator) {
            this.validator = validator;
        }

        @Override public Void visit(SqlIdentifier id) {
            validator.removeValidatedNodeType(id);
            return super.visit(id);
        }

        @Override public Void visit(SqlCall call) {
            validator.removeValidatedNodeType(call);
            return super.visit(call);
        }
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        return this.signatures;
    }
}
