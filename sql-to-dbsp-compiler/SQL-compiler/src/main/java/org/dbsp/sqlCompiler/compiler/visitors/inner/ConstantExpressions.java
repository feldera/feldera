package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEnumValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQuestionExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.Logger;

import java.util.HashSet;

/** Builds a list with all expressions that are "constant" */
public class ConstantExpressions extends InnerVisitor {
    final HashSet<Long> constants;

    public ConstantExpressions(DBSPCompiler compiler) {
        super(compiler);
        this.constants = new HashSet<>();
    }

    public boolean isConstant(DBSPExpression expression) {
        return this.constants.contains(expression.id);
    }

    boolean allConstant(DBSPExpression... expressions) {
        for (var e: expressions)
            if (!this.isConstant(e))
                return false;
        return true;
    }

    void setConstant(DBSPExpression expression) {
        Logger.INSTANCE.belowLevel(this, 1)
                .appendSupplier(expression::toString)
                .append(" is constant")
                .newline();
        this.constants.add(expression.getId());
    }

    @Override
    public void postorder(DBSPLiteral literal) {
        this.setConstant(literal);
    }

    @Override
    public void postorder(DBSPUnaryExpression expression) {
        if (this.isConstant(expression.source))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPIsNullExpression expression) {
        if (this.isConstant(expression.expression))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPQuestionExpression expression) {
        if (this.isConstant(expression.source))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPSomeExpression expression) {
        if (this.isConstant(expression.expression))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPQualifyTypeExpression expression) {
        if (this.isConstant(expression.expression))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPPathExpression expression) {
        this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPUnwrapExpression expression) {
        if (this.isConstant(expression.expression))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        if (this.isConstant(expression.left) && this.isConstant(expression.right))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPApplyMethodExpression expression) {
        if (this.isConstant(expression.self) && this.allConstant(expression.arguments))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        if (this.allConstant(expression.arguments))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPBaseTupleExpression expression) {
        if (expression.fields == null || this.allConstant(expression.fields))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        ResolveReferences resolve = new ResolveReferences(this.compiler, true);
        resolve.apply(expression);
        if (!resolve.freeVariablesFound)
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPCloneExpression expression) {
        if (this.isConstant(expression.expression))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPBorrowExpression expression) {
        if (this.isConstant(expression.expression))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPDerefExpression expression) {
        if (this.isConstant(expression.expression))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPEnumValue expression) {
        this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        if (this.isConstant(expression.source))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPArrayExpression expression) {
        if (expression.getElementType().code == DBSPTypeCode.ANY) {
            throw new CompilationError("Could not infer a type for array elements; " +
                    "please specify it using CAST(array AS X ARRAY)", expression.getNode());
        }
        if (expression.isConstant())
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPMapExpression expression) {
        if (expression.isConstant())
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPConstructorExpression expression) {
        if (this.isConstant(expression.function) && this.allConstant(expression.arguments))
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPGeoPointConstructor expression) {
        if (expression.isConstant())
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPIfExpression expression) {
        if (this.isConstant(expression.condition) &&
                this.isConstant(expression.positive) &&
                (expression.negative == null || this.isConstant(expression.negative))) {
            this.setConstant(expression);
        }
    }

    @Override
    public void postorder(DBSPZSetExpression expression) {
        if (expression.isConstant())
            this.setConstant(expression);
    }

    @Override
    public void postorder(DBSPIndexedZSetExpression expression) {
        if (expression.isConstant())
            this.setConstant(expression);
    }
}
