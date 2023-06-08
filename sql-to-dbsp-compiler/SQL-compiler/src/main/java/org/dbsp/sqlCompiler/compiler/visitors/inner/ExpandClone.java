package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;

/**
 * Expand t.clone() for t a tuple type into
 * Tuple::new(t.0, t.1, ... ).
 */
public class ExpandClone extends InnerRewriteVisitor {
    public ExpandClone(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        DBSPType type = expression.getNonVoidType();
        DBSPTypeTuple tuple = type.as(DBSPTypeTuple.class);
        if (tuple == null) {
            this.map(expression, expression);
            return VisitDecision.STOP;
        }
        DBSPExpression[] fields = new DBSPExpression[tuple.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = expression.expression.field(i).applyCloneIfNeeded();
        }
        DBSPExpression result = new DBSPTupleExpression(fields);
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
