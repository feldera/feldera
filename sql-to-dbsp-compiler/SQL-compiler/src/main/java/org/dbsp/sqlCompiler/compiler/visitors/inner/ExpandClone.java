package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;

/** Expand t.clone() for t a tuple type into
 * Tup::new(t.0, t.1, ... ). */
public class ExpandClone extends InnerRewriteVisitor {
    public ExpandClone(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPType type = source.getType();
        boolean isRef = type.is(DBSPTypeRef.class);
        if (isRef) {
            type = type.to(DBSPTypeRef.class).type;
        }
        DBSPTypeTupleBase tuple = type.as(DBSPTypeTupleBase.class);
        if (tuple == null) {
            this.map(expression, source.applyCloneIfNeeded());
            return VisitDecision.STOP;
        }
        DBSPExpression[] fields = new DBSPExpression[tuple.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = source.field(i).applyCloneIfNeeded();
        }
        DBSPExpression result = tuple.makeTuple(fields);
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
