package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;

import java.util.ArrayList;
import java.util.List;

/** Use static values when possible.  Creates declarations for static values
 * and replaces their uses with PathExpressions that refer to the declarations. */
public class LazyStatics extends InnerRewriteVisitor {
    /** A reference to the declaration for each string literal */
    final List<Pair<DBSPStringLiteral, DBSPPathExpression>> canonical;
    public final List<DBSPStaticItem> newDeclarations;

    public LazyStatics(DBSPCompiler compiler) {
        super(compiler, false);
        this.canonical = new ArrayList<>();
        this.newDeclarations = new ArrayList<>();
    }

    @Override
    public VisitDecision preorder(DBSPMapExpression expression) {
        if (!expression.isConstant()) {
            return super.preorder(expression);
        }
        // No need to recurse; just do this expression
        DBSPStaticExpression stat = new DBSPStaticExpression(expression.getNode(), expression);
        DBSPStaticItem item = new DBSPStaticItem(stat);
        this.newDeclarations.add(item);
        DBSPExpression result = item.getReference();
        this.map(expression, result.applyClone());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral expression) {
        DBSPPathExpression canonical = null;
        for (var e: this.canonical) {
            if (expression.getType().sameType(e.getKey().getType()) &&
                    expression.sameValue(e.getKey())) {
                 canonical = e.getValue();
                 break;
            }
        }

        if (canonical == null) {
            DBSPStaticExpression stat = new DBSPStaticExpression(expression.getNode(), expression);
            DBSPStaticItem item = new DBSPStaticItem(stat);
            this.newDeclarations.add(item);
            canonical = item.getReference();
            this.canonical.add(new Pair<>(expression, canonical));
        }
        this.map(expression, canonical.applyClone());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPArrayExpression expression) {
        if (!expression.isConstant()) {
            return super.preorder(expression);
        }
        // No need to recurse; just do this expression
        DBSPStaticExpression stat = new DBSPStaticExpression(expression.getNode(), expression);
        DBSPStaticItem item = new DBSPStaticItem(stat);
        this.newDeclarations.add(item);
        DBSPExpression result = item.getReference();
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
