package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;

import java.util.ArrayList;
import java.util.List;

/** Use static values when possible.  If 'declare' is true, creates declarations for static values
 * and replaces their uses with PathExpressions that refer to the declarations. */
public class LazyStatics extends InnerRewriteVisitor {
    /** A reference to the declaration for each string literal */
    final List<Pair<DBSPStringLiteral, DBSPPathExpression>> canonical;
    public final List<DBSPStaticItem> newDeclarations;
    /** If true, create declarations, otherwise create just static expressions */
    final boolean declare;

    public LazyStatics(DBSPCompiler compiler, boolean declare) {
        super(compiler, false);
        this.canonical = new ArrayList<>();
        this.newDeclarations = new ArrayList<>();
        this.declare = declare;
    }

    @Override
    public VisitDecision preorder(DBSPMapExpression expression) {
        if (!expression.isConstant()) {
            return super.preorder(expression);
        }
        // No need to recurse; just do this expression
        DBSPExpression result;
        DBSPStaticExpression stat = new DBSPStaticExpression(expression.getNode(), expression);
        if (this.declare) {
            DBSPStaticItem item = new DBSPStaticItem(stat);
            this.newDeclarations.add(item);
            result = item.getReference();
        } else {
            result = stat;
        }
        this.map(expression, result.applyClone());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral expression) {
        DBSPExpression canonical = null;
        for (var e: this.canonical) {
            if (expression.getType().sameType(e.getKey().getType()) &&
                    expression.sameValue(e.getKey())) {
                 canonical = e.getValue();
                 break;
            }
        }

        if (canonical == null) {
            DBSPStaticExpression stat = new DBSPStaticExpression(expression.getNode(), expression);
            if (this.declare) {
                DBSPStaticItem item = new DBSPStaticItem(stat);
                this.newDeclarations.add(item);
                DBSPPathExpression path = item.getReference();
                canonical = path;
                this.canonical.add(new Pair<>(expression, path));
            } else {
                canonical = stat;
            }
        }
        this.map(expression, canonical.applyClone());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStaticExpression expression) {
        if (this.declare) {
            DBSPStaticItem item = new DBSPStaticItem(expression);
            this.newDeclarations.add(item);
            DBSPExpression result = item.getReference();
            this.map(expression, result);
        } else {
            this.map(expression, expression);
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPArrayExpression expression) {
        if (!expression.isConstant()) {
            return super.preorder(expression);
        }
        // No need to recurse; just do this expression
        DBSPStaticExpression stat = new DBSPStaticExpression(expression.getNode(), expression);
        DBSPExpression result;
        if (this.declare) {
            if (expression.getElementType().code == DBSPTypeCode.ANY) {
                throw new CompilationError("Could not infer a type for array elements; " +
                        "please specify it using CAST(array AS X ARRAY)", expression.getNode());
            }
            DBSPStaticItem item = new DBSPStaticItem(stat);
            this.newDeclarations.add(item);
            result = item.getReference();
        } else {
            result = stat;
        }
        this.map(expression, result.applyClone());
        return VisitDecision.STOP;
    }
}
