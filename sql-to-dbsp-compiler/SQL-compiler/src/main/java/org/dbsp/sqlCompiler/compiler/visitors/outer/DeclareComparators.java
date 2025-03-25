package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEqualityComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPComparatorItem;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Collects all {@link DBSPComparatorExpression} that appear in some
 * {@link DBSPCustomOrdExpression} and replaces then with declarations
 * and references to these declarations.  Collects all new declarations in
 * the newDeclarations list. */
public class DeclareComparators extends InnerRewriteVisitor {
    final Map<DBSPExpression, DBSPPathExpression> done = new HashMap<>();
    public final List<DBSPComparatorItem> newDeclarations = new ArrayList<>();

    public DeclareComparators(DBSPCompiler compiler) {
        super(compiler, false);
    }

    DBSPPathExpression process(DBSPComparatorExpression expression) {
        // Some comparators can be reused.
        DBSPPathExpression path = this.done.get(expression);
        if (path == null) {
            DBSPComparatorItem item = new DBSPComparatorItem(expression);
            this.newDeclarations.add(item);
            path = item.getReference();
            Utilities.putNew(this.done, expression, path);
        }
        return path;
    }

    @Override
    public VisitDecision preorder(DBSPEqualityComparatorExpression expression) {
        // Do NOT replace the comparator in this expression.
        this.map(expression, expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDirectComparatorExpression expression) {
        DBSPPathExpression path = this.process(expression);
        this.map(expression, path);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNoComparatorExpression expression) {
        DBSPPathExpression path = this.process(expression);
        this.map(expression, path);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldComparatorExpression expression) {
        DBSPPathExpression path = this.process(expression);
        this.map(expression, path);
        return VisitDecision.STOP;
    }
}
