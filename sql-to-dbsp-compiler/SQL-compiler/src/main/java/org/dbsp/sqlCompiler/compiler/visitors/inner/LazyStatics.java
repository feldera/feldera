package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Use static values when possible.  If 'declare' is true, creates declarations for static values
 * and replaces their uses with PathExpressions that refer to the declarations. */
public class LazyStatics extends ExpressionTranslator {
    // Yes, we extend ExpressionTranslator, but we override some preorder methods.
    // This is counterintuitive, but we want the default translator behavior,
    // except when we find a constant expression, and then we replace it immediately in preorder.

    /** A reference to the declaration for each literal */
    final List<Pair<DBSPLiteral, DBSPPathExpression>> canonical;
    public final List<DBSPStaticItem> newDeclarations;
    /** If true, create declarations, otherwise create just static expressions */
    final boolean declare;
    @Nullable
    ConstantExpressions constants = null;

    public LazyStatics(DBSPCompiler compiler, boolean declare) {
        super(compiler);
        this.canonical = new ArrayList<>();
        this.newDeclarations = new ArrayList<>();
        this.declare = declare;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (this.context.isEmpty()) {
            this.constants = new ConstantExpressions(this.compiler);
            this.constants.apply(expression);
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPPathExpression expression) {
        // Do not convert into a static
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPLiteral literal) {
        // Do not convert into a static
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPBorrowExpression literal) {
        // Do not convert into a static
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPBaseTupleExpression expression) {
        if (expression.fields == null)
            return VisitDecision.CONTINUE;
        return super.preorder(expression);
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        if (this.constants == null || !this.constants.isConstant(expression)) {
            return VisitDecision.CONTINUE;
        }

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

    VisitDecision replaceLiteral(DBSPLiteral expression) {
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
    public VisitDecision preorder(DBSPStringLiteral expression) {
        return this.replaceLiteral(expression);
    }

    @Override
    public VisitDecision preorder(DBSPDecimalLiteral expression) {
        return this.replaceLiteral(expression);
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
}
