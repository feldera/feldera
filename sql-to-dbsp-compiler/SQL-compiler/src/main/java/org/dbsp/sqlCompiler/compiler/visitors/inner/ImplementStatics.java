package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLazyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Use static values when possible.  If 'declare' is true, creates declarations for static values
 * and replaces their uses with PathExpressions that refer to the declarations.
 * A variable bound to a lazy cell with a constant value is replaced by a static. */
public class ImplementStatics extends ExpressionTranslator {
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
    final ResolveReferences resolver;
    /** Maps the declaration of each variable bound to a lazy cell with a constant value
     * to a reference to the static that replaces the cell */
    final Map<IDBSPDeclaration, DBSPExpression> constantCells;

    /** Used to renumber indexes in {@link org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression}.
     * These expressions hold an index within the operator they belong to; when made global, the index
     * has to be recomputed. */
    static class RenumberErrorHandlers extends ExpressionTranslator {
        int index;

        @Override
        protected void set(IDBSPInnerNode node, IDBSPInnerNode translation) {
            if (this.translationMap.containsKey(node)) {
                return;
            }
            this.translationMap.putNew(node, translation);
        }

        public RenumberErrorHandlers(DBSPCompiler compiler) {
            super(compiler);
            this.index = 0;
        }

        @Override
        public void postorder(DBSPHandleErrorExpression expression) {
            DBSPExpression source = this.getE(expression.source);
            DBSPExpression result = new DBSPHandleErrorExpression(
                    expression.getNode(), this.index++, expression.runtimeBehavior, source, expression.hasSourcePosition);
            this.map(expression, result);
        }
    }

    final RenumberErrorHandlers renumber;

    public ImplementStatics(DBSPCompiler compiler, boolean declare) {
        super(compiler);
        this.canonical = new ArrayList<>();
        this.newDeclarations = new ArrayList<>();
        this.declare = declare;
        this.renumber = new RenumberErrorHandlers(compiler);
        this.resolver = new ResolveReferences(compiler, false);
        this.constantCells = new HashMap<>();
    }

    @Override
    protected void set(IDBSPInnerNode node, IDBSPInnerNode translation) {
        if (this.translationMap.containsKey(node)) {
            return;
        }
        this.translationMap.putNew(node, translation);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (this.context.isEmpty()) {
            this.constants = new ConstantExpressions(this.compiler);
            this.constants.apply(expression);
            this.resolver.apply(expression);
            this.constantCells.clear();
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
    public VisitDecision preorder(DBSPBaseTupleExpression expression) {
        if (expression.fields == null)
            return VisitDecision.CONTINUE;
        // If any field is a closure, do not generate a static.
        // This is used mostly for handling the CONVERT_MAP binary expression second argument.
        for (DBSPExpression field : expression.fields) {
            if (field.is(DBSPClosureExpression.class)) {
                return VisitDecision.CONTINUE;
            }
        }
        return super.preorder(expression);
    }

    /** True if {@code type} is a reference or a tuple with a reference field. */
    static boolean hasReference(DBSPType type) {
        if (type.is(DBSPTypeRef.class))
            return true;
        DBSPTypeTupleBase tuple = type.as(DBSPTypeTupleBase.class);
        return tuple != null && Linq.any(tuple.tupFields, ImplementStatics::hasReference);
    }

    /** True if a static should hold the value of 'expression': a static can hold the value,
     * and computing it once saves work. */
    boolean shouldBeStatic(DBSPExpression expression) {
        if (expression.is(DBSPClosureExpression.class) ||
                expression.is(DBSPPathExpression.class) ||
                expression.is(DBSPLiteral.class))
            return false;
        // A tuple with a closure field is the second argument of MAP_CONVERT
        DBSPBaseTupleExpression tuple = expression.as(DBSPBaseTupleExpression.class);
        if (tuple != null &&
                (tuple.fields == null || Linq.any(tuple.fields, f -> f.is(DBSPClosureExpression.class))))
            return false;
        // A static cannot hold a reference to a temporary value
        return this.constants != null && this.constants.isConstant(expression) &&
                !hasReference(expression.getType());
    }

    /** Create a static that holds the value of 'expression'; return a reference to the static */
    DBSPExpression makeStatic(DBSPExpression expression) {
        DBSPExpression renumbered = this.renumber.apply(expression).to(DBSPExpression.class);
        String name = DBSPStaticExpression.generateName(renumbered, this.operatorContext, this.compiler);
        DBSPStaticExpression stat = new DBSPStaticExpression(expression.getNode(), renumbered, name);
        if (!this.declare)
            return stat;
        DBSPStaticItem item = new DBSPStaticItem(stat);
        this.newDeclarations.add(item);
        return item.getReference();
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        if (!this.shouldBeStatic(expression))
            return VisitDecision.CONTINUE;
        this.map(expression, this.makeStatic(expression).applyClone());
        return VisitDecision.STOP;
    }

    /** If 'initializer' is a lazy cell of the form {@code LazyCell::new(|| e)}, and a static
     * should hold 'e', make a static for 'e'.  The static then replaces the cell as the value of
     * the variable that 'declaration' declares.
     * @param declaration A let statement or a let expression that binds 'initializer' to a variable.
     * @return True if 'initializer' is such a cell. */
    boolean makeCellStatic(IDBSPDeclaration declaration, @Nullable DBSPExpression initializer) {
        // A node shared by two parents is visited twice
        if (this.constantCells.containsKey(declaration))
            return true;
        if (initializer == null || !initializer.is(DBSPLazyExpression.class))
            return false;
        DBSPExpression value = initializer.to(DBSPLazyExpression.class).expression;
        if (!this.shouldBeStatic(value))
            return false;
        Utilities.putNew(this.constantCells, declaration, this.makeStatic(value));
        return true;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement statement) {
        if (this.makeCellStatic(statement, statement.initializer)) {
            // A static replaces the cell that 'statement' declares;
            // the translation of the enclosing block does not contain 'statement'
            return VisitDecision.STOP;
        }
        return super.preorder(statement);
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression expression) {
        if (this.done(expression) || !this.makeCellStatic(expression, expression.initializer))
            return super.preorder(expression);
        expression.consumer.accept(this);
        this.map(expression, this.getE(expression.consumer));
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPBlockExpression expression) {
        if (this.done(expression))
            return;
        List<DBSPStatement> statements = new ArrayList<>();
        for (DBSPStatement statement : expression.contents) {
            DBSPLetStatement let = statement.as(DBSPLetStatement.class);
            if (let != null && this.constantCells.containsKey(let))
                continue;
            statements.add(this.get(statement).to(DBSPStatement.class));
        }
        DBSPExpression last = this.getEN(expression.lastExpression);
        this.map(expression, new DBSPBlockExpression(statements, last));
    }

    /** If 'expression' is a variable bound to a lazy cell whose value became a static,
     * return a reference to the static; otherwise return null. */
    @Nullable
    DBSPExpression staticOfCell(DBSPExpression expression) {
        DBSPVariablePath var = expression.as(DBSPVariablePath.class);
        if (var == null)
            return null;
        IDBSPDeclaration declaration = this.resolver.reference.get(var);
        if (declaration == null)
            return null;
        return this.constantCells.get(declaration);
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        // (*p) becomes a clone of the static, as any other constant
        DBSPExpression stat = this.staticOfCell(expression.expression);
        if (stat == null)
            return super.preorder(expression);
        this.map(expression, stat.deepCopy().applyClone());
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPVariablePath var) {
        Utilities.enforce(this.staticOfCell(var) == null,
                () -> "Lazy cell " + var + " is used without a dereference");
        super.postorder(var);
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
            String name = DBSPStaticExpression.generateName(expression, this.operatorContext, this.compiler);
            DBSPStaticExpression stat = new DBSPStaticExpression(expression.getNode(), expression, name);
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
