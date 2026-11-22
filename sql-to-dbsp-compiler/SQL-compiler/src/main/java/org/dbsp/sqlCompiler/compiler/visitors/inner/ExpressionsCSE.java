package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLazyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeLazy;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Given information computed by {@link ValueNumbering}, replace common subexpressions with variables
 * in {@link DBSPLetExpression}s. */
// Example rewrite:
// |t_1: &Tup3<i32?, i32?, i32?>|
//   Tup2::new((((- *t_1.0) / *t_1.1) * ((- *t_1.0) / *t_1.1)), ((*t_1.2) + 1) + (*t_1.2 + 1))
// is converted to
// |t_1: &Tup3<i32?, i32?, i32?>|
// {
//    let t_2 = LazyCell::new(|| ((- *t_1.0) / *t_1.1));
//    {
//        let t_3 = LazyCell::new(|| (*t_1.2 + 1));
//        Tup2::new((*t_2 * *t_2), (*t_3 + *t_3))
//    }
// }
public class ExpressionsCSE extends ExpressionTranslator {
    final Map<DBSPExpression, ValueNumbering.CanonicalExpression> numbering;
    /** CSE variables, keyed by the innermost closure containing the expression's occurrences.
     * The generated Rust closures capture variables by move, so a variable declared in a
     * closure cannot be shared with (nested) sibling closures; each closure gets its own copy. */
    final Map<DBSPClosureExpression, Map<DBSPExpression, DBSPVariablePath>> cseVariables;
    final List<Assignment> assignments;

    record Assignment(DBSPClosureExpression owner, DBSPVariablePath var,
                      DBSPExpression expression, Set<IDBSPDeclaration> dependsOn) implements RustCircuitPass.LazyBinding {
        Assignment {
            Utilities.enforce(var.getType().deref().sameType(expression.getType()));
        }

        @Override
        public String toString() {
            return "Assignment{" +
                    "var=" + this.var +
                    ", dependsOn=" + Linq.list(Linq.map(this.dependsOn, IDBSPDeclaration::getName)) +
                    ", expression=" + this.expression +
                    '}';
        }
    }

    public ExpressionsCSE(DBSPCompiler compiler, Map<DBSPExpression, ValueNumbering.CanonicalExpression> numbering) {
        super(compiler);
        this.numbering = numbering;
        this.cseVariables = new HashMap<>();
        this.assignments = new ArrayList<>();
    }

    /** The innermost closure that the currently-visited node is nested in,
     * or null if the node is not inside a closure. */
    @Nullable
    DBSPClosureExpression enclosingClosure() {
        for (int i = this.context.size() - 1; i >= 0; i--) {
            IDBSPInnerNode node = this.context.get(i);
            if (node.is(DBSPClosureExpression.class))
                return node.to(DBSPClosureExpression.class);
        }
        return null;
    }

    /** True for the expressions that CSE does not replace with variables; they only wrap or group values */
    static boolean neverShared(DBSPExpression expression) {
        return expression.is(DBSPBorrowExpression.class)
                || expression.is(DBSPCloneExpression.class)
                || expression.is(DBSPDerefExpression.class)
                || expression.is(DBSPTupleExpression.class)
                || expression.is(DBSPRawTupleExpression.class);
    }

    /** Set the replacement for 'expression'.  This looks up the value numbering map,
     * and if there is no associated variable already storing this result, it maps 'expression' to 'result'.
     * @param expression  Expression to rewrite.
     * @param result      Suggested replacement for expression.
     */
    @Override
    protected void map(DBSPExpression expression, DBSPExpression result) {
        Utilities.enforce(!neverShared(expression));
        ValueNumbering.CanonicalExpression canonical = this.numbering.get(expression);
        DBSPClosureExpression owner = this.enclosingClosure();
        if (canonical != null && owner != null) {
            Map<DBSPExpression, DBSPVariablePath> variables =
                    this.cseVariables.computeIfAbsent(owner, o -> new HashMap<>());
            DBSPVariablePath var = null;
            if (variables.containsKey(canonical.expression)) {
                // Variable already exists
                var = Utilities.getExists(variables, canonical.expression);
            } else if (canonical.expression != result &&
                    canonical.expensive &&
                    canonical.manyUsers(this.numbering, owner)) {
                // Variable is worth creating and expression is used at least twice in 'owner'
                var = new DBSPTypeLazy(expression.getType()).var();
                Utilities.putNew(variables, canonical.expression, var);
                this.assignments.add(new Assignment(owner, var, result, canonical.dependsOn));
            }
            if (var != null) {
                // An immutable borrow of the shared value needs no clone
                IDBSPInnerNode parent = this.getParent();
                DBSPBorrowExpression borrow = parent == null ? null : parent.as(DBSPBorrowExpression.class);
                if (borrow != null && !borrow.mut)
                    result = var.deref();
                else
                    result = var.deref().applyCloneIfNeeded();
            }
            // If variable hasn't been created now, use the 'result' received as parameter
        }
        if (!this.translationMap.containsKey(expression)) {
            super.map(expression, result);
        }
    }

    @Override
    public void postorder(DBSPCloneExpression node) {
        DBSPExpression expression = this.getE(node.expression);
        // super.map does NOT CSE
        super.map(node, expression.applyClone());
    }

    @Override
    public void postorder(DBSPBorrowExpression node) {
        DBSPExpression expression = this.getE(node.expression);
        // super.map does NOT CSE
        super.map(node, expression.borrow(node.mut));
    }

    @Override
    public void postorder(DBSPRawTupleExpression node) {
        if (node.fields != null) {
            DBSPExpression[] fields = this.get(node.fields);
            // super.map does NOT CSE
            super.map(node, new DBSPRawTupleExpression(node.getNode(), node.getType().to(DBSPTypeRawTuple.class), fields));
        } else {
            // super.map does NOT CSE
            super.map(node, node.getType().none());
        }
    }

    @Override
    public void postorder(DBSPTupleExpression node) {
        if (node.fields != null) {
            DBSPExpression[] fields = this.get(node.fields);
            // super.map does NOT CSE
            super.map(node, new DBSPTupleExpression(node.getNode(), node.getType().to(DBSPTypeTuple.class), fields));
        } else {
            // super.map does NOT CSE
            super.map(node, node.getType().none());
        }
    }

    @Override
    public void postorder(DBSPDerefExpression node) {
        DBSPExpression expression = this.getE(node.expression);
        // super.map does NOT CSE
        super.map(node, expression.deref());
    }

    @Override
    public VisitDecision preorder(DBSPAssignmentExpression node) {
        throw new InternalCompilerError("CSE does not work on expressions that use assignment");
    }

    /** Group the bindings consumed at this scope by the declaration after which
     * they are inserted. Bindings at closure entry use the closure itself as the key. */
    private Map<Long, List<Assignment>> takeBindings(List<? extends IDBSPDeclaration> declarations,
            @Nullable DBSPClosureExpression closure) {
        if (this.assignments.isEmpty() || (closure == null && declarations.isEmpty()))
            return Map.of();
        boolean outermost = closure != null && this.context.isEmpty();
        Integer[] placements;
        if (this.compiler.options.ioOptions.rustOptimizerPasses) {
            var scope = new RustCircuitPass.BindingScope(
                    declarations.stream().map(IDBSPDeclaration::getId).toList(),
                    closure == null ? null : closure.id, outermost);
            var pending = this.assignments.stream().map(a -> new RustCircuitPass.PendingBinding(
                    a.owner.id, a.dependsOn.stream().map(IDBSPDeclaration::getId).toList())).toList();
            placements = RustCircuitPass.bindingPlacements(scope, pending);
        } else {
            placements = new Integer[this.assignments.size()];
            for (int i = 0; i < this.assignments.size(); i++) {
                Assignment a = this.assignments.get(i);
                if (closure != null && (outermost || a.owner == closure)) {
                    placements[i] = 0;
                    continue;
                }
                for (int j = declarations.size() - 1; j >= 0; j--) {
                    if (a.dependsOn.contains(declarations.get(j))) {
                        placements[i] = closure == null ? j : 0;
                        break;
                    }
                }
            }
        }
        Map<Long, List<Assignment>> inserted = new HashMap<>();
        List<Assignment> remaining = new ArrayList<>();
        for (int i = 0; i < placements.length; i++) {
            Integer position = placements[i];
            Assignment a = this.assignments.get(i);
            if (position == null) {
                remaining.add(a);
            } else {
                long anchor = closure == null ? declarations.get(position).getId() : closure.id;
                inserted.computeIfAbsent(anchor, ignored -> new ArrayList<>()).add(a);
            }
        }
        this.assignments.clear();
        this.assignments.addAll(remaining);
        return inserted;
    }

    /** Earlier bindings enclose later ones, which may refer to them. */
    private DBSPExpression bind(List<Assignment> bindings, DBSPExpression body) {
        if (!bindings.isEmpty() && this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.bindLazyExpression(this.compiler, bindings, body);
        for (int i = bindings.size() - 1; i >= 0; i--) {
            Assignment assignment = bindings.get(i);
            body = new DBSPLetExpression(assignment.var, new DBSPLazyExpression(assignment.expression), body);
        }
        return body;
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression node) {
        node.initializer.accept(this);
        DBSPExpression initializer = this.getE(node.initializer);
        node.consumer.accept(this);
        DBSPExpression consumer = this.getE(node.consumer);
        var inserted = this.takeBindings(List.of(node), null);
        consumer = bind(inserted.getOrDefault(node.id, List.of()), consumer);
        this.map(node, new DBSPLetExpression(node.variable, initializer, consumer));
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPBlockExpression expression) {
        DBSPExpression last = this.getEN(expression.lastExpression);
        var declarations = this.assignments.isEmpty() ? List.<DBSPLetStatement>of() :
                expression.contents.stream().filter(DBSPLetStatement.class::isInstance)
                        .map(DBSPLetStatement.class::cast).toList();
        var inserted = this.takeBindings(declarations, null);
        List<DBSPStatement> result = new ArrayList<>();
        for (DBSPStatement stat : expression.contents) {
            result.add(this.get(stat).to(DBSPStatement.class));
            var bindings = inserted.get(stat.getId());
            if (bindings == null)
                continue;
            if (this.compiler.options.ioOptions.rustOptimizerPasses)
                result.addAll(RustCircuitPass.lazyStatements(this.compiler, bindings));
            else
                for (Assignment a : bindings)
                    result.add(new DBSPLetStatement(a.var.variable, new DBSPLazyExpression(a.expression)));
        }
        this.map(expression, new DBSPBlockExpression(result, last));
    }

    @Override
    public void postorder(DBSPClosureExpression node) {
        var inserted = this.takeBindings(List.of(node.parameters), node);
        DBSPExpression translation = bind(inserted.getOrDefault(node.id, List.of()), this.getE(node.body));
        DBSPExpression result = new DBSPClosureExpression(node.getNode(), translation, node.parameters);
        this.map(node, result);
        if (!node.sameFields(result)) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("CSE rewrote").newline()
                    .appendSupplier(node::toString).newline()
                    .append("to").newline()
                    .appendSupplier(result::toString).newline();
        }
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.assignments.clear();
        this.cseVariables.clear();
        super.startVisit(node);
    }

    @Override
    public void endVisit() {
        Utilities.enforce(this.assignments.isEmpty(), () -> "Unused CSE expressions");
        super.endVisit();
    }
}
