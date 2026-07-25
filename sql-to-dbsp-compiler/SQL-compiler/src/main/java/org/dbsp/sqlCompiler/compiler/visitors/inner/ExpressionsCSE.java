package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
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
import java.util.Collections;
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
                      DBSPExpression expression, Set<IDBSPDeclaration> dependsOn) {
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

    /** Set the replacement for 'expression'.  This looks up the value numbering map,
     * and if there is no associated variable already storing this result, it maps 'expression' to 'result'.
     * @param expression  Expression to rewrite.
     * @param result      Suggested replacement for expression.
     */
    @Override
    protected void map(DBSPExpression expression, DBSPExpression result) {
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
                    canonical.manyUsers(this.numbering)) {
                // Variable is worth creating and expression is used at least twice
                var = new DBSPTypeLazy(expression.getType()).var();
                Utilities.putNew(variables, canonical.expression, var);
                this.assignments.add(new Assignment(owner, var, result, canonical.dependsOn));
            }
            if (var != null)
                result = var.deref().applyCloneIfNeeded();
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

    @Override
    public VisitDecision preorder(DBSPLetExpression node) {
        node.initializer.accept(this);
        DBSPExpression initializer = this.getE(node.initializer);
        // Effects of initializer should be visible while processing consumer
        node.consumer.accept(this);
        DBSPExpression consumer = this.getE(node.consumer);

        List<Assignment> assignments = new ArrayList<>(this.assignments);
        // LetExpressions are created inside-out, so we need to reverse the assignments
        Collections.reverse(assignments);
        this.assignments.clear();
        for (Assignment assign : assignments) {
            if (assign.dependsOn.contains(node)) {
                consumer = new DBSPLetExpression(
                        assign.var, new DBSPLazyExpression(assign.expression), consumer);
            } else {
                // Put it back, we'll insert it later
                this.assignments.add(assign);
            }
        }
        Collections.reverse(this.assignments);
        this.map(node, new DBSPLetExpression(node.variable, initializer, consumer));
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPBlockExpression expression) {
        DBSPExpression last = this.getEN(expression.lastExpression);
        if (this.assignments.isEmpty()) {
            List<DBSPStatement> stats = Linq.map(
                    expression.contents, e -> this.get(e).to(DBSPStatement.class));
            this.map(expression, new DBSPBlockExpression(stats, last));
            return;
        }

        List<DBSPStatement> result = new ArrayList<>();
        // Iterate over original statements
        for (DBSPStatement stat: expression.contents) {
            DBSPStatement repl = this.get(stat).to(DBSPStatement.class);
            // But insert the transformed statements
            result.add(repl);
            if (stat.is(DBSPLetStatement.class)) {
                // Add all statements that depend on stat
                DBSPLetStatement let = stat.to(DBSPLetStatement.class);
                List<Assignment> assignments = new ArrayList<>(this.assignments);
                this.assignments.clear();
                for (Assignment assign : assignments) {
                    if (assign.dependsOn.contains(let)) {
                        var add = new DBSPLetStatement(
                                assign.var.variable, new DBSPLazyExpression(assign.expression));
                        result.add(add);
                    } else {
                        this.assignments.add(assign);
                    }
                }
            }
        }
        // Put back in the list of available assignments
        this.map(expression, new DBSPBlockExpression(result, last));
    }

    @Override
    public void postorder(DBSPClosureExpression node) {
        boolean outer = this.context.isEmpty();
        DBSPExpression translation = this.getE(node.body);
        List<Assignment> assignments = new ArrayList<>(this.assignments);
        this.assignments.clear();
        Collections.reverse(assignments);
        for (Assignment assign : assignments) {
            // Insert in the closure that contains the expression's occurrences;
            // the outermost closure collects everything left over
            boolean insert = outer || assign.owner == node;
            if (!insert) {
                for (DBSPParameter param : node.parameters) {
                    if (assign.dependsOn.contains(param)) {
                        insert = true;
                        break;
                    }
                }
            }
            if (insert) {
                translation = new DBSPLetExpression(
                        assign.var, new DBSPLazyExpression(assign.expression), translation);
            } else {
                // Put it back, we'll insert it later
                this.assignments.add(assign);
            }
        }

        Collections.reverse(this.assignments);
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
