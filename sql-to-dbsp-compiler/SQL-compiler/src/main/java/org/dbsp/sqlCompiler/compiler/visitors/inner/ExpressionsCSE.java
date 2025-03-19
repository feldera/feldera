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
import org.dbsp.sqlCompiler.ir.expression.DBSPLazyCellExpression;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Given information computed by {@link ValueNumbering}, replace common subexpressions with variables
 * in {@link DBSPLetExpression}s */
public class ExpressionsCSE extends ExpressionTranslator {
    final Map<DBSPExpression, ValueNumbering.CanonicalExpression> numbering;
    final Map<DBSPExpression, DBSPVariablePath> cseVariables;
    final List<Assignment> assignments;

    record Assignment(DBSPVariablePath var, DBSPExpression expression, Set<IDBSPDeclaration> dependsOn) {
        Assignment {
            assert var.getType().deref().sameType(expression.getType());
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

    @Override
    void map(DBSPExpression expression, DBSPExpression result) {
        ValueNumbering.CanonicalExpression canon = this.numbering.get(expression);
        assert this.operatorContext != null;
        if (canon != null) {
            DBSPVariablePath var = null;
            if (this.cseVariables.containsKey(canon.expression)) {
                // Variable already exists
                var = Utilities.getExists(this.cseVariables, canon.expression);
            } else if (canon.expression != result &&
                    canon.expensive &&
                    canon.manyUsers(this.numbering)) {
                // Variable is worth creating
                var = new DBSPTypeLazy(expression.getType()).var();
                Utilities.putNew(this.cseVariables, canon.expression, var);
                this.assignments.add(new Assignment(var, result, canon.dependsOn));
            }
            if (var != null)
                result = var.deref().applyCloneIfNeeded();
            // If variable hasn't been created now, use the suggested result
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
    public void postorder(DBSPLetExpression node) {
        DBSPExpression consumer = this.getE(node.consumer);
        List<Assignment> assignments = new ArrayList<>(this.assignments);
        // LetExpressions are created inside-out, so we need to reverse the assignments
        Collections.reverse(assignments);
        this.assignments.clear();
        for (Assignment assign : assignments) {
            if (assign.dependsOn.contains(node)) {
                consumer = new DBSPLetExpression(
                        assign.var, new DBSPLazyCellExpression(assign.expression), consumer);
            } else {
                // Put it back, we'll insert it later
                this.assignments.add(assign);
            }
        }
        Collections.reverse(this.assignments);
        this.map(node, new DBSPLetExpression(node.variable, node.initializer, consumer));
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
                                assign.var.variable, new DBSPLazyCellExpression(assign.expression));
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
            boolean insert = outer;
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
                        assign.var, new DBSPLazyCellExpression(assign.expression), translation);
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
        assert this.assignments.isEmpty()
                : "Unused CSE expressions";
        super.endVisit();
    }
}
