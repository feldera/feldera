package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** Resolves the variable references by pointing each variable reference
 * to a declaration that introduced the variable. */
public class ResolveReferences extends InnerVisitor {
    private final Scopes<String, IDBSPDeclaration> substitutionContext;
    public final ReferenceMap reference;
    /** If true allow "free variables" that have no declarations */
    public final boolean allowFreeVariables;

    public ResolveReferences(DBSPCompiler compiler, boolean allowFreeVariables) {
        super(compiler);
        this.substitutionContext = new Scopes<>();
        this.reference = new ReferenceMap();
        this.allowFreeVariables = allowFreeVariables;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath variable) {
        IDBSPDeclaration declaration = this.substitutionContext.get(variable.variable);
        if (declaration == null) {
            if (!this.allowFreeVariables)
                throw new InternalCompilerError("Could not resolve " + variable);
            else
                return VisitDecision.STOP;
        }
        this.reference.declare(variable, declaration);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPParameter parameter) {
        this.substitutionContext.substitute(parameter.name, parameter);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression block) {
        this.substitutionContext.newContext();
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPBlockExpression block) {
        this.substitutionContext.popContext();
    }

    @Override
    public void postorder(DBSPLetStatement statement) {
        this.substitutionContext.substitute(statement.variable, statement);
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression expression) {
        this.substitutionContext.newContext();
        this.substitutionContext.substitute(expression.variable.variable, expression);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPLetExpression expression) {
        this.substitutionContext.popContext();
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.substitutionContext.newContext();
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        this.substitutionContext.popContext();
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.substitutionContext.clear();
        this.substitutionContext.newContext();
        this.reference.clear();
        super.startVisit(node);
    }

    @Override
    public void endVisit() {
        this.substitutionContext.popContext();
        this.substitutionContext.mustBeEmpty();
        super.endVisit();
    }
}
