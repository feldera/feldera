package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;

/**
 * Resolves the variable references by pointing each
 * to its declaration.
 */
public class ResolveReferences extends InnerVisitor {
    private final SubstitutionContext<IDBSPDeclaration> substitutionContext;
    public final ReferenceMap reference;

    public ResolveReferences(IErrorReporter reporter) {
        super(reporter);
        this.substitutionContext = new SubstitutionContext<>();
        this.reference = new ReferenceMap();
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath variable) {
        IDBSPDeclaration declaration = this.substitutionContext.get(variable.variable);
        if (declaration == null)
            throw new InternalCompilerError("Could not resolve", variable);
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
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.substitutionContext.newContext();
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        this.substitutionContext.popContext();
    }

    @Override
    public void startVisit() {
        this.substitutionContext.clear();
        this.substitutionContext.newContext();
        this.reference.clear();
        super.startVisit();
    }

    @Override
    public void endVisit() {
        this.substitutionContext.popContext();
        this.substitutionContext.mustBeEmpty();
        super.endVisit();
    }
}
