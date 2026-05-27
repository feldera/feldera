package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Simplifies expressions based on their context within a conditional;
 * if (a) { ... a ... } simplifies the second occurrence of 'a' to 'true'. */
public class SimplifyConditionals implements IRTransform {
    final ContainsIf containsIf;
    final FindConstantExpressions findConstants;
    final ReplaceConstantExpressions replaceConstants;
    final DBSPCompiler compiler;
    
    public SimplifyConditionals(DBSPCompiler compiler) {
        this.compiler = compiler;
        this.containsIf = new ContainsIf(compiler);
        this.findConstants = new FindConstantExpressions(compiler);
        this.replaceConstants = new ReplaceConstantExpressions(compiler, this.findConstants.constants);
    }
    
    @Override
    public void setOperatorContext(DBSPOperator operator) {
        this.containsIf.setOperatorContext(operator);
        this.findConstants.setOperatorContext(operator);
        this.replaceConstants.setOperatorContext(operator);
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        if (!node.is(DBSPExpression.class))
            return node;
        this.containsIf.apply(node);
        if (this.containsIf.found()) {
            var tree = node.to(DBSPExpression.class).ensureTree(this.compiler);
            this.findConstants.apply(tree);
            return this.replaceConstants.apply(tree);
        } else {
            return node;
        }
    }

    record ExpressionWithContext(DBSPExpression expression, Scopes<String, IDBSPDeclaration> scopes) {}

    /** Check if an expression contains a {@link DBSPIfExpression} subexpression. */
    static class ContainsIf extends InnerVisitor {
        boolean found = false;

        ContainsIf(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPIfExpression expression) {
            this.found = true;
            return VisitDecision.STOP;
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            super.startVisit(node);
            this.found = false;
        }

        public boolean found() {
            return this.found;
        }
    }

    /** Symbolically evaluate a program and detect Boolean expressions that are constant
     * because they are guarded by an if expression.  E.g., in `if (a) { ... a ... }`
     * the second `a` is `true`.  Requires analyzed expressions to be trees. */
    static class FindConstantExpressions extends SymbolicInterpreter<ExpressionWithContext> {
        // Output result is assembled here: for each expression its actual value
        final Map<DBSPExpression, Boolean> constants;
        // Expressions that are currently known to be constant
        final Map<ExpressionWithContext, Boolean> state;
        // Scope of expression currently being analyzed
        final Scopes<String, IDBSPDeclaration> substitutionContext;
        // Maps each declaration to itself; needed by the EquivalenceContext
        final Substitution<IDBSPDeclaration, IDBSPDeclaration> identitySubstitution;

        public FindConstantExpressions(DBSPCompiler compiler) {
            super(compiler);
            this.constants = new HashMap<>();
            this.state = new HashMap<>();
            this.substitutionContext = new Scopes<>();
            this.identitySubstitution = new Substitution<>();
        }

        @Override
        public VisitDecision preorder(DBSPIfExpression expression) {
            expression.condition.accept(this);
            ExpressionWithContext conditionValue = this.get(expression.condition);
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("Entering positive branch ")
                    .appendSupplier(expression.condition::toString);
            this.state.put(conditionValue, true);
            expression.positive.accept(this);
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("Leaving positive branch ")
                    .appendSupplier(expression.condition::toString);
            this.state.remove(conditionValue);

            if (expression.negative != null) {
                Logger.INSTANCE.belowLevel(this, 2)
                        .append("Entering negative branch ")
                        .appendSupplier(expression.condition::toString);
                this.state.put(conditionValue, false);
                expression.negative.accept(this);
                Logger.INSTANCE.belowLevel(this, 2)
                        .append("Leaving negative branch ")
                        .appendSupplier(expression.condition::toString);
                this.state.remove(conditionValue);
            }

            if (this.maybeGet(expression) == null) {
                ExpressionWithContext result = new ExpressionWithContext(expression, this.substitutionContext.clone());
                this.set(expression, result);
            }
            return VisitDecision.STOP;
        }

        /** Check if an expression can be substituted with a constant */
        void checkIfConstant(DBSPExpression expression) {
            if (this.constants.containsKey(expression))
                // We already know it's a constant
                return;
            if (expression.getType().is(DBSPTypeBool.class) && !expression.getType().mayBeNull) {
                // Compare with all constant expressions in the current state
                for (var exp: this.state.keySet()) {
                    EquivalenceContext context = new EquivalenceContext(
                            this.substitutionContext, exp.scopes, this.identitySubstitution);
                    if (context.equivalent(expression, exp.expression)) {
                        boolean value = this.state.get(exp);
                        Logger.INSTANCE.belowLevel(this, 1)
                                .append("Detected constant expression ")
                                .appendSupplier(expression::toString)
                                .append(" = ")
                                .append(value);
                        Utilities.putNew(this.constants, expression, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void postorder(DBSPExpression expression) {
            if (this.maybeGet(expression) != null)
                // We already have a value for this expression
                return;
            this.checkIfConstant(expression);
            ExpressionWithContext result = new ExpressionWithContext(expression, this.substitutionContext.clone());
            this.set(expression, result);
        }

        // Most of the methods below are from ResolveReferences; Java has no multiple inheritance,
        // so we need to reimplement them.

        @Override
        public VisitDecision preorder(DBSPVariablePath variable) {
            IDBSPDeclaration declaration = this.substitutionContext.get(variable.variable);
            if (declaration == null) {
                throw new InternalCompilerError("Could not resolve " + variable);
            }
            this.checkIfConstant(variable);
            ExpressionWithContext result = new ExpressionWithContext(variable, this.substitutionContext.clone());
            this.set(variable, result);
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPParameter parameter) {
            this.substitutionContext.substitute(parameter.name, parameter);
            Utilities.putNew(this.identitySubstitution, parameter, parameter);
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
            Utilities.putNew(this.identitySubstitution, statement, statement);
        }

        @Override
        public VisitDecision preorder(DBSPLetExpression expression) {
            // The initialized is evaluated before the new declaration takes place
            expression.initializer.accept(this);
            this.substitutionContext.newContext();
            this.substitutionContext.substitute(expression.variable.variable, expression);
            Utilities.putNew(this.identitySubstitution, expression, expression);
            expression.consumer.accept(this);
            this.substitutionContext.popContext();
            return VisitDecision.STOP;
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
        public void startVisit(IDBSPInnerNode node) {
            this.state.clear();
            this.constants.clear();
            this.identitySubstitution.clear();
            this.substitutionContext.clear();
            this.substitutionContext.newContext();
            super.startVisit(node);
        }

        @Override
        public void endVisit() {
            this.substitutionContext.popContext();
            this.substitutionContext.mustBeEmpty();
            super.endVisit();
        }
    }

    /** Rewrite expressions that are known to be constant booleans replacing them
     * with the actual constant value. */
    static class ReplaceConstantExpressions extends ExpressionTranslator {
        final Map<DBSPExpression, Boolean> constants;
        
        public ReplaceConstantExpressions(DBSPCompiler compiler, Map<DBSPExpression, Boolean> constants) {
            super(compiler);
            this.constants = constants;
        }

        boolean process(DBSPExpression expression) {
            if (this.constants.containsKey(expression)) {
                var result = this.constants.get(expression);
                var lit = new DBSPBoolLiteral(expression.getNode(), expression.getType(), result);
                this.map(expression, lit);
                return true;
            }
            return false;
        }

        // We only need to handle expressions that can have a Boolean type

        @Override
        public void postorder(DBSPVariablePath var) {
            if (!this.process(var)) {
                super.postorder(var);
            }
        }

        @Override
        public void postorder(DBSPApplyExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPApplyMethodExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPBinaryExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPCastExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPDerefExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPFieldExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPIsNullExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPPathExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPUnaryExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }

        @Override
        public void postorder(DBSPUnwrapExpression node) {
            if (!this.process(node)) {
                super.postorder(node);
            }
        }
    }
}
