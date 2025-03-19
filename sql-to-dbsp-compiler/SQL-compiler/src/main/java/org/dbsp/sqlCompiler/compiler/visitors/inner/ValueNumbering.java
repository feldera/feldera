package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Find common subexpressions, write them into the 'canonical' map. */
public class ValueNumbering extends InnerVisitor {
    record ExpressionUser(DBSPExpression expression, int operand) {
        @Override
        public String toString() {
            return "ExpressionUser(" + this.expression.getId() + " " +
                    this.expression + ", operand=" + this.operand + ")";
        }
    }

    public static class CanonicalExpression {
        final DBSPExpression expression;
        /** Expressions which use this CanonicalExpression as a subexpression */
        Set<ExpressionUser> users;
        boolean expensive;
        /** List of declarations whose values are used by this expression */
        final Set<IDBSPDeclaration> dependsOn;

        CanonicalExpression(DBSPExpression expression, Set<IDBSPDeclaration> dependsOn, boolean expensive) {
            this.expression = expression;
            this.users = new HashSet<>();
            this.expensive = expensive;
            this.dependsOn = dependsOn;
        }

        void use(DBSPExpression user, int operand) {
            this.users.add(new ExpressionUser(user, operand));
        }

        @Override
        public String toString() {
            return this.expression.getId() + " " +
                    this.expression + " (" + this.users.size() + ") " +
                    Linq.list(Linq.map(this.dependsOn, IDBSPDeclaration::getName)) +
                    (this.expensive ? "*" : "");
        }

        /** Check if an expression has multiple users.
         *
         * @param translation A map from expression to canonical expression.
         * @return True if there are at least 2 users. */
        boolean manyUsers(Map<DBSPExpression, CanonicalExpression> translation) {
            if (this.users.size() < 2)
                return false;
            Set<ExpressionUser> canon = new HashSet<>();
            for (var l: this.users) {
                if (translation.containsKey(l.expression)) {
                    DBSPExpression canonical = translation.get(l.expression).expression;
                    canon.add(new ExpressionUser(canonical, l.operand));
                } else {
                    canon.add(new ExpressionUser(this.expression, l.operand));
                }
                if (canon.size() > 1)
                    return true;
            }
            return false;
        }
    }

    /** Maps each operator to its canonical representative */
    public final Map<DBSPExpression, CanonicalExpression> canonical;
    final Map<String, CanonicalExpression> representation;
    final ResolveReferences resolver;
    /** If true we cannot CSE safely */
    public boolean foundAssignment;
    public List<Boolean> inClosure;

    public ValueNumbering(DBSPCompiler compiler) {
        super(compiler);
        this.canonical = new HashMap<>();
        this.representation = new HashMap<>();
        this.resolver = new ResolveReferences(compiler, false);
        this.foundAssignment = false;
        this.inClosure = new ArrayList<>();
    }

    @SuppressWarnings("unused")
    public void dump(IIndentStream stream) {
        for (var e: this.canonical.entrySet()) {
            stream.append(e.getKey().getId())
                    .append(" ")
                    .append(e.getKey().toString())
                    .append("->")
                    .append(e.getValue().toString())
                    .newline();
        }
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.inClosure.add(true);
        return super.preorder(expression);
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        Utilities.removeLast(this.inClosure);
        super.postorder(expression);
    }

    @Override
    public void postorder(DBSPLiteral literal) {
        Representation repr;
        boolean expensive = literal.getType().is(DBSPTypeDecimal.class);
        if (literal.isNull()) {
            repr = new Representation("None::" + literal.getType(), false);
        } else {
            repr = new Representation(literal + ":" + literal.getType(), expensive);
        }
        this.checkRepresentation(literal, repr, false);
    }

    Representation getId(DBSPExpression expression, DBSPExpression user, int operand) {
        if (!this.canonical.containsKey(expression))
            // Invalid representation
            return new Representation("", new HashSet<>(), false, true);
        CanonicalExpression canon = Utilities.getExists(this.canonical, expression);
        canon.use(user, operand);
        return new Representation(Long.toString(canon.expression.getId()), canon.dependsOn, canon.expensive, false);
    }

    record Representation(String repr, Set<IDBSPDeclaration> dependsOn, boolean expensive, boolean invalid) {
        Representation() {
            this("", new HashSet<>(), false, false);
        }

        Representation(String repr, boolean expensive) {
            this(repr, new HashSet<>(), expensive, false);
        }

        @CheckReturnValue
        public Representation add(Representation repr) {
            return new Representation(
                    this.repr + repr.repr,
                    Utilities.concatSet(this.dependsOn, repr.dependsOn),
                    this.expensive || repr.expensive,
                    repr.invalid || this.invalid);
        }

        @CheckReturnValue
        public Representation add(String repr) {
            return new Representation(this.repr + repr, this.dependsOn, this.expensive, this.invalid);
        }
    }
    
    @Override
    public void postorder(DBSPBinaryExpression expression) {
        Representation repr = this.getId(expression.left, expression, 0)
                .add(expression.opcode.toString())
                .add(this.getId(expression.right, expression, 1));
        boolean expensive = switch (expression.opcode) {
            case MUL, MAP_CONVERT, INTERVAL_MUL, ADD, SUB, DIV, DIV_NULL, MOD, MUL_WEIGHT, CONCAT, IS_DISTINCT, SQL_INDEX,
                 MAP_INDEX, VARIANT_INDEX, RUST_INDEX, INTERVAL_DIV -> true;
            default -> false;
        };
        this.checkRepresentation(expression, repr, expensive);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        Representation repr = new Representation("", false)
                .add(this.getId(expression.function, expression, 0)).add("(");
        int operand = 1;
        for (DBSPExpression arg : expression.arguments)
            repr = repr.add(this.getId(arg, expression, operand++)).add(",");
        // For some polymorphic functions, such as vec!() we need the return type as well.
        repr = repr.add(")").add(expression.type.toString());
        this.checkRepresentation(expression, repr, true);
    } 
    
    @Override
    public void postorder(DBSPApplyMethodExpression expression) {
        Representation repr = new Representation()
                .add(this.getId(expression.self, expression, 0)).add(".")
                .add(this.getId(expression.function, expression, 1)).add("(");
        int operand = 2;
        for (DBSPExpression arg : expression.arguments)
            repr = repr.add(this.getId(arg, expression, operand++)).add(",");
        // For some polymorphic functions, such as vec!() we need the return type as well.
        repr = repr.add(")").add(expression.type.toString());
        this.checkRepresentation(expression, repr, true);
    }
        
    @Override public void postorder(DBSPAssignmentExpression expression) {
        this.foundAssignment = true;
    }

    @Override public void postorder(DBSPBorrowExpression expression) {
        Representation repr = new Representation("&", false)
                .add(this.getId(expression.expression, expression, 0));
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPCastExpression expression) {
        Representation repr = new Representation("(" + expression.type + "," + expression.safe + ")", false)
                .add(this.getId(expression.source, expression, 0));
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPCloneExpression expression) {
        Representation repr = this.getId(expression.expression, expression, 0)
                .add(".clone()");
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPDerefExpression expression) {
        Representation repr = new Representation("*", false)
                .add(this.getId(expression.expression, expression, 0));
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPFieldExpression expression) {
        Representation repr = this.getId(expression.expression, expression, 0)
                .add("." + expression.fieldNo);
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPIfExpression expression) {
        Representation repr = new Representation("if", false)
                .add(this.getId(expression.condition, expression, 0))
                .add("?")
                .add(this.getId(expression.positive, expression, 1));
        if (expression.negative != null)
            repr = repr.add(":").add(this.getId(expression.negative, expression, 2));
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPIsNullExpression expression) {
        Representation repr = this.getId(expression.expression, expression, 0)
                .add(".is_null()");
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPLetExpression expression) {
        Representation repr = new Representation("", Linq.set(expression), false, false)
                .add("let " + expression.variable + " = ")
                .add(this.getId(expression.initializer, expression, 0))
                .add(" in ").add(this.getId(expression.consumer, expression, 1));
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPPathExpression expression) {
        this.checkRepresentation(expression,
                new Representation(expression.path.asString(), false), false);
    }

    @Override public void postorder(DBSPRawTupleExpression expression) {
        Representation repr = new Representation();
        if (expression.fields == null) {
            repr = repr.add(expression + ":" + expression.type);
        } else {
            repr = repr.add("(");
            int operand = 0;
            for (DBSPExpression elem : expression.fields)
                repr = repr.add(this.getId(elem, expression, operand++)).add(",");
            repr = repr.add(")");
        }
        this.checkRepresentation(expression, repr, expression.fields != null && expression.size() > 0);
    }

    @Override public void postorder(DBSPSomeExpression expression) {
        Representation repr = new Representation("Some(", false)
                .add(this.getId(expression.expression, expression, 0))
                .add(")");
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPTupleExpression expression) {
        Representation repr = new Representation();
        if (expression.fields == null) {
            repr = repr.add(expression + ":" + expression.type);
        } else {
            repr = repr.add("Tup(");
            int operand = 0;
            for (DBSPExpression elem : expression.fields)
                repr = repr.add(this.getId(elem, expression, operand++)).add(",");
            repr = repr.add(")");
        }
        this.checkRepresentation(expression, repr, expression.fields != null && expression.size() > 0);
    }

    @Override public void postorder(DBSPUnaryExpression expression) {
        Representation repr = new Representation(expression.opcode.toString(), false)
                .add(this.getId(expression.source, expression, 0));
        boolean expensive = expression.opcode != DBSPOpcode.NOT;
        this.checkRepresentation(expression, repr, expensive);
    }

    @Override public void postorder(DBSPVariablePath var) {
        IDBSPDeclaration decl = this.resolver.reference.get(var);
        assert decl != null;
        Representation repr = new Representation(
                Long.toString(decl.getId()), Linq.set(decl), false, false);
        this.checkRepresentation(var, repr, false);
    }

    void checkRepresentation(DBSPExpression expression, Representation repr, boolean expensive) {
        if (repr.invalid || this.inClosure.isEmpty())
            return;
        if (this.representation.containsKey(repr.repr())) {
            CanonicalExpression expr = Utilities.getExists(this.representation, repr.repr());
            this.setCanonical(expression, expr);
        } else {
            this.newCanonical(expression, repr.repr(), repr.dependsOn, repr.expensive() || expensive);
        }
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        // This node may not be a DBSPClosureExpression.
        // It can be a e.g., Fold constructor.
        Logger.INSTANCE.belowLevel(this, 1)
                .append("CSE analyzing ")
                .appendSupplier(node::toString)
                .newline();
        super.startVisit(node);
        this.canonical.clear();
        this.representation.clear();
        this.resolver.apply(node);
        this.foundAssignment = false;
    }

    @Override
    public void endVisit() {
        if (this.getDebugLevel() >= 1) {
            IIndentStream stream = Logger.INSTANCE.belowLevel(this, 1);
            this.dump(stream);
        }
    }
    
    void newCanonical(DBSPExpression expression, String repr,
                      Set<IDBSPDeclaration> dependsOn, boolean expensive) {
        CanonicalExpression canonical = new CanonicalExpression(expression, dependsOn, expensive);
        Utilities.putNew(this.representation, repr, canonical);
        this.setCanonical(expression, canonical);
    } 

    void setCanonical(DBSPExpression expression, CanonicalExpression canonical) {
        Logger.INSTANCE.belowLevel(this, 2)
                .append("CSE ")
                .append(expression.id)
                .append(" ")
                .appendSupplier(expression::toString)
                .append(" -> ")
                .appendSupplier(canonical::toString)
                .newline();
        this.canonical.put(expression, canonical);
    }
}
