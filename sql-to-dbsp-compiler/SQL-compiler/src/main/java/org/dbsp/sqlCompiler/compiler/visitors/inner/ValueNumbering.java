package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Find common subexpressions, write them into the 'canonical' map. */
public class ValueNumbering extends InnerVisitor {
    /** 'closure' is the innermost closure that contains 'expression' */
    record ExpressionUser(DBSPExpression expression, int operand, @Nullable DBSPClosureExpression closure) {
        @Override
        public String toString() {
            return "ExpressionUser(" + this.expression.getId() + " " +
                    this.expression + ", operand=" + this.operand +
                    ", closure=" + (this.closure == null ? "none" : this.closure.getId()) + ")";
        }
    }

    public static class CanonicalExpression {
        final DBSPExpression expression;
        /** Expressions which use this CanonicalExpression as a subexpression */
        final Set<ExpressionUser> users;
        final boolean expensive;
        /** List of declarations whose values are used by this expression */
        final Set<IDBSPDeclaration> dependsOn;

        CanonicalExpression(DBSPExpression expression, Set<IDBSPDeclaration> dependsOn, boolean expensive) {
            this.expression = expression;
            this.users = new HashSet<>();
            this.expensive = expensive;
            this.dependsOn = dependsOn;
        }

        void use(DBSPExpression user, int operand, @Nullable DBSPClosureExpression closure) {
            this.users.add(new ExpressionUser(user, operand, closure));
        }

        @Override
        public String toString() {
            return this.expression.getId() + " " +
                    this.expression + " (" + this.users.size() + ") " +
                    Linq.list(Linq.map(this.dependsOn, IDBSPDeclaration::getName)) +
                    (this.expensive ? "*" : "");
        }

        /** Check if an expression has multiple users in one closure.
         *
         * @param translation A map from expression to canonical expression.
         * @param closure     Only the users that are in this closure, and not in a closure nested in it, count.
         * @return True if there are at least 2 users. */
        boolean manyUsers(Map<DBSPExpression, CanonicalExpression> translation, DBSPClosureExpression closure) {
            if (this.users.size() < 2)
                return false;
            Set<ExpressionUser> canon = new HashSet<>();
            for (var l: this.users) {
                if (l.closure != closure)
                    continue;
                if (translation.containsKey(l.expression)) {
                    DBSPExpression canonical = translation.get(l.expression).expression;
                    canon.add(new ExpressionUser(canonical, l.operand, l.closure));
                } else {
                    canon.add(l);
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
    /** True if the expression contains an assignment, or a mutable borrow of an expression whose
     * base is not a variable; CSE is then unsafe */
    public boolean foundAssignment;
    /** The closures that contain the visited node, innermost last */
    final List<DBSPClosureExpression> closures;
    /** Declarations of the variables whose value can change while the expression is evaluated:
     * variables with a mutable reference type, and the base variables of expressions borrowed
     * with {@code &mut}.
     * Expressions that read these variables get no value number. */
    final Set<IDBSPDeclaration> mutable;

    public ValueNumbering(DBSPCompiler compiler) {
        super(compiler);
        this.canonical = new HashMap<>();
        this.representation = new HashMap<>();
        this.resolver = new ResolveReferences(compiler, false);
        this.foundAssignment = false;
        this.closures = new ArrayList<>();
        this.mutable = new HashSet<>();
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
        this.closures.add(expression);
        return super.preorder(expression);
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        Utilities.removeLast(this.closures);
        super.postorder(expression);
    }

    @Nullable
    DBSPClosureExpression innermostClosure() {
        return this.closures.isEmpty() ? null : Utilities.last(this.closures);
    }

    @Override
    public void postorder(DBSPLiteral literal) {
        // All literals are cheap
        Representation repr;
        if (literal.isNull()) {
            repr = new Representation("None::" + literal.getType(), false);
        } else {
            repr = new Representation(literal + ":" + literal.getType(), false);
        }
        this.checkRepresentation(literal, repr, false);
    }

    /** Record 'user' as a user of the canonical representative of 'expression'.
     * Returns the representative, or null if 'expression' has no value number. */
    @Nullable
    CanonicalExpression use(DBSPExpression expression, DBSPExpression user, int operand) {
        CanonicalExpression canon = this.canonical.get(expression);
        if (canon != null)
            canon.use(user, operand, this.innermostClosure());
        return canon;
    }

    Representation getId(DBSPExpression expression, DBSPExpression user, int operand) {
        CanonicalExpression canon = this.use(expression, user, operand);
        if (canon == null)
            // Invalid representation
            return new Representation("", new HashSet<>(), false, true);
        // Delimited with # symbols
        String id = "#" + canon.expression.getId() + "#";
        return new Representation(id, canon.dependsOn, canon.expensive, false);
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
        // For timestamp arithmetic operations the type of the result is important to discriminate the operation.
        Representation repr = new Representation("(" + expression.type + ")", false)
                .add(this.getId(expression.left, expression, 0))
                .add(expression.opcode.toString())
                .add(this.getId(expression.right, expression, 1));
        boolean expensive = switch (expression.opcode) {
            case MUL, MAP_CONVERT, MUL_INTERVAL, ADD, SUB, DIV, DIV_NULL, MOD, MUL_WEIGHT, CONCAT, IS_DISTINCT, SQL_INDEX,
                 MAP_INDEX, VARIANT_INDEX, RUST_INDEX, DIV_INTERVAL, ADD_SATURATING, SUB_SATURATING,
                 MUL_SATURATING, DIV_SATURATING, MUL_INTERVAL_SATURATING, DIV_INTERVAL_SATURATING -> true;
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
        
    @Override
    public void postorder(DBSPBlockExpression expression) {
        // The block has no value number; it is the user of its let initializers and of its result
        int operand = 0;
        for (DBSPStatement stat : expression.contents) {
            DBSPLetStatement let = stat.as(DBSPLetStatement.class);
            if (let != null && let.initializer != null)
                this.use(let.initializer, expression, operand);
            operand++;
        }
        if (expression.lastExpression != null)
            this.use(expression.lastExpression, expression, operand);
    }

    @Override public void postorder(DBSPAssignmentExpression expression) {
        this.foundAssignment = true;
    }

    /** Find the declaration of the variable at the base of 'expression', e.g., p in (*p).1.
     * The base is what remains after removing field accesses and dereferences.
     * Returns null if the base is not a variable. */
    @Nullable
    IDBSPDeclaration baseDeclaration(DBSPExpression expression) {
        while (true) {
            if (expression.is(DBSPFieldExpression.class))
                expression = expression.to(DBSPFieldExpression.class).expression;
            else if (expression.is(DBSPDerefExpression.class))
                expression = expression.to(DBSPDerefExpression.class).expression;
            else
                break;
        }
        DBSPVariablePath var = expression.as(DBSPVariablePath.class);
        if (var == null)
            return null;
        return this.resolver.reference.get(var);
    }

    @Override public void postorder(DBSPBorrowExpression expression) {
        if (expression.mut) {
            // A mutable borrow gets no value number: each one can change the borrowed value
            IDBSPDeclaration base = this.baseDeclaration(expression.expression);
            if (base == null)
                this.foundAssignment = true;
            else
                this.mutable.add(base);
            return;
        }
        Representation repr = new Representation("&", false)
                .add(this.getId(expression.expression, expression, 0));
        this.checkRepresentation(expression, repr, false);
    }

    @Override public void postorder(DBSPCastExpression expression) {
        Representation repr = new Representation(
                expression.safe.name() + "(" + expression.type + "," + expression.safe.name() + ")", false)
                .add(this.getId(expression.source, expression, 0));
        boolean expensive = expression.type.is(DBSPTypeString.class)
                || expression.source.type.is(DBSPTypeString.class);
        this.checkRepresentation(expression, repr, expensive);
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
        // toString includes the generic arguments
        this.checkRepresentation(expression,
                new Representation(expression.path.toString(), false), false);
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
            // The type distinguishes nullable tuples
            repr = repr.add("(" + expression.type + ")Tup(");
            int operand = 0;
            for (DBSPExpression elem : expression.fields)
                repr = repr.add(this.getId(elem, expression, operand++)).add(",");
            repr = repr.add(")");
        }
        this.checkRepresentation(expression, repr, expression.fields != null && expression.size() > 0);
    }

    @Override public void postorder(DBSPUnaryExpression expression) {
        // The type distinguishes conversions such as INTEGER_TO_DECIMAL to different precisions
        Representation repr = new Representation("(" + expression.type + ")" + expression.opcode, false)
                .add(this.getId(expression.source, expression, 0));
        boolean expensive = expression.opcode != DBSPOpcode.NOT;
        this.checkRepresentation(expression, repr, expensive);
    }

    @Override public void postorder(DBSPVariablePath var) {
        IDBSPDeclaration decl = this.resolver.reference.get(var);
        Utilities.enforce(decl != null);
        DBSPTypeRef ref = var.getType().as(DBSPTypeRef.class);
        if (ref != null && ref.mutable)
            this.mutable.add(decl);
        Representation repr = new Representation(
                Long.toString(decl.getId()), Linq.set(decl), false, false);
        this.checkRepresentation(var, repr, false);
    }

    void checkRepresentation(DBSPExpression expression, Representation repr, boolean expensive) {
        if (repr.invalid || this.closures.isEmpty())
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
        // It can be e.g., Fold constructor.
        Logger.INSTANCE.belowLevel(this, 1)
                .append("CSE analyzing ")
                .appendSupplier(node::toString)
                .newline();
        super.startVisit(node);
        this.canonical.clear();
        this.representation.clear();
        this.resolver.apply(node);
        this.foundAssignment = false;
        this.mutable.clear();
    }

    @Override
    public void endVisit() {
        // An expression that reads a mutable variable has no canonical representative
        Iterator<Map.Entry<DBSPExpression, CanonicalExpression>> entries = this.canonical.entrySet().iterator();
        while (entries.hasNext()) {
            CanonicalExpression canon = entries.next().getValue();
            if (!Collections.disjoint(canon.dependsOn, this.mutable))
                entries.remove();
        }
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
