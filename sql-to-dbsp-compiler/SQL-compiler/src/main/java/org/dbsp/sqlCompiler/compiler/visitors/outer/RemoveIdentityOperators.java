package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Removes Map and MapIndex operators that compute the identity function.
 *
 * <p>Such an operator can only be removed if its garbage-collection operators
 * can move to its input.  {@link Decide} scans the graph and picks, for each
 * input and dimension, the one operator that may move; {@link RemoveOrReplace} then
 * rewrites the circuit.  Operators that cannot be removed become
 * {@link DBSPNoopOperator}s. */
public class RemoveIdentityOperators extends Passes {
    public RemoveIdentityOperators(DBSPCompiler compiler) {
        super("RemoveIdentityOperators", compiler);
        Graph graph = new Graph(compiler);
        this.add(graph);
        Decide decide = new Decide(compiler, graph.getGraphs());
        this.add(decide);
        this.add(new RemoveOrReplace(compiler, decide.remove));
    }

    enum GCKind {
        KEYS,
        VALUES
    }

    @Nullable
    static GCKind gcKind(DBSPOperator operator) {
        if (operator.is(DBSPIntegrateTraceRetainKeysOperator.class))
            return GCKind.KEYS;
        if (operator.is(DBSPIntegrateTraceRetainValuesOperator.class) ||
                operator.is(DBSPIntegrateTraceRetainNValuesOperator.class))
            return GCKind.VALUES;
        return null;
    }

    /** Check whether a closure is an "identity" function for a Map or MapIndex operator.
     * There are two possible shapes we check:
     * |x| *x
     * and
     * |(&k, &v)| (*k, *v) */
    public static boolean isIdentityFunction(DBSPClosureExpression expression) {
        if (expression.parameters.length != 1)
            return false;
        // After some fast negative checks we compare equivalence with
        // an identity function of the appropriate type.
        DBSPType paramType = expression.parameters[0].getType();
        if (paramType.is(DBSPTypeRef.class)) {
            if (!paramType.deref().sameType(expression.getResultType()))
                return false;
            DBSPVariablePath var = paramType.var();
            DBSPTypeTupleBase tuple = paramType.deref().as(DBSPTypeTupleBase.class);
            DBSPClosureExpression id;
            if (tuple != null) {
                id = new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.deref()), tuple.mayBeNull).closure(var);
            } else {
                id = var.deref().applyCloneIfNeeded().closure(var);
            }
            return EquivalenceContext.equiv(expression, id);
        } else if (paramType.is(DBSPTypeRawTuple.class)) {
            DBSPTypeRawTuple raw = paramType.to(DBSPTypeRawTuple.class);
            if (raw.size() != 2) {
                return false;
            }
            if (!raw.tupFields[0].is(DBSPTypeRef.class) || !raw.tupFields[1].is(DBSPTypeRef.class))
                return false;
            if (!expression.getResultType().is(DBSPTypeRawTuple.class))
                return false;
            DBSPTypeRawTuple result = expression.getResultType().to(DBSPTypeRawTuple.class);
            if (result.size() != 2 ||
                    !raw.tupFields[0].deref().sameType(result.tupFields[0]) ||
                    !raw.tupFields[1].deref().sameType(result.tupFields[1]))
                return false;
            DBSPVariablePath var = paramType.var();
            DBSPClosureExpression id = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.field(0).deref()), false),
                    new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.field(1).deref()), false)
            ).closure(var);
            return EquivalenceContext.equiv(expression, id);
        } else {
            return false;
        }
    }

    /** True for a Map or MapIndex operator that computes the identity. */
    static boolean isIdentityOperator(DBSPOperator operator) {
        if (!operator.is(DBSPMapOperator.class) && !operator.is(DBSPMapIndexOperator.class))
            return false;
        DBSPSimpleOperator simple = operator.to(DBSPSimpleOperator.class);
        if (simple.function == null || !simple.function.is(DBSPClosureExpression.class))
            return false;
        DBSPUnaryOperator unary = operator.to(DBSPUnaryOperator.class);
        if (!unary.input().outputType().sameType(unary.outputType()))
            return false;
        return isIdentityFunction(simple.getClosureFunction());
    }

    /** Decides which identity operators can be removed.
     *
     * <p>Removing an operator moves GC operators attached to its output to its input;
     * a node cannot have incompatible GC operators. */
    static class Decide extends CircuitWithGraphsVisitor {
        /** Operators to remove, mapped to true if it may be removed and
         * false if it becomes a noop */
        public final Map<DBSPOperator, Boolean> remove = new HashMap<>();
        final List<Candidate> candidates = new ArrayList<>();

        /** @param toMove    GC kinds that removal would move to the input.
         *  @param existing GC kinds that exist on the input. */
        record Candidate(DBSPUnaryOperator operator, OutputPort input,
                         Set<GCKind> toMove, Set<GCKind> existing) {}

        Decide(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
        }

        @Override
        public Token startVisit(IDBSPOuterNode node) {
            this.remove.clear();
            this.candidates.clear();
            return super.startVisit(node);
        }

        /** What kinds of GC operators are attached to an existing port? */
        Set<GCKind> getGCKinds(OutputPort port) {
            Set<GCKind> result = EnumSet.noneOf(GCKind.class);
            for (var successor : this.getGraph().getSuccessors(port.node())) {
                if (successor.port() != 0)
                    continue;
                GCKind retention = gcKind(successor.node());
                if (retention != null
                        && successor.node().to(DBSPBinaryOperator.class).left().equals(port))
                    result.add(retention);
            }
            return result;
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            this.consider(operator);
        }

        @Override
        public void postorder(DBSPMapIndexOperator operator) {
            this.consider(operator);
        }

        void consider(DBSPUnaryOperator operator) {
            if (!isIdentityOperator(operator))
                return;
            OutputPort input = operator.input();
            this.candidates.add(new Candidate(operator, input,
                    this.getGCKinds(operator.outputPort()),
                    this.getGCKinds(input)));
        }

        @Override
        public void endVisit() {
            Map<OutputPort, Set<GCKind>> claimed = new HashMap<>();
            for (Candidate candidate : this.candidates) {
                if (candidate.toMove().isEmpty()) {
                    this.remove.put(candidate.operator(), true);
                    continue;
                }
                Set<GCKind> taken =
                        claimed.computeIfAbsent(candidate.input(), p -> EnumSet.copyOf(candidate.existing()));
                boolean canRemove = taken.stream().noneMatch(candidate.toMove()::contains);
                if (canRemove)
                    taken.addAll(candidate.toMove());
                this.remove.put(candidate.operator(), canRemove);
            }
            super.endVisit();
        }
    }

    /** Applies the decisions taken by {@link Decide}. */
    static class RemoveOrReplace extends CircuitCloneVisitor {
        final Map<DBSPOperator, Boolean> actions;

        RemoveOrReplace(DBSPCompiler compiler, Map<DBSPOperator, Boolean> actions) {
            super(compiler, false);
            this.actions = actions;
        }

        boolean replaceIdentity(DBSPUnaryOperator operator) {
            Boolean remove = this.actions.get(operator);
            if (remove == null)
                // Not an identity operator
                return false;
            OutputPort input = this.mapped(operator.input());
            if (remove) {
                this.map(operator.outputPort(), input, false);
            } else {
                this.map(operator, new DBSPNoopOperator(operator.getRelNode(), input));
            }
            return true;
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            if (!this.replaceIdentity(operator))
                super.postorder(operator);
        }

        @Override
        public void postorder(DBSPMapIndexOperator operator) {
            if (!this.replaceIdentity(operator))
                super.postorder(operator);
        }
    }
}
