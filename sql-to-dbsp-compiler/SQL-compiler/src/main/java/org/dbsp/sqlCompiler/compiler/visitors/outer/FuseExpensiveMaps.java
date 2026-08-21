package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Expensive;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.ContainsNow;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Fuses one or more sibling {@link DBSPMapOperator}s that share
 * expensive common subexpressions into a single map followed by one projection
 * per original map.
 * Maps whose functions call now() are left alone. */
public class FuseExpensiveMaps extends Passes {
    public FuseExpensiveMaps(DBSPCompiler compiler) {
        super("FuseExpensiveMaps", compiler);
        FindFusableMaps find = new FindFusableMaps(compiler);
        this.add(find);
        this.add(new Fuse(compiler, find));
    }

    /** A map that may be fused with siblings.
     * @param operator The original map operator.
     * @param function A single-parameter closure with a tuple body.
     * @param minimal  Minimal expensive subexpressions of the function body. */
    record MapInfo(DBSPMapOperator operator, DBSPClosureExpression function,
                   List<DBSPExpression> minimal) {
        DBSPParameter param() {
            return this.function.parameters[0];
        }

        DBSPTupleExpression body() {
            return this.function.body.to(DBSPTupleExpression.class);
        }
    }

    /** True if the expressions compute the same value, given each map's parameter.
     * The only variable an expression may reference is its map's parameter */
    static boolean equivalent(MapInfo left, DBSPExpression newLeft, MapInfo right, DBSPExpression newRight) {
        return EquivalenceContext.equiv(newLeft.closure(left.param()), newRight.closure(right.param()));
    }

    /** Collects the minimal expensive subexpressions of a closure's body:
     * expensive nodes with no expensive proper subexpression.
     * A collected expression may reference only the closure's parameter. */
    static class CollectExpensiveExpressions extends InnerVisitor {
        // Minimal expensive sub-expressions: no subexpressions are expensive
        final List<DBSPExpression> minimal = new ArrayList<>();
        /** For each expensive node on the visit path, minimal.size() when entered */
        final List<Integer> marks = new ArrayList<>();
        final DBSPParameter parameter;
        /** The declaration of each variable in the closure */
        final ReferenceMap references;

        public CollectExpensiveExpressions(DBSPCompiler compiler, DBSPClosureExpression function) {
            super(compiler);
            this.parameter = function.parameters[0];
            ResolveReferences resolver = new ResolveReferences(compiler, false);
            resolver.apply(function);
            this.references = resolver.reference;
        }

        /** True if every variable inside the expression resolves to a
         * declaration inside it or to the closure parameter */
        boolean closed(DBSPExpression expression) {
            final Set<IDBSPDeclaration> declared = new HashSet<>();
            final List<DBSPVariablePath> used = new ArrayList<>();

            InnerVisitor scanner = new InnerVisitor(this.compiler()) {
                @Override
                public VisitDecision preorder(DBSPType type) {
                    return VisitDecision.STOP;
                }

                @Override
                public void postorder(IDBSPInnerNode node) {
                    if (node.is(IDBSPDeclaration.class))
                        declared.add(node.to(IDBSPDeclaration.class));
                }

                @Override
                public void postorder(DBSPVariablePath variable) {
                    used.add(variable);
                }
            };
            scanner.apply(expression);
            for (DBSPVariablePath variable : used) {
                IDBSPDeclaration declaration = this.references.get(variable);
                Utilities.enforce(declaration != null,
                        () -> "Variable " + variable + " has no declaration");
                if (declaration != this.parameter && !declared.contains(declaration))
                    return false;
            }
            return true;
        }

        @Override
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }

        @Override
        // Do not look inside nested closures
        public VisitDecision preorder(DBSPClosureExpression closure) {
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPForExpression expression) {
            // Statement-like, never a shareable value
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPExpression expression) {
            if (!Expensive.isExpensive(this.compiler(), expression))
                // If this expression is not expensive, no subexpressions can be either
                return VisitDecision.STOP;
            this.marks.add(this.minimal.size());
            return VisitDecision.CONTINUE;
        }

        @Override
        public void postorder(DBSPExpression expression) {
            int mark = Utilities.removeLast(this.marks);
            if (this.minimal.size() == mark && this.closed(expression))
                // expression is expensive (since preorder didn't stop),
                // no subexpression was collected (otherwise minimal would be
                // longer), and it only uses the parameter: minimal.
                // A rejected open fragment leaves the mark untouched, so its
                // nearest closed expensive ancestor is collected instead.
                this.minimal.add(expression);
        }
    }

    /** Groups maps by their input port and finds clusters worth fusing:
     * siblings that share an expensive subexpression. */
    static class FindFusableMaps extends CircuitVisitor {
        final ContainsNow containsNow;
        final Map<OutputPort, List<MapInfo>> groups = new LinkedHashMap<>();
        /** The cluster each fused map belongs to, keyed by cluster member.
         * A single-member cluster computes an expensive expression twice. */
        final Map<DBSPMapOperator, List<MapInfo>> clusters = new HashMap<>();

        public FindFusableMaps(DBSPCompiler compiler) {
            super(compiler);
            this.containsNow = new ContainsNow(compiler, true);
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            if (!operator.getFunction().is(DBSPClosureExpression.class))
                return;
            DBSPClosureExpression function = operator.getClosureFunction();
            this.containsNow.apply(function);
            if (this.containsNow.found)
                return;
            if (!Expensive.isExpensive(this.compiler(), function))
                return;
            Simplify simplify = new Simplify(this.compiler());
            function = simplify.apply(function).to(DBSPClosureExpression.class);
            if (!function.body.is(DBSPTupleExpression.class))
                return;
            CollectExpensiveExpressions collector =
                    new CollectExpensiveExpressions(this.compiler(), function);
            collector.apply(function.body);
            if (collector.minimal.isEmpty())
                return;
            MapInfo info = new MapInfo(operator, function, collector.minimal);
            this.groups.computeIfAbsent(operator.input(), p -> new ArrayList<>()).add(info);
        }

        /** A fingerprint that equivalent expressions share: the printout of
         * the canonical form of the expression closed over its parameter.
         * Comparing fingerprints for equality is a fast equivalence check for
         * closed closures. */
        String fingerprint(MapInfo info, DBSPExpression expression) {
            return CanonicalForm.asString(this.compiler(), expression.closure(info.param()));
        }

        boolean hasDuplicatedExpensiveField(MapInfo info) {
            DBSPExpression[] fields = Objects.requireNonNull(info.body().fields);
            for (int i = 0; i < fields.length; i++) {
                if (!Expensive.isExpensive(this.compiler(), fields[i]))
                    continue;
                for (int j = 0; j < i; j++)
                    if (equivalent(info, fields[j], info, fields[i]))
                        return true;
            }
            return false;
        }

        /** The root of the cluster that map 'i' belongs to in the union-find forest */
        int find(int[] parent, int i) {
            while (parent[i] != i)
                i = parent[i];
            return i;
        }

        @Override
        public void endVisit() {
            for (List<MapInfo> group : this.groups.values()) {
                int n = group.size();
                // Union-find forest over the group: parent[i] points towards
                // the root of i's cluster, and maps with one root fuse
                // together.
                int[] parent = new int[n];
                for (int i = 0; i < n; i++)
                    parent[i] = i;
                // Maps sharing a fingerprint are joined; linear in the total
                // number of collected expressions, instead of comparing all
                // expressions of all pairs of maps
                Map<String, Integer> firstWithPrint = new HashMap<>();
                for (int i = 0; i < n; i++) {
                    MapInfo info = group.get(i);
                    Set<String> prints = new HashSet<>();
                    for (DBSPExpression expression : info.minimal())
                        prints.add(this.fingerprint(info, expression));
                    for (String print : prints) {
                        Integer first = firstWithPrint.putIfAbsent(print, i);
                        if (first == null)
                            continue;
                        int ri = this.find(parent, first);
                        int rj = this.find(parent, i);
                        if (ri != rj)
                            parent[rj] = ri;
                    }
                }

                Map<Integer, List<MapInfo>> components = new LinkedHashMap<>();
                for (int i = 0; i < n; i++)
                    components.computeIfAbsent(this.find(parent, i), r -> new ArrayList<>())
                            .add(group.get(i));
                for (List<MapInfo> members : components.values()) {
                    if (members.size() == 1 &&
                            !this.hasDuplicatedExpensiveField(members.get(0)))
                        // "Fusing" a single map when it contains a repeated computation
                        continue;
                    for (MapInfo member : members)
                        Utilities.putNew(this.clusters, member.operator(), members);
                }
            }
            super.endVisit();
        }
    }

    /** Rewrites the clusters found by {@link FindFusableMaps} */
    static class Fuse extends CircuitCloneVisitor {
        final FindFusableMaps found;

        Fuse(DBSPCompiler compiler, FindFusableMaps found) {
            super(compiler, false);
            this.found = found;
        }

        /** Index of an equivalent column, or -1 */
        static int indexOf(List<DBSPExpression> columns, DBSPExpression expression, DBSPVariablePath var) {
            for (int i = 0; i < columns.size(); i++)
                if (EquivalenceContext.equiv(columns.get(i).closure(var), expression.closure(var)))
                    return i;
            return -1;
        }

        /** Replace a cluster of sibling maps with one fused map computing the
         * distinct fields of all members, followed by one projection per member. */
        void fuse(List<MapInfo> cluster) {
            MapInfo first = cluster.get(0);
            OutputPort source = this.mapped(first.operator().input());
            DBSPVariablePath var = first.param().getType().var();

            List<DBSPExpression> columns = new ArrayList<>();
            List<int[]> memberColumns = new ArrayList<>();
            for (MapInfo member : cluster) {
                DBSPExpression body = member.function().call(var).reduce(this.compiler());
                Utilities.enforce(body.is(DBSPTupleExpression.class),
                        () -> "Fused map body is not a tuple: " + body);
                DBSPExpression[] fields = Objects.requireNonNull(
                        body.to(DBSPTupleExpression.class).fields);
                int[] cols = new int[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    int col = indexOf(columns, fields[i], var);
                    if (col < 0) {
                        col = columns.size();
                        columns.add(fields[i]);
                    }
                    cols[i] = col;
                }
                memberColumns.add(cols);
            }

            DBSPTupleExpression fusedTuple =
                    new DBSPTupleExpression(columns.toArray(new DBSPExpression[0]));
            DBSPMapOperator fused = new DBSPMapOperator(
                    first.operator().getRelNode(), fusedTuple.closure(var), source);
            this.addOperator(fused);

            for (int m = 0; m < cluster.size(); m++) {
                MapInfo member = cluster.get(m);
                int[] cols = memberColumns.get(m);
                DBSPVariablePath row = fusedTuple.getType().ref().var();
                DBSPExpression[] fields = new DBSPExpression[cols.length];
                for (int i = 0; i < cols.length; i++)
                    fields[i] = row.deref().field(cols[i]).applyCloneIfNeeded();
                DBSPTypeTuple outputType =
                        member.operator().getOutputZSetElementType().to(DBSPTypeTuple.class);
                DBSPClosureExpression projection = new DBSPTupleExpression(
                        member.operator().getNode(), outputType, fields).closure(row);
                DBSPMapOperator projected = new DBSPMapOperator(
                        member.operator().getRelNode(), projection, fused.outputPort())
                        .addAnnotation(new IsProjection(columns.size()), DBSPMapOperator.class);
                this.map(member.operator().outputPort(), projected.outputPort(), true);
            }
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            if (this.remap.containsKey(operator.outputPort()))
                // Fused when the first member of its cluster was visited
                return;
            List<MapInfo> cluster = this.found.clusters.get(operator);
            if (cluster != null) {
                this.fuse(cluster);
                return;
            }
            super.postorder(operator);
        }
    }
}
