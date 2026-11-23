package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Supply Java printer spellings and bind Rust's value numbers to the original expression objects. */
public final class NativeValueNumbering {
    private NativeValueNumbering() {}

    public record Spellings(Map<Long, String> types, Map<Long, String> literals, Map<Long, String> paths,
                            Map<String, String> symbols, Map<Long, String> absent_tuples, boolean rewrite) {}
    public record Site(long parent, int operand, @Nullable Long closure) {}
    public record Group(long representative, boolean expensive, List<Long> declarations,
                        List<Long> occurrences, List<Integer> users, Set<Long> shared) {}
    public record Result(Group[] groups, Site[] sites, long[] mutable, boolean unknown_write) {}

    public static Spellings spellings(Collection<IDBSPInnerNode> nodes, boolean rewrite) {
        var types = new HashMap<Long, String>();
        var literals = new HashMap<Long, String>();
        var paths = new HashMap<Long, String>();
        var symbols = new HashMap<String, String>();
        var absent = new HashMap<Long, String>();
        for (var node : nodes) {
            if (!(node instanceof DBSPExpression e))
                continue;
            if (e instanceof DBSPPathExpression path) {
                paths.put(e.id, path.path.toString());
                continue;
            }
            if (e instanceof DBSPLiteral literal) {
                literals.put(e.id, literal.isNull() ? null : literal.toString());
            } else if (e instanceof DBSPBaseTupleExpression tuple) {
                if (tuple.fields == null)
                    absent.put(e.id, e.toString());
                else if (tuple.isRaw())
                    continue;
            } else if (e instanceof DBSPUnaryExpression unary) {
                symbols.putIfAbsent(unary.opcode.name(), unary.opcode.toString());
            } else if (e instanceof DBSPBinaryExpression binary) {
                symbols.putIfAbsent(binary.opcode.name(), binary.opcode.toString());
            } else if (!(e instanceof DBSPApplyBaseExpression || e instanceof DBSPCastExpression)) {
                continue;
            }
            types.computeIfAbsent(e.type.getId(), ignored -> e.type.toString());
        }
        return new Spellings(types, literals, paths, symbols, absent, rewrite);
    }

    public static void bind(Result result, Map<Long, IDBSPInnerNode> nodes, ValueNumbering analysis, boolean rewrite) {
        analysis.foundAssignment = result.unknown_write();
        for (long id : result.mutable())
            analysis.mutable.add(Utilities.getExists(nodes, id).to(IDBSPDeclaration.class));
        for (var group : result.groups()) {
            var dependencies = new HashSet<IDBSPDeclaration>();
            for (long id : group.declarations())
                dependencies.add(Utilities.getExists(nodes, id).to(IDBSPDeclaration.class));
            var expression = Utilities.getExists(nodes, group.representative()).to(DBSPExpression.class);
            var value = new ValueNumbering.CanonicalExpression(expression, dependencies, group.expensive(),
                    rewrite ? group.shared() : null);
            for (var index : group.users()) {
                var site = result.sites()[index];
                var parent = Utilities.getExists(nodes, site.parent()).to(DBSPExpression.class);
                var closure = site.closure() == null ? null
                        : Utilities.getExists(nodes, site.closure()).to(DBSPClosureExpression.class);
                value.users.add(new ValueNumbering.ExpressionUser(parent, site.operand(), closure));
            }
            for (long id : group.occurrences())
                analysis.canonical.put(Utilities.getExists(nodes, id).to(DBSPExpression.class), value);
        }
    }
}
