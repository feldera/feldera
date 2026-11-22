package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Bind a Rust value-numbering result to the live expressions used by the CSE rewriter. */
public final class NativeValueNumbering {
    public sealed interface Event permits Use, Define {}
    public record Use(long input, int site) implements Event {}
    public record Site(long parent, int operand, Long closure) {}
    public record Define(long expression, List<String> fragments, List<Long> operands,
                         List<Long> declarations, boolean expensive, boolean wrapper) implements Event {}
    public record Request(List<Event> events, Set<Long> mutable, boolean rewrite, List<Site> sites) {}
    public record Group(long representative, boolean expensive, List<Long> declarations,
                        List<Long> occurrences, List<Integer> users, Set<Long> shared) {}

    private final List<Event> events = new ArrayList<>();
    private final List<ValueNumbering.ExpressionUser> users = new ArrayList<>();
    private final Map<Long, DBSPExpression> expressions = new HashMap<>();
    private final Map<Long, IDBSPDeclaration> bindings = new HashMap<>();

    private boolean hasExpensiveValue;

    void use(DBSPExpression input, ValueNumbering.ExpressionUser site) {
        this.events.add(new Use(input.id, this.users.size()));
        this.users.add(site);
    }

    void define(DBSPExpression expression, ValueNumbering.Representation key, boolean expensive) {
        this.expressions.put(expression.id, expression);
        var fragments = new ArrayList<String>();
        var operands = new ArrayList<Long>();
        int start = 0;
        for (var operand : key.operands()) {
            fragments.add(key.repr().substring(start, operand.offset()));
            operands.add(operand.expression());
            start = operand.offset();
        }
        fragments.add(key.repr().substring(start));
        var declarations = new ArrayList<Long>();
        for (var binding : key.dependsOn()) {
            declarations.add(binding.getId());
            this.bindings.put(binding.getId(), binding);
        }
        this.hasExpensiveValue |= expensive;
        this.events.add(new Define(expression.id, fragments, operands, declarations, expensive,
                ExpressionsCSE.neverShared(expression)));
    }

    void finish(Map<DBSPExpression, ValueNumbering.CanonicalExpression> numbers,
                Set<IDBSPDeclaration> mutable, boolean rewrite) {
        if (this.expressions.isEmpty() || (rewrite && !this.hasExpensiveValue))
            return;
        var writes = new HashSet<Long>();
        for (var binding : mutable)
            writes.add(binding.getId());
        var sites = new ArrayList<Site>();
        if (rewrite) {
            for (var user : this.users)
                sites.add(new Site(user.expression().id, user.operand(), user.closure() == null ? null : user.closure().id));
        }
        for (var group : RustCircuitPass.numberValues(new Request(this.events, writes, rewrite, sites))) {
            var dependencies = new HashSet<IDBSPDeclaration>();
            for (long id : group.declarations())
                dependencies.add(Utilities.getExists(this.bindings, id));
            var value = new ValueNumbering.CanonicalExpression(
                    Utilities.getExists(this.expressions, group.representative()), dependencies, group.expensive(),
                    rewrite ? group.shared() : null);
            for (var site : group.users())
                value.users.add(this.users.get(site));
            for (long id : group.occurrences())
                numbers.put(Utilities.getExists(this.expressions, id), value);
        }
    }
}
