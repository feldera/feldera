package org.dbsp.sqlCompiler.compiler.visitors.inner;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Bind a Rust value-numbering result to the live expressions used by the CSE rewriter. */
public final class NativeValueNumbering extends InnerVisitor {
    /** The live printer supplies spellings for types, literals, paths and operators.
     * Rust builds the key from these fields and the canonical operands. */
    @JsonTypeInfo(use = JsonTypeInfo.Id.SIMPLE_NAME, property = "kind")
    public sealed interface Form {}
    public record Literal(String type, @Nullable String value) implements Form {}
    public record Path(String text) implements Form {}
    public record Read() implements Form {}
    public record Let(String name, long declaration) implements Form {}
    public record Binary(String type, String opcode, String symbol) implements Form {}
    public record Unary(String type, String opcode, String symbol) implements Form {}
    public record Call(String type, boolean method) implements Form {}
    public record Cast(String type, String safety, boolean sourceString, boolean targetString) implements Form {}
    public record Tuple(String type) implements Form {}
    public record RawTuple() implements Form {}
    public record AbsentTuple(String type, String value) implements Form {}
    public record Field(int index) implements Form {}
    public enum Operator { Borrow, Clone, Deref, If, IsNull, Some }
    public record Simple(Operator operator) implements Form {}

    public sealed interface Event permits Use, Define {}
    public record Use(long input, int site) implements Event {}
    public record Site(long parent, int operand, Long closure) {}
    public record Define(long expression, Form form, long[] operands) implements Event {}
    public record Request(List<Event> events, List<Long> mutable_reads, List<Long> mutable_places,
                          boolean rewrite, List<Site> sites) {}
    public record Group(long representative, boolean expensive, List<Long> declarations,
                        List<Long> occurrences, List<Integer> users, Set<Long> shared) {}

    private final List<Event> events = new ArrayList<>();
    private final List<Long> mutableBorrows = new ArrayList<>();
    private final List<Long> mutableReads = new ArrayList<>();
    private final List<ValueNumbering.ExpressionUser> users = new ArrayList<>();
    private final Map<Long, DBSPExpression> expressions = new HashMap<>();
    private boolean hasVariable;

    private final ValueNumbering analysis;
    private final List<DBSPClosureExpression> closures = new ArrayList<>();

    NativeValueNumbering(ValueNumbering analysis) {
        super(analysis.compiler);
        this.analysis = analysis;
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression node) {
        this.closures.add(node);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPClosureExpression node) {
        Utilities.removeLast(this.closures);
    }

    private void use(DBSPExpression parent, int operand, DBSPExpression input) {
        this.events.add(new Use(input.id, this.users.size()));
        this.users.add(new ValueNumbering.ExpressionUser(parent, operand,
                this.closures.isEmpty() ? null : Utilities.last(this.closures)));
    }

    private void define(DBSPExpression expression, Form form, DBSPExpression... operands) {
        long[] ids = new long[operands.length];
        for (int i = 0; i < operands.length; i++) {
            this.use(expression, i, operands[i]);
            ids[i] = operands[i].id;
        }
        if (!this.closures.isEmpty()) {
            this.expressions.put(expression.id, expression);
            this.events.add(new Define(expression.id, form, ids));
        }
    }

    @Override
    public void postorder(DBSPLiteral e) {
        this.define(e, new Literal(e.type.toString(), e.isNull() ? null : e.toString()));
    }

    @Override
    public void postorder(DBSPPathExpression e) {
        this.define(e, new Path(e.path.toString()));
    }

    @Override
    public void postorder(DBSPVariablePath e) {
        this.hasVariable = true;
        if (e.type instanceof DBSPTypeRef ref && ref.mutable)
            this.mutableReads.add(e.id);
        this.define(e, new Read());
    }

    @Override
    public void postorder(DBSPLetExpression e) {
        this.define(e, new Let(e.variable.toString(), e.id), e.initializer, e.consumer);
    }

    @Override
    public void postorder(DBSPBinaryExpression e) {
        this.define(e, new Binary(e.type.toString(), e.opcode.name(), e.opcode.toString()),
                e.left, e.right);
    }

    @Override
    public void postorder(DBSPUnaryExpression e) {
        this.define(e, new Unary(e.type.toString(), e.opcode.name(), e.opcode.toString()), e.source);
    }

    @Override
    public void postorder(DBSPApplyBaseExpression e) {
        List<DBSPExpression> operands = new ArrayList<>();
        if (e instanceof DBSPApplyMethodExpression method)
            operands.add(method.self);
        operands.add(e.function);
        operands.addAll(Arrays.asList(e.arguments));
        this.define(e, new Call(e.type.toString(), e instanceof DBSPApplyMethodExpression),
                operands.toArray(DBSPExpression[]::new));
    }

    @Override
    public void postorder(DBSPCastExpression e) {
        this.define(e, new Cast(e.type.toString(), e.safe.name(),
                e.source.type instanceof DBSPTypeString, e.type instanceof DBSPTypeString), e.source);
    }

    @Override
    public void postorder(DBSPBaseTupleExpression e) {
        if (e.fields == null)
            this.define(e, new AbsentTuple(e.type.toString(), e.toString()));
        else
            this.define(e, e.isRaw() ? new RawTuple() : new Tuple(e.type.toString()), e.fields);
    }

    @Override
    public void postorder(DBSPFieldExpression e) {
        this.define(e, new Field(e.fieldNo), e.expression);
    }

    @Override
    public void postorder(DBSPBorrowExpression e) {
        if (!e.mut)
            this.define(e, new Simple(Operator.Borrow), e.expression);
        else
            this.mutableBorrows.add(e.expression.id);
    }

    @Override
    public void postorder(DBSPCloneExpression e) {
        this.define(e, new Simple(Operator.Clone), e.expression);
    }

    @Override
    public void postorder(DBSPDerefExpression e) {
        this.define(e, new Simple(Operator.Deref), e.expression);
    }

    @Override
    public void postorder(DBSPIsNullExpression e) {
        this.define(e, new Simple(Operator.IsNull), e.expression);
    }

    @Override
    public void postorder(DBSPSomeExpression e) {
        this.define(e, new Simple(Operator.Some), e.expression);
    }

    @Override
    public void postorder(DBSPIfExpression e) {
        if (e.negative == null)
            this.define(e, new Simple(Operator.If), e.condition, e.positive);
        else
            this.define(e, new Simple(Operator.If), e.condition, e.positive, e.negative);
    }

    @Override
    public void postorder(DBSPBlockExpression e) {
        for (int i = 0; i < e.contents.size(); i++)
            if (e.contents.get(i) instanceof DBSPLetStatement let && let.initializer != null)
                this.use(e, i, let.initializer);
        if (e.lastExpression != null)
            this.use(e, e.contents.size(), e.lastExpression);
    }

    @Override
    public void postorder(DBSPAssignmentExpression e) {
        this.analysis.foundAssignment = true;
    }

    void finish(IDBSPInnerNode root, Map<DBSPExpression, ValueNumbering.CanonicalExpression> numbers,
                Set<IDBSPDeclaration> mutable, boolean rewrite) {
        if (!this.hasVariable && this.mutableBorrows.isEmpty()
                && (this.expressions.isEmpty() || (rewrite && this.users.size() < 2)))
            return;
        var sites = new ArrayList<Site>();
        if (rewrite) {
            for (var user : this.users)
                sites.add(new Site(user.expression().id, user.operand(), user.closure() == null ? null : user.closure().id));
        }
        var result = RustCircuitPass.resolveAndNumber(this.compiler, root,
                new Request(this.events, this.mutableReads, this.mutableBorrows, rewrite, sites));
        this.analysis.foundAssignment |= result.unknownWrite();
        mutable.addAll(result.mutable());
        for (var group : result.groups()) {
            var dependencies = new HashSet<IDBSPDeclaration>();
            for (long id : group.declarations())
                dependencies.add(result.declaration(id));
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
