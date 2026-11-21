package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitDispatcher;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.JsonStream;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

/** Project the inner roots a pass inspects, preserving shared expression and type identities. */
final class OptimizerExpressions {
    enum Roots { INITIALIZERS, INDEX_TYPES, DECLARED_FUNCTIONS }

    private OptimizerExpressions() {}

    /** The validator reports the first offending call in Java postorder. Only
     * the first source range for each spelling can contribute a diagnostic. */
    static byte[] executableCalls(DBSPCompiler compiler, DBSPCircuit circuit) {
        var first = new LinkedHashMap<String, DBSPApplyExpression>();
        var visitor = new InnerVisitor(compiler) {
            final Set<IDBSPInnerNode> seen = Collections.newSetFromMap(new IdentityHashMap<>());

            @Override
            public VisitDecision preorder(IDBSPInnerNode node) {
                return this.seen.add(node) ? VisitDecision.CONTINUE : VisitDecision.STOP;
            }

            @Override
            public void postorder(DBSPApplyExpression call) {
                String name = call.getFunctionName();
                if (name != null)
                    first.putIfAbsent(name, call);
            }
        };
        new CircuitDispatcher(compiler, visitor, true).apply(circuit);
        var json = JsonStream.compactCircuit();
        json.beginArray();
        first.forEach((name, call) -> {
            json.beginArray();
            json.append(name).append(call.getNode().getPositionRange()).endArray();
        });
        return json.endArray().toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Named calls in executable operator expressions, excluding the clock source. */
    static byte[] operatorCalls(DBSPCompiler compiler, DBSPCircuit circuit) {
        record Call(String name, int arity) {}
        var calls = new LinkedHashSet<Call>();
        var visitor = new InnerVisitor(compiler) {
            final Set<IDBSPInnerNode> seen = Collections.newSetFromMap(new IdentityHashMap<>());

            @Override
            public VisitDecision preorder(IDBSPInnerNode node) {
                if (node instanceof DBSPType || !this.seen.add(node))
                    return VisitDecision.STOP;
                if (node instanceof DBSPApplyExpression apply) {
                    String name = apply.getFunctionName();
                    if (name != null)
                        calls.add(new Call(name, apply.arguments.length));
                }
                return VisitDecision.CONTINUE;
            }
        };
        new CircuitDispatcher(compiler, visitor, false) {
            @Override
            public VisitDecision preorder(DBSPNowOperator node) {
                return VisitDecision.STOP;
            }
        }.apply(circuit);
        var json = JsonStream.compactCircuit();
        json.beginArray();
        for (var call : calls) {
            json.beginArray();
            json.append(call.name()).append(call.arity()).endArray();
        }
        return json.endArray().toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Summarize the executable inner graph before asking Rust whether it needs the full snapshot. */
    static byte[] inventory(DBSPCompiler compiler, DBSPCircuit circuit) {
        var visitor = new InnerVisitor(compiler) {
            final Set<IDBSPInnerNode> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            final EnumSet<DBSPCastExpression.CastType> casts = EnumSet.noneOf(DBSPCastExpression.CastType.class);
            final EnumSet<DBSPOpcode> binary = EnumSet.noneOf(DBSPOpcode.class);
            boolean unwrap;
            boolean conditional;

            @Override
            public VisitDecision preorder(IDBSPInnerNode node) {
                if (node instanceof DBSPType || !this.seen.add(node))
                    return VisitDecision.STOP;
                if (node instanceof DBSPCastExpression cast)
                    this.casts.add(cast.safe);
                else if (node instanceof DBSPBinaryExpression expression)
                    this.binary.add(expression.opcode);
                else if (node instanceof DBSPIfExpression)
                    this.conditional = true;
                else if (node instanceof DBSPUnwrapExpression)
                    this.unwrap = true;
                return VisitDecision.CONTINUE;
            }
        };
        new CircuitDispatcher(compiler, visitor, true).apply(circuit);
        var json = JsonStream.compactCircuit();
        json.beginObject().label("casts").beginArray();
        for (var cast : visitor.casts)
            json.append(cast.name());
        json.endArray().label("binary").beginArray();
        for (var opcode : visitor.binary)
            json.append(opcode.name());
        json.endArray().label("unwrap").append(visitor.unwrap)
                .label("conditional").append(visitor.conditional).endObject();
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    static byte[] encode(DBSPCompiler compiler, DBSPCircuit circuit, Roots roots) {
        var json = JsonStream.compactCircuit();
        var inner = new ToJsonInnerVisitor(compiler, json, 1, true);
        json.beginObject().appendClass(circuit).label("id").append(circuit.id);
        json.label("declarations").beginArray();
        for (var declaration : circuit.declarations) {
            if (roots != Roots.INDEX_TYPES && declaration.item instanceof DBSPFunctionItem) {
                json.beginObject().appendClass(declaration).label("id").append(declaration.id);
                declaration.accept(inner);
                json.endObject();
            }
        }
        json.endArray().label("allOperators").beginArray();
        if (roots != Roots.DECLARED_FUNCTIONS)
            writeOperators(circuit.getAllOperators(), roots, inner);
        json.endArray().endObject();
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void writeOperators(Iterable<DBSPOperator> operators, Roots roots, ToJsonInnerVisitor inner) {
        var json = inner.stream;
        for (var op : operators) {
            if (op instanceof DBSPNestedOperator nested) {
                writeOperators(nested.getAllOperators(), roots, inner);
            } else if (roots == Roots.INDEX_TYPES &&
                    (op instanceof DBSPMapIndexOperator || op instanceof DBSPFlatMapIndexOperator)) {
                json.beginObject().appendClass(op).label("id").append(op.id).label("outputType");
                op.outputType(0).accept(inner);
                json.endObject();
            } else if (roots == Roots.INITIALIZERS && op instanceof DBSPSourceMultisetOperator table &&
                    table.metadata.getColumns().stream().anyMatch(column -> column.defaultValue != null)) {
                json.beginObject().appendClass(table).label("id").append(table.id);
                json.label("metadata").beginObject().label("columnMetadata").beginArray();
                for (var column : table.metadata.getColumns()) {
                    json.beginObject();
                    if (column.defaultValue != null) {
                        json.label("defaultValue");
                        column.defaultValue.accept(inner);
                    }
                    json.endObject();
                }
                json.endArray().endObject();
                json.endObject();
            }
        }
    }
}
