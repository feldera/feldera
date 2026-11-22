package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitDispatcher;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/** Project the inner roots a pass inspects, preserving shared expression and type identities. */
final class OptimizerExpressions {
    enum Roots { INITIALIZERS, INDEX_TYPES, DECLARED_FUNCTIONS, FILTER_PREDICATES }

    private OptimizerExpressions() {}

    record CallSite(long root, long id, String name, int arity) {}
    record CallSites(List<CallSite> sites, Map<Long, DBSPExpression> expressions) {}

    static CallSites callSites(DBSPCompiler compiler, Iterable<DBSPExpression> roots) {
        record Signature(String name, int arity) {}
        var sites = new ArrayList<CallSite>();
        var expressions = new HashMap<Long, DBSPExpression>();
        for (var root : roots) {
            // A signature's last occurrence supplies its diagnostic position.
            var last = new LinkedHashMap<Signature, DBSPApplyExpression>();
            new InnerVisitor(compiler) {
                @Override
                public VisitDecision preorder(DBSPType type) {
                    return VisitDecision.STOP;
                }

                @Override
                public VisitDecision preorder(DBSPApplyExpression call) {
                    String name = call.getFunctionName();
                    if (name != null) {
                        var signature = new Signature(name, call.arguments.length);
                        last.remove(signature);
                        last.put(signature, call);
                    }
                    return super.preorder(call);
                }
            }.apply(root);
            last.forEach((signature, call) -> {
                sites.add(new CallSite(root.getId(), call.getId(), signature.name(), signature.arity()));
                expressions.put(call.getId(), call);
            });
        }
        return new CallSites(sites, expressions);
    }

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

    /** Visit roots in circuit-rewriter order. Only roots containing a candidate
     * expression need to cross JNI. Type annotations contain no executable code. */
    static DBSPCircuit rewriteSelected(DBSPCompiler compiler, DBSPCircuit circuit,
                                       boolean declarations, Predicate<DBSPExpression> candidate,
                                       Function<IDBSPInnerNode, IDBSPInnerNode> rewrite) {
        var visitor = new InnerVisitor(compiler) {
            final Set<IDBSPInnerNode> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            boolean found;

            @Override
            public VisitDecision preorder(IDBSPInnerNode node) {
                if (this.found || node instanceof DBSPType || !this.seen.add(node))
                    return VisitDecision.STOP;
                if (node instanceof DBSPExpression expression && candidate.test(expression)) {
                    this.found = true;
                    return VisitDecision.STOP;
                }
                return VisitDecision.CONTINUE;
            }

            @Override
            public IDBSPInnerNode apply(IDBSPInnerNode root) {
                if (root instanceof DBSPType)
                    return root;
                this.found = false;
                this.seen.clear();
                super.apply(root);
                return this.found ? rewrite.apply(root) : root;
            }
        };
        return visitor.getCircuitRewriter(declarations).apply(circuit);
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

    /** Emit the closure roots in one IR identity space. Field-use analysis never follows input edges. */
    static byte[] mapFunctions(DBSPCompiler compiler, DBSPCircuit circuit, Iterable<DBSPUnaryOperator> readers) {
        var json = JsonStream.compactCircuit();
        var inner = new OptimizerExpressionWriter(compiler, json, 0, false);
        json.beginObject().appendClass(circuit).label("id").append(circuit.id);
        json.label("declarations").beginArray();
        json.endArray().label("allOperators").beginArray();
        for (var reader : readers) {
            json.beginObject().appendClass(reader).label("id").append(reader.id).label("function");
            reader.getFunction().accept(inner);
            json.endObject();
        }
        json.endArray();
        inner.writeDeferredDefinitions();
        json.endObject();
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    static byte[] encode(DBSPCompiler compiler, DBSPCircuit circuit, Roots roots) {
        var json = JsonStream.compactCircuit();
        var inner = new OptimizerExpressionWriter(compiler, json, 1, true);
        json.beginObject().appendClass(circuit).label("id").append(circuit.id);
        json.label("declarations").beginArray();
        for (var declaration : circuit.declarations) {
            if ((roots == Roots.INITIALIZERS || roots == Roots.DECLARED_FUNCTIONS)
                    && declaration.item instanceof DBSPFunctionItem) {
                json.beginObject().appendClass(declaration).label("id").append(declaration.id);
                declaration.accept(inner);
                json.endObject();
            }
        }
        json.endArray().label("allOperators").beginArray();
        if (roots != Roots.DECLARED_FUNCTIONS)
            writeOperators(circuit.getAllOperators(), roots, inner);
        json.endArray();
        inner.writeDeferredDefinitions();
        json.endObject();
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void writeOperators(Iterable<DBSPOperator> operators, Roots roots, ToJsonInnerVisitor inner) {
        var json = inner.stream;
        for (var op : operators) {
            if (op instanceof DBSPNestedOperator nested) {
                writeOperators(nested.getAllOperators(), roots, inner);
            } else if (roots == Roots.FILTER_PREDICATES && op instanceof DBSPFilterOperator filter) {
                json.beginObject().appendClass(filter).label("id").append(filter.id).label("function");
                filter.getFunction().accept(inner);
                json.endObject();
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
