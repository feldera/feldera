package org.dbsp.sqlCompiler.compiler.backend.dot;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerCircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

/** Visitor which emits the circuit nodes in a graphviz file */
public class ToDotNodesVisitor extends CircuitVisitor {
    protected final IndentStream stream;
    // A higher value -> more details
    protected final int details;

    public ToDotNodesVisitor(IErrorReporter reporter, IndentStream stream, int details) {
        super(reporter);
        this.stream = stream;
        this.details = details;
    }

    static String isMultiset(DBSPSimpleOperator operator) {
        return operator.isMultiset ? "" : "*";
    }

    static String annotations(DBSPSimpleOperator operator) {
        return operator.annotations.toDotString();
    }

    @Override
    public VisitDecision preorder(DBSPSourceBaseOperator node) {
        String name = node.operation;
        this.stream.append(node.getOutputName())
                .append(" [ shape=box style=filled fillcolor=lightgrey label=\"")
                .append(node.getIdString())
                .append(isMultiset(node))
                .append(annotations(node))
                .append(" ")
                .append(name)
                .append("\" ]")
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstantOperator node) {
        this.stream.append(node.getOutputName())
                .append(" [ shape=box,label=\"")
                .append(node.getIdString())
                .append(isMultiset(node))
                .append(annotations(node))
                .append(" ")
                .append(getFunction(node))
                .append("\" ]")
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPViewBaseOperator node) {
        this.stream.append(node.getOutputName())
                .append(" [ shape=box,label=\"")
                .append(node.getIdString())
                .append(isMultiset(node))
                .append(annotations(node))
                .append(" ")
                .append(node.viewName)
                .append("\"")
                .append(" style=filled fillcolor=lightgrey")
                .append("]")
                .newline();
        return VisitDecision.STOP;
    }

    String convertFunction(DBSPExpression expression) {
        String result = ToRustInnerVisitor.toRustString(
                this.errorReporter, expression, CompilerOptions.getDefault(), true);
        result = result.replace("\n", "\\l");
        return Utilities.escapeDoubleQuotes(result);
    }

    String getFunction(DBSPSimpleOperator node) {
        DBSPExpression expression = node.function;
        if (node.is(DBSPAggregateOperatorBase.class)) {
            DBSPAggregateOperatorBase aggregate = node.to(DBSPAggregateOperatorBase.class);
            if (aggregate.aggregate != null) {
                expression = aggregate.aggregate.compact(this.errorReporter);
            }
        } else if (node.is(DBSPPartitionedRollingAggregateWithWaterlineOperator.class)) {
            DBSPPartitionedRollingAggregateWithWaterlineOperator aggregate =
                    node.to(DBSPPartitionedRollingAggregateWithWaterlineOperator.class);
            if (aggregate.aggregate != null) {
                expression = aggregate.aggregate.compact(this.errorReporter);
            }
        }
        if (expression == null)
            return "";
        if (node.is(DBSPFlatMapOperator.class)) {
            if (expression.is(DBSPFlatmap.class)) {
                expression = LowerCircuitVisitor.rewriteFlatmap(expression.to(DBSPFlatmap.class));
            }
        }
        if (node.is(DBSPJoinFilterMapOperator.class)) {
            expression = LowerCircuitVisitor.lowerJoinFilterMapFunctions(
                    this.errorReporter,
                    node.to(DBSPJoinFilterMapOperator.class));
        }
        return this.convertFunction(expression);
    }

    String getColor(DBSPSimpleOperator operator) {
        return switch (operator.operation) {
            case "waterline" -> " style=filled fillcolor=lightgreen";
            case "controlled_filter" -> " style=filled fillcolor=cyan";
            case "apply", "apply2" -> " style=filled fillcolor=yellow";
            case "integrate_trace_retain_keys",
                 "partitioned_rolling_aggregate_with_waterline", "window",
                 "integrate_trace_retain_values" -> " style=filled fillcolor=pink";
            // stateful operators
            case "distinct",
                 // all aggregates require an upsert, which is stateful, even the ones that are linear
                 "aggregate", "partitioned_rolling_aggregate",
                 "stream_aggregate", "chain_aggregate",
                 // some joins require integrators
                 "join", "join_flatmap", "asof_join", "join_index",
                 // delays contain state, but not that much
                 "delay_trace", // "delay", "differentiate",
                 // group operators
                 "topK", "lag_custom_order", "upsert",
                 "integrate" -> " style=filled fillcolor=orangered";
            default -> "";
        };
    }

    String shorten(String operation) {
        if (operation.startsWith("integrate_trace"))
            return operation.substring("integrate_trace_".length());
        if (operation.equals("aggregate_linear_postprocess"))
            return "aggregate_linear";
        return operation;
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator node) {
        this.stream.append(node.getOutputName())
                .append(" [ shape=box")
                .append(this.getColor(node))
                .append(" label=\"")
                .append(node.getIdString())
                .append(isMultiset(node))
                .append(annotations(node))
                .append(" ")
                .append(shorten(node.operation))
                .append(node.comment != null ? node.comment : "");
        if (this.details > 3) {
            this.stream
                    .append("(")
                    .append(this.getFunction(node))
                    .append(")\\l");
        }
        this.stream.append("\" ]")
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator node) {
        this.stream.append("subgraph cluster_")
                .append(node.id)
                .append(" {").increase()
                .append("color=black;")
                .newline();
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPNestedOperator node) {
        this.stream.decrease().append("}").newline();
    }

    @Override
    public VisitDecision preorder(DBSPWaterlineOperator node) {
        this.stream.append(node.getOutputName())
                .append(" [ shape=box")
                .append(this.getColor(node))
                .append(" label=\"")
                .append(node.getIdString())
                .append(isMultiset(node))
                .append(annotations(node))
                .append(" ")
                .append(shorten(node.operation))
                .append(node.comment != null ? node.comment : "");
        if (this.details > 3) {
            this.stream
                    .append("(")
                    .append(this.convertFunction(node.init))
                    .append(", ")
                    .append(this.convertFunction(node.extractTs))
                    .append(", ")
                    .append(this.getFunction(node))
                    .append(")\\l");
        }
        this.stream.append("\" ]")
                .newline();
        return VisitDecision.STOP;
    }
}