/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOutputOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.rust.LowerCircuitVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

/** This visitor dumps the circuit to a dot file, so it can be visualized.
 * A utility method creates a jpg or png or other format supported by dot. */
public class ToDotVisitor extends CircuitVisitor implements IWritesLogs {
    protected final IndentStream stream;
    // A higher value -> more details
    protected final int details;
    protected final Set<DBSPOperator> edgesLabeled;

    public ToDotVisitor(IErrorReporter reporter, IndentStream stream, int details) {
        super(reporter);
        this.stream = stream;
        this.details = details;
        this.edgesLabeled = new HashSet<>();
    }

    static String isMultiset(DBSPOperator operator) {
        return operator.isMultiset ? "" : "*";
    }

    static String annotations(DBSPOperator operator) {
        if (operator.annotations.isEmpty())
            return "";
        return " " + operator.annotations;
    }

    @Override
    public VisitDecision preorder(DBSPSourceBaseOperator node) {
        String name = node.operation;
        if (node.is(DBSPDelayOutputOperator.class))
            name = "delay";
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

    public String getEdgeLabel(DBSPOperator source) {
        return source.getOutputRowType().toString();
    }

    void addInputs(DBSPOperator node) {
        for (DBSPOperator input : node.inputs) {
            this.stream.append(input.getOutputName())
                    .append(" -> ")
                    .append(node.getOutputName());
            if (this.details >= 2 && !this.edgesLabeled.contains(input)) {
                this.stream.append(" [xlabel=")
                        .append(Utilities.doubleQuote(this.getEdgeLabel(input)))
                        .append("]");
                this.edgesLabeled.add(input);
            }
            this.stream.append(";")
                    .newline();
        }
    }

    @Override
    public VisitDecision preorder(DBSPDelayOperator node) {
        DBSPOperator input = node.input();
        if (node.output != null) {
            // Add the edge which isn't represented explicitly in the graph
            this.stream.append(input.getOutputName())
                    .append(" -> ")
                    .append(node.output.getOutputName())
                    .append(";")
                    .newline();
            return VisitDecision.STOP;
        }
        return this.preorder((DBSPUnaryOperator) node);
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
        this.addInputs(node);
        return VisitDecision.STOP;
    }

    String getFunction(DBSPOperator node) {
        DBSPExpression expression = node.function;
        if (node.is(DBSPAggregateOperatorBase.class)) {
            DBSPAggregateOperatorBase aggregate = node.to(DBSPAggregateOperatorBase.class);
            if (aggregate.aggregate != null) {
                DBSPAggregate.Implementation impl = aggregate.aggregate.combine(this.errorReporter);
                expression = impl.asFold(true);
            }
        } else if (node.is(DBSPPartitionedRollingAggregateWithWaterlineOperator.class)) {
            DBSPPartitionedRollingAggregateWithWaterlineOperator aggregate =
                    node.to(DBSPPartitionedRollingAggregateWithWaterlineOperator.class);
            if (aggregate.aggregate != null) {
                DBSPAggregate.Implementation impl = aggregate.aggregate.combine(this.errorReporter);
                expression = impl.asFold(true);
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
        String result = ToRustInnerVisitor.toRustString(
                this.errorReporter, expression, CompilerOptions.getDefault(), true);
        result = result.replace("\n", "\\l");
        return Utilities.escapeDoubleQuotes(result);
    }

    String getColor(DBSPOperator operator) {
        return switch (operator.operation) {
            case "waterline" -> " style=filled fillcolor=lightgreen";
            case "controlled_filter" -> " style=filled fillcolor=cyan";
            case "apply", "apply2" -> " style=filled fillcolor=yellow";
            case "integrate_trace_retain_keys",
                 "partitioned_rolling_aggregate_with_waterline",
                 "integrate_trace_retain_values" -> " style=filled fillcolor=pink";
            // stateful operators
            case "distinct",
                 // all aggregates require an upsert, which is stateful, even the ones that are linear
                 "aggregate", "partitioned_rolling_aggregate", "aggregate_linear",
                 "stream_aggregate", "stream_aggregate_linear",
                 "partitioned_tree_aggregate",
                 // some joins require integrators
                 "join", "join_flatmap",
                 // delays contain state
                 "delay_trace", "delay", "differentiate",
                 // group operators
                 "topK", "lag_custom_order", "upsert",
                 "integrate" -> " style=filled fillcolor=red";
            default -> "";
        };
    }

    @Override
    public VisitDecision preorder(DBSPOperator node) {
        this.stream.append(node.getOutputName())
                .append(" [ shape=box")
                .append(this.getColor(node))
                .append(" label=\"")
                .append(node.getIdString())
                .append(isMultiset(node))
                .append(annotations(node))
                .append(" ")
                .append(node.operation)
                .append(node.comment != null ? node.comment : "");
        if (this.details > 3) {
            this.stream
                    .append("(")
                    .append(this.getFunction(node))
                    .append(")\\l");
        }
        this.stream.append("\" ]")
                .newline();
        this.addInputs(node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        this.setCircuit(circuit);
        this.stream.append("digraph ")
                .append(circuit.name);
        circuit.circuit.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        this.stream.append("{")
                .increase();
        this.stream.append("ordering=\"in\"").newline();
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPPartialCircuit circuit) {
        this.stream.decrease()
                .append("}")
                .newline();
    }

    public interface VisitorConstructor {
        CircuitVisitor create(IndentStream stream);
    }

    public static void toDot(String fileName,
                      @Nullable String outputFormat, DBSPCircuit circuit, VisitorConstructor constructor) {
        if (circuit.isEmpty())
            return;
        System.out.println("Writing circuit to " + fileName);
        Logger.INSTANCE.belowLevel("ToDotVisitor", 1)
                .append("Writing circuit to ")
                .append(fileName)
                .newline();
        File tmp = null;
        try {
            tmp = File.createTempFile("tmp", ".dot");
            tmp.deleteOnExit();
            PrintWriter writer = new PrintWriter(tmp.getAbsolutePath());
            IndentStream stream = new IndentStream(writer);
            CircuitVisitor visitor = constructor.create(stream);
            circuit.accept(visitor);
            writer.close();
            if (outputFormat != null)
                Utilities.runProcess(".", "dot", "-T", outputFormat,
                        "-o", fileName, tmp.getAbsolutePath());
        } catch (Exception ex) {
            if (tmp != null) {
                try {
                    System.out.println(Utilities.readFile(tmp.toPath()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            throw new RuntimeException(ex);
        }
    }

    public static void toDot(IErrorReporter reporter, String fileName, int details,
                             @Nullable String outputFormat, DBSPCircuit circuit) {
        toDot(fileName, outputFormat, circuit, stream -> new ToDotVisitor(reporter, stream, details));
    }
}
