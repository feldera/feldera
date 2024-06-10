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

/**
 * This visitor dumps the circuit to a dot file, so it can be visualized.
 * A utility method can create a jpg or png or other format supported by dot.
 */
public class ToDotVisitor extends CircuitVisitor implements IWritesLogs {
    private final IndentStream stream;
    // If true show code, otherwise just topology
    private final boolean details;
    private final Set<DBSPOperator> edgesLabeled;

    public ToDotVisitor(IErrorReporter reporter, IndentStream stream, boolean details) {
        super(reporter);
        this.stream = stream;
        this.details = details;
        this.edgesLabeled = new HashSet<>();
    }

    @Override
    public VisitDecision preorder(DBSPSourceBaseOperator node) {
        String name = node.tableName;
        if (node.is(DBSPDelayOutputOperator.class))
            name = "delay";
        this.stream.append(node.getOutputName())
                .append(" [ shape=box,label=\"")
                .append(node.getIdString())
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
                .append(" ")
                .append(getFunction(node))
                .append("\" ]")
                .newline();
        return VisitDecision.STOP;
    }

    void addInputs(DBSPOperator node) {
        for (DBSPOperator input: node.inputs) {
            this.stream.append(input.getOutputName())
                    .append(" -> ")
                    .append(node.getOutputName());
            if (this.details && !this.edgesLabeled.contains(input)) {
                this.stream.append(" [label=")
                        .append(Utilities.doubleQuote(input.getOutputRowType().toString()))
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
            expression = LowerCircuitVisitor.rewriteFlatmap(expression.to(DBSPFlatmap.class));
        }
        String result = ToRustInnerVisitor.toRustString(
                this.errorReporter, expression, CompilerOptions.getDefault(), true);
        result = result.replace("\n", "\\l");
        return Utilities.escapeDoubleQuotes(result);
    }

    String getColor(DBSPOperator operator) {
        return switch (operator.operation) {
            case "waterline_monotonic" -> " style=filled fillcolor=lightgreen";
            case "controlled_filter" -> " style=filled fillcolor=cyan";
            case "apply" -> " style=filled fillcolor=yellow";
            case "integrate_trace_retain_keys", "integrate_trace_retain_values" -> " style=filled fillcolor=pink";
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
                .append(" ")
                .append(node.operation)
                .append(node.comment != null ? node.comment : "");
        if (this.details) {
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
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPPartialCircuit circuit) {
        this.stream.decrease()
                .append("}")
                .newline();
    }

    public static void toDot(IErrorReporter reporter, String fileName, boolean details,
                             @Nullable String outputFormat, DBSPCircuit circuit) {
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
            circuit.accept(new ToDotVisitor(reporter, stream, details));
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
}
