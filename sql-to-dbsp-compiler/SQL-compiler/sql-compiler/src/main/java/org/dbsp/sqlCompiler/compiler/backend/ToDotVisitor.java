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

import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
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
import java.io.PrintWriter;

/**
 * This visitor dumps the circuit to a dot file, so it can be visualized.
 * A utility method can create a jpg or png or other format supported by dot.
 */
public class ToDotVisitor extends CircuitVisitor implements IWritesLogs {
    private final IndentStream stream;

    public ToDotVisitor(IErrorReporter reporter, IndentStream stream) {
        super(reporter);
        this.stream = stream;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator node) {
        this.stream.append(node.outputName)
                .append(" [ shape=box,label=\"")
                .append(node.id)
                .append(" ")
                .append(node.outputName)
                .append("\" ]")
                .newline();
        return VisitDecision.STOP;
    }

    void addInputs(DBSPOperator node) {
        for (DBSPOperator input: node.inputs) {
            this.stream.append(input.outputName)
                    .append(" -> ")
                    .append(node.outputName)
                    .append(";")
                    .newline();
        }
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator node) {
        this.stream.append(node.outputName)
                .append(" [ shape=box,label=\"")
                .append(node.id)
                .append(" ")
                .append(node.outputName)
                .append("\" ]")
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
        }
        if (expression == null)
            return "";
        // Do some manually some lowering.
        if (node.is(DBSPFlatMapOperator.class)) {
            expression = LowerCircuitVisitor.rewriteFlatmap(expression.to(DBSPFlatmap.class));
        }
        String function = ToRustInnerVisitor.toRustString(this.errorReporter, expression, true);
        // Graphviz left-justify using \l.
        String result = function.replace("\n", "\\l");
        return Utilities.escape(result);
    }

    @Override
    public VisitDecision preorder(DBSPOperator node) {
        this.stream.append(node.outputName)
                .append(" [ shape=box,label=\"")
                .append(node.id)
                .append(" ")
                .append(node.operation)
                .append("(")
                .append(this.getFunction(node))
                // For some reason there needs to be one \\l at the very end.
                .append(")\\l\" ]")
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

    public static void toDot(IErrorReporter reporter, String fileName,
                             @Nullable String outputFormat, DBSPCircuit circuit) {
        try {
            Logger.INSTANCE.belowLevel("ToDotVisitor", 1)
                    .append("Writing circuit to ")
                    .append(fileName)
                    .newline();
            File tmp = File.createTempFile("tmp", ".dot");
            tmp.deleteOnExit();
            PrintWriter writer = new PrintWriter(tmp.getAbsolutePath());
            IndentStream stream = new IndentStream(writer);
            circuit.accept(new ToDotVisitor(reporter, stream));
            writer.close();
            if (outputFormat != null)
                Utilities.runProcess(".", "dot", "-T", outputFormat,
                        "-o", fileName, tmp.getAbsolutePath());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
