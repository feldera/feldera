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

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.OperatorPort;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import java.util.HashSet;
import java.util.Set;

/** This visitor dumps the edges circuit to a dot filed. */
public class ToDotEdgesVisitor extends CircuitVisitor implements IWritesLogs {
    protected final IndentStream stream;
    // A higher value -> more details
    protected final int details;
    protected final Set<DBSPOperator> edgesLabeled;

    public ToDotEdgesVisitor(IErrorReporter reporter, IndentStream stream, int details) {
        super(reporter);
        this.stream = stream;
        this.details = details;
        this.edgesLabeled = new HashSet<>();
    }

    public String getEdgeLabel(OperatorPort source) {
        DBSPType type = source.getOutputRowType();
        return ToRustInnerVisitor.toRustString(
                this.errorReporter, type, CompilerOptions.getDefault(), true);
    }

    @Override
    public void startVisit(IDBSPOuterNode node) {
        super.startVisit(node);
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator node) {
        for (OperatorPort i : node.inputs) {
            DBSPSimpleOperator input = i.node().to(DBSPSimpleOperator.class);
            if (input.is(DBSPSourceViewDeclarationOperator.class)) {
                assert this.circuit != null;
                input = input.to(DBSPSourceViewDeclarationOperator.class)
                        .getCorrespondingView(this.getParent());
            }
            this.stream.append(input.getOutputName())
                    .append(" -> ")
                    .append(node.getOutputName());
            if (this.details >= 2 && !this.edgesLabeled.contains(input)) {
                String label = this.getEdgeLabel(i);
                this.stream.append(" [xlabel=")
                        .append(Utilities.doubleQuote(label))
                        .append("]");
                this.edgesLabeled.add(input);
            }
            this.stream.append(";")
                    .newline();
        }
        return VisitDecision.STOP;
    }

    public interface VisitorConstructor {
        CircuitVisitor create(IndentStream stream);
    }
}
