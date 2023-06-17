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

package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

/**
 * Generate Rust for a circuit, but with an API using handles.
 * Output generated has this structure:
 * pub fn test_circuit(workers: usize) -> (DBSPHandle, Catalog) {
 *     let mut catalog = Catalog::new();
 *     let (circuit, handles) = Runtime::init_circuit(workers, |circuit| {
 *         let (input, handle0) = circuit.add_input_zset::<TestStruct, i32>();
 *         let handle1 = input.output();
 *         (handle0, handle1)
 *     }).unwrap();
 *     catalog.register_input_zset_handle("test_input1", handles.0);
 *     catalog.register_output_batch_handle("test_output1", handles.1);
 *     (circuit, catalog)
 * }
 */
public class ToRustHandleVisitor extends ToRustVisitor {
    private final String functionName;
    int inputHandleIndex = 0;
    int outputHandleIndex = 0;

    public ToRustHandleVisitor(IErrorReporter reporter, IndentStream builder, String functionName) {
        super(reporter, builder);
        this.functionName = functionName;
    }

    @Override
    public VisitDecision preorder(DBSPSourceOperator operator) {
        this.writeComments(operator)
                .append("let (")
                .append(operator.outputName)
                .append(", handle")
                .append(this.inputHandleIndex++)
                .append(") = circuit.add_input_zset::<");
        DBSPTypeZSet type = operator.getType().to(DBSPTypeZSet.class);
        type.elementType.accept(this.innerVisitor);
        this.builder.append(", ");
        type.weightType.accept(this.innerVisitor);
        this.builder.append(">();");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        this.setCircuit(circuit);
        circuit.circuit.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        this.outputHandleIndex = circuit.getInputCount();
        this.builder.append("pub fn ")
                .append(this.functionName)
                .append("(workers: usize) -> (DBSPHandle, Catalog) {")
                .increase()
                .newline()
                .append("let (circuit, catalog) = Runtime::init_circuit(workers, |circuit| {")
                .increase()
                .append("let mut catalog = Catalog::new();");

        for (IDBSPNode node : circuit.getAllOperators())
            super.processNode(node);

        int index = 0;
        for (DBSPOperator i : circuit.inputOperators) {
            this.builder.append("catalog.register_input_zset(")
                    .append(Utilities.doubleQuote(i.getName()))
                    .append(", ")
                    .append(i.outputName)
                    .append(".clone(), handle")
                    .append(index++)
                    .append(");")
                    .newline();
        }
        for (DBSPSinkOperator o : circuit.outputOperators) {
            this.builder.append("catalog.register_output_zset(")
                    .append(Utilities.doubleQuote(o.getName()))
                    .append(", ")
                    .append(o.input().getName())
                    .append(");")
                    .newline();
        }

        this.builder.append("Ok(catalog)")
                .newline()
                .decrease()
                .append("}).unwrap();")
                .newline();

        this.builder
                .append("(circuit, catalog)")
                .newline()
                .decrease()
                .append("}")
                .newline();
        return VisitDecision.STOP;
    }

    public static String toRustString(IErrorReporter reporter, IDBSPOuterNode node, String functionName) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustVisitor visitor = new ToRustHandleVisitor(reporter, stream, functionName);
        node.accept(visitor);
        return builder.toString();
    }
}
