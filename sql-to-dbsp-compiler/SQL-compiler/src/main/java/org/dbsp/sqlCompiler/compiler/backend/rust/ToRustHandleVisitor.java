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
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

/**
 * Generate Rust for a circuit, but with an API using handles.
 * Output generated has this structure:
 * pub fn test_circuit(workers: usize) -> (DBSPHandle, Catalog) {
 *     let (circuit, catalog) = Runtime::init_circuit(workers, |circuit| {
 *         let mut catalog = Catalog::new();
 *         let (input, handle0) = circuit.add_input_zset::<TestStruct, i32>();
 *         catalog.register_input_zset("test_input1", input, handles.0);
 *         catalog.register_output_zset("test_output1", input);
 *         Ok(catalog)
 *     }).unwrap();
 *     (circuit, catalog)
 * }
 */
public class ToRustHandleVisitor extends ToRustVisitor {
    private final String functionName;
    int inputHandleIndex = 0;

    /**
     * @param reporter      Used to report errors.
     * @param builder       Output is constructed here.
     * @param functionName  Name of generated function.
     */
    public ToRustHandleVisitor(IErrorReporter reporter, IndentStream builder, String functionName) {
        super(reporter, builder);
        this.functionName = functionName;
    }

    void generateFromTrait(DBSPTypeStruct type) {
        DBSPTypeTuple tuple = type.toTuple();
        this.builder.append("impl From<")
                .append(type.sanitizedName)
                .append("> for ");
        tuple.accept(this.innerVisitor);
        this.builder.append(" {")
                .increase()
                .append("fn from(table: ")
                .append(type.sanitizedName)
                .append(") -> Self");
        this.builder.append(" {")
                .increase()
                .append(tuple.getName())
                .append("::new(");
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            this.builder.append("table.")
                    .append(field.sanitizedName)
                    .append(",");
        }
        this.builder.append(")").newline();
        this.builder.decrease()
                .append("}")
                .newline()
                .decrease()
                .append("}")
                .newline();

        this.builder.append("impl From<");
        tuple.accept(this.innerVisitor);
        this.builder.append("> for ")
                .append(type.sanitizedName);
        this.builder.append(" {")
                .increase()
                .append("fn from(tuple: ");
        tuple.accept(this.innerVisitor);
        this.builder.append(") -> Self");
        this.builder.append(" {")
                .increase()
                .append("Self {")
                .increase();
        int index = 0;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            this.builder
                .append(field.sanitizedName)
                .append(": tuple.")
                .append(index++)
                .append(",")
                .newline();
        }
        this.builder.decrease().append("}").newline();
        this.builder.decrease()
                .append("}")
                .newline()
                .decrease()
                .append("}")
                .newline();
    }

    private void generateRenameMacro(String tableName, DBSPTypeStruct type) {
        this.builder.append("deserialize_table_record!(");
        this.builder.append(type.sanitizedName)
                .append("[")
                .append(Utilities.doubleQuote(tableName))
                .append(", ")
                .append(type.fields.size())
                .append("] {")
                .increase();
        boolean first = true;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            if (!first)
                this.builder.append(",").newline();
            first = false;
            this.builder.append("(")
                    .append(field.sanitizedName)
                    .append(", ")
                    .append(Utilities.doubleQuote(field.name))
                    .append(", ")
                    .append(Boolean.toString(field.nameIsQuoted))
                    .append(", ");
            field.type.accept(this.innerVisitor);
            this.builder.append(", ")
                    .append(field.type.mayBeNull ? "Some(None)" : "None")
                    .append(")");
        }
        this.builder.newline()
                .decrease()
                .append("});")
                .newline();
    }

    @Override
    public VisitDecision preorder(DBSPSourceOperator operator) {
        DBSPTypeStruct type = operator.originalRowType;
        DBSPStructItem item = new DBSPStructItem(type);
        item.accept(this.innerVisitor);
        this.generateFromTrait(type);
        this.generateRenameMacro(operator.outputName, type);

        this.writeComments(operator)
                .append("let (")
                .append(operator.outputName)
                .append(", handle")
                .append(this.inputHandleIndex++)
                .append(") = circuit.add_input_set::<");

        DBSPTypeZSet zsetType = operator.getType().to(DBSPTypeZSet.class);
        zsetType.elementType.accept(this.innerVisitor);
        this.builder.append(", ");
        zsetType.weightType.accept(this.innerVisitor);
        this.builder.append(">();");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
        DBSPTypeStruct type = operator.originalRowType;
        DBSPStructItem item = new DBSPStructItem(type);
        item.accept(this.innerVisitor);
        this.generateFromTrait(type);
        this.generateRenameMacro(operator.outputName, type);
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

        // Register input streams in the catalog.
        int index = 0;
        for (DBSPSourceOperator i : circuit.inputOperators) {
            this.builder.append("catalog.register_input_set::<_, ");
            i.originalRowType.accept(this.innerVisitor);
            this.builder.append(">(")
                    .append(Utilities.doubleQuote(i.getName()))
                    .append(", ")
                    .append(i.outputName)
                    .append(".clone(), handle")
                    .append(index++)
                    .append(");")
                    .newline();
        }

        // Register output streams in the catalog.
        for (DBSPSinkOperator o : circuit.outputOperators) {
            this.builder.append("catalog.register_output_zset::<_, ");
            o.originalRowType.accept(this.innerVisitor);
            this.builder.append(">(")
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

    public static String toRustString(
            IErrorReporter reporter, IDBSPOuterNode node, String functionName) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustVisitor visitor = new ToRustHandleVisitor(reporter, stream, functionName);
        node.accept(visitor);
        return builder.toString();
    }
}
