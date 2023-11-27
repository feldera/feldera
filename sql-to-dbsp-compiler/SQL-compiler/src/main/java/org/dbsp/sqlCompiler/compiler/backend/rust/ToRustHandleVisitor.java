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

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

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

    /**
     * Generate calls to the Rust macros that generate serialization and deserialization code
     * for the struct.
     *
     * @param tableName Table or view whose type is being described.
     * @param type      Type of record in the table.
     * @param metadata  Metadata for the input columns (null for an output view).
     */
    private void generateRenameMacro(String tableName, DBSPTypeStruct type,
                                     @Nullable List<InputColumnMetadata> metadata) {
        this.builder.append("deserialize_table_record!(");
        this.builder.append(type.sanitizedName)
                .append("[")
                .append(Utilities.doubleQuote(tableName))
                .append(", ")
                .append(type.fields.size())
                .append("] {")
                .increase();
        boolean first = true;
        int index = 0;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            if (!first)
                this.builder.append(",").newline();
            first = false;
            String name = field.name;
            boolean quoted = field.nameIsQuoted;
            InputColumnMetadata meta = null;
            if (metadata == null) {
                // output
                name = name.toUpperCase();
                quoted = false;
            } else {
                meta = metadata.get(index);
            }
            this.builder.append("(")
                    .append(field.sanitizedName)
                    .append(", ")
                    .append(Utilities.doubleQuote(name))
                    .append(", ")
                    .append(Boolean.toString(quoted))
                    .append(", ");
            field.type.accept(this.innerVisitor);
            this.builder.append(", ");
            if (meta == null || meta.defaultValue == null) {
                this.builder.append(field.type.mayBeNull ? "Some(None)" : "None");
            } else {
                this.builder.append("Some(");
                meta.defaultValue.accept(this.innerVisitor);
                this.builder.append(")");
            }
            this.builder.append(")");
            index++;
        }
        this.builder.newline()
                .decrease()
                .append("});")
                .newline();

        this.builder.append("serialize_table_record!(");
        this.builder.append(type.sanitizedName)
                .append("[")
                .append(type.fields.size())
                .append("]{")
                .increase();
        first = true;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            if (!first)
                this.builder.append(",").newline();
            first = false;
            String name = field.name;
            if (metadata == null) {
                // output
                name = name.toUpperCase(Locale.ENGLISH);
            }
            this.builder
                    .append(field.sanitizedName)
                    .append("[")
                    .append(Utilities.doubleQuote(name))
                    .append("]")
                    .append(": ");
            field.type.accept(this.innerVisitor);
        }
        this.builder.newline()
                .decrease()
                .append("});")
                .newline();
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        DBSPTypeStruct type = operator.originalRowType;
        DBSPStructItem item = new DBSPStructItem(type);
        item.accept(this.innerVisitor);
        this.generateFromTrait(type);
        this.generateRenameMacro(operator.outputName, type, operator.metadata);

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
    public VisitDecision preorder(DBSPSourceMapOperator operator) {
        DBSPTypeStruct type = operator.originalRowType;
        // Generate struct type definition
        DBSPStructItem item = new DBSPStructItem(type);
        item.accept(this.innerVisitor);
        // Traits for serialization
        this.generateFromTrait(type);
        // Macro for serialization
        this.generateRenameMacro(operator.outputName, type, operator.metadata);
        // Generate key type definition
        item = new DBSPStructItem(operator.getKeyStructType());
        item.accept(this.innerVisitor);
        // Trait for serialization
        this.generateFromTrait(operator.getKeyStructType());
        this.generateRenameMacro(item.type.name, item.type, operator.metadata);

        this.writeComments(operator)
                .append("let (")
                .append(operator.outputName)
                .append(", handle")
                .append(this.inputHandleIndex++)
                .append(") = circuit.add_input_map::<");

        DBSPTypeIndexedZSet ix = operator.getOutputIndexedZSetType();
        ix.keyType.accept(this.innerVisitor);
        this.builder.append(", ");
        ix.elementType.accept(this.innerVisitor);
        this.builder.append(", ");
        ix.weightType.accept(this.innerVisitor);
        this.builder.append(">();");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
        DBSPTypeStruct type = operator.originalRowType;
        DBSPStructItem item = new DBSPStructItem(type);
        item.accept(this.innerVisitor);
        this.generateFromTrait(type);
        this.generateRenameMacro(operator.outputName, type, null);
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
                .append("(workers: usize) -> Result<(DBSPHandle, Catalog), DBSPError> {")
                .increase()
                .newline()
                .append("let (circuit, catalog) = Runtime::init_circuit(workers, |circuit| {")
                .increase()
                .append("let mut catalog = Catalog::new();")
                .newline();

        for (IDBSPNode node : circuit.getAllOperators())
            super.processNode(node);

        // Register input streams in the catalog.
        int index = 0;
        for (DBSPSourceBaseOperator i : circuit.inputOperators) {
            if (i.is(DBSPSourceMultisetOperator.class)) {
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
            } else {
                DBSPSourceMapOperator map = i.to(DBSPSourceMapOperator.class);
                this.builder.append("catalog.register_input_map::<");
                DBSPTypeStruct keyStructType = map.getKeyStructType();
                keyStructType.toTuple().accept(this.innerVisitor);
                this.builder.append(", ");
                keyStructType.accept(this.innerVisitor);
                this.builder.append(", ");
                map.getOutputIndexedZSetType().elementType.accept(this.innerVisitor);
                this.builder.append(", ");
                map.originalRowType.accept(this.innerVisitor);
                this.builder.append(", ");
                map.getOutputIndexedZSetType().weightType.accept(this.innerVisitor);
                this.builder.append(", _>(")
                        .append(Utilities.doubleQuote(i.getName()))
                        .append(", ")
                        .append(i.outputName)
                        .append(".clone(), handle")
                        .append(index++)
                        .append(", ");
                map.getKeyFunc().accept(this.innerVisitor);
                this.builder.append(");")
                        .newline();
            }
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
                .append("})?;")
                .newline();

        this.builder
                .append("Ok((circuit, catalog))")
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
