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
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowAggregateOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.InputTableMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.NameGen;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** This visitor generates a Rust implementation of a circuit. */
public class ToRustVisitor extends CircuitVisitor {
    protected final IndentStream builder;
    // Assemble here the list of streams that is output by the circuit
    protected final IndentStream streams;
    final InnerVisitor innerVisitor;
    final boolean useHandles;
    final CompilerOptions options;
    final ProgramMetadata metadata;

    /* Example output generated when 'generateCatalog' is true:
     * pub fn test_circuit(workers: usize) -> (DBSPHandle, Catalog) {
     *     let (circuit, catalog) = Runtime::init_circuit(workers, |circuit| {
     *         let mut catalog = Catalog::new();
     *         let (input, handle0) = circuit.add_input_zset::<TestStruct, i32>();
     *         catalog.register_input_zset(input, handles.0, input0_metadata);
     *         catalog.register_output_zset(input, output0_metadata);
     *         Ok(catalog)
     *     }).unwrap();
     *     (circuit, catalog)
     * }
     */

    public ToRustVisitor(IErrorReporter reporter, IndentStream builder,
                         CompilerOptions options, ProgramMetadata metadata) {
        super(reporter);
        this.options = options;
        this.builder = builder;
        this.innerVisitor = new ToRustInnerVisitor(reporter, builder, false);
        this.useHandles = this.options.ioOptions.emitHandles;
        StringBuilder streams = new StringBuilder();
        this.streams = new IndentStream(streams);
        this.metadata = metadata;
    }

    protected void generateFromTrait(DBSPTypeStruct type) {
        DBSPTypeTuple tuple = type.toTuple();
        this.builder.append("impl From<")
                .append(type.sanitizedName)
                .append("> for ");
        tuple.accept(this.innerVisitor);
        this.builder.append(" {")
                .increase()
                .append("fn from(table: r#")
                .append(type.sanitizedName)
                .append(") -> Self");
        this.builder.append(" {")
                .increase()
                .append(tuple.getName())
                .append("::new(");
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            this.builder.append("table.r#")
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
        this.builder.append("> for r#")
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
                    .append("r#")
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
    protected void generateRenameMacro(String tableName, DBSPTypeStruct type,
                                      @Nullable InputTableMetadata metadata) {
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
            DBSPTypeUser user = field.type.as(DBSPTypeUser.class);
            boolean isOption = user != null && user.name.equals("Option");
            if (!first)
                this.builder.append(",").newline();
            first = false;
            String name = field.name;
            boolean quoted = field.nameIsQuoted;
            InputColumnMetadata meta = null;
            if (metadata == null) {
                // output
                switch (this.options.languageOptions.unquotedCasing) {
                    case "upper":
                        name = name.toUpperCase(Locale.ENGLISH);
                        break;
                    case "lower":
                        name = name.toLowerCase(Locale.ENGLISH);
                        break;
                    default:
                        break;
                }
                quoted = false;
            } else {
                meta = metadata.getColumnMetadata(field.name);
            }
            this.builder.append("(")
                    .append("r#")
                    .append(field.sanitizedName)
                    .append(", ")
                    .append(Utilities.doubleQuote(name))
                    .append(", ")
                    .append(Boolean.toString(quoted))
                    .append(", ");
            field.type.accept(this.innerVisitor);
            this.builder.append(", ");
            if (isOption)
                this.builder.append("Some(");
            if (meta == null || meta.defaultValue == null) {
                this.builder.append(field.type.mayBeNull ? "Some(None)" : "None");
            } else {
                this.builder.append("Some(");
                meta.defaultValue.accept(this.innerVisitor);
                this.builder.append(")");
            }
            if (isOption)
                this.builder.append(")");

            if (isOption && user.typeArgs[0].mayBeNull) {
                // Option<Option<...>>
                this.builder.append(", |x| if x.is_none() { Some(None) } else {x}");
            }
            this.builder.append(")");
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
                switch (this.options.languageOptions.unquotedCasing) {
                    case "upper":
                        name = name.toUpperCase(Locale.ENGLISH);
                        break;
                    case "lower":
                        name = name.toLowerCase(Locale.ENGLISH);
                        break;
                    default:
                        break;
                }
            }
            this.builder
                    .append("r#")
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

    void processNode(IDBSPNode node) {
        DBSPOperator op = node.as(DBSPOperator.class);
        if (op != null)
            this.generateOperator(op);
        IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
        if (inner != null) {
            inner.accept(this.innerVisitor);
            this.builder.newline();
        }
    }

    void generateOperator(DBSPOperator operator) {
        String str = operator.getNode().toInternalString();
        this.writeComments(str);
        operator.accept(this);
        this.builder.newline();
    }

    String handleName(DBSPOperator operator) {
        return "handle" + operator.id;
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        this.builder.append("pub fn ")
                .append(circuit.name);
        circuit.circuit.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        StringBuilder b = new StringBuilder();
        IndentStream signature = new IndentStream(b);
        ToRustInnerVisitor inner = new ToRustInnerVisitor(this.errorReporter, signature, false);

        if (!this.useHandles) {
            signature.append("Catalog");
        } else {
            signature.append("(");
            for (DBSPOperator input: circuit.inputOperators.values()) {
                DBSPType type;
                DBSPTypeZSet zset = input.outputType.as(DBSPTypeZSet.class);
                if (zset != null) {
                    type = new DBSPTypeUser(
                            zset.getNode(), DBSPTypeCode.USER, "ZSetHandle", false,
                            zset.elementType);
                } else {
                    DBSPTypeIndexedZSet ix = input.outputType.to(DBSPTypeIndexedZSet.class);
                    type = new DBSPTypeUser(
                            ix.getNode(), DBSPTypeCode.USER, "SetHandle", false,
                            ix.elementType);
                }
                type.accept(inner);
                signature.append(", ");
            }
            for (DBSPSinkOperator output: circuit.outputOperators.values()) {
                DBSPType outputType = output.input().outputType;
                signature.append("OutputHandle<");
                outputType.accept(inner);
                signature.append(">, ");
            }
            signature.append(")");
        }

        this.builder
                .append("(cconf: CircuitConfig) -> Result<(DBSPHandle, ")
                .append(signature.toString())
                .append("), DBSPError> {")
                .increase()
                .newline()
                .append("let (circuit, streams) = Runtime::init_circuit(cconf, |circuit| {")
                .increase();
        if (!this.useHandles)
            this.builder.append("let mut catalog = Catalog::new();").newline();

        for (IDBSPNode node : circuit.getAllOperators())
            this.processNode(node);

        if (!this.useHandles)
            this.builder.append("Ok(catalog)");
        else
            this.builder.append("Ok((")
                    .append(this.streams.toString())
                    .append("))");
        this.builder.newline()
                .decrease()
                .append("})?;")
                .newline();

        this.builder
                .append("Ok((circuit, streams))")
                .newline()
                .decrease()
                .append("}")
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        DBSPTypeStruct type = operator.originalRowType;
        DBSPStructItem item = new DBSPStructItem(type);
        item.accept(this.innerVisitor);
        this.generateFromTrait(type);
        this.generateRenameMacro(operator.tableName, type, operator.metadata);

        this.writeComments(operator)
                .append("let (")
                .append(operator.getOutputName())
                .append(", ")
                .append(this.handleName(operator))
                .append(") = circuit.add_input_zset::<");

        DBSPTypeZSet zsetType = operator.getType().to(DBSPTypeZSet.class);
        zsetType.elementType.accept(this.innerVisitor);
        this.builder.append(">();").newline();
        if (!this.useHandles) {
            this.builder.append("catalog.register_input_zset::<_, ");
            IHasSchema tableDescription = this.metadata.getTableDescription(operator.tableName);
            DBSPStrLiteral json = new DBSPStrLiteral(tableDescription.asJson().toString(), false, true);
            operator.originalRowType.accept(this.innerVisitor);
            this.builder.append(">(")
                    .append(operator.getOutputName())
                    .append(".clone(), ")
                    .append(this.handleName(operator))
                    .append(", ");
            json.accept(this.innerVisitor);
            this.builder.append(");")
                    .newline();
        } else {
            this.streams.append(this.handleName(operator))
                    .append(", ");
        }
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
        this.generateRenameMacro(operator.tableName, type, operator.metadata);

        DBSPTypeStruct keyStructType = operator.getKeyStructType(
                // TODO: this should be a fresh name.
                operator.originalRowType.sanitizedName + "_key");
        // Generate key type definition
        item = new DBSPStructItem(keyStructType);
        item.accept(this.innerVisitor);

        // Trait for serialization
        this.generateFromTrait(keyStructType);
        this.generateRenameMacro(item.type.name, item.type, operator.metadata);

        DBSPTypeStruct upsertStruct = operator.getStructUpsertType(
                operator.originalRowType.sanitizedName + "_upsert");
        // Generate key type definition
        item = new DBSPStructItem(upsertStruct);
        item.accept(this.innerVisitor);
        // Trait for serialization
        this.generateFromTrait(upsertStruct);
        this.generateRenameMacro(item.type.name, item.type, operator.metadata);

        this.writeComments(operator)
                .append("let (")
                .append(operator.getOutputName())
                .append(", ")
                .append(this.handleName(operator))
                .append(") = circuit.add_input_map::<");

        DBSPTypeIndexedZSet ix = operator.getOutputIndexedZSetType();
        ix.keyType.accept(this.innerVisitor);
        this.builder.append(", ");
        ix.elementType.accept(this.innerVisitor);
        this.builder.append(", ");
        upsertStruct.toTuple().accept(this.innerVisitor);
        this.builder.append(", _>(").increase();
        {
            // Upsert update function
            this.builder.append("Box::new(|updated: &mut ");
            type.toTuple().accept(this.innerVisitor);
            this.builder.append(", changes: &");
            upsertStruct.toTuple().accept(this.innerVisitor);
            this.builder.append("| {");
            int index = 0;
            for (DBSPTypeStruct.Field field: upsertStruct.fields.values()) {
                String name = field.sanitizedName;
                if (!keyStructType.hasField(field.name)) {
                    this.builder.append("if let Some(")
                            .append(name)
                            .append(") = &changes.")
                            .append(index)
                            .append(" { ")
                            .append("updated.")
                            .append(index)
                            .append(" = ")
                            .append(name)
                            .append(".clone(); }")
                            .newline();
                }
                index++;
            }
            this.builder.append("})");
        }

        this.builder.decrease().append(");").newline();
        if (!this.useHandles) {
            IHasSchema tableDescription = this.metadata.getTableDescription(operator.tableName);
            DBSPStrLiteral json = new DBSPStrLiteral(tableDescription.asJson().toString(), false, true);
            this.builder.append("catalog.register_input_map::<");
            keyStructType.toTuple().accept(this.innerVisitor);
            this.builder.append(", ");
            keyStructType.accept(this.innerVisitor);
            this.builder.append(", ");
            operator.getOutputIndexedZSetType().elementType.accept(this.innerVisitor);
            this.builder.append(", ");
            operator.originalRowType.accept(this.innerVisitor);
            this.builder.append(", ");
            upsertStruct.toTuple().accept(this.innerVisitor);
            this.builder.append(", ");
            upsertStruct.accept(this.innerVisitor);
            this.builder.append(", _, _>(")
                    .append(operator.getOutputName())
                    .append(".clone(), ")
                    .append(this.handleName(operator))
                    .append(", ");
            operator.getKeyFunc().accept(this.innerVisitor);
            this.builder.append(", ");
            operator.getUpdateKeyFunc(upsertStruct).accept(this.innerVisitor);
            this.builder.append(", ");
            json.accept(this.innerVisitor);
            this.builder.append(");")
                    .newline();
        } else {
            this.streams.append(this.handleName(operator))
                    .append(", ");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDistinctOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.input().getOutputName())
                .append(".")
                .append(operator.operation)
                .append("();");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPControlledFilterOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.inputs.get(0).getOutputName());

        boolean isZset = operator.getType().is(DBSPTypeZSet.class);
        this.builder.append(".apply2(&")
                .append(operator.inputs.get(1).getOutputName())
                .append(", ");
        this.builder.append("|d, c| ");
        if (isZset)
            this.builder.append("zset_");
        else
            this.builder.append("indexed_zset_");
        this.builder.append("filter_comparator(d, c, ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(")");
        this.builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName());
        this.builder.append(" = ")
                .append(operator.inputs.get(0).getOutputName());

        this.builder.append(".")
                .append(operator.operation)
                .append("(&")
                .append(operator.inputs.get(1).getOutputName())
                // FIXME: temporary workaround until the compiler learns about
                // TypedBox's
                .append(".apply(|bound| TypedBox::<_, DynData>::new(bound.clone()))")
                .append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPWaterlineOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.input().getOutputName())
                .append(".")
                .append(operator.operation)
                .append("::<_, DynData, _, _>(");
        // This part is different from a standard operator.
        operator.init.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        // FIXME: temporary fix until the compiler learns to work with the
        // `TypedBox` type.
        this.builder.append(").inner_typed();");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
        this.writeComments(operator);
        DBSPTypeStruct type = operator.originalRowType;
        DBSPStructItem item = new DBSPStructItem(type);
        item.accept(this.innerVisitor);
        this.generateFromTrait(type);
        this.generateRenameMacro(operator.viewName, type, null);
        if (!this.useHandles) {
            IHasSchema description = this.metadata.getViewDescription(operator.viewName);
            DBSPStrLiteral json = new DBSPStrLiteral(description.asJson().toString(), false, true);
            this.builder.append("catalog.register_output_zset::<_, ");
            operator.originalRowType.accept(this.innerVisitor);
            this.builder.append(">(")
                    .append(operator.input().getOutputName())
                    .append(".clone()")
                    .append(", ");
            json.accept(this.innerVisitor);
            this.builder.append(");")
                    .newline();
        } else {
            this.builder.append("let ")
                    .append(this.handleName(operator))
                    .append(" = ")
                    .append(operator.input().getOutputName())
                    .append(".output();").newline();
            this.streams.append(this.handleName(operator))
                    .append(", ");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            builder.append(operator.inputs.get(0).getOutputName())
                    .append(".");
        builder.append(operator.operation)
                .append("(");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                builder.append(",");
            builder.append("&")
                    .append(operator.inputs.get(i).getOutputName());
        }
        if (operator.function != null) {
            if (operator.inputs.size() > 1)
                builder.append(", ");
            operator.function.accept(this.innerVisitor);
        }
        builder.append(");");
        return VisitDecision.STOP;
    }

    /**
     * Helper function for generateComparator and generateCmpFunc.
     * @param fieldNo  Field index that is compared.
     * @param ascending Comparison direction.
     */
    void emitCompareField(int fieldNo, boolean ascending) {
        this.builder.append("let ord = left.")
                .append(fieldNo)
                .append(".cmp(&right.")
                .append(fieldNo)
                .append(");")
                .newline();
        this.builder.append("if ord != Ordering::Equal { return ord");
        if (!ascending)
            this.builder.append(".reverse()");
        this.builder.append("};")
                .newline();
    }

    /**
     * Helper function for generateCmpFunc.
     * This could be part of an inner visitor too.
     * But we don't handle DBSPComparatorExpressions in the same way in
     * any context: we do it differently in TopK and Sort.
     * This is for TopK.
     * @param comparator  Comparator expression to generate Rust for.
     * @param fieldsCompared  Accumulate here a list of all fields compared.
     */
    void generateComparator(DBSPComparatorExpression comparator, Set<Integer> fieldsCompared) {
        // This could be done with a visitor... but there are only two cases
        if (comparator.is(DBSPNoComparatorExpression.class))
            return;
        DBSPFieldComparatorExpression fieldComparator = comparator.to(DBSPFieldComparatorExpression.class);
        this.generateComparator(fieldComparator.source, fieldsCompared);
        if (fieldsCompared.contains(fieldComparator.fieldNo))
            throw new InternalCompilerError("Field " + fieldComparator.fieldNo + " used twice in sorting");
        fieldsCompared.add(fieldComparator.fieldNo);
        this.emitCompareField(fieldComparator.fieldNo, fieldComparator.ascending);
    }

    void generateCmpFunc(DBSPExpression function, String structName) {
        //    impl CmpFunc<(String, i32, i32)> for AscDesc {
        //        fn cmp(left: &(String, i32, i32), right: &(String, i32, i32)) -> std::cmp::Ordering {
        //            let ord = left.1.cmp(&right.1);
        //            if ord != Ordering::Equal { return ord; }
        //            let ord = right.2.cmp(&left.2);
        //            if ord != Ordering::Equal { return ord; }
        //            let ord = left.3.cmp(&right.3);
        //            if ord != Ordering::Equal { return ord; }
        //            return Ordering::Equal;
        //        }
        //    }
        DBSPComparatorExpression comparator = function.to(DBSPComparatorExpression.class);
        DBSPType type = comparator.tupleType();
        this.builder.append("impl CmpFunc<");
        type.accept(this.innerVisitor);
        this.builder.append("> for ")
                .append(structName)
                .append(" {")
                .increase()
                .append("fn cmp(left: &");
        type.accept(this.innerVisitor);
        this.builder.append(", right: &");
        type.accept(this.innerVisitor);
        this.builder.append(") -> std::cmp::Ordering {")
                .increase();
        // This is a subtle aspect. The comparator compares on some fields,
        // but we have to generate a comparator on ALL the fields, to avoid
        // declaring values as equal when they aren't really.
        Set<Integer> fieldsCompared = new HashSet<>();
        this.generateComparator(comparator, fieldsCompared);
        // Now compare on the fields that we didn't compare on.
        // The order doesn't really matter.
        for (int i = 0; i < type.to(DBSPTypeTuple.class).size(); i++) {
            if (fieldsCompared.contains(i)) continue;
            this.emitCompareField(i, true);
        }
        this.builder.append("return Ordering::Equal;")
                .newline();
        this.builder
                .decrease()
                .append("}")
                .newline()
                .decrease()
                .append("}")
                .newline();
    }

    DBSPClosureExpression generateEqualityComparison(DBSPExpression comparator) {
        CalciteObject node = comparator.getNode();
        DBSPExpression result = new DBSPBoolLiteral(true);
        DBSPComparatorExpression comp = comparator.to(DBSPComparatorExpression.class);
        DBSPType type = comp.tupleType().ref();
        DBSPVariablePath left = new DBSPVariablePath("left", type);
        DBSPVariablePath right = new DBSPVariablePath("right", type);
        while (comparator.is(DBSPFieldComparatorExpression.class)) {
            DBSPFieldComparatorExpression fc = comparator.to(DBSPFieldComparatorExpression.class);
            DBSPExpression eq = new DBSPBinaryExpression(node, new DBSPTypeBool(node, false),
                    DBSPOpcode.IS_NOT_DISTINCT, left.deref().field(fc.fieldNo), right.deref().field(fc.fieldNo));
            result = new DBSPBinaryExpression(node, eq.getType(), DBSPOpcode.AND, result, eq);
            comparator = fc.source;
        }
        return result.closure(left.asParameter(), right.asParameter());
    }

    @Override
    public VisitDecision preorder(DBSPIndexedTopKOperator operator) {
        // TODO: this should be a fresh identifier.
        String structName = "Cmp" + operator.getOutputName();
        this.builder.append("struct ")
                .append(structName)
                .append(";")
                .newline();

        // Generate a CmpFunc impl for the new struct.
        this.generateCmpFunc(operator.getFunction(), structName);

        String streamOperation = "topk_custom_order";
        if (operator.outputProducer != null) {
            streamOperation = switch (operator.numbering) {
                case ROW_NUMBER -> "topk_row_number_custom_order";
                case RANK -> "topk_rank_custom_order";
                case DENSE_RANK -> "topk_dense_rank_custom_order";
            };
        }

        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.input().getOutputName())
                .append(".")
                .append(streamOperation)
                .append("::<")
                .append(structName);
                if (operator.outputProducer != null) {
                    this.builder.append(", _, _");
                    if (operator.numbering != DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER)
                        this.builder.append(", _");
                }
                this.builder.append(">(");
        DBSPExpression cast = operator.limit.cast(
                new DBSPTypeUSize(CalciteObject.EMPTY, operator.limit.getType().mayBeNull));
        cast.accept(this.innerVisitor);
        if (operator.outputProducer != null) {
            if (operator.numbering != DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER) {
                this.builder.append(", ");
                DBSPExpression equalityComparison = this.generateEqualityComparison(
                        operator.getFunction());
                equalityComparison.accept(this.innerVisitor);
            }
            this.builder.append(", ");
            operator.outputProducer.accept(this.innerVisitor);
        }
        builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPWindowAggregateOperator operator) {
        // We generate two DBSP operator calls: partitioned_rolling_aggregate
        // and map_index
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        String tmp = new NameGen("stream").nextName();
        this.writeComments(operator)
                .append("let ")
                .append(tmp)
                .append(" = ")
                .append(operator.input().getOutputName())
                // FIXME: `as_partitioned_zset()` is a temporary workaround.
                .append(".as_partitioned_zset().partitioned_rolling_aggregate(");
        operator.getFunction().accept(this.innerVisitor);
        builder.append(", ");
        operator.window.accept(this.innerVisitor);
        builder.append(");")
                .newline();

        this.builder.append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        builder.append(" = " )
                .append(tmp)
                .append(".map_index(|(key, ts_agg)| { ")
                .append("( Tup2::new(key.clone(), ts_agg.0), ts_agg.1.unwrap_or_default() )")
                .append("});");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregateOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        builder.append(operator.input().getOutputName())
                    .append(".");
        builder.append(operator.operation)
                .append("(");
        operator.getFunction().accept(this.innerVisitor);
        builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSumOperator operator) {
        this.writeComments(operator)
                    .append("let ")
                    .append(operator.getOutputName())
                    .append(": ");
        new DBSPTypeStream(operator.outputType).accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(operator.inputs.get(0).getOutputName())
                        .append(".");
        this.builder.append(operator.operation)
                    .append("([");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                this.builder.append(", ");
            this.builder.append("&").append(operator.inputs.get(i).getOutputName());
        }
        this.builder.append("]);");
        return VisitDecision.STOP;
    }

    IIndentStream writeComments(@Nullable String comment) {
        if (comment == null || comment.isEmpty())
            return this.builder;
        String[] parts = comment.split("\n");
        parts = Linq.map(parts, p -> "// " + p, String.class);
        return this.builder.intercalate("\n", parts);
    }

     IIndentStream writeComments(DBSPOperator operator) {
        return this.writeComments(operator.getClass().getSimpleName() +
                " " + operator.getIdString() +
                (operator.comment != null ? "\n" + operator.comment : ""));
    }

    @Override
    public VisitDecision preorder(DBSPJoinOperator operator) {
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        new DBSPTypeStream(operator.outputType).accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(operator.inputs.get(0).getOutputName())
                    .append(".");
        this.builder.append(operator.operation)
                .append("(&");
        this.builder.append(operator.inputs.get(1).getOutputName());
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstantOperator operator) {
        assert operator.function != null;
        builder.append("let ")
                .append(operator.getOutputName())
                .append(" = ")
                .append("circuit.add_source(Generator::new(|| ");
        this.builder.append("if Runtime::worker_index() == 0 {");
        operator.function.accept(this.innerVisitor);
        this.builder.append("} else {");
        DBSPZSetLiteral empty = DBSPZSetLiteral.emptyWithElementType(
                operator.getOutputZSetElementType());
        empty.accept(this.innerVisitor);
        this.builder.append("}));");
        return VisitDecision.STOP;
    }

    public static String toRustString(IErrorReporter reporter, DBSPCircuit node, CompilerOptions options) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustVisitor visitor = new ToRustVisitor(reporter, stream, options, node.getMetadata());
        visitor.apply(node);
        return builder.toString();
    }
}
