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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Recursive;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledKeyFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeclareComparators;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** This visitor generates a Rust implementation of a circuit. */
public class ToRustVisitor extends CircuitVisitor {
    protected final IIndentStream builder;
    public final ToRustInnerVisitor innerVisitor;
    final boolean useHandles;
    /** How are nodes named in Rust?  false - human-friendly, true - compiler-friendly */
    boolean preferHash = false;
    final CompilerOptions options;
    final ProgramMetadata metadata;
    /** Declarations global; outside circuits */
    final Set<String> globalDeclarations;
    /** Declarations inside a circuit */
    final Set<String> perCircuitDeclarations;

    /* Example output generated when 'generateCatalog' is true:
     * pub fn circuit0(workers: usize) -> (DBSPHandle, Catalog) {
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

    /**
     * Create a visitor which emits rust code for a circuit.
     * @param compiler  Compiler used to compile; used for options and error reporting, for example.
     * @param builder   Emit the output here.
     * @param metadata  Program metadata for the program compiled.
     * @param skip      Global declarations emitted are added here, so they are not emitted twice in the same file.
     */
    public ToRustVisitor(DBSPCompiler compiler, IIndentStream builder, ProgramMetadata metadata,
                         Set<String> skip) {
        super(compiler);
        this.options = compiler.options;
        this.builder = builder;
        this.useHandles = compiler.options.ioOptions.emitHandles;
        this.metadata = metadata;
        this.innerVisitor = this.createInnerVisitor(builder);
        this.globalDeclarations = skip;
        this.perCircuitDeclarations = new HashSet<>();
    }

    ToRustInnerVisitor createInnerVisitor(IIndentStream builder) {
        return new ToRustInnerVisitor(this.compiler(), builder, false);
    }

    public ToRustVisitor withPreferHash(boolean preferHash) {
        this.preferHash = preferHash;
        return this;
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

    public void generateOperator(DBSPOperator operator) {
        if (this.compiler.options.ioOptions.verbosity > 1) {
            String str = operator.getNode().toInternalString();
            this.writeComments(str);
        }

        DeclareComparators comparators = new DeclareComparators(this.compiler);
        operator.accept(comparators);
        for (var decl: comparators.newDeclarations) {
            if (!this.perCircuitDeclarations.contains(decl.getName())) {
                decl.accept(this.innerVisitor);
                this.perCircuitDeclarations.add(decl.getName());
            }
        }
        this.innerVisitor.setComparatorDeclarations(comparators.newDeclarations);

        if (operator.is(DBSPViewDeclarationOperator.class))
            // No output produced for view declarations
            return;

        operator.accept(this);
        this.builder.newline();
        if (operator.is(DBSPSimpleOperator.class) &&
                !operator.is(DBSPSinkOperator.class) &&
                this.compiler.options.ioOptions.verbosity > 0) {
            DBSPSimpleOperator simple = operator.to(DBSPSimpleOperator.class);
            this.builder.append("// ")
                    .append(simple.getNodeName(this.preferHash))
                    .append(".inspect(|batch| println!(\"")
                    .append(simple.getNodeName(this.preferHash))
                    .append("={batch:?}\"));")
                    .newline();
        }
    }

    String handleName(DBSPSimpleOperator operator) {
        String compactName = operator.getCompactName();
        return "handle" + compactName;
    }

    @Override
    public VisitDecision preorder(DBSPDeclaration decl) {
        decl.item.accept(this.innerVisitor);
        return VisitDecision.STOP;
    }

    boolean declareInside(DBSPDeclaration decl) {
        return decl.item.is(DBSPStaticItem.class) || decl.item.is(DBSPFunctionItem.class);
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        IndentStream signature = new IndentStreamBuilder();
        ToRustInnerVisitor inner = this.createInnerVisitor(signature);

        for (DBSPDeclaration decl : circuit.declarations) {
            if (!this.declareInside(decl) && !this.globalDeclarations.contains(decl.getName())) {
                decl.accept(this.innerVisitor);
                this.globalDeclarations.add(decl.getName());
            }
        }

        this.builder.append("pub fn ")
                .append(circuit.getName());

        if (!this.useHandles) {
            signature.append("Catalog");
        } else {
            signature.append("(").increase();
            for (DBSPSimpleOperator input: circuit.sourceOperators.values()) {
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
                signature.append(",").newline();
            }
            for (DBSPViewBaseOperator output: circuit.sinkOperators.values()) {
                DBSPType outputType = output.input().outputType();
                signature.append("OutputHandle<");
                outputType.accept(inner);
                signature.append(">,").newline();
            }
            signature.decrease().append(")");
        }

        this.builder
                .append("(cconf: CircuitConfig) -> Result<(DBSPHandle, ")
                .append(signature.toString())
                .append("), Error> {")
                .increase()
                .newline()
                .append("let (circuit, streams) = Runtime::init_circuit(cconf, |circuit| {")
                .increase();
        if (!this.useHandles)
            this.builder.append("let mut catalog = Catalog::new();").newline();

        for (DBSPDeclaration decl: circuit.declarations) {
            if (this.declareInside(decl)) {
                decl.accept(this);
                this.builder.newline().newline();
            }
        }

        // Process sources first
        for (DBSPOperator node : circuit.getAllOperators())
            if (node.is(DBSPSourceBaseOperator.class))
                this.processNode(node);

        for (DBSPOperator node : circuit.getAllOperators())
            if (!node.is(DBSPSourceBaseOperator.class))
                this.processNode(node);

        // Hack: if a view has a 'rust' property, emit the attached code here
        for (ProgramIdentifier view: circuit.getOutputViews()) {
            DBSPSinkOperator sink = circuit.getSink(view);
            assert sink != null;
            if (sink.metadata.properties != null) {
                String rust = sink.metadata.properties.getPropertyValue("rust");
                if (rust != null) {
                    this.builder.append(rust).newline();
                }
            }
        }

        if (!this.useHandles)
            this.builder.append("Ok(catalog)");
        else {
            this.builder.append("Ok((");
            for (DBSPSourceTableOperator operator: circuit.sourceOperators.values())
                this.builder.append(this.handleName(operator)).append(", ");
            for (DBSPSinkOperator operator: circuit.sinkOperators.values())
                this.builder.append(this.handleName(operator)).append(", ");
            this.builder.append("))");
        }
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

    String getInputName(DBSPOperator operator, int input) {
        if (this.preferHash)
            return "i" + input;
        else
            return operator.inputs.get(input).getName(false);
    }

    @Override
    public VisitDecision preorder(DBSPDeltaOperator delta) {
        this.builder.append("let ")
                .append(delta.getNodeName(this.preferHash))
                .append(" = ")
                .append(this.getInputName(delta, 0))
                .append(".delta0(circuit)")
                .append(this.markDistinct(delta))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator operator) {
        boolean recursive = operator.hasAnnotation(a -> a.is(Recursive.class));
        if (!recursive)
            throw new InternalCompilerError("NestedOperator not recursive");

        this.builder.append("let (");
        for (int i = 0; i < operator.outputCount(); i++) {
            OutputPort port = operator.internalOutputs.get(i);
            if (port == null)
                this.builder.append("_, ");
            else
                this.builder.append(port.getName(this.preferHash)).append(", ");
        }
        this.builder.append(") = ")
                .append("circuit.recursive(|circuit, (");
        for (int i = 0; i < operator.outputCount(); i++) {
            ProgramIdentifier view = operator.outputViews.get(i);
            DBSPViewDeclarationOperator decl = operator.declarationByName.get(view);
            if (decl != null) {
                this.builder.append(decl.getNodeName(this.preferHash)).append(", ");
            } else {
                // view is not really recursive
                this.builder.append("_").append(", ");
            }
        }
        this.builder.append("): (");
        for (int i = 0; i < operator.outputCount(); i++) {
            if (operator.internalOutputs.get(i) == null) {
                this.builder.append("()").append(", ");
            } else {
                DBSPType streamType = new DBSPTypeStream(operator.outputType(i), false);
                streamType.accept(this.innerVisitor);
                this.builder.append(", ");
            }
        }
        this.builder.append(")| {").increase();
        for (IDBSPNode node : operator.getAllOperators())
            this.processNode(node);

        this.builder.append("Ok((");
        for (int i = 0; i < operator.outputCount(); i++) {
            OutputPort port = operator.internalOutputs.get(i);
            if (port != null)
                this.builder.append(port.getName(this.preferHash));
            else
                this.builder.append("()");
            this.builder.append(", ");
        }
        this.builder.append("))").newline()
                .decrease()
                .append("}).unwrap();")
                .newline();
        return VisitDecision.STOP;
    }

    /** Remove properties.connectors from a json tree.
     * If the properties become empty, remove them too. */
    JsonNode stripConnectors(JsonNode json) {
        if (!json.isObject())
            return json;
        ObjectNode j = (ObjectNode) json;
        ObjectNode props = (ObjectNode) j.get("properties");
        if (props != null) {
            props.remove("connectors");
            if (props.isEmpty())
                j.remove("properties");
        }
        return json;
    }

    static class FindNestedStructs extends InnerVisitor {
        final List<DBSPTypeStruct> structs;

        FindNestedStructs(DBSPCompiler compiler, List<DBSPTypeStruct> result) {
            super(compiler);
            this.structs = result;
        }

        @Override
        public void postorder(DBSPTypeStruct struct) {
            for (DBSPTypeStruct str: this.structs)
                if (str.name.equals(struct.name))
                    return;
            this.structs.add(struct);
        }
    }

    Set<String> structsGenerated = new HashSet<>();

    void generateStructHelpers(DBSPType struct, @Nullable TableMetadata metadata) {
        List<DBSPTypeStruct> nested = new ArrayList<>();
        FindNestedStructs fn = new FindNestedStructs(this.compiler, nested);
        fn.apply(struct);
        for (DBSPTypeStruct s: nested) {
            DBSPStructItem item = new DBSPStructItem(s, metadata);
            if (this.structsGenerated.contains(item.getName()))
                continue;
            item.accept(this.innerVisitor);
            this.structsGenerated.add(item.getName());
        }
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        this.writeComments(operator)
                .append("let (")
                .append(operator.getNodeName(this.preferHash))
                .append(", ")
                .append(this.handleName(operator))
                .append(") = circuit.add_input_zset::<");

        DBSPTypeZSet zsetType = operator.getType().to(DBSPTypeZSet.class);
        zsetType.elementType.accept(this.innerVisitor);
        this.builder.append(">();").newline();
        if (this.options.ioOptions.sqlNames) {
            this.builder.append("let ")
                    .append(operator.tableName.name())
                    .append(" = &")
                    .append(operator.getNodeName(this.preferHash))
                    .append(";")
                    .newline();
        }
        if (!this.useHandles) {
            this.generateStructHelpers(operator.originalRowType, operator.metadata);
            String registerFunction = operator.metadata.materialized ?
                    "register_materialized_input_zset" : "register_input_zset";
            this.builder.append("catalog.")
                    .append(registerFunction)
                    .append("::<_, ");
            IHasSchema tableDescription = this.metadata.getTableDescription(operator.tableName);
            JsonNode j = tableDescription.asJson();
            j = this.stripConnectors(j);
            DBSPStrLiteral json = new DBSPStrLiteral(j.toString(), true);
            operator.originalRowType.accept(this.innerVisitor);
            this.builder.append(">(")
                    .append(operator.getNodeName(this.preferHash))
                    .append(".clone(), ")
                    .append(this.handleName(operator))
                    .append(", ");
            json.accept(this.innerVisitor);
            this.builder.append(");")
                    .newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPViewDeclarationOperator operator) {
        // No output produced
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMapOperator operator) {
        DBSPTypeStruct type = operator.originalRowType;
        DBSPTypeStruct keyStructType = operator.getKeyStructType(
                new ProgramIdentifier(operator.originalRowType.sanitizedName + "_key", false));
        DBSPTypeStruct upsertStruct = operator.getStructUpsertType(
                new ProgramIdentifier(operator.originalRowType.sanitizedName + "_upsert", false));
        this.writeComments(operator)
                .append("let (")
                .append(operator.getNodeName(this.preferHash))
                .append(", ")
                .append(this.handleName(operator))
                .append(") = circuit.add_input_map::<");

        DBSPTypeIndexedZSet ix = operator.getOutputIndexedZSetType();
        ix.keyType.accept(this.innerVisitor);
        this.builder.append(", ");
        ix.elementType.accept(this.innerVisitor);
        this.builder.append(", ");
        DBSPType upsertTuple = upsertStruct.toTupleDeep();
        upsertTuple.accept(this.innerVisitor);
        this.builder.append(", _>(").increase();
        {
            // Upsert update function
            this.builder.append("Box::new(|updated: &mut ");
            type.toTupleDeep().accept(this.innerVisitor);
            this.builder.append(", changes: &");
            upsertStruct.toTupleDeep().accept(this.innerVisitor);
            this.builder.append("| {");
            int index = 0;
            for (DBSPTypeStruct.Field field: upsertStruct.fields.values()) {
                String name = field.getSanitizedName();
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
        if (this.options.ioOptions.sqlNames) {
            this.builder.append("let ")
                    .append(operator.tableName.name())
                    .append(" = &")
                    .append(operator.getNodeName(this.preferHash))
                    .append(";")
                    .newline();
        }
        if (!this.useHandles) {
            this.generateStructHelpers(type, operator.metadata);
            this.generateStructHelpers(keyStructType, operator.metadata);
            this.generateStructHelpers(upsertStruct, operator.metadata);

            IHasSchema tableDescription = this.metadata.getTableDescription(operator.tableName);
            JsonNode j = tableDescription.asJson();
            j = this.stripConnectors(j);
            DBSPStrLiteral json = new DBSPStrLiteral(j.toString(), true);
            String registerFunction = operator.metadata.materialized ?
                    "register_materialized_input_map" : "register_input_map";
            this.builder.append("catalog.")
                    .append(registerFunction)
                    .append("::<");
            keyStructType.toTuple().accept(this.innerVisitor);
            this.builder.append(", ");
            keyStructType.accept(this.innerVisitor);
            this.builder.append(", ");
            operator.getOutputIndexedZSetType().elementType.accept(this.innerVisitor);
            this.builder.append(", ");
            operator.originalRowType.accept(this.innerVisitor);
            this.builder.append(", ");
            upsertStruct.toTupleDeep().accept(this.innerVisitor);
            this.builder.append(", ");
            upsertStruct.accept(this.innerVisitor);
            this.builder.append(", _, _>(")
                    .append(operator.getNodeName(this.preferHash))
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
        }
        return VisitDecision.STOP;
    }

    public DBSPType streamType(DBSPSimpleOperator operator) {
        return operator.outputStreamType(0, this.inOuterCircuit());
    }

    @Override
    public VisitDecision preorder(DBSPDistinctOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("()")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPControlledKeyFilterOperator operator) {
        this.builder.append("let (")
                .append(operator.getOutput(0).getName(this.preferHash))
                .append(",")
                .append(operator.getOutput(1).getName(this.preferHash))
                .append("): (");
        boolean isOuter = this.inOuterCircuit();
        DBSPType streamType = operator.outputStreamType(0, isOuter);
        streamType.accept(this.innerVisitor);
        this.builder.append(", ");
        streamType = operator.outputStreamType(1, isOuter);
        streamType.accept(this.innerVisitor);
        this.builder.append(") = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("(&")
                .append(this.getInputName(operator, 1))
                .append(", ");
        operator.function.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.error.accept(this.innerVisitor);
        this.builder.append(");");
        return VisitDecision.STOP;
    }

    VisitDecision retainOperator(DBSPBinaryOperator operator) {
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash));
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0));

        this.builder.append(".")
                .append(operator.operation)
                .append("(&")
                .append(this.getInputName(operator, 1))
                // FIXME: temporary workaround until the compiler learns about TypedBox
                .append(".apply(|bound| TypedBox::<_, DynData>::new(bound.clone()))")
                .append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        return this.retainOperator(operator);
    }

    @Override
    public VisitDecision preorder(DBSPIntegrateTraceRetainValuesOperator operator) {
        return this.retainOperator(operator);
    }

    @Override
    public VisitDecision preorder(DBSPWindowOperator operator) {
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("((")
                .append(operator.lowerInclusive)
                .append(", ")
                .append(operator.upperInclusive)
                .append("), &")
                .append(this.getInputName(operator, 1))
                .append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPWaterlineOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("(");
        // This part is different from a standard operator.
        operator.init.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.extractTs.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        // FIXME: temporary fix until the compiler learns about `TypedBox` type.
        this.builder.append(").inner_typed();");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
        this.writeComments(operator);
        if (!this.useHandles) {
            DBSPType type = operator.originalRowType;
            this.generateStructHelpers(type, null);
            if (operator.isIndex()) {
                DBSPTypeRawTuple raw = operator.originalRowType.to(DBSPTypeRawTuple.class);
                assert raw.size() == 2;

                this.builder.append("catalog.register_index");
                this.builder.append("::<").increase();
                operator.getOutputIndexedZSetType().keyType.accept(this.innerVisitor);
                this.builder.append(",").newline();
                raw.tupFields[0].accept(this.innerVisitor);
                this.builder.append(",").newline();
                operator.getOutputIndexedZSetType().elementType.accept(this.innerVisitor);
                this.builder.append(",").newline();
                raw.tupFields[1].accept(this.innerVisitor);
                this.builder.decrease().append(">");
                this.builder.append("(").newline()
                        .append(this.getInputName(operator, 0))
                        .append(".clone()")
                        .append(", &SqlIdentifier::from(\"");
                this.builder.append(operator.viewName.toString())
                        .append("\"), &SqlIdentifier::from(\"")
                        .append(operator.query);
                this.builder.append("\"),")
                        .newline().append("&vec!(");
                DBSPTypeStruct keyStruct = raw.tupFields[0].to(DBSPTypeStruct.class);
                boolean first = true;
                for (Iterator<ProgramIdentifier> it = keyStruct.getFieldNames(); it.hasNext(); ) {
                    ProgramIdentifier field = it.next();
                    if (!first)
                        this.builder.append(",");
                    first = false;
                    this.builder.append("&SqlIdentifier::from(\"")
                            .append(field.toString())
                            .append("\")");
                }
                this.builder.append("));")
                        .newline();
            } else {
                IHasSchema description = this.metadata.getViewDescription(operator.viewName);
                JsonNode j = description.asJson();
                j = this.stripConnectors(j);
                DBSPStrLiteral json = new DBSPStrLiteral(j.toString(), true);
                String registerFunction = switch (operator.metadata.viewKind) {
                    case MATERIALIZED -> "register_materialized_output_zset";
                    case LOCAL -> throw new InternalCompilerError("Sink operator for local view " + operator);
                    case STANDARD -> "register_output_zset";
                };
                this.builder.append("catalog.")
                        .append(registerFunction)
                        .append("::<_, ");
                operator.originalRowType.accept(this.innerVisitor);
                this.builder.append(">(")
                        .append(this.getInputName(operator, 0))
                        .append(".clone()")
                        .append(", ");
                json.accept(this.innerVisitor);
                this.builder.append(");")
                        .newline();
            }
            if (this.options.ioOptions.sqlNames) {
                this.builder.append("let ")
                        .append(operator.viewName.name())
                        .append(" = &")
                        .append(this.getInputName(operator, 0))
                        .append(";")
                        .newline();
            }
        } else {
            this.builder.append("let ")
                    .append(this.handleName(operator))
                    .append(" = ")
                    .append(this.getInputName(operator, 0))
                    .append(".output();").newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(this.getInputName(operator, 0))
                    .append(".");
        this.builder.append(operator.operation)
                .append("(");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                this.builder.append(",");
            this.builder.append("&")
                    .append(this.getInputName(operator, i));
        }
        if (operator.function != null) {
            if (operator.inputs.size() > 1)
                this.builder.append(", ");
            operator.function.accept(this.innerVisitor);
        }
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPChainAggregateOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("(");
        operator.init.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    DBSPClosureExpression generateEqualityComparison(DBSPExpression comparator) {
        CalciteObject node = comparator.getNode();
        DBSPExpression result = new DBSPBoolLiteral(true);
        DBSPComparatorExpression comp = comparator.to(DBSPComparatorExpression.class);
        DBSPType type = comp.comparedValueType().ref();
        DBSPVariablePath left = type.var();
        DBSPVariablePath right = type.var();
        while (comparator.is(DBSPFieldComparatorExpression.class)) {
            DBSPFieldComparatorExpression fc = comparator.to(DBSPFieldComparatorExpression.class);
            DBSPExpression eq = new DBSPBinaryExpression(node, new DBSPTypeBool(node, false),
                    DBSPOpcode.IS_DISTINCT, left.deref().field(fc.fieldNo), right.deref().field(fc.fieldNo)).not();
            result = new DBSPBinaryExpression(node, eq.getType(), DBSPOpcode.AND, result, eq);
            comparator = fc.source;
        }
        DBSPExpression closure = result.closure(left, right);
        CanonicalForm form = new CanonicalForm(this.compiler);
        return form.apply(closure).to(DBSPClosureExpression.class);
    }

    @Override
    public VisitDecision preorder(DBSPIndexedTopKOperator operator) {
        DBSPExpression comparator = operator.getFunction();
        String streamOperation = "topk_custom_order";
        if (operator.outputProducer != null) {
            streamOperation = switch (operator.numbering) {
                case ROW_NUMBER -> "topk_row_number_custom_order";
                case RANK -> "topk_rank_custom_order";
                case DENSE_RANK -> "topk_dense_rank_custom_order";
            };
        }

        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(streamOperation)
                .append("::<");
        this.builder.append(comparator.to(DBSPComparatorExpression.class).getComparatorStructName());
        if (operator.outputProducer != null) {
            this.builder.append(", _, _");
            if (operator.numbering != DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER)
                this.builder.append(", _");
        }
        this.builder.append(">(");
        DBSPExpression cast = operator.limit.cast(
                DBSPTypeUSize.create(operator.limit.getType().mayBeNull), false);
        cast.accept(this.innerVisitor);
        if (operator.outputProducer != null) {
            if (operator.numbering != DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER) {
                this.builder.append(", ");
                DBSPExpression comp2 = operator.equalityComparator.comparator;
                DBSPExpression equalityComparison = this.generateEqualityComparison(comp2);
                equalityComparison.accept(this.innerVisitor);
            }
            this.builder.append(", ");
            operator.outputProducer.accept(this.innerVisitor);
        }
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    String markDistinct(DBSPOperator operator, @SuppressWarnings("SameParameterValue") int outputNo) {
        if (!operator.isMultiset(outputNo))
            return ".mark_distinct()";
        return "";
    }

    String markDistinct(DBSPSimpleOperator operator) {
        return this.markDistinct(operator, 0);
    }

    @Override
    public VisitDecision preorder(DBSPAsofJoinOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("(&")
                .append(this.getInputName(operator, 1))
                .append(", ")
                .newline();
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(", ");
        operator.leftTimestamp.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.rightTimestamp.accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    VisitDecision processJoinIndexOperator(DBSPJoinBaseOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append("join_index")
                .append("(&")
                .append(this.getInputName(operator, 1))
                .append(", ")
                .newline();
        // The function signature does not correspond to
        // the DBSP join_index operator; it must produce an iterator.
        DBSPClosureExpression closure = operator.getClosureFunction();
        closure = closure.body.some().closure(closure.parameters);
        closure.accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPJoinIndexOperator operator) {
        return this.processJoinIndexOperator(operator);
    }

    public VisitDecision preorder(DBSPStreamJoinIndexOperator operator) {
        if (this.options.languageOptions.incrementalize)
            throw new UnimplementedException("Not yet implemented");
        return this.processJoinIndexOperator(operator);
    }

    @Override
    public VisitDecision preorder(DBSPLagOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("::<_, _, _, ");
        operator.comparator.accept(this.innerVisitor);
        this.builder.append(", _>")
                .append("(");
        DBSPISizeLiteral offset = new DBSPISizeLiteral(operator.offset);
        offset.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.projection.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    void emitWindowBound(DBSPWindowBoundExpression bound) {
        String beforeAfter = bound.isPreceding ? "Before" : "After";
        this.builder.append("RelOffset::")
                .append(beforeAfter)
                .append("(");
        bound.representation.accept(this.innerVisitor);
        this.builder.append(")");
    }

    boolean inOuterCircuit() {
        return this.getParent().is(DBSPCircuit.class);
    }

    @Override
    public VisitDecision preorder(DBSPPartitionedRollingAggregateOperator operator) {
        DBSPType outputStreamType = operator.outputStreamType(0, this.inOuterCircuit());
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(" : ");
        outputStreamType.accept(this.innerVisitor);
        this.builder.newline().append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("(");
        operator.partitioningFunction.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(", ");
        this.builder.append("RelRange::new(").increase();
        this.emitWindowBound(operator.lower);
        this.builder.append(",").newline();
        this.emitWindowBound(operator.upper);
        this.builder.decrease().append("))")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
        DBSPType outputStreamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(" : ");
        outputStreamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(this.getInputName(operator, 0))
                .append(".")
                .append(operator.operation)
                .append("(&")
                .append(this.getInputName(operator, 1))
                .append(", ");
        operator.partitioningFunction.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(", ");
        this.builder.append("RelRange::new(");
        this.emitWindowBound(operator.lower);
        this.builder.append(", ");
        this.emitWindowBound(operator.upper);
        this.builder.append("))")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregateOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        this.builder.append(this.getInputName(operator, 0))
                    .append(".");
        this.builder.append(operator.operation)
                .append("(");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregateLinearPostprocessOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        this.builder.append(this.getInputName(operator, 0))
                .append(".");
        this.builder.append(operator.operation)
                .append("(");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(", ");
        operator.postProcess.accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregateLinearPostprocessRetainKeysOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        this.builder.append(this.getInputName(operator, 0))
                .append(".");
        this.builder.append(operator.operation)
                .append("(&");
        this.builder.append(this.getInputName(operator, 1))
                // FIXME: temporary workaround until the compiler learns about TypedBox
                .append(".apply(|bound| TypedBox::<_, DynData>::new(bound.clone()))")
                .append(", ");
        operator.retainKeysFunction.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(", ");
        operator.postProcess.accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPViewOperator operator) {
        this.builder.append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(" = ")
                .append(this.getInputName(operator, 0))
                .append(";")
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSumOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                    .append("let ")
                    .append(operator.getNodeName(this.preferHash))
                    .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(this.getInputName(operator, 0))
                        .append(".");
        this.builder.append(operator.operation)
                    .append("([");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                this.builder.append(", ");
            this.builder.append("&").append(this.getInputName(operator, i));
        }
        this.builder.append("]");
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    IIndentStream writeComments(@Nullable String comment) {
        if (comment == null || comment.isEmpty())
            return this.builder;
        String[] parts = comment.split("\n");
        parts = Linq.map(parts, p -> "// " + p, String.class);
        return this.builder.intercalate("\n", parts);
    }

    IIndentStream writeComments(DBSPSimpleOperator operator) {
        if (this.options.ioOptions.verbosity < 1)
            return this.builder;
        boolean more = this.options.ioOptions.verbosity > 1;
        return this.writeComments(operator.getClass().getSimpleName() +
                " " + operator.getIdString() +
                (more ? (operator.comment != null ? "\n" + operator.comment : "") : ""));
    }

    @Override
    public VisitDecision preorder(DBSPJoinOperator operator) {
        DBSPType streamType = this.streamType(operator);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(this.getInputName(operator, 0))
                    .append(".");
        this.builder.append(operator.operation)
                .append("(&");
        this.builder.append(this.getInputName(operator, 1));
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    VisitDecision constantLike(DBSPSimpleOperator operator) {
        DBSPType streamType = this.streamType(operator);
        assert operator.function != null;
        this.builder.append("let ")
                .append(operator.getNodeName(this.preferHash))
                .append(":");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append("circuit.add_source(Generator::new(|| ");
        this.builder.append("if Runtime::worker_index() == 0 {");
        operator.function.accept(this.innerVisitor);
        this.builder.append("} else {");
        if (operator.outputType.is(DBSPTypeZSet.class)) {
            DBSPZSetExpression empty = DBSPZSetExpression.emptyWithElementType(
                    operator.getOutputZSetElementType());
            empty.accept(this.innerVisitor);
        } else {
            assert operator.function.to(DBSPIndexedZSetExpression.class).isEmpty();
            operator.function.accept(this.innerVisitor);
        }
        this.builder.append("}));");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstantOperator operator) {
        assert !operator.incremental; // TODO
        return this.constantLike(operator);
    }

    @Override
    public VisitDecision preorder(DBSPNowOperator operator) {
        return this.constantLike(operator);
    }

    public static String toRustString(DBSPCompiler compiler, DBSPCircuit circuit, Set<String> declarationsDone) {
        IndentStream stream = new IndentStreamBuilder();
        ToRustVisitor visitor = new ToRustVisitor(compiler, stream, circuit.getMetadata(), declarationsDone);
        visitor.apply(circuit);
        return stream.toString();
    }
}
