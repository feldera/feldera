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
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EliminateStructs;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructWithHelperItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeOption;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** This visitor generates a Rust implementation of a circuit. */
public class ToRustVisitor extends CircuitVisitor {
    protected final IndentStream builder;
    final ToRustInnerVisitor innerVisitor;
    final boolean useHandles;
    final CompilerOptions options;
    final ProgramMetadata metadata;
    final Set<ProgramIdentifier> structsGenerated;

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

    public ToRustVisitor(DBSPCompiler compiler, IndentStream builder, ProgramMetadata metadata) {
        super(compiler);
        this.options = compiler.options;
        this.builder = builder;
        this.useHandles = compiler.options.ioOptions.emitHandles;
        this.metadata = metadata;
        this.structsGenerated = new HashSet<>();
        this.innerVisitor = this.createInnerVisitor(builder);
    }

    ToRustInnerVisitor createInnerVisitor(IndentStream builder) {
        return new ToRustInnerVisitor(this.compiler(), builder, false);
    }

    void generateInto(String field, DBSPType sourceType, DBSPType targetType) {
        if (sourceType.is(DBSPTypeOption.class)) {
            DBSPType fieldType;
            if (targetType.is(DBSPTypeOption.class)) {
                fieldType = targetType.to(DBSPTypeOption.class).typeArgs[0];
            } else {
                assert targetType.mayBeNull;
                fieldType = targetType.withMayBeNull(false);
            }
            this.builder.append(field);
            DBSPTypeOption option = sourceType.to(DBSPTypeOption.class);
            this.builder.append(".map(|x| ");
            this.generateInto("x", option.typeArgs[0], fieldType);
            this.builder.append(")");
        } else if (sourceType.mayBeNull && !sourceType.is(DBSPTypeNull.class)) {
            this.builder.append(field);
            this.builder.append(".map(|x|").increase();
            this.generateInto("x", sourceType.withMayBeNull(false), targetType.withMayBeNull(false));
            this.builder.decrease().append(")");
        } else if (sourceType.is(DBSPTypeArray.class)) {
            this.builder.append("Arc::new(Arc::unwrap_or_clone(");
            this.builder.append(field);
            DBSPTypeArray vec = sourceType.to(DBSPTypeArray.class);
            DBSPTypeArray targetArray = targetType.to(DBSPTypeArray.class);
            this.builder.append(").into_iter().map(|y|").increase();
            this.generateInto("y", vec.getElementType(), targetArray.getElementType());
            this.builder.decrease().append(")")
                    .newline()
                    .append(".collect::<");
            targetArray.innerType().accept(this.innerVisitor);
            this.builder.append(">())");
        } else if (sourceType.is(DBSPTypeMap.class)) {
            this.builder.append("Arc::new(Arc::unwrap_or_clone(");
            this.builder.append(field);
            DBSPTypeMap map = sourceType.to(DBSPTypeMap.class);
            DBSPTypeMap tMap = targetType.to(DBSPTypeMap.class);
            this.builder.append(").into_iter().map(|(k,v)|").increase()
                    .append("(");
            this.generateInto("k", map.getKeyType(), tMap.getKeyType());
            this.builder.append(", ");
            this.generateInto("v", map.getValueType(), tMap.getValueType());
            this.builder.decrease().append("))")
                    .newline()
                    .append(".collect::<");
            tMap.innerType().accept(this.innerVisitor);
            this.builder.append(">())");
        } else {
            this.builder.append(field);
            this.builder.append(".into()");
        }
    }

    protected void generateFromTrait(DBSPTypeStruct type) {
        EliminateStructs es = new EliminateStructs(this.compiler());
        DBSPTypeTuple tuple = es.apply(type).to(DBSPTypeTuple.class);
        this.builder.append("impl From<")
                .append(type.sanitizedName)
                .append("> for ");
        tuple.accept(this.innerVisitor);
        this.builder.append(" {")
                .increase()
                .append("fn from(t: ")
                .append(type.sanitizedName)
                .append(") -> Self");
        this.builder.append(" {")
                .increase()
                .append(tuple.getName())
                .append("::new(");
        int index = 0;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            this.generateInto("t." + field.getSanitizedName(), field.type, tuple.tupFields[index]);
            this.builder.append(", ");
            index++;
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
                .append("fn from(t: ");
        tuple.accept(this.innerVisitor);
        this.builder.append(") -> Self");
        this.builder.append(" {")
                .increase()
                .append("Self {")
                .increase();
        index = 0;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            this.builder
                    .append(field.getSanitizedName())
                    .append(": ");
            this.generateInto("t." + index, field.type, field.type);
            this.builder.append(", ")
                .newline();
            index++;
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
     * @param type      Type of record in the table.
     * @param metadata  Metadata for the input columns (null for an output view). */
    protected void generateRenameMacro(DBSPTypeStruct type,
                                       @Nullable TableMetadata metadata) {
        this.builder.append("deserialize_table_record!(");
        this.builder.append(type.sanitizedName)
                .append("[")
                .append(Utilities.doubleQuote(type.name.name()))
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
            ProgramIdentifier name = field.name;
            String simpleName = name.name();
            InputColumnMetadata meta = null;
            if (metadata == null) {
                // output
                simpleName = this.options.canonicalName(name);
            } else {
                meta = metadata.getColumnMetadata(field.name);
            }
            this.builder.append("(")
                    .append(field.getSanitizedName())
                    .append(", ")
                    .append(Utilities.doubleQuote(simpleName))
                    .append(", ")
                    .append(Boolean.toString(name.isQuoted()))
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
            ProgramIdentifier name = field.name;
            String simpleName = name.name();
            if (metadata == null) {
                // output
                switch (this.options.languageOptions.unquotedCasing) {
                    case "upper":
                        simpleName = name.name().toUpperCase(Locale.ENGLISH);
                        break;
                    case "lower":
                        simpleName = name.name().toLowerCase(Locale.ENGLISH);
                        break;
                    default:
                        break;
                }
            }
            this.builder
                    .append(field.getSanitizedName())
                    .append("[")
                    .append(Utilities.doubleQuote(simpleName))
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
        if (this.compiler.options.ioOptions.verbosity > 0) {
            String str = operator.getNode().toInternalString();
            this.writeComments(str);
        }
        operator.accept(this);
        this.builder.newline();
        if (operator.is(DBSPSimpleOperator.class) &&
                !operator.is(DBSPSinkOperator.class) &&
                this.compiler.options.ioOptions.verbosity > 0) {
            DBSPSimpleOperator simple = operator.to(DBSPSimpleOperator.class);
            this.builder.append("// ")
                    .append(simple.getOutputName())
                    .append(".inspect(|batch| println!(\"")
                    .append(simple.getOutputName())
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

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        IndentStream signature = new IndentStreamBuilder();
        ToRustInnerVisitor inner = this.createInnerVisitor(signature);

        for (DBSPDeclaration item: circuit.declarations) {
            if (item.item.is(DBSPStructWithHelperItem.class)) {
                DBSPStructWithHelperItem i = item.item.to(DBSPStructWithHelperItem.class);
                this.generateStructHelpers(i.type, null);
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

        FindFunctions funcFinder = new FindFunctions(this.compiler);
        funcFinder.getCircuitVisitor(true).apply(circuit);

        for (DBSPDeclaration item: circuit.declarations) {
            // Generate functions used locally
            if (item.item.is(DBSPFunctionItem.class)) {
                // Do not generate code for functions not used.
                // TODO: does not work, need to compute the transitive closure
                // if (!funcFinder.found.contains(func.function.name)) continue;

                item.accept(this);
                this.builder.newline().newline();
            }
        }

        // Process sources first
        for (IDBSPNode node : circuit.getAllOperators())
            if (node.is(DBSPSourceBaseOperator.class))
                this.processNode(node);

        FindComparators compFinder = new FindComparators(this.compiler);
        FindStatics staticsFinder = new FindStatics(this.compiler);
        compFinder.getCircuitVisitor(false).apply(circuit);
        staticsFinder.getCircuitVisitor(false).apply(circuit);

        for (DBSPComparatorExpression comparator: compFinder.found)
            this.generateCmpFunc(comparator);
        for (DBSPStaticExpression comparator: staticsFinder.found)
            this.generateStatic(comparator);

        for (IDBSPNode node : circuit.getAllOperators())
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

    @Override
    public VisitDecision preorder(DBSPDeltaOperator delta) {
        this.builder.append("let ")
                .append(delta.getOutputName())
                .append(" = ")
                .append(delta.input().getOutputName())
                .append(".delta0(child)")
                .append(this.markDistinct(delta))
                .append(";")
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator operator) {
        boolean recursive = operator.hasAnnotation(a -> a.is(Recursive.class));
        if (!recursive)
            throw new InternalCompilerError("NestedOperator not recursive");

        this.builder.append("let (");
        for (int i = 0; i < operator.outputCount(); i++) {
            this.builder.append(operator.getOutputName(i)).append(", ");
        }
        this.builder.append(") = ")
                .append("circuit.recursive(|child, (");
        for (int i = 0; i < operator.outputCount(); i++) {
            ProgramIdentifier view = operator.outputViews.get(i);
            DBSPViewDeclarationOperator decl = operator.declarationByName.get(view);
            if (decl != null) {
                this.builder.append(decl.getOutputName()).append(", ");
            } else {
                // view is not really recursive
                this.builder.append("_").append(", ");
            }
        }
        this.builder.append("): (");
        for (int i = 0; i < operator.outputCount(); i++) {
            operator.streamType(i).accept(this.innerVisitor);
            this.builder.append(", ");
        }
        this.builder.append(")| {").increase().newline();
        for (IDBSPNode node : operator.getAllOperators())
            this.processNode(node);

        this.builder.append("Ok((");
        for (int i = 0; i < operator.outputCount(); i++) {
            OutputPort port = operator.internalOutputs.get(i);
            if (port != null)
                this.builder.append(port.getOutputName());
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

    static class FindNestedStructs extends InnerVisitor {
        final List<DBSPTypeStruct> structs;

        public FindNestedStructs(DBSPCompiler compiler, List<DBSPTypeStruct> result) {
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

    void findNestedStructs(DBSPType struct, List<DBSPTypeStruct> result) {
        FindNestedStructs fn = new FindNestedStructs(this.compiler, result);
        fn.apply(struct);
    }

    void generateStructDeclarations(DBSPTypeStruct struct) {
        DBSPStructItem item = new DBSPStructItem(struct);
        item.accept(this.innerVisitor);
    }

    void generateStructHelpers(DBSPType type, @Nullable TableMetadata metadata) {
        List<DBSPTypeStruct> nested = new ArrayList<>();
        findNestedStructs(type, nested);
        for (DBSPTypeStruct s: nested) {
            if (this.structsGenerated.contains(s.name))
                continue;
            s = s.withMayBeNull(false).to(DBSPTypeStruct.class);
            this.generateStructDeclarations(s);
            this.generateFromTrait(s);
            this.generateRenameMacro(s, metadata);
            this.structsGenerated.add(s.name);
        }
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

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        DBSPTypeStruct type = operator.originalRowType;
        if (!this.useHandles)
            this.generateStructHelpers(type, operator.metadata);

        this.writeComments(operator, false)
                .append("let (")
                .append(operator.getOutputName())
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
                    .append(operator.getOutputName())
                    .append(";")
                    .newline();
        }
        if (!this.useHandles) {
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
                    .append(operator.getOutputName())
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
        if (!this.useHandles)
            this.generateStructHelpers(type, operator.metadata);

        DBSPTypeStruct keyStructType = operator.getKeyStructType(
                new ProgramIdentifier(operator.originalRowType.sanitizedName + "_key", false));
        if (!this.useHandles)
            this.generateStructHelpers(keyStructType, operator.metadata);

        DBSPTypeStruct upsertStruct = operator.getStructUpsertType(
                new ProgramIdentifier(operator.originalRowType.sanitizedName + "_upsert", false));
        if (!this.useHandles)
            this.generateStructHelpers(upsertStruct, operator.metadata);

        this.writeComments(operator, false)
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
                    .append(operator.getOutputName())
                    .append(";")
                    .newline();
        }
        if (!this.useHandles) {
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
                .append("()")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPControlledKeyFilterOperator operator) {
        this.builder.append("let (")
                .append(operator.getOutputName(0))
                .append(", ")
                .append(operator.getOutputName(1))
                .append("): (");
        operator.streamType(0).accept(this.innerVisitor);
        this.builder.append(", ");
        operator.streamType(1).accept(this.innerVisitor);
        this.builder.append(") = ")
                .append(operator.left().getOutputName())
                .append(".")
                .append(operator.operation)
                .append("(&")
                .append(operator.right().getOutputName())
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
                .append(operator.getOutputName());
        this.builder.append(" = ")
                .append(operator.left().getOutputName());

        this.builder.append(".")
                .append(operator.operation)
                .append("(&")
                .append(operator.right().getOutputName())
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
                .append(operator.getOutputName())
                .append(" = ")
                .append(operator.left().getOutputName())
                .append(".")
                .append(operator.operation)
                .append("((")
                .append(operator.lowerInclusive)
                .append(", ")
                .append(operator.upperInclusive)
                .append("), &")
                .append(operator.right().getOutputName())
                .append(")")
                .append(this.markDistinct(operator))
                .append(";");
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
                .append("(");
        // This part is different from a standard operator.
        operator.init.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.extractTs.accept(this.innerVisitor);
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
        DBSPType type = operator.originalRowType;
        if (!this.useHandles) {
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
                        .append(operator.input().getOutputName())
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
                        .append(operator.input().getOutputName())
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
                        .append(operator.input().getOutputName())
                        .append(";")
                        .newline();
            }
        } else {
            this.builder.append("let ")
                    .append(this.handleName(operator))
                    .append(" = ")
                    .append(operator.input().getOutputName())
                    .append(".output();").newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(operator.inputs.get(0).getOutputName())
                    .append(".");
        this.builder.append(operator.operation)
                .append("(");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                this.builder.append(",");
            this.builder.append("&")
                    .append(operator.inputs.get(i).getOutputName());
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
                .append("(");
        operator.init.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
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
        this.builder.append(" };")
                .newline();
    }

    /** Helper function for generateComparator and generateCmpFunc.
     * @param ascending Comparison direction. */
    void emitCompare(boolean ascending) {
        this.builder.append("let ord = left.cmp(&right);")
                .newline()
                .append("if ord != Ordering::Equal { return ord");
        if (!ascending)
            this.builder.append(".reverse()");
        this.builder.append(" };")
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
        if (comparator.is(DBSPFieldComparatorExpression.class)) {
            DBSPFieldComparatorExpression fieldComparator = comparator.to(DBSPFieldComparatorExpression.class);
            this.generateComparator(fieldComparator.source, fieldsCompared);
            if (fieldsCompared.contains(fieldComparator.fieldNo))
                throw new InternalCompilerError("Field " + fieldComparator.fieldNo + " used twice in sorting");
            fieldsCompared.add(fieldComparator.fieldNo);
            this.emitCompareField(fieldComparator.fieldNo, fieldComparator.ascending);
        } else {
            DBSPDirectComparatorExpression direct = comparator.to(DBSPDirectComparatorExpression.class);
            this.generateComparator(direct.source, fieldsCompared);
            this.emitCompare(direct.ascending);
        }
    }

    /** Generate a comparator */
    void generateCmpFunc(DBSPComparatorExpression comparator) {
        // impl CmpFunc<(String, i32, i32)> for AscDesc {
        //     fn cmp(left: &(String, i32, i32), right: &(String, i32, i32)) -> std::cmp::Ordering {
        //         let ord = left.1.cmp(&right.1);
        //         if ord != Ordering::Equal { return ord; }
        //         let ord = right.2.cmp(&left.2);
        //         if ord != Ordering::Equal { return ord; }
        //         let ord = left.3.cmp(&right.3);
        //         if ord != Ordering::Equal { return ord; }
        //         return Ordering::Equal;
        //     }
        // }
        String structName = comparator.getComparatorStructName();
        this.builder.append("struct ")
                .append(structName)
                .append(";")
                .newline();

        DBSPType type = comparator.comparedValueType();
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
        if (type.is(DBSPTypeTuple.class)) {
            for (int i = 0; i < type.to(DBSPTypeTuple.class).size(); i++) {
                if (fieldsCompared.contains(i)) continue;
                this.emitCompareField(i, true);
            }
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

    /** Generate a static value */
    void generateStatic(DBSPStaticExpression stat) {
        // static NAME: LazyLock<type> = LazyLock::new(|| expression);
        String name = stat.getName();
        this.builder.append("static ")
                .append(name)
                .append(": LazyLock<");
        stat.getType().accept(this.innerVisitor);
        this.builder.append("> = LazyLock::new(|| ");
        stat.initializer.accept(this.innerVisitor);
        this.builder.append(");").newline();
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
        return result.closure(left, right);
    }

    @Override
    public VisitDecision preorder(DBSPIndexedTopKOperator operator) {
        DBSPComparatorExpression comparator = operator.getFunction().to(DBSPComparatorExpression.class);
        this.generateCmpFunc(comparator);
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
                .append(comparator.getComparatorStructName());
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
                DBSPExpression equalityComparison = this.generateEqualityComparison(
                        operator.getFunction());
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
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.left().getOutputName())
                .append(".")
                .append(operator.operation)
                .append("(&")
                .append(operator.right().getOutputName())
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
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.left().getOutputName())
                .append(".")
                .append("join_index")
                .append("(&")
                .append(operator.right().getOutputName())
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
        this.generateCmpFunc(operator.comparator);
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
                .append("::<_, _, _, ")
                .append(operator.comparator.getComparatorStructName())
                .append(", _>")
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

    @Override
    public VisitDecision preorder(DBSPPartitionedRollingAggregateOperator operator) {
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                // the output type is not correct, so we don't write it
                .append(" = ")
                .append(operator.input().getOutputName())
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
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                // the output type is not correct, so we don't write it
                .append(" = ")
                .append(operator.left().getOutputName())
                .append(".")
                .append(operator.operation)
                .append("(&")
                .append(operator.right().getOutputName())
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
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        this.builder.append(operator.input().getOutputName())
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
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        this.builder.append(operator.input().getOutputName())
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
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getOutputName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        this.builder.append(operator.left().getOutputName())
                .append(".");
        this.builder.append(operator.operation)
                .append("(&");
        this.builder.append(operator.right().getOutputName())
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
                .append(operator.getOutputName())
                .append(" = ")
                .append(operator.input().getOutputName())
                .append(";")
                .newline();
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

    IIndentStream writeComments(DBSPSimpleOperator operator, boolean verbose) {
        return this.writeComments(operator.getClass().getSimpleName() +
                " " + operator.getIdString() +
                (verbose ? (operator.comment != null ? "\n" + operator.comment : "") : ""));
    }

    IIndentStream writeComments(DBSPSimpleOperator operator) {
        return this.writeComments(operator, true);
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
            this.builder.append(operator.left().getOutputName())
                    .append(".");
        this.builder.append(operator.operation)
                .append("(&");
        this.builder.append(operator.right().getOutputName());
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(")")
                .append(this.markDistinct(operator))
                .append(";");
        return VisitDecision.STOP;
    }

    /** Collects all {@link DBSPComparatorExpression} that appear in some
     * {@link DBSPCustomOrdExpression}. */
    static class FindComparators extends InnerVisitor {
        final List<DBSPComparatorExpression> found = new ArrayList<>();

        public FindComparators(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPCustomOrdExpression expression) {
            this.found.add(expression.comparator);
        }
    }

    /** Collects all {@link DBSPComparatorExpression} that appear in some
     * {@link DBSPCustomOrdExpression}. */
    static class FindFunctions extends InnerVisitor {
        final Set<String> found = new HashSet<>();

        public FindFunctions(DBSPCompiler compiler) {
            super(compiler);
        }

        void add(DBSPExpression function) {
            if (function.is(DBSPPathExpression.class)) {
                String name = function.to(DBSPPathExpression.class).toString();
                this.found.add(name);
            }
        }

        @Override
        public void postorder(DBSPApplyExpression expression) {
            this.add(expression.function);
        }

        public void postorder(DBSPApplyMethodExpression expression) {
            this.add(expression.function);
        }
    }

    /** Collects all {@link DBSPStaticExpression}s that appear in an expression */
    static class FindStatics extends InnerVisitor {
        final Set<DBSPStaticExpression> found = new HashSet<>();

        public FindStatics(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPStaticExpression expression) {
            this.found.add(expression);
        }
    }

    VisitDecision constantLike(DBSPSimpleOperator operator) {
        assert operator.function != null;
        this.builder.append("let ")
                .append(operator.getOutputName())
                .append(":");
        operator.outputStreamType.accept(this.innerVisitor);
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

    public static String toRustString(DBSPCompiler compiler, DBSPCircuit node) {
        IndentStream stream = new IndentStreamBuilder();
        ToRustVisitor visitor = new ToRustVisitor(compiler, stream, node.getMetadata());
        visitor.apply(node);
        return stream.toString();
    }
}
