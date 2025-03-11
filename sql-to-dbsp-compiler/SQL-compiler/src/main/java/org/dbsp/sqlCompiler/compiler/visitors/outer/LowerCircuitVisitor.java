package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApply2Operator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructWithHelperItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Lowers a circuit's representation; most changes are about
 * generating compilable Rust for operator functions. */
public class LowerCircuitVisitor extends CircuitCloneVisitor {
    public LowerCircuitVisitor(DBSPCompiler compiler) {
        super(compiler, false);
    }

    /** Rewrite a flatmap operation into a Rust method call.
     * @param flatmap  Flatmap operation to rewrite. */
    public static DBSPExpression rewriteFlatmap(DBSPFlatmap flatmap) {
        //   move |x: &Tuple2<Array<i32>, Option<i32>>, | -> _ {
        //     let x0: Array<i32> = (*x.0).clone();
        //     let x1: x.1.clone();
        //     let array_clone = (*x).0.clone();
        //     array_clone.into_iter().map({
        //        move |e: i32, | -> Tuple3<Vec<i32>, Option<i32>, i32> {
        //            Tuple3::new(x0.clone(), x1.clone(), e)
        //        }
        //     })
        DBSPVariablePath rowVar = new DBSPVariablePath(flatmap.inputElementType.ref());
        DBSPType eType = flatmap.getCollectionElementType();
        DBSPType collectionElementType = eType;
        List<DBSPStatement> statements = new ArrayList<>();
        List<DBSPExpression> resultColumns = new ArrayList<>();
        for (int i = 0; i < flatmap.leftInputIndexes.size(); i++) {
            int index = flatmap.leftInputIndexes.get(i);
            // let x0: Vec<i32> = x.0.clone();
            // let x1: x.1.clone();
            DBSPExpression field = rowVar.deref().field(index).applyCloneIfNeeded();
            DBSPVariablePath fieldClone = field.getType().var();
            if (flatmap.shuffle.emitsIndex(i)) {
                DBSPLetStatement stat = new DBSPLetStatement(fieldClone.variable, field);
                statements.add(stat);
            }
            resultColumns.add(fieldClone.applyCloneIfNeeded());
        }

        if (flatmap.ordinalityIndexType != null) {
            eType = new DBSPTypeRawTuple(DBSPTypeUSize.INSTANCE, eType);
            // e.0 is usize, from the Rust library, so we do arithmetic using usize
            // and convert at the end.
        }
        DBSPVariablePath e = eType.var();

        // e.0 + 1
        DBSPExpression e0plus1 = null;
        if (flatmap.ordinalityIndexType != null) {
            e0plus1 = new DBSPBinaryExpression(flatmap.getNode(),
                    DBSPTypeUSize.INSTANCE, DBSPOpcode.ADD,
                    e.field(0),
                    new DBSPUSizeLiteral(1)).cast(flatmap.ordinalityIndexType, false);
        }

        if (flatmap.rightProjections != null) {
            for (DBSPClosureExpression clo: flatmap.rightProjections) {
                List<DBSPExpression> fields;
                DBSPExpression base = e;
                if (flatmap.ordinalityIndexType != null)
                    base = e.field(1);

                if (collectionElementType.is(DBSPTypeTupleBase.class)) {
                    // apply closure to (e.1.0, e.1.1, ..., e.0+1)
                    fields = DBSPTupleExpression.flatten(base).allFields();
                } else {
                    // apply closure to (e.1, e.0+1)
                    fields = Linq.list(base.applyCloneIfNeeded());
                }
                if (flatmap.ordinalityIndexType != null)
                    fields.add(e0plus1);
                DBSPExpression argument = new DBSPTupleExpression(
                        fields, flatmap.getCollectionElementType().mayBeNull);
                resultColumns.add(clo.call(argument.borrow()).applyCloneIfNeeded());
            }
        } else {
            if (flatmap.ordinalityIndexType != null) {
                // e.1, as produced by the iterator
                DBSPExpression eField1 = e.field(1);
                // If this is a tuple, flatten it.
                if (eField1.getType().is(DBSPTypeTuple.class)) {
                    resultColumns.addAll(DBSPTypeTuple.flatten(eField1));
                } else {
                    resultColumns.add(eField1.applyCloneIfNeeded());
                }
            } else if (e.getType().is(DBSPTypeTupleBase.class)) {
                // Calcite's UNNEST has a strange semantics:
                // If e is a tuple type, unpack its fields here
                DBSPTypeTupleBase tuple = e.getType().to(DBSPTypeTupleBase.class);
                for (int ei = 0; ei < tuple.size(); ei++)
                    resultColumns.add(e.field(ei).applyCloneIfNeeded());
            } else {
                // e
                resultColumns.add(e);
            }
            if (flatmap.ordinalityIndexType != null) {
                resultColumns.add(e0plus1);
            }
        }
        resultColumns = flatmap.shuffle.shuffle(resultColumns);

        // let array = if (*x).0.is_none() {
        //    vec!()
        // } else {
        //    (*x).0.clone().unwrap()
        // };
        // or
        // let array = (*x).0.clone();
        DBSPExpression extractArray = flatmap.collectionExpression.call(rowVar).applyClone();
        DBSPLetStatement statement = new DBSPLetStatement("array", extractArray);
        statements.add(statement);
        DBSPType arrayType = extractArray.getType();

        DBSPExpression arrayExpression;
        if (arrayType.mayBeNull) {
            DBSPExpression condition = statement.getVarReference().is_null();
            DBSPExpression empty = new DBSPTypeVec(collectionElementType, false).emptyVector();
            DBSPExpression contents = statement.getVarReference().unwrap().deref().applyClone();
            arrayExpression = new DBSPIfExpression(flatmap.getNode(), condition, empty, contents);
        } else {
            arrayExpression = statement.getVarReference().deref().applyClone();
        }
        statement = new DBSPLetStatement("array_clone", arrayExpression);
        statements.add(statement);

        // move |e: i32, | -> Tuple3<Vec<i32>, Option<i32>, i32> {
        //   Tuple3::new(x0.clone(), x1.clone(), e)
        // }
        DBSPClosureExpression toTuple = new DBSPTupleExpression(resultColumns, false)
                .closure(e);
        DBSPExpression iter = new DBSPApplyMethodExpression(flatmap.getNode(), "into_iter",
                DBSPTypeAny.getDefault(),
                statement.getVarReference());
        if (flatmap.ordinalityIndexType != null) {
            iter = new DBSPApplyMethodExpression(flatmap.getNode(), "enumerate",
                    DBSPTypeAny.getDefault(), iter);
        }
        DBSPExpression function = new DBSPApplyMethodExpression(flatmap.getNode(),
                "map", DBSPTypeAny.getDefault(),
                iter, toTuple);
        DBSPExpression block = new DBSPBlockExpression(statements, function);
        return block.closure(rowVar);
    }

    @Override
    public void postorder(DBSPFlatMapOperator node) {
        DBSPSimpleOperator result;
        if (node.getFunction().is(DBSPFlatmap.class)) {
            List<OutputPort> sources = Linq.map(node.inputs, this::mapped);
            DBSPExpression function = rewriteFlatmap(node.getFunction().to(DBSPFlatmap.class));
            result = node.withFunction(function, node.outputType).withInputs(sources, this.force);
            this.map(node, result);
        } else {
            super.postorder(node);
        }
    }

    @Override
    public void postorder(DBSPApplyOperator node) {
        if (this.getDebugLevel() < 1) {
            super.postorder(node);
            return;
        }
        // Instrument apply functions to print their parameters
        DBSPClosureExpression func = node.getClosureFunction();
        DBSPExpression print = new DBSPApplyExpression("println!", DBSPTypeAny.getDefault(),
                new DBSPStrLiteral(func.parameters[0].name + "={:?}"), func.parameters[0].asVariable());
        DBSPExpression block = new DBSPBlockExpression(
                Linq.list(new DBSPExpressionStatement(print)),
                func.body);
        DBSPSimpleOperator instrumented = node.withFunction(block.closure(func.parameters), func.getResultType())
                .withInputs(Linq.map(node.inputs, this::mapped), false);
        this.map(node, instrumented);
    }

    @Override
    public void postorder(DBSPApply2Operator node) {
        if (this.getDebugLevel() < 1) {
            super.postorder(node);
            return;
        }
        // Instrument apply functions to print their parameters
        DBSPClosureExpression func = node.getClosureFunction();
        DBSPExpression print = new DBSPApplyExpression("println!", DBSPTypeAny.getDefault(),
                new DBSPStrLiteral(func.parameters[0].name + "={:?}," + func.parameters[1].name + "={:?}"),
                func.parameters[0].asVariable(), func.parameters[1].asVariable());
        DBSPExpression block = new DBSPBlockExpression(
                Linq.list(new DBSPExpressionStatement(print)),
                func.body);
        DBSPSimpleOperator instrumented = node.withFunction(block.closure(func.parameters), func.getResultType())
                .withInputs(Linq.map(node.inputs, this::mapped), false);
        this.map(node, instrumented);
    }

    void generateStructHelpers(DBSPType struct, @Nullable TableMetadata metadata) {
        List<DBSPTypeStruct> nested = new ArrayList<>();
        FindNestedStructs fn = new FindNestedStructs(this.compiler, nested);
        fn.apply(struct);
        DBSPCircuit underConstruction = this.getUnderConstructionCircuit();
        for (DBSPTypeStruct s: nested) {
            DBSPStructWithHelperItem item = new DBSPStructWithHelperItem(s, metadata);
            DBSPDeclaration previous = underConstruction.getDeclaration(item.getName());
            if (previous != null) {
                if (previous.is(DBSPStructWithHelperItem.class))
                    continue;
                // Replace a struct declaration with a more general StructWithHelper
                underConstruction.replaceDeclaration(new DBSPDeclaration(item));
            } else {
                underConstruction.addDeclaration(new DBSPDeclaration(item));
            }
        }
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        if (!this.compiler.options.ioOptions.emitHandles) {
            this.generateStructHelpers(operator.originalRowType, operator.metadata);
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSourceMapOperator operator) {
        if (!this.compiler.options.ioOptions.emitHandles) {
            DBSPTypeStruct type = operator.originalRowType;
            this.generateStructHelpers(type, operator.metadata);

            DBSPTypeStruct keyStructType = operator.getKeyStructType(
                    new ProgramIdentifier(operator.originalRowType.sanitizedName + "_key", false));
            this.generateStructHelpers(keyStructType, operator.metadata);

            DBSPTypeStruct upsertStruct = operator.getStructUpsertType(
                    new ProgramIdentifier(operator.originalRowType.sanitizedName + "_upsert", false));
            this.generateStructHelpers(upsertStruct, operator.metadata);
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        if (!this.compiler.options.ioOptions.emitHandles) {
            this.generateStructHelpers(operator.originalRowType, null);
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator node) {
        if (node.function != null) {
            // OrderBy
            super.postorder(node);
            return;
        }

        DBSPExpression function = node.getAggregate().asFold(this.compiler());
        DBSPSimpleOperator result = new DBSPStreamAggregateOperator(
                node.getRelNode(), node.getOutputIndexedZSetType(),
                function, null, this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPAggregateOperator node) {
        if (node.function != null) {
            // OrderBy
            super.postorder(node);
            return;
        }
        DBSPExpression function = node.getAggregate().asFold(this.compiler());
        DBSPSimpleOperator result = new DBSPAggregateOperator(
                node.getRelNode(), node.getOutputIndexedZSetType(),
                function, null, this.mapped(node.input()));
        this.map(node, result);
    }

    public static DBSPClosureExpression lowerJoinFilterMapFunctions(
            DBSPCompiler compiler, DBSPJoinFilterMapOperator node) {
        if (node.filter == null)
            return node.getClosureFunction();
        if (node.map == null) {
            // Generate code of the form
            // let tmp = join(...);
            // if (filter(tmp)) {
            //    Some(tmp)
            // } else {
            //     None
            // }
            DBSPClosureExpression expression = node.getClosureFunction();
            DBSPClosureExpression filter = node.filter.to(DBSPClosureExpression.class);
            DBSPLetStatement let = new DBSPLetStatement("tmp", expression.body);
            DBSPExpression cond = filter.call(let.getVarReference().borrow()).reduce(compiler);
            DBSPExpression tmp = let.getVarReference();
            DBSPIfExpression ifexp = new DBSPIfExpression(
                    node.getRelNode(),
                    cond,
                    tmp.some(),
                    tmp.getType().withMayBeNull(true).none());
            DBSPBlockExpression block = new DBSPBlockExpression(Linq.list(let), ifexp);
            return block.closure(expression.parameters);
        } else {
            // Generate code of the form
            // if (filter(join(...)) {
            //    Some(map(join(...)))
            // } else {
            //    None
            // }
            DBSPClosureExpression expression = node.getClosureFunction();
            DBSPClosureExpression filter = node.filter.to(DBSPClosureExpression.class);
            DBSPExpression cond = filter
                    .call(expression.body.borrow())
                    .reduce(compiler);
            DBSPExpression map = node.map.to(DBSPClosureExpression.class)
                    .call(expression.body.borrow())
                    .reduce(compiler);
            DBSPIfExpression ifexp = new DBSPIfExpression(
                    node.getRelNode(),
                    cond,
                    map.some(),
                    map.getType().withMayBeNull(true).none());
            return ifexp.closure(expression.parameters);
        }
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator node) {
        if (node.filter == null) {
            // Already lowered
            super.postorder(node);
            return;
        }
        DBSPExpression newFunction = lowerJoinFilterMapFunctions(this.compiler(), node);
        DBSPSimpleOperator result = new DBSPJoinFilterMapOperator(node.getRelNode(), node.getOutputZSetType(),
                newFunction, null, null, node.isMultiset,
                this.mapped(node.left()), this.mapped(node.right()))
                .copyAnnotations(node);
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPNoopOperator node) {
        DBSPSimpleOperator replacement;
        if (node.outputType.is(DBSPTypeZSet.class)) {
            replacement = new DBSPMapOperator(node.getRelNode(), node.getFunction(),
                    node.getOutputZSetType(), this.mapped(node.input()));
        } else {
            replacement = new DBSPMapIndexOperator(node.getRelNode(), node.getFunction(),
                    node.getOutputIndexedZSetType(), this.mapped(node.input()));
        }
        this.map(node, replacement);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator node) {
        if (node.aggregate == null) {
            super.postorder(node);
            return;
        }
        DBSPExpression function = node.getAggregate().asFold(this.compiler());
        DBSPSimpleOperator result = new DBSPPartitionedRollingAggregateOperator(node.getRelNode(),
                node.partitioningFunction, function, null, node.lower, node.upper,
                node.getOutputIndexedZSetType(), this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
        if (node.aggregate == null) {
            super.postorder(node);
            return;
        }
        DBSPExpression function = node.aggregate.asFold(this.compiler());
        DBSPSimpleOperator result = new DBSPPartitionedRollingAggregateWithWaterlineOperator(node.getRelNode(),
                node.partitioningFunction, function, null, node.lower, node.upper,
                node.getOutputIndexedZSetType(),
                this.mapped(node.left()), this.mapped(node.right()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPChainOperator node) {
        DBSPClosureExpression function = node.chain.collapse(this.compiler);
        DBSPSimpleOperator result;
        if (node.outputType.is(DBSPTypeZSet.class)) {
            result = new DBSPFlatMapOperator(
                    node.getRelNode(), function, node.getOutputZSetType(),
                    node.isMultiset, this.mapped(node.input()));
        } else {
            result = new DBSPFlatMapIndexOperator(
                    node.getRelNode(), function, node.getOutputIndexedZSetType(),
                    node.isMultiset, this.mapped(node.input()));
        }
        this.map(node, result);
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
}
