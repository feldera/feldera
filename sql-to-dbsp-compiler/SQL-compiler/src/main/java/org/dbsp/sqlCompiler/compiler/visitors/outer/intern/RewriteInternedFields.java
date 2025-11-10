package org.dbsp.sqlCompiler.compiler.visitors.outer.intern;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregator;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeInterned;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.IdShuffle;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Oouter visitor which rewrites the graph to convert string types to interned strings */
public class RewriteInternedFields extends CircuitCloneVisitor {
    /** Operators that collect all interned strings */
    private final List<OutputPort> dynamicStrings;
    private final Map<DBSPSourceTableOperator, Intern.InternedColumnList> internedInputs;
    private final Set<DBSPStringLiteral> allStringLiterals;

    public RewriteInternedFields(DBSPCompiler compiler,
                                 Map<DBSPSourceTableOperator, Intern.InternedColumnList> internedInputs) {
        super(compiler, false);
        this.internedInputs = internedInputs;
        this.dynamicStrings = new ArrayList<>();
        this.preservesTypes = false;
        this.allStringLiterals = new HashSet<>();
    }

    boolean internConstants() {
        return !this.internedInputs.isEmpty();
    }

    /** Given a source operator that needs to have some fields interned, create the following nodes:
     * source (x0, S0, x1, S1)  where SX is a (string) field that will be interned
     *   | \
     *  map \  (x0, intern(cast_to_nullable(S0)), x1, intern(cast_to_nullable(S1)), )
     *       map (array(cast_to_nullable(S0), cast_to_nullable(S1))
     *        \
     *       flatmap    flattens the array - inserted in allStrings
     *  An array is used only if more than 1 interned field is necessary.
     *  If there is no array, the flatmap is not used.
     */
    @Override
    public void postorder(DBSPSourceMultisetOperator source) {
        Intern.InternedColumnList list = this.internedInputs.get(source);
        if (list == null || list.isEmpty()) {
            this.map(source, source);
            return;
        }

        this.addOperator(source);
        boolean useArray = list.size() > 1;
        var tuple = source.getOutputZSetElementType().to(DBSPTypeTuple.class);
        DBSPVariablePath var = tuple.ref().var();
        DBSPVariablePath var1 = tuple.ref().var();
        DBSPExpression[] fields = new DBSPExpression[tuple.size()];
        DBSPExpression[] originalFields = new DBSPExpression[list.size()];
        int index = 0;
        for (int i = 0; i < tuple.size(); i++) {
            DBSPType fieldType = tuple.tupFields[i];
            DBSPExpression field = var.deref().field(i).applyCloneIfNeeded();
            if (list.contains(i) && fieldType.code == DBSPTypeCode.STRING) {
                fields[i] = field.cast(field.getNode(), InternInner.nullableString, false);
                field = InternInner.callIntern(field);
                DBSPExpression field1 = var1.deref().field(i).applyCloneIfNeeded()
                        .cast(field.getNode(), InternInner.nullableString, false);
                originalFields[index] = field1;
                index++;
            }
            fields[i] = field;
        }
        DBSPClosureExpression closure = new DBSPTupleExpression(fields).closure(var);
        CalciteRelNode node = CalciteEmptyRel.INSTANCE; // Can't use the source itself
        DBSPMapOperator map = new DBSPMapOperator(node, closure, source.outputPort());
        this.map(source, map);

        DBSPSimpleOperator unnest;

        // Map that extracts only the interned fields.
        if (tuple.size() == 1) {
             unnest = source;
        } else {
            DBSPExpression internedFields;
            if (useArray)
                internedFields = new DBSPTupleExpression(new DBSPArrayExpression(originalFields));
            else
                internedFields = new DBSPTupleExpression(originalFields);
            DBSPTypeTuple unnestInputType = internedFields.type.to(DBSPTypeTuple.class);
            DBSPClosureExpression extractor = internedFields.closure(var1);
            unnest = new DBSPMapOperator(node, extractor, source.outputPort());
            this.addOperator(unnest);

            if (useArray) {
                DBSPType outputType = new DBSPTypeTuple(InternInner.nullableString);
                // Tuple is nullable - required by flatmap
                DBSPVariablePath var4 = new DBSPTypeTuple(CalciteObject.EMPTY, true, Linq.list(InternInner.nullableString))
                        .ref().var();
                DBSPClosureExpression f0 = var4.deref().field(0).applyClone().closure(var4);
                var var2 = unnestInputType.ref().var();
                DBSPFlatmap function = new DBSPFlatmap(
                        node,
                        unnestInputType, // input type
                        var2.deref().field(0).closure(var2), // collection expression
                        Linq.list(), // left input indexes
                        Linq.list(f0), // right projections
                        null, // ordinalityIndexType
                        new IdShuffle(1));
                unnest = new DBSPFlatMapOperator(node, function,
                        new DBSPTypeZSet(outputType), this.internConstants(), unnest.outputPort());
                this.addOperator(unnest);
            }
        }

        if (!unnest.getOutputZSetElementType().sameType(InternInner.nullableString)) {
            DBSPVariablePath v = unnest.getOutputZSetElementType().ref().var();
            DBSPClosureExpression clo = new DBSPTupleExpression(
                    v.deref()
                            .field(0)
                            .applyClone()
                            .cast(v.getNode(), InternInner.nullableString, false))
                    .closure(v);
            unnest = new DBSPMapOperator(node, clo, unnest.outputPort());
            this.addOperator(unnest);
        }
        this.dynamicStrings.add(unnest.outputPort());
    }

    void processUnary(DBSPUnaryOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            this.map(operator, operator);
            return;
        }
        InternInner interner = new InternInner(this.compiler, this.internConstants(),
                false, input.getOutputRowRefType());
        DBSPExpression function = interner.apply(operator.getFunction()).to(DBSPExpression.class);
        DBSPType outputType;
        if (operator.is(DBSPFilterOperator.class)) {
            outputType = input.outputType();
        } else {
            outputType = createCollectionType(function.getType()
                    .to(DBSPTypeFunction.class)
                    .resultType);
        }
        if (!HasInternedTypes.check(this.compiler, outputType))
            Utilities.enforce(outputType.sameType(operator.outputType),
                    () -> "Output type not preserved for " + operator + " during interning:\n" +
            outputType + ", expected\n" + operator.outputType);
        DBSPSimpleOperator replacement = operator.with(
                function, outputType, Linq.list(input), false)
                .to(DBSPSimpleOperator.class);
        this.allStringLiterals.addAll(interner.globalStrings);
        this.map(operator, replacement);
    }

    static DBSPType createCollectionType(DBSPType elementType) {
        if (elementType.is(DBSPTypeTupleBase.class))
            return elementType.to(DBSPTypeTupleBase.class).intoCollectionType();
        return new DBSPTypeZSet(elementType);
    }

    @Override
    public void typecheckBackEdges(DBSPNestedOperator operator) {
        for (DBSPOperator node: operator.getAllOperators()) {
            // Check that back-edges still type-check
            if (node.is(DBSPViewDeclarationOperator.class)) {
                DBSPViewDeclarationOperator decl = node.to(DBSPViewDeclarationOperator.class);
                DBSPSimpleOperator source = decl.getCorrespondingView(operator);
                if (source == null) {
                    int index = operator.outputViews.indexOf(decl.originalViewName());
                    source = operator.internalOutputs.get(index).simpleNode();
                }
                DBSPType outputType = node.getOutput(0).outputType();
                if (!outputType.sameType(source.outputType)) {
                    this.compiler.reportError(decl.viewDeclaration.getPositionRange(), "Type mismatch",
                            "Type declared for view " + decl.metadata.tableName.singleQuote() + "\n" +
                                    decl.originalRowType.asSqlString() +
                                    "\ndoes not match the inferred type\n" +
                                            source.getOutputZSetElementType().asSqlString() +
                            "\n(The INTERNED annotations must match).");
                }
            }
        }
    }

    @Override
    public void postorder(DBSPHopOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            super.postorder(operator);
            return;
        }

        DBSPType[] outputFields = new DBSPType[operator.outputType.getToplevelFieldCount()];
        DBSPTypeTuple inputElementType = input.getOutputZSetElementType().to(DBSPTypeTuple.class);
        DBSPTypeTuple originalElementType = operator.getOutputZSetElementType().to(DBSPTypeTuple.class);
        for (int i = 0; i < outputFields.length; i++) {
            if (i < inputElementType.size())
                outputFields[i] = inputElementType.getFieldType(i);
            else
                outputFields[i] = originalElementType.getFieldType(i);
        }
        DBSPTypeZSet outputType = new DBSPTypeZSet(new DBSPTypeTuple(outputFields));
        DBSPHopOperator result = new DBSPHopOperator(
                operator.getRelNode(), operator.timestampIndex,
                operator.interval, operator.start, operator.size, outputType, input);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            super.postorder(operator);
            return;
        }

        DBSPFlatmap flatMap = operator.getFunction().to(DBSPFlatmap.class);
        InternInner interner = new InternInner(this.compiler, this.internConstants(),
                false, input.getOutputRowRefType());
        DBSPFlatmap function = interner.apply(flatMap).to(DBSPFlatmap.class);
        DBSPSimpleOperator replacement = new DBSPFlatMapOperator(
                operator.getRelNode(),
                function,
                new DBSPTypeZSet(function.getType().to(DBSPTypeFunction.class).resultType),
                operator.isMultiset,
                input);
        this.allStringLiterals.addAll(interner.globalStrings);
        this.map(operator, replacement);
    }

    /** Given an operator computing on indexed Z-sets, unintern all fields in the value
      * part of the indexed ZSet, and pass through the indexed part unchanged */
    void uninternValue(DBSPUnaryOperator operator) {
        OutputPort input = this.mapped(operator.input());
        var zSet = input.getOutputIndexedZSetType();
        var originalZset = operator.input().getOutputIndexedZSetType();
        if (!HasInternedTypes.check(this.compiler, zSet.elementType)) {
            super.postorder(operator);
            return;
        }

        DBSPVariablePath var = input.getOutputIndexedZSetType().getKVRefType().var();
        DBSPExpression convertValue = this.unintern(var.field(1).deref(), originalZset.elementType);
        DBSPClosureExpression converter = new DBSPRawTupleExpression(
                DBSPTupleExpression.flatten(var.field(0).deref()),
                convertValue).closure(var);
        DBSPMapIndexOperator reindex = new DBSPMapIndexOperator(
                operator.getRelNode(),
                converter, input);
        this.addOperator(reindex);

        DBSPType originalValueType = operator.getOutputIndexedZSetType().elementType;
        DBSPType outputType = new DBSPTypeIndexedZSet(zSet.getNode(), zSet.keyType, originalValueType);
        DBSPSimpleOperator result = operator.with(operator.getFunction(), outputType,
                Linq.list(reindex.outputPort()), false)
                .to(DBSPSimpleOperator.class);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator operator) {
        // Unfortunately we have to unintern everything in the value,
        // since the comparator will touch all the input value fields
        // TODO: maybe this can be improved
        this.uninternValue(operator);
    }

    @Override
    public void postorder(DBSPLagOperator operator) {
        // Unfortunately we have to unintern everything in the value,
        // since the comparator will touch all the input value fields
        // TODO: maybe this can be improved
        this.uninternValue(operator);
    }

    void aggregate(DBSPAggregateOperatorBase operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            super.postorder(operator);
            return;
        }
        DBSPTypeIndexedZSet inputType = input.getOutputIndexedZSetType();

        InternInner interner = new InternInner(this.compiler, false, false,
                inputType.elementType.ref());
        DBSPAggregateList aggregateList = null;
        DBSPAggregator aggregator = null;
        DBSPType rewrittenResultType;
        if (operator.aggregateList != null) {
            aggregateList = operator.aggregateList;
            aggregateList = interner.apply(aggregateList).to(DBSPAggregateList.class);
            rewrittenResultType = aggregateList.getType().to(DBSPTypeFunction.class).resultType;
        } else {
            aggregator = operator.getAggregator();
            Utilities.enforce(aggregator != null);
            aggregator = interner.apply(aggregator).to(DBSPAggregator.class);
            rewrittenResultType = aggregator.getType().to(DBSPTypeFunction.class).resultType;
        }

        DBSPSimpleOperator replacement;
        if (operator.is(DBSPStreamAggregateOperator.class)) {
            replacement = new DBSPStreamAggregateOperator(operator.getRelNode(),
                    new DBSPTypeIndexedZSet(inputType.getNode(), inputType.keyType, rewrittenResultType),
                    aggregator, aggregateList, input);
        } else if (operator.is(DBSPAggregateOperator.class)) {
            replacement = new DBSPAggregateOperator(operator.getRelNode(),
                    new DBSPTypeIndexedZSet(inputType.getNode(), inputType.keyType, rewrittenResultType),
                    aggregator, aggregateList, input);
        } else {
            throw new InternalCompilerError("Unexpected aggregate operator " + operator);
        }
        this.map(operator, replacement);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        this.aggregate(operator);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        this.aggregate(operator);
    }

    @Override
    public void postorder(DBSPViewDeclarationOperator operator) {
        var tuple = operator.getOutputZSetElementType().to(DBSPTypeTuple.class);
        Utilities.enforce(operator.metadata.getColumnCount() == tuple.size());
        DBSPType[] fields = new DBSPType[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
            DBSPType fieldType = tuple.tupFields[i];
            var metadata = operator.metadata.getColumnMetadata(i);
            if (metadata.interned && fieldType.code == DBSPTypeCode.STRING) {
                fields[i] = DBSPTypeInterned.INSTANCE;
            } else {
                fields[i] = fieldType;
            }
        }
        DBSPType outputType = new DBSPTypeZSet(new DBSPTypeTuple(fields));
        DBSPSimpleOperator result = operator.withFunction(null, outputType)
                .to(DBSPSimpleOperator.class);
        this.map(operator, result);
    }

    OutputPort uninternStream(CalciteRelNode node, OutputPort port) {
        OutputPort newStream = this.mapped(port);
        if (newStream.equals(port))
            return newStream;
        DBSPType newType = newStream.outputType();
        if (!HasInternedTypes.check(this.compiler, newType))
            return newStream;
        if (newType.is(DBSPTypeZSet.class)) {
            DBSPTypeZSet zset = port.getOutputZSetType();
            DBSPVariablePath var = zset.elementType.ref().var();
            // Essentially an identity function
            DBSPClosureExpression id =
                    ExpressionCompiler.expandTuple(var.getNode(), var.deref())
                            .closure(var);
            InternInner intern = new InternInner(this.compiler, false, true,
                    newStream.getOutputZSetElementType());
            DBSPClosureExpression uninterner = intern.apply(id).to(DBSPClosureExpression.class);
            DBSPMapOperator map = new DBSPMapOperator(node, uninterner, newStream);
            this.addOperator(map);
            return map.outputPort();
        } else if (newType.is(DBSPTypeIndexedZSet.class)) {
            DBSPTypeIndexedZSet ix = port.getOutputIndexedZSetType();
            DBSPVariablePath var = ix.getKVRefType().var();
            // Essentially an identity function
            DBSPClosureExpression id =
                    new DBSPRawTupleExpression(
                            ExpressionCompiler.expandTuple(var.getNode(), var.field(0).deref()),
                            ExpressionCompiler.expandTuple(var.getNode(), var.field(1).deref()))
                            .closure(var);
            InternInner intern = new InternInner(this.compiler, false, true,
                    newStream.getOutputIndexedZSetType().getKVRefType());
            DBSPClosureExpression uninterner = intern.apply(id).to(DBSPClosureExpression.class);
            DBSPMapIndexOperator map = new DBSPMapIndexOperator(node, uninterner, newStream);
            this.addOperator(map);
            return map.outputPort();
        } else {
            throw new InternalCompilerError("Unexpected stream type " + newType);
        }
    }

    void uninternAllInputs(DBSPSimpleOperator operator) {
        List<OutputPort> inputs = Linq.map(
                operator.inputs, i -> this.uninternStream(operator.getRelNode(), i));
        DBSPSimpleOperator result = operator.withInputs(inputs, false)
                .to(DBSPSimpleOperator.class);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        // TODO: this can be optimized
        this.uninternAllInputs(operator);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
        // TODO: this can be optimized
        this.uninternAllInputs(operator);
    }

    @Override
    public void postorder(DBSPAsofJoinOperator operator) {
        // TODO: this can be optimized
        this.uninternAllInputs(operator);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            super.postorder(operator);
            return;
        }
        DBSPTypeIndexedZSet inputType = input.getOutputIndexedZSetType();

        InternInner interner = new InternInner(this.compiler, false,
                false, inputType.elementType.ref());
        DBSPClosureExpression rewritten = interner.apply(operator.getFunction()).to(DBSPClosureExpression.class);
        DBSPSimpleOperator replacement = new DBSPAggregateLinearPostprocessOperator(operator.getRelNode(),
                new DBSPTypeIndexedZSet(
                        inputType.getNode(), inputType.keyType, operator.getOutputIndexedZSetType().elementType),
                rewritten, operator.postProcess, input);
        this.map(operator, replacement);
    }

    /** Return a type that prefers uninterned fields to non-interned fields */
    DBSPType lessInternedType(DBSPType left, DBSPType right) {
        if (left.sameType(right))
            return left;
        if (left.code == DBSPTypeCode.INTERNED_STRING) {
            Utilities.enforce(right.code == DBSPTypeCode.STRING);
            return right;
        }
        if (right.code == DBSPTypeCode.INTERNED_STRING) {
            Utilities.enforce(left.code == DBSPTypeCode.STRING);
            return left;
        }
        Utilities.enforce(left.code == right.code);
        if (left.code == DBSPTypeCode.TUPLE || left.code == DBSPTypeCode.RAW_TUPLE) {
            DBSPTypeTupleBase leftTuple = left.to(DBSPTypeTupleBase.class);
            DBSPTypeTupleBase rightTuple = right.to(DBSPTypeTupleBase.class);
            Utilities.enforce(leftTuple.size() == rightTuple.size());
            List<DBSPType> fields = new ArrayList<>(leftTuple.size());
            for (int i = 0; i < leftTuple.size(); i++)
                fields.add(lessInternedType(leftTuple.getFieldType(i), rightTuple.getFieldType(i)));
            return leftTuple.makeRelatedTupleType(fields);
        }
        throw new InternalCompilerError("Unexpected type " + left);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<OutputPort> inputs = Linq.map(operator.inputs, this::mapped);
        if (!operator.inputsDiffer(inputs, false)) {
            super.postorder(operator);
            return;
        }

        DBSPType commonType = inputs.get(0).getOutputRowType();
        for (int i = 1; i < inputs.size(); i++)
            commonType = this.lessInternedType(commonType, inputs.get(i).getOutputRowType());
        DBSPTypeTupleBase tuple = commonType.to(DBSPTypeTuple.class);
        for (int i = 0; i < inputs.size(); i++) {
            DBSPType inputType = inputs.get(i).getOutputRowType();
            if (inputType.sameType(commonType))
                continue;

            DBSPVariablePath var = inputs.get(i).getOutputRowRefType().var();
            DBSPSimpleOperator map;
            if (inputType.code == DBSPTypeCode.RAW_TUPLE) {
                DBSPExpression convert =
                        new DBSPRawTupleExpression(
                                this.unintern(var.field(0).deref(), tuple.tupFields[0]),
                                this.unintern(var.field(1).deref(), tuple.tupFields[1]));
                DBSPClosureExpression converter = convert.closure(var);
                map = new DBSPMapIndexOperator(operator.getRelNode(), converter, inputs.get(i));
            } else {
                DBSPExpression convert = this.unintern(var.deref(), commonType);
                DBSPClosureExpression converter = convert.closure(var);
                map = new DBSPMapOperator(operator.getRelNode(), converter, inputs.get(i));
            }
            inputs.set(i, map.outputPort());
            this.addOperator(map);
        }
        DBSPSumOperator result = new DBSPSumOperator(operator.getRelNode(), inputs);
        this.map(operator, result);
    }

    OutputPort uninternKey(OutputPort source, DBSPTypeIndexedZSet sourceType, DBSPTypeTuple commonKeyType) {
        if (sourceType.keyType.sameType(commonKeyType))
            return source;
        DBSPVariablePath var = sourceType.getKVRefType().var();
        DBSPExpression convertKey = this.unintern(var.field(0).deref(), commonKeyType);
        DBSPClosureExpression converter = new DBSPRawTupleExpression(
                convertKey,
                DBSPTupleExpression.flatten(var.field(1).deref())).closure(var);
        DBSPMapIndexOperator result = new DBSPMapIndexOperator(
                source.operator.getRelNode(),
                converter, source);
        this.addOperator(result);
        return result.outputPort();
    }

    void processJoin(DBSPJoinBaseOperator operator) {
        // If key types to not match, they have to be reconciled.
        // Currently, use unintern; the opposite is possible too, but requires more work.
        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        if (left.equals(operator.left()) && right.equals(operator.right())) {
            this.map(operator, operator);
            return;
        }
        DBSPTypeIndexedZSet leftType = left.getOutputIndexedZSetType();
        DBSPTypeIndexedZSet rightType = right.getOutputIndexedZSetType();

        DBSPTypeTuple leftKey = leftType.keyType.to(DBSPTypeTuple.class);
        DBSPTypeTuple rightKey = rightType.keyType.to(DBSPTypeTuple.class);
        DBSPTypeTuple commonKeyType = lessInternedType(leftKey, rightKey).to(DBSPTypeTuple.class);
        left = this.uninternKey(left, leftType, commonKeyType);
        right = this.uninternKey(right, rightType, commonKeyType);

        InternInner interner = new InternInner(this.compiler, false, false, commonKeyType.ref(),
                leftType.elementType.ref(), rightType.elementType.ref());
        DBSPClosureExpression function = interner.apply(operator.getFunction()).to(DBSPClosureExpression.class);
        DBSPSimpleOperator replacement = operator.with(function,
                function.getResultType().to(DBSPTypeTupleBase.class).intoCollectionType(),
                Linq.list(left, right), false)
                .to(DBSPSimpleOperator.class);
        this.map(operator, replacement);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        // If key types to not match, they have to be reconciled.
        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        if (left.equals(operator.left()) && right.equals(operator.right())) {
            this.map(operator, operator);
            return;
        }
        DBSPTypeIndexedZSet leftType = left.getOutputIndexedZSetType();
        DBSPTypeIndexedZSet rightType = right.getOutputIndexedZSetType();

        DBSPTypeTuple leftKey = leftType.keyType.to(DBSPTypeTuple.class);
        DBSPTypeTuple rightKey = rightType.keyType.to(DBSPTypeTuple.class);
        DBSPTypeTuple commonKeyType = lessInternedType(leftKey, rightKey).to(DBSPTypeTuple.class);
        left = this.uninternKey(left, leftType, commonKeyType);
        right = this.uninternKey(right, rightType, commonKeyType);
        DBSPSimpleOperator replacement = new DBSPStreamAntiJoinOperator(operator.getRelNode(), left, right)
                .copyAnnotations(operator);
        this.map(operator, replacement);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.processUnary(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            super.postorder(operator);
            return;
        }
        var ix = input.getOutputIndexedZSetType();
        // This is not really used in 'operator.with', but we compute it anyway
        var outputType = new DBSPTypeZSet(ix.getNode(), ix.elementType);
        DBSPSimpleOperator replacement = operator.with(
                operator.function, outputType, Linq.list(input), false);
        Utilities.enforce(outputType.sameType(replacement.outputType));
        this.map(operator, replacement);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        this.processUnary(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        this.processUnary(operator);
    }

    static class HasInternedTypes extends InnerVisitor {
        public boolean hasInternedTypes = false;

        HasInternedTypes(DBSPCompiler compiler) {
            super(compiler);
        }

        public VisitDecision preorder(DBSPTypeInterned unused) {
            this.hasInternedTypes = true;
            return VisitDecision.STOP;
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            super.startVisit(node);
            this.hasInternedTypes = false;
        }

        public static boolean check(DBSPCompiler compiler, IDBSPInnerNode node) {
            HasInternedTypes hi = new HasInternedTypes(compiler);
            hi.apply(node);
            return hi.hasInternedTypes;
        }
    }

    /** Given an expression with a specified type bring it to the desired type,
     * by converting using unintern each interned string that needs to be uninterned.
     * The type must be always "less" interned than the expression type - i.e.,
     * never require a call to "intern". */
    DBSPExpression unintern(DBSPExpression expr, DBSPType desiredType) {
        DBSPType type = expr.getType();
        return switch (type.code) {
            case INTERNED_STRING -> (desiredType.code == type.code) ?
                expr.applyClone() :
                InternInner.callUnintern(expr.applyCloneIfNeeded(), desiredType);
            case TUPLE, RAW_TUPLE -> {
                if (!HasInternedTypes.check(this.compiler, type)) {
                    yield expr.applyClone();
                }
                if (type.mayBeNull)
                    expr = expr.nullabilityCast(type.withMayBeNull(false), false);
                DBSPTypeTupleBase tuple = desiredType.to(DBSPTypeTupleBase.class);
                DBSPExpression[] fields = new DBSPExpression[tuple.size()];
                for (int i = 0; i < tuple.size(); i++) {
                    fields[i] = unintern(expr.field(i), tuple.getFieldType(i));
                }
                DBSPExpression convertedTuple;
                if (expr.getType().code == DBSPTypeCode.RAW_TUPLE) {
                    convertedTuple = new DBSPRawTupleExpression(fields);
                } else {
                    convertedTuple = new DBSPTupleExpression(fields);
                }
                if (type.mayBeNull) {
                    DBSPExpression condition = expr.is_null();
                    Utilities.enforce(desiredType.mayBeNull);
                    DBSPExpression positive = desiredType.none();
                    yield new DBSPIfExpression(expr.getNode(), condition,
                            positive, convertedTuple.nullabilityCast(type, false));
                } else {
                    yield convertedTuple;
                }
            }
            default -> expr.applyCloneIfNeeded();
        };
    }

    /** Given an expression and a target type, convert each field of the
     * expression that needs to be interned by calling intern. */
    DBSPExpression intern(DBSPExpression expr, DBSPType destinationType) {
        return switch (destinationType.code) {
            case INTERNED_STRING -> {
                if (expr.getType().sameType(destinationType))
                    yield expr;
                yield InternInner.callIntern(expr);
            }
            case TUPLE, RAW_TUPLE -> {
                DBSPTypeTupleBase tuple = destinationType.to(DBSPTypeTupleBase.class);
                DBSPExpression[] fields = new DBSPExpression[tuple.size()];
                for (int i = 0; i < tuple.size(); i++) {
                    fields[i] = intern(expr.field(i), tuple.getFieldType(i));
                }
                if (destinationType.code == DBSPTypeCode.RAW_TUPLE)
                    yield new DBSPRawTupleExpression(fields);
                else
                    yield new DBSPTupleExpression(fields);
            }
            default -> expr;
        };
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        OutputPort input = this.mapped(operator.input());
        HasInternedTypes find = new HasInternedTypes(this.compiler);
        find.apply(input.outputType());
        if (!find.hasInternedTypes) {
            super.postorder(operator);
            return;
        }

        DBSPSimpleOperator map;
        if (input.outputType().is(DBSPTypeZSet.class)) {
            DBSPVariablePath var = input.getOutputZSetElementType().ref().var();
            DBSPExpression expr = this.unintern(var.deref(), operator.getOutputZSetElementType());
            find.apply(expr.getType());
            Utilities.enforce(!find.hasInternedTypes);
            map = new DBSPMapOperator(operator.getRelNode(), expr.closure(var), input);
        } else {
            DBSPTypeIndexedZSet ix = operator.input().getOutputIndexedZSetType();
            DBSPVariablePath var = input.getOutputIndexedZSetType().getKVRefType().var();
            DBSPExpression expr =
                    new DBSPRawTupleExpression(
                        this.unintern(var.field(0).deref(), ix.keyType),
                        this.unintern(var.field(1).deref(), ix.elementType));
            find.apply(expr.getType());
            Utilities.enforce(!find.hasInternedTypes);
            map = new DBSPMapIndexOperator(operator.getRelNode(), expr.closure(var), input);
        }
        this.addOperator(map);

        Utilities.enforce(operator.outputType.sameType(map.outputType));
        DBSPSimpleOperator result = operator
                .withInputs(Linq.list(map.outputPort()), false)
                .to(DBSPSimpleOperator.class);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        // Create the DBSPInternOperator whose input contains all interned strings.
        if (!this.allStringLiterals.isEmpty()) {
            DBSPZSetExpression result = DBSPZSetExpression.emptyWithElementType(
                    new DBSPTypeTuple(InternInner.nullableString));
            for (var lit : this.allStringLiterals) {
                result.append(new DBSPTupleExpression(
                        lit.cast(lit.getNode(), InternInner.nullableString, false)));
            }
            DBSPConstantOperator constant = new DBSPConstantOperator(
                    CalciteEmptyRel.INSTANCE, result, false, false);
            this.addOperator(constant);
            this.dynamicStrings.add(constant.outputPort());
        }

        OutputPort collected;
        if (this.dynamicStrings.isEmpty()) {
            super.postorder(circuit);
            return;
        } else if (this.dynamicStrings.size() > 1) {
            DBSPSumOperator sum = new DBSPSumOperator(CalciteEmptyRel.INSTANCE, this.dynamicStrings);
            this.addOperator(sum);
            collected = sum.outputPort();
        } else {
            collected = this.dynamicStrings.get(0);
        }

        // Filter away nulls
        DBSPVariablePath var = collected.getOutputZSetElementType().ref().var();
        DBSPClosureExpression func = var.deref().field(0).is_null().not().closure(var);
        DBSPFilterOperator filter = new DBSPFilterOperator(CalciteEmptyRel.INSTANCE, func, collected);
        this.addOperator(filter);

        // Cast to non-null
        var = var.getType().var();
        func = new DBSPTupleExpression(var.deref()
                .field(0)
                .nullabilityCast(DBSPTypeString.varchar(false), false)
                .applyCloneIfNeeded()
        ).closure(var);
        DBSPMapOperator map = new DBSPMapOperator(CalciteEmptyRel.INSTANCE, func, filter.outputPort());
        this.addOperator(map);

        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(CalciteEmptyRel.INSTANCE, map.outputPort());
        this.addOperator(diff);

        DBSPInternOperator result = new DBSPInternOperator(diff.outputPort());
        this.addOperator(result);
        super.postorder(circuit);
    }
}
