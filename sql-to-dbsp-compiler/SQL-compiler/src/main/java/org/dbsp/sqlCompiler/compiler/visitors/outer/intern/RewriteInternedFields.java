package org.dbsp.sqlCompiler.compiler.visitors.outer.intern;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInternOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeInterned;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
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
import java.util.Objects;
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
     * source (x, S, x, S)  where S is a (string) field that will be interned
     *   |
     *  map   (x, intern(cast_to_nullable(S)), x, intern(cast_to_nullable(*)), array(cast_to_nullable(S), cast_to_nullable(S)))
     *   | \
     *  map \    produces the tuple without the array - replaces the current operator
     *       flatmap    flattens the array - inserted in allStrings
     *  An array is used only if more than 1 interned field is necessary.  If there is no array, the flatmap is a map.
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
        DBSPExpression[] fields = new DBSPExpression[tuple.size() + 1];
        DBSPExpression[] internedFields = new DBSPExpression[list.size()];
        int index = 0;
        for (int i = 0; i < tuple.size(); i++) {
            DBSPType fieldType = tuple.tupFields[i];
            DBSPExpression field = var.deref().field(i).applyCloneIfNeeded();
            if (list.contains(i)) {
                Utilities.enforce(fieldType.code == DBSPTypeCode.STRING);
                internedFields[index] = field.cast(field.getNode(), InternInner.nullableString, false);
                field = InternInner.callIntern(field);
                index++;
            }
            fields[i] = field;
        }
        if (useArray) {
            DBSPArrayExpression array = new DBSPArrayExpression(internedFields);
            fields[tuple.size()] = array;
        } else {
            fields[tuple.size()] = internedFields[0];
        }
        var extendedTuple = new DBSPTupleExpression(fields);
        DBSPClosureExpression closure = extendedTuple.closure(var);
        CalciteRelNode node = CalciteEmptyRel.INSTANCE; // Can't use the source itself
        DBSPMapOperator map = new DBSPMapOperator(node, closure, source.outputPort());
        this.addOperator(map);

        // Left side map
        DBSPVariablePath var1 = extendedTuple.getType().ref().var();
        DBSPExpression[] projection = new DBSPExpression[tuple.size()];
        for (int i = 0; i < tuple.size(); i++)
            projection[i] = var1.deref().field(i).applyCloneIfNeeded();
        DBSPClosureExpression closure1 = new DBSPTupleExpression(projection).closure(var1);
        DBSPMapOperator map1 = new DBSPMapOperator(node, closure1, map.outputPort());
        this.map(source, map1);

        // Right size map/flatmap
        DBSPVariablePath var2 = extendedTuple.getType().ref().var();
        DBSPSimpleOperator unnest;
        if (useArray) {
            DBSPType outputType = new DBSPTypeTuple(InternInner.nullableString);
            // Tuple (type of var4) is nullable
            DBSPVariablePath var4 = new DBSPTypeTuple(CalciteObject.EMPTY, true, Linq.list(InternInner.nullableString))
                    .ref().var();
            DBSPClosureExpression f0 = var4.deref().field(0).applyClone().closure(var4);
            DBSPFlatmap function = new DBSPFlatmap(
                    node,
                    extendedTuple.getTypeAsTuple(), // input type
                    var2.deref().field(tuple.size()).closure(var2), // collection expression
                    Linq.list(), // left input indexes
                    Linq.list(f0), // right projections
                    null, // ordinalityIndexType
                    new IdShuffle(2));
            unnest = new DBSPFlatMapOperator(node, function,
                    new DBSPTypeZSet(outputType), this.internConstants(), map.outputPort());
        } else {
            DBSPExpression justSTring = var2.deref().field(tuple.size()).applyClone();
            DBSPClosureExpression closure3 = new DBSPTupleExpression(justSTring).closure(var2);
            unnest = new DBSPMapOperator(node, closure3, map.outputPort());
        }

        this.addOperator(unnest);
        this.dynamicStrings.add(unnest.outputPort());
    }

    void processUnary(DBSPUnaryOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            this.map(operator, operator);
            return;
        }
        InternInner interner = new InternInner(this.compiler, this.internConstants(), input.getOutputRowRefType());
        DBSPClosureExpression function = interner.apply(operator.getFunction())
                .to(DBSPClosureExpression.class);
        DBSPType outputType;
        if (operator.is(DBSPFilterOperator.class)) {
            outputType = input.outputType();
        } else {
            outputType = function.getResultType().to(DBSPTypeTupleBase.class).intoCollectionType();
        }
        DBSPSimpleOperator replacement = operator.with(
                function, outputType, Linq.list(input), false);
        this.allStringLiterals.addAll(interner.globalStrings);
        this.map(operator, replacement);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            super.postorder(operator);
            return;
        }

        DBSPFlatmap flatMap = operator.getFunction().to(DBSPFlatmap.class);
        InternInner interner = new InternInner(this.compiler, this.internConstants(), input.getOutputRowRefType());
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

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            super.postorder(operator);
            return;
        }
        DBSPTypeIndexedZSet inputType = input.getOutputIndexedZSetType();

        InternInner interner = new InternInner(this.compiler, false, inputType.elementType.ref());
        DBSPAggregateList aggregateList = Objects.requireNonNull(operator.aggregateList);
        DBSPAggregateList rewritten = interner.apply(aggregateList).to(DBSPAggregateList.class);
        DBSPType rewrittenResultType = rewritten.getType().to(DBSPTypeFunction.class).resultType;
        DBSPSimpleOperator replacement = new DBSPStreamAggregateOperator(operator.getRelNode(),
                new DBSPTypeIndexedZSet(inputType.getNode(), inputType.keyType, rewrittenResultType),
                null, rewritten, input);
        this.map(operator, replacement);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.equals(operator.input())) {
            super.postorder(operator);
            return;
        }
        DBSPTypeIndexedZSet inputType = input.getOutputIndexedZSetType();

        InternInner interner = new InternInner(this.compiler, false, inputType.elementType.ref());
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

        InternInner interner = new InternInner(this.compiler, false, commonKeyType.ref(),
                leftType.elementType.ref(), rightType.elementType.ref());
        DBSPClosureExpression function = interner.apply(operator.getFunction()).to(DBSPClosureExpression.class);
        DBSPSimpleOperator replacement = operator.with(function,
                function.getResultType().to(DBSPTypeTupleBase.class).intoCollectionType(),
                Linq.list(left, right), false);
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
    public void postorder(DBSPStreamJoinOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPAsofJoinOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPConcreteAsofJoinOperator operator) {
        this.processJoin(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.processUnary(operator);
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
    }

    /** Given an expression with a specified type, convert each field that
     * is interned into a string by calling unintern. */
    DBSPExpression unintern(DBSPExpression expr, DBSPType originalType) {
        return switch (expr.getType().code) {
            case INTERNED_STRING -> InternInner.callUnintern(expr.applyCloneIfNeeded(), originalType);
            case TUPLE, RAW_TUPLE -> {
                DBSPTypeTupleBase tuple = originalType.to(DBSPTypeTupleBase.class);
                DBSPExpression[] fields = new DBSPExpression[tuple.size()];
                for (int i = 0; i < tuple.size(); i++) {
                    fields[i] = unintern(expr.field(i), tuple.getFieldType(i));
                }
                if (expr.getType().code == DBSPTypeCode.RAW_TUPLE)
                    yield new DBSPRawTupleExpression(fields);
                else
                    yield new DBSPTupleExpression(fields);
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
    public void postorder(DBSPViewOperator operator) {
        OutputPort input = this.mapped(operator.input());
        HasInternedTypes find = new HasInternedTypes(this.compiler);
        find.apply(input.outputType());
        if (!find.hasInternedTypes) {
            super.postorder(operator);
            return;
        }

        DBSPVariablePath var = input.getOutputZSetElementType().ref().var();
        DBSPExpression expr = this.unintern(var.deref(), operator.getOutputZSetElementType());
        find.apply(expr.getType());
        Utilities.enforce(!find.hasInternedTypes);

        DBSPMapOperator map = new DBSPMapOperator(operator.getRelNode(), expr.closure(var), input);
        this.addOperator(map);

        DBSPSimpleOperator result = operator.withInputs(Linq.list(map.outputPort()), false);
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
                .nullabilityCast(DBSPTypeString.varchar(false), false))
                .closure(var);
        DBSPMapOperator map = new DBSPMapOperator(CalciteEmptyRel.INSTANCE, func, filter.outputPort());
        this.addOperator(map);

        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(CalciteEmptyRel.INSTANCE, map.outputPort());
        this.addOperator(diff);

        DBSPInternOperator result = new DBSPInternOperator(diff.outputPort());
        this.addOperator(result);
        super.postorder(circuit);
    }
}
