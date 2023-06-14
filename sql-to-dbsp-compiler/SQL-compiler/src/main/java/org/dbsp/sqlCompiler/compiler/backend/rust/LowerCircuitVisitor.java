package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Lowers a circuit's representation.
 * - converts DBSPAggregate into basic operations.
 * - converts DBSPFlatmap into basic operations.
 */
public class LowerCircuitVisitor extends CircuitCloneVisitor {
    public LowerCircuitVisitor(IErrorReporter reporter) {
        super(reporter, false);
    }

    /**
     * Creates a DBSP Fold object from an Aggregate.
     */
    public static DBSPExpression createAggregator(
            IErrorReporter reporter, DBSPAggregate aggregate,
            boolean compact) {
        // Example for a pair of count+sum aggregations:
        // let zero_count: isize = 0;
        // let inc_count = |acc: isize, v: &usize, w: isize| -> isize { acc + 1 * w };
        // let zero_sum: isize = 0;
        // let inc_sum = |ac: isize, v: &usize, w: isize| -> isize { acc + (*v as isize) * w) }
        // let zero = (zero_count, inc_count);
        // let inc = |acc: &mut (isize, isize), v: &usize, w: isize| {
        //     *acc = (inc_count(acc.0, v, w), inc_sum(acc.1, v, w))
        // }
        // let post_count = identity;
        // let post_sum = identity;
        // let post =  move |a: (i32, i32), | -> Tuple2<_, _> {
        //            Tuple2::new(post_count(a.0), post_sum(a.1)) };
        // let fold = Fold::with_output((zero_count, zero_sum), inc, post);
        // let count_sum = input.aggregate(fold);
        // let result = count_sum.map(|k,v|: (&(), &Tuple1<isize>|) { *v };
        int parts = aggregate.components.length;
        DBSPExpression[] zeros = new DBSPExpression[parts];
        DBSPExpression[] increments = new DBSPExpression[parts];
        DBSPExpression[] posts = new DBSPExpression[parts];
        DBSPType[] accumulatorTypes = new DBSPType[parts];
        DBSPType[] semigroups = new DBSPType[parts];
        DBSPType weightType = null;
        for (int i = 0; i < parts; i++) {
            DBSPAggregate.Implementation implementation = aggregate.components[i];
            DBSPType incType = implementation.increment.getResultType();
            zeros[i] = implementation.zero;
            increments[i] = implementation.increment;
            if (implementation.increment.parameters.length != 3)
                throw new RuntimeException("Expected increment function to have 3 parameters: "
                        + implementation.increment);
            DBSPType lastParamType = implementation.increment.parameters[2].getType();
            // Extract weight type from increment function signature.
            // It may not be "Weight" anymore.
            if (weightType == null)
                weightType = lastParamType;
            else
                if (!weightType.sameType(lastParamType))
                    throw new RuntimeException("Not all increment functions have the same type "
                        + weightType + " and " + lastParamType);
            accumulatorTypes[i] = Objects.requireNonNull(incType);
            semigroups[i] = implementation.semigroup;
            posts[i] = implementation.getPostprocessing();
        }

        DBSPTypeRawTuple accumulatorType = new DBSPTypeRawTuple(accumulatorTypes);
        DBSPVariablePath accumulator = accumulatorType.ref(true).var("a");
        DBSPVariablePath postAccumulator = accumulatorType.var("a");

        BetaReduction reducer = new BetaReduction(reporter);
        DBSPVariablePath weightVar = new DBSPVariablePath("w", Objects.requireNonNull(weightType));
        for (int i = 0; i < parts; i++) {
            DBSPExpression accumulatorField = accumulator.field(i);
            DBSPExpression expr = increments[i].call(
                    accumulatorField, aggregate.rowVar, weightVar);
            increments[i] = Objects.requireNonNull(reducer.apply(expr)).to(DBSPExpression.class);
            DBSPExpression postAccumulatorField = postAccumulator.field(i);
            expr = posts[i].call(postAccumulatorField);
            posts[i] = Objects.requireNonNull(reducer.apply(expr)).to(DBSPExpression.class);
        }
        DBSPAssignmentExpression accumulatorBody = new DBSPAssignmentExpression(
                accumulator.deref(), new DBSPRawTupleExpression(increments));
        DBSPExpression accumFunction = accumulatorBody.closure(
                accumulator.asParameter(), aggregate.rowVar.asParameter(),
                weightVar.asParameter());
        DBSPClosureExpression postClosure = new DBSPTupleExpression(posts).closure(postAccumulator.asParameter());
        DBSPType[] typeArgs;
        if (compact) {
            typeArgs = new DBSPType[0];
        } else {
            typeArgs = new DBSPType[4];
            typeArgs[0] = DBSPTypeAny.INSTANCE;
            typeArgs[1] = new DBSPTypeSemigroup(semigroups, accumulatorTypes);
            typeArgs[2] = DBSPTypeAny.INSTANCE;
            typeArgs[3] = DBSPTypeAny.INSTANCE;
        }
        DBSPExpression constructor = DBSPTypeAny.INSTANCE.path(
                new DBSPPath(
                        new DBSPSimplePathSegment("Fold", typeArgs),
                        new DBSPSimplePathSegment("with_output")));
        return constructor.call(
                new DBSPRawTupleExpression(zeros),
                accumFunction, postClosure);
    }

    /**
     * Rewrite a flatmap operation into a Rust method call.
     * @param flatmap  Flatmap operation to rewrite.
     */
    public static DBSPExpression rewriteFlatmap(DBSPFlatmap flatmap) {
        //   move |x: &Tuple2<Vec<i32>, Option<i32>>, | -> _ {
        //     let xA: Vec<i32> = x.0.clone();
        //     let xB: x.1.clone();
        //     x.0.clone().into_iter().map({
        //        move |e: i32, | -> Tuple3<Vec<i32>, Option<i32>, i32> {
        //            Tuple3::new(xA.clone(), xB.clone(), e)
        //        }
        //     })
        DBSPVariablePath rowVar = new DBSPVariablePath("x", flatmap.inputElementType);
        DBSPType eType = flatmap.collectionElementType;
        if (flatmap.indexType != null)
            eType = new DBSPTypeRawTuple(DBSPTypeUSize.INSTANCE, eType);
        DBSPVariablePath elem = new DBSPVariablePath("e", eType);
        List<DBSPStatement> clones = new ArrayList<>();
        List<DBSPExpression> resultColumns = new ArrayList<>();
        for (int i = 0; i < flatmap.outputFieldIndexes.size(); i++) {
            int index = flatmap.outputFieldIndexes.get(i);
            if (index == DBSPFlatmap.ITERATED_ELEMENT) {
                if (flatmap.indexType != null) {
                    // e.1, as produced by the iterator
                    resultColumns.add(elem.field(1));
                } else {
                    // e
                    resultColumns.add(elem);
                }
            } else if (index == DBSPFlatmap.COLLECTION_INDEX) {
                // The INDEX field produced WITH ORDINALITY
                Objects.requireNonNull(flatmap.indexType);
                resultColumns.add(new DBSPBinaryExpression(null,
                        DBSPTypeUSize.INSTANCE, DBSPOpcode.ADD,
                        elem.field(0),
                        new DBSPUSizeLiteral(1)).cast(flatmap.indexType));
            } else {
                // let xA: Vec<i32> = x.0.clone();
                // let xB: x.1.clone();
                DBSPExpression field = rowVar.field(index).applyCloneIfNeeded();
                DBSPVariablePath fieldClone = new DBSPVariablePath("x" + index, field.getType());
                DBSPLetStatement stat = new DBSPLetStatement(fieldClone.variable, field);
                clones.add(stat);
                resultColumns.add(fieldClone.applyClone());
            }
        }
        // move |e: i32, | -> Tuple3<Vec<i32>, Option<i32>, i32> {
        //   Tuple3::new(xA.clone(), xB.clone(), e)
        // }
        DBSPClosureExpression toTuple = new DBSPTupleExpression(resultColumns, false)
                .closure(elem.asParameter());
        DBSPExpression iter = new DBSPApplyMethodExpression("into_iter", DBSPTypeAny.INSTANCE,
                rowVar.field(flatmap.collectionFieldIndex).applyCloneIfNeeded());
        if (flatmap.indexType != null) {
            iter = new DBSPApplyMethodExpression("enumerate", DBSPTypeAny.INSTANCE, iter);
        }
        DBSPExpression function = new DBSPApplyMethodExpression(
                "map", DBSPTypeAny.INSTANCE,
                iter, toTuple);
        DBSPExpression block = new DBSPBlockExpression(clones, function);
        return block.closure(rowVar.asRefParameter());
    }

    @Override
    public void postorder(DBSPFlatMapOperator node) {
        DBSPOperator result;
        if (node.getFunction().is(DBSPFlatmap.class)) {
            List<DBSPOperator> sources = Linq.map(node.inputs, this::mapped);
            DBSPExpression function = rewriteFlatmap(node.getFunction().to(DBSPFlatmap.class));
            result = node.withFunction(function, node.outputType).withInputs(sources, this.force);
            this.map(node, result);
        } else {
            super.postorder(node);
        }
    }

    @Override
    public void postorder(DBSPAggregateOperator node) {
        if (node.function != null) {
            // OrderBy
            super.postorder(node);
            return;
        }
        DBSPExpression function = createAggregator(this.errorReporter, node.getAggregate(), false);
        DBSPOperator result = new DBSPAggregateOperator(node.getNode(), node.keyType, node.outputElementType,
                node.weightType, function, null, this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPIncrementalAggregateOperator node) {
        if (node.function != null) {
            // OrderBy
            super.postorder(node);
            return;
        }
        DBSPExpression function = createAggregator(this.errorReporter, node.getAggregate(), false);
        DBSPOperator result = new DBSPIncrementalAggregateOperator(node.getNode(), node.keyType, node.outputElementType,
                DBSPTypeWeight.INSTANCE, function, null, this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPWindowAggregateOperator node) {
        if (node.aggregate == null) {
            super.postorder(node);
            return;
        }
        DBSPExpression function = createAggregator(this.errorReporter, node.getAggregate(), false);
        DBSPOperator result = new DBSPWindowAggregateOperator(node.getNode(),
                function, null, node.window,
                node.partitionKeyType, node.timestampType, node.aggregateType,
                DBSPTypeWeight.INSTANCE, this.mapped(node.input()));
        this.map(node, result);
    }
}
