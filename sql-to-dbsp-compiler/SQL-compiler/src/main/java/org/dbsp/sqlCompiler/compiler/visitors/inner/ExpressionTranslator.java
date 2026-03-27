package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** A visitor which translates expressions and statements, mostly in postorder */
public class ExpressionTranslator extends TranslateVisitor<IDBSPInnerNode> implements IWritesLogs {
    public ExpressionTranslator(DBSPCompiler compiler) {
        super(compiler);
    }

    public DBSPExpression getE(DBSPExpression expression) {
        return this.get(expression).to(DBSPExpression.class);
    }

    @Nullable
    public DBSPExpression getEN(@Nullable DBSPExpression expression) {
        if (expression == null)
            return null;
        IDBSPInnerNode result = this.getN(expression);
        if (result == null)
            return null;
        return result.to(DBSPExpression.class);
    }

    public DBSPExpression[] get(DBSPExpression[] expressions) {
        return Linq.map(expressions, this::getE, DBSPExpression.class);
    }

    protected void map(DBSPExpression expression, DBSPExpression result) {
        if (expression.sameFields(result)) {
            this.set(expression, expression);
        } else {
            this.set(expression, result);
        }
    }

    protected void map(DBSPStatement statement, DBSPStatement result) {
        if (statement.sameFields(result))
            this.set(statement, statement);
        else
            this.set(statement, result);
    }

    protected void clear() {
        this.translationMap.clear();
    }

    boolean done(IDBSPInnerNode node) {
        return this.maybeGet(node) != null;
    }

    @Override
    public void postorder(DBSPApplyExpression node) {
        if (this.done(node))
            return;
        DBSPExpression function = this.getE(node.function);
        DBSPExpression[] args = this.get(node.arguments);
        DBSPExpression result = new DBSPApplyExpression(function, node.getType(), args);
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPApplyMethodExpression node) {
        if (this.done(node))
            return;
        DBSPExpression function = this.getE(node.function);
        DBSPExpression[] args = this.get(node.arguments);
        DBSPExpression self = this.getE(node.self);
        DBSPExpression result = new DBSPApplyMethodExpression(function, node.getType(), self, args);
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPArrayExpression node) {
        if (this.done(node))
            return;
        if (node.data == null) {
            this.map(node, node);
        } else {
            List<DBSPExpression> data = Linq.map(node.data, this::getE);
            this.map(node, new DBSPArrayExpression(node.getNode(), node.getType(), data));
        }
    }

    @Override
    public void postorder(DBSPAssignmentExpression node) {
        if (this.done(node))
            return;
        DBSPExpression left = this.getE(node.left);
        DBSPExpression right = this.getE(node.right);
        this.map(node, new DBSPAssignmentExpression(left, right));
    }

    @Override
    public void postorder(DBSPBinaryExpression node) {
        if (this.done(node))
            return;
        DBSPExpression left = this.getE(node.left);
        DBSPExpression right = this.getE(node.right);
        this.map(node, new DBSPBinaryExpression(node.getNode(),
                node.getType(),
                node.opcode,
                left,
                right));
    }

    @Override
    public void postorder(DBSPTimeAddSub node) {
        if (this.done(node))
            return;
        DBSPExpression left = this.getE(node.left);
        DBSPExpression right = this.getE(node.right);
        this.map(node, new DBSPTimeAddSub(node.getNode(),
                node.getType(),
                node.opcode,
                left,
                right));
    }

    @Override
    public void postorder(DBSPBlockExpression node) {
        if (this.done(node))
            return;
        List<DBSPStatement> statements =
                Linq.map(node.contents, c -> this.get(c).to(DBSPStatement.class));
        DBSPExpression lastExpression = this.getEN(node.lastExpression);
        this.map(node, new DBSPBlockExpression(statements, lastExpression));
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        if (this.done(type))
            return VisitDecision.STOP;
        this.set(type, type);
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPBorrowExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, expression.borrow(node.mut));
    }

    @Override
    public void postorder(DBSPCastExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.source);
        this.map(node, new DBSPCastExpression(node.getNode(), expression, node.getType(), node.safe));
    }

    @Override
    public void postorder(DBSPCloneExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, expression.applyClone());
    }

    @Override
    public void postorder(DBSPClosureExpression node) {
        if (this.done(node))
            return;
        DBSPExpression body = this.getE(node.body);
        this.map(node, new DBSPClosureExpression(node.getNode(), body, node.parameters));
    }

    @Override
    public void postorder(DBSPConditionalIncrementExpression node) {
        if (this.done(node))
            return;
        DBSPExpression left = this.getE(node.left);
        DBSPExpression right = this.getE(node.right);
        DBSPExpression condition = this.getEN(node.condition);
        this.map(node, new DBSPConditionalIncrementExpression(
                node.getNode(), node.opcode, node.getType(), left, right, condition));
    }

    @Override
    public void postorder(DBSPFold fold) {
        if (this.done(fold))
            return;
        DBSPExpression zero = this.getE(fold.zero);
        DBSPClosureExpression increment = this.getE(fold.increment).to(DBSPClosureExpression.class);
        DBSPClosureExpression postProcessing = this.getE(fold.postProcess).to(DBSPClosureExpression.class);
        DBSPFold result = new DBSPFold(fold.getNode(), fold.semigroup, zero, increment, postProcessing);
        this.map(fold, result);
    }

    @Override
    public void postorder(DBSPMinMax aggregator) {
        if (this.done(aggregator))
            return;
        DBSPExpression post = this.getEN(aggregator.postProcessing);
        @Nullable DBSPClosureExpression postClosure = post != null ? post.to(DBSPClosureExpression.class) : null;
        DBSPMinMax result = new DBSPMinMax(aggregator.getNode(), aggregator.getType(), postClosure, aggregator.aggregation);
        this.map(aggregator, result);
    }

    @Override
    public void postorder(DBSPConstructorExpression node) {
        if (this.done(node))
            return;
        DBSPExpression function = this.getE(node.function);
        DBSPExpression[] arguments = this.get(node.arguments);
        this.map(node, new DBSPConstructorExpression(function, node.getType(), arguments));
    }

    @Override
    public void postorder(DBSPCustomOrdExpression node) {
        if (this.done(node))
            return;
        DBSPExpression source = this.getE(node.source);
        DBSPExpression comparator = this.getE(node.comparator);
        this.map(node, new DBSPCustomOrdExpression(node.getNode(), source, comparator.to(DBSPComparatorExpression.class)));
    }

    @Override
    public void postorder(DBSPCustomOrdField node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPCustomOrdField(expression, node.fieldNo));
    }

    @Override
    public void postorder(DBSPDerefExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPDerefExpression(expression));
    }

    @Override
    public void postorder(DBSPDirectComparatorExpression node) {
        if (this.done(node))
            return;
        DBSPExpression source = this.getE(node.source);
        this.map(node, new DBSPDirectComparatorExpression(
                node.getNode(), source.to(DBSPComparatorExpression.class), node.ascending));
    }

    @Override
    public void postorder(DBSPExpression node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(DBSPExpressionStatement node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPExpressionStatement(expression));
    }

    @Override
    public void postorder(DBSPComment node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(DBSPFieldComparatorExpression node) {
        if (this.done(node))
            return;
        DBSPExpression source = this.getE(node.source);
        this.map(node, new DBSPFieldComparatorExpression(
                node.getNode(), source.to(DBSPComparatorExpression.class),
                node.fieldNo, node.ascending, node.nullsFirst));
    }

    @Override
    public void postorder(DBSPFieldExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPFieldExpression(node.getNode(), expression, node.fieldNo));
    }

    @Override
    public void postorder(DBSPFlatmap node) {
        if (this.done(node))
            return;
        DBSPExpression collectionExpression = this.getE(node.collectionExpression);
        List<DBSPClosureExpression> rightProjections = null;
        if (node.rightProjections != null)
            rightProjections = Linq.map(node.rightProjections,
                    e -> this.getE(e).to(DBSPClosureExpression.class));
        DBSPClosureExpression closure = collectionExpression.to(DBSPClosureExpression.class);
        Utilities.enforce(closure.parameters.length == 1);
        this.map(node, new DBSPFlatmap(
                node.getNode(),
                closure.parameters[0].type.deref().to(DBSPTypeTuple.class),
                closure, node.leftInputIndexes,
                rightProjections, node.ordinalityIndexType, node.shuffle));
    }

    @Override
    public void postorder(DBSPForExpression node) {
        throw new UnimplementedException();
    }

    @Override
    public void postorder(DBSPGeoPointConstructor node) {
        if (this.done(node))
            return;
        DBSPExpression left = this.getEN(node.left);
        DBSPExpression right = this.getEN(node.right);
        this.map(node, new DBSPGeoPointConstructor(node.getNode(), left, right, node.type));
    }

    @Override
    public void postorder(DBSPHandleErrorExpression node) {
        if (this.done(node))
            return;
        DBSPExpression source = this.getE(node.source);
        this.map(node, new DBSPHandleErrorExpression(node.getNode(), node.index, node.runtimeBehavior,
                source, node.hasSourcePosition));
    }

    @Override
    public void postorder(DBSPStructItem node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(DBSPIfExpression node) {
        if (this.done(node))
            return;
        DBSPExpression condition = this.getE(node.condition);
        DBSPExpression positive = this.getE(node.positive);
        DBSPExpression negative = this.getEN(node.negative);
        this.map(node, new DBSPIfExpression(node.getNode(), condition, positive, negative));
    }

    @Override
    public void postorder(DBSPIndexedZSetExpression node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    public void postorder(DBSPIsNullExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPIsNullExpression(node.getNode(), expression));
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression node) {
        if (this.done(node))
            return VisitDecision.STOP;
        // This one is done in preorder
        node.initializer.accept(this);
        DBSPExpression initializer = this.getE(node.initializer);
        // Effects of initializer should be visible while processing consumer
        node.consumer.accept(this);
        DBSPExpression consumer = this.getE(node.consumer);
        DBSPExpression result = new DBSPLetExpression(node.variable, initializer, consumer);
        this.map(node, result);
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPLetStatement node) {
        if (this.done(node))
            return;
        DBSPExpression initializer = this.getEN(node.initializer);
        DBSPStatement result;
        if (initializer != null)
            result = new DBSPLetStatement(node.variable, initializer, node.mutable);
        else
            result = new DBSPLetStatement(node.variable, node.type, node.mutable);
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPLiteral node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(DBSPMapExpression node) {
        if (this.done(node))
            return;
        List<DBSPExpression> keys = null;
        if (node.keys != null)
            keys = Linq.map(node.keys, this::getE);
        List<DBSPExpression> values = null;
        if (node.values != null)
            values = Linq.map(node.values, this::getE);
        this.map(node, new DBSPMapExpression(node.mapType, keys, values));
    }

    @Override
    public void postorder(DBSPNoComparatorExpression node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(DBSPPathExpression node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(DBSPQualifyTypeExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPQualifyTypeExpression(expression, node.types));
    }

    @Override
    public void postorder(DBSPQuestionExpression node) {
        if (this.done(node))
            return;
        DBSPExpression source = this.getE(node.source);
        this.map(node, source.question());
    }

    @Override
    public void postorder(DBSPRawTupleExpression node) {
        if (this.done(node))
            return;
        if (node.fields != null) {
            DBSPExpression[] fields = this.get(node.fields);
            DBSPExpression result = new DBSPRawTupleExpression(
                    node.getNode(), node.getType().to(DBSPTypeRawTuple.class), fields);
            this.map(node, result);
        } else {
            this.map(node, node.getType().none());
        }
    }

    @Override
    public void postorder(DBSPReturnExpression node) {
        if (this.done(node))
            return;
        DBSPExpression argument = this.getE(node.argument);
        this.map(node, new DBSPReturnExpression(node.getNode(), argument));
    }

    @Override
    public void postorder(DBSPSomeExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPSomeExpression(node.getNode(), expression));
    }

    @Override
    public void postorder(DBSPSortExpression node) {
        if (this.done(node))
            return;
        DBSPExpression comparator = this.getE(node.comparator);
        this.map(node, new DBSPSortExpression(node.getNode(), node.elementType,
                comparator.to(DBSPComparatorExpression.class)));
    }

    @Override
    public void postorder(DBSPStaticExpression node) {
        if (this.done(node))
            return;
        DBSPExpression initializer = this.getE(node.initializer);
        this.map(node, new DBSPStaticExpression(node.getNode(), initializer, node.getName()));
    }

    @Override
    public void postorder(DBSPTupleExpression node) {
        if (this.done(node))
            return;
        if (node.fields != null) {
            DBSPExpression[] fields = this.get(node.fields);
            DBSPExpression result = new DBSPTupleExpression(node.getNode(), node.getType().to(DBSPTypeTuple.class), fields);
            this.map(node, result);
        } else {
            this.map(node, node.getType().none());
        }
    }

    @Override
    public void postorder(DBSPUnaryExpression node) {
        if (this.done(node))
            return;
        DBSPExpression source = this.getE(node.source);
        this.map(node, new DBSPUnaryExpression(node.getNode(), node.type, node.opcode, source));
    }

    @Override
    public void postorder(DBSPUnsignedUnwrapExpression node) {
        if (this.done(node))
            return;
        DBSPExpression source = this.getE(node.source);
        this.map(node, new DBSPUnsignedUnwrapExpression(
                node.getNode(), source, node.type, node.ascending, node.nullsLast));
    }

    @Override
    public void postorder(DBSPUnsignedWrapExpression node) {
        if (this.done(node))
            return;
        DBSPExpression source = this.getE(node.source);
        this.map(node, new DBSPUnsignedWrapExpression(
                node.getNode(), source, node.ascending, node.nullsLast));
    }

    @Override
    public void postorder(DBSPUnwrapCustomOrdExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPUnwrapCustomOrdExpression(expression));
    }

    @Override
    public void postorder(DBSPUnwrapExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPUnwrapExpression(node.message, expression));
    }

    @Override
    public void postorder(DBSPFailExpression node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(DBSPVariablePath node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(DBSPVariantExpression node) {
        if (this.done(node))
            return;
        DBSPExpression value = this.getEN(node.value);
        this.map(node, new DBSPVariantExpression(value, node.getType().mayBeNull));
    }

    @Override
    public void postorder(DBSPLazyExpression node) {
        if (this.done(node))
            return;
        DBSPExpression expression = this.getE(node.expression);
        this.map(node, new DBSPLazyExpression(expression));
    }

    @Override
    public void postorder(DBSPWindowBoundExpression node) {
        if (this.done(node))
            return;
        DBSPExpression representation = this.getE(node.representation);
        this.map(node, new DBSPWindowBoundExpression(node.getNode(), node.isPreceding, representation));
    }

    @Override
    public void postorder(DBSPZSetExpression node) {
        if (this.done(node))
            return;
        Map<DBSPExpression, Long> data = new HashMap<>();
        for (Map.Entry<DBSPExpression, Long> e : node.data.entrySet()) {
            DBSPExpression key = this.getE(e.getKey());
            if (data.containsKey(key))
                data.put(key, data.get(key) + e.getValue());
            else
                data.put(key, e.getValue());
        }
        this.map(node, new DBSPZSetExpression(data, node.elementType));
    }

    @Override
    public void postorder(LinearAggregate node) {
        if (this.done(node))
            return;
        DBSPExpression map = this.getE(node.map);
        DBSPExpression postProcess = this.getE(node.postProcess);
        DBSPExpression emptySetResult = this.getE(node.emptySetResult);
        this.map(node, new LinearAggregate(node.getNode(), map.to(DBSPClosureExpression.class),
                postProcess.to(DBSPClosureExpression.class), emptySetResult));
    }

    @Override
    public void postorder(NoExpression node) {
        if (this.done(node))
            return;
        this.map(node, node);
    }

    @Override
    public void postorder(NonLinearAggregate node) {
        if (this.done(node))
            return;
        DBSPExpression zero = this.getE(node.zero);
        DBSPExpression increment = this.getE(node.increment);
        DBSPExpression postProcess = this.getEN(node.postProcess);
        DBSPExpression emptySetResult = this.getE(node.emptySetResult);
        this.map(node, new NonLinearAggregate(node.getNode(), zero, increment.to(DBSPClosureExpression.class),
                postProcess != null ? postProcess.to(DBSPClosureExpression.class) : null, emptySetResult, node.semigroup));
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        this.startVisit(node);
        node.accept(this);
        IDBSPInnerNode result = this.get(node);
        this.endVisit();
        return result;
    }

    @Override
    public void postorder(DBSPFunction function) {
        if (this.done(function))
            return;
        DBSPExpression body = this.getEN(function.body);
        DBSPFunction result = new DBSPFunction(function.getNode(),
                function.name, function.parameters, function.returnType, body, function.annotations);
        if (result.sameFields(function)) {
            this.set(function, function);
        } else {
            this.set(function, result);
        }
    }

    @Override
    public void postorder(DBSPAggregateList aggregate) {
        if (this.done(aggregate))
            return;
        DBSPExpression rowVar = this.getE(aggregate.rowVar);
        List<IAggregate> implementations =
                Linq.map(aggregate.aggregates, c -> {
                    IDBSPInnerNode result = this.getE(c);
                    return result.to(IAggregate.class);
                });
        DBSPAggregateList result = new DBSPAggregateList(
                aggregate.getNode(), rowVar.to(DBSPVariablePath.class), implementations);
        if (result.sameFields(aggregate)) {
            this.set(aggregate, aggregate);
        } else {
            this.set(aggregate, result);
        }
    }

    @Override
    public void postorder(DBSPFunctionItem item) {
        if (this.done(item))
            return;
        IDBSPInnerNode result = this.get(item.function);
        this.map(item, new DBSPFunctionItem(result.to(DBSPFunction.class)));
    }

    @Override
    public void postorder(DBSPStaticItem item) {
        if (this.done(item))
            return;
        DBSPExpression expression = this.getE(item.expression);
        DBSPItem result = new DBSPStaticItem(expression.to(DBSPStaticExpression.class));
        this.map(item, result);
    }

    public CircuitRewriter circuitRewriter(boolean processDeclarations) {
        return new CircuitRewriter(this.compiler, this, processDeclarations);
    }

    /** Create a circuit rewriter with a predicate that selects which node to optimize */
    public CircuitRewriter circuitRewriter(Predicate<DBSPOperator> toOptimize) {
        return new CircuitRewriter(this.compiler, this, false, toOptimize);
    }

    /** Apply the expression translator repeatedly until reaching a fixed-point */
    public DBSPExpression fixedPoint(DBSPExpression expression, int max) {
        int iterations = 0;
        while (true) {
            DBSPExpression result = this.apply(expression).to(DBSPExpression.class);
            if (result == expression)
                return result;
            expression = result;
            iterations++;
            if (iterations == max) {
                throw new InternalCompilerError(this + ": Convergence not reached after " + max + " iterations");
            }
        }
    }
}
