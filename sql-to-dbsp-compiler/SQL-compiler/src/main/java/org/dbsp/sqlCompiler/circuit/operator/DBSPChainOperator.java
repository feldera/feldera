package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPReturnExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** A Chain operator performs a linear chain of Map/Filter/MapIndex operations.
 * In the end it is lowered to a {@link DBSPFlatMapOperator} or
 * {@link DBSPFlatMapIndexOperator} operator, depending on the last operation. */
public class DBSPChainOperator extends DBSPUnaryOperator {
    public final ComputationChain chain;

    public DBSPChainOperator(CalciteRelNode node, ComputationChain chain, boolean isMultiset, OutputPort source) {
        super(node, "chain" + chain.summary(), null, chain.getOutputType(), isMultiset, source);
        this.chain = chain;
        Utilities.enforce(this.chain.size() > 1);
    }

    public enum ComputationKind {
        Map,
        Filter,
        MapIndex
    }

    public record Computation(ComputationKind kind, DBSPClosureExpression closure) {
        public Computation {
            Utilities.enforce(closure.parameters.length == 1);
        }

        void checkInputType(DBSPType inputType) {
            DBSPTypeZSet zSet = inputType.as(DBSPTypeZSet.class);
            DBSPTypeIndexedZSet iZSet = inputType.as(DBSPTypeIndexedZSet.class);
            DBSPType argType;
            if (zSet != null) {
                argType = zSet.elementType.ref();
            } else {
                Utilities.enforce(iZSet != null);
                argType = iZSet.getKVRefType();
            }
            DBSPType paramType = closure.parameters[0].getType();
            if (!paramType.sameType(argType))
                throw new InternalCompilerError("Expected function to accept\n" + argType +
                        " as argument " + 0 + " but it expects\n" + paramType);
        }

        boolean equivalent(Computation o) {
            return this.kind == o.kind && EquivalenceContext.equiv(this.closure, o.closure);
        }

        public static Computation fromJson(JsonNode node, JsonDecoder decoder) {
            ComputationKind kind = ComputationKind.valueOf(Utilities.getStringProperty(node, "kind"));
            DBSPClosureExpression closure = fromJsonInner(node, "closure", decoder, DBSPClosureExpression.class);
            return new Computation(kind, closure);
        }

        public char summary() {
            return switch (this.kind) {
                case Map -> 'M';
                case Filter -> 'F';
                case MapIndex -> 'I';
            };
        }
    }

    /** A chain of functions; if the chain is empty, it is equivalent to the identity function */
    public record ComputationChain(DBSPType inputType, List<Computation> computations) {
        public ComputationChain(DBSPType inputType) {
            this(inputType, new ArrayList<>());
        }

        DBSPType getOutputType(int index) {
            Computation computation = this.computations.get(index);
            return switch (computation.kind) {
                case Map -> new DBSPTypeZSet(computation.closure.getResultType());
                case Filter -> (index == 0) ? this.inputType : this.getOutputType(index - 1);
                case MapIndex -> new DBSPTypeIndexedZSet(
                        computation.closure.getResultType().to(DBSPTypeRawTuple.class));
            };
        }

        DBSPType getOutputType() {
            if (this.computations.isEmpty())
                return inputType;
            return this.getOutputType(this.computations.size() - 1);
        }

        public ComputationChain add(Computation computation) {
            computation.checkInputType(this.getOutputType());
            return new ComputationChain(this.inputType, Linq.append(this.computations, computation));
        }

        public static ComputationChain fromJson(JsonNode node, JsonDecoder decoder) {
            DBSPType inputType = fromJsonInner(node, "inputType", decoder, DBSPType.class);
            List<Computation> computations = Linq.list(Linq.map(
                    node.elements(), e -> Computation.fromJson(e, decoder)));
            return new ComputationChain(inputType, computations);
        }

        DBSPExpression call(DBSPCompiler compiler, DBSPClosureExpression closure, DBSPExpression arg) {
            if (arg.getType().is(DBSPTypeRawTuple.class)) {
                return closure.call(new DBSPRawTupleExpression(
                        arg.field(0).simplify().borrow(),
                        arg.field(1).simplify().borrow()))
                        .reduce(compiler);
            } else {
                return closure.call(arg.borrow()).reduce(compiler);
            }
        }

        public DBSPClosureExpression collapse(DBSPCompiler compiler) {
            Utilities.enforce(this.computations.size() > 1);
            DBSPVariablePath inputVar;
            DBSPExpression currentArg;
            if (this.inputType.is(DBSPTypeZSet.class)) {
                inputVar = this.inputType.to(DBSPTypeZSet.class).getElementType().ref().var();
                currentArg = inputVar.deref();
            } else {
                inputVar = this.inputType.to(DBSPTypeIndexedZSet.class).getKVRefType().var();
                currentArg = new DBSPRawTupleExpression(
                        inputVar.field(0).deref(), inputVar.field(1).deref());
            }
            // Value returned when a filter fails
            DBSPExpression none;
            DBSPType resultType = this.getOutputType();
            if (resultType.is(DBSPTypeZSet.class))
                resultType = resultType.to(DBSPTypeZSet.class).elementType;
            else
                resultType = resultType.to(DBSPTypeIndexedZSet.class).getKVType();
            none = resultType.withMayBeNull(true).none();
            List<DBSPStatement> statements = new ArrayList<>();
            for (Computation comp: this.computations) {
                CalciteObject node = comp.closure().getNode();
                DBSPStatement stat;
                switch (comp.kind) {
                    case Map, MapIndex: {
                        DBSPVariablePath nextVar = comp.closure.getResultType().var();
                        stat = new DBSPLetStatement(nextVar.variable,
                                this.call(compiler, comp.closure, currentArg));
                        currentArg = nextVar;
                        break;
                    }
                    case Filter: {
                        stat = new DBSPExpressionStatement(
                                new DBSPIfExpression(node, this.call(compiler, comp.closure, currentArg).not(),
                                        new DBSPReturnExpression(node, none),
                                        null));
                        break;
                    }
                    default: {
                        throw new InternalCompilerError("unreachable");
                    }
                }
                statements.add(stat);
            }
            DBSPExpression last = currentArg.some();
            DBSPBlockExpression block = new DBSPBlockExpression(statements, last);
            return block.closure(inputVar);
        }

        public int size() {
            return this.computations.size();
        }

        boolean equivalent(ComputationChain chain) {
            if (this.computations.size() != chain.size())
                return false;
            for (int i = 0; i < this.computations.size(); i++) {
                Computation c = this.computations.get(i);
                Computation o = chain.computations.get(i);
                if (!c.equivalent(o))
                    return false;
            }
            return true;
        }

        public String summary() {
            StringBuilder builder = new StringBuilder();
            for (var op: this.computations)
                builder.append(op.summary());
            return builder.toString();
        }
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(function == null);
            return new DBSPChainOperator(this.getRelNode(), this.chain, this.isMultiset, newInputs.get(0))
                    .copyAnnotations(this);
        }
        return this;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        visitor.startArrayProperty("chain");
        for (var operation: this.chain.computations())
            operation.closure().accept(visitor);
        visitor.endArrayProperty("chain");
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPChainOperator otherOperator = other.as(DBSPChainOperator.class);
        if (otherOperator == null)
            return false;
        return this.chain.equivalent(otherOperator.chain);
    }

    @SuppressWarnings("unused")
    public static DBSPChainOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        ComputationChain chain = ComputationChain.fromJson(node.get("chain"), decoder);
        return new DBSPChainOperator(CalciteEmptyRel.INSTANCE,
                chain, info.isMultiset(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPChainOperator.class);
    }
}
