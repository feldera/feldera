package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.circuit.annotation.NoInc;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Implements the "now" operator.
 * This requires:
 * - using the input stream called now
 * - rewriting map operators that have calls to "now()' into a join followed by a map
 * - rewriting the invocations to the now() function in the map function to references to the input variable */
public class ImplementNow extends Passes {
    /** Discovers whether an expression contains a call to the now() function */
    static class ContainsNow extends InnerVisitor {
        public boolean found;

        public ContainsNow(IErrorReporter reporter) {
            super(reporter);
            this.found = false;
        }

        static boolean isNow(DBSPApplyExpression node) {
            String name = node.getFunctionName();
            return name != null && name.equalsIgnoreCase("now");
        }

        static DBSPTypeTimestamp timestampType() {
            return new DBSPTypeTimestamp(CalciteObject.EMPTY, false);
        }

        @Override
        public VisitDecision preorder(DBSPApplyExpression node) {
            if (isNow(node)) {
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("Found 'now' call")
                        .newline();
                found = true;
                return VisitDecision.STOP;
            }
            return super.preorder(node);
        }

        public boolean found() {
            return this.found;
        }
    }

    /** Remove the 'now' input table */
    public static class RemoveNow extends CircuitCloneVisitor {
        final ICompilerComponent compiler;

        RemoveNow(IErrorReporter errorReporter, ICompilerComponent compiler) {
            super(errorReporter, false);
            this.compiler = compiler;
        }

        @Override
        public void postorder(DBSPSourceMultisetOperator map) {
            if (map.tableName.equalsIgnoreCase("now")) {
                // Return without adding it to the circuit.
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("Removing table 'now'")
                        .newline();
                this.compiler.compiler().removeNowTable();
                return;
            }
            super.postorder(map);
        }
    }

    /** Rewrites a closure of the form |t: &T| ... now() ...
     * to a closure of the form |t: &T1| ... (*t).last,
     * where T1 is T with an extra Timestamp field, and
     * last is the index of the last field of T1. */
    static class RewriteNowExpression extends InnerRewriteVisitor {
        /** Replacement for the now function */
        @Nullable
        DBSPExpression nowReplacement;
        /** Replacement for the parameter references */
        @Nullable
        DBSPExpression parameterReplacement;
        @Nullable
        ReferenceMap refMap;

        public RewriteNowExpression(IErrorReporter reporter) {
            super(reporter);
            this.nowReplacement = null;
            this.parameterReplacement = null;
            this.refMap = null;
        }

        @Override
        public VisitDecision preorder(DBSPApplyExpression expression) {
            // A function call that is `now()` is replaced with the `nowReplacement`
            if (ContainsNow.isNow(expression)) {
                this.map(expression, Objects.requireNonNull(this.nowReplacement));
                return VisitDecision.STOP;
            }
            return super.preorder(expression);
        }

        @Override
        public VisitDecision preorder(DBSPVariablePath var) {
            // A variable that references the parameter is replaced with the `parameterReplacement` expression
            IDBSPDeclaration decl = Objects.requireNonNull(this.refMap).getDeclaration(var);
            if (decl.is(DBSPParameter.class)) {
                this.map(var, Objects.requireNonNull(this.parameterReplacement));
                return VisitDecision.STOP;
            }
            return super.preorder(var);
        }

        @Override
        public VisitDecision preorder(DBSPClosureExpression closure) {
            assert this.context.isEmpty();
            assert closure.parameters.length == 1;
            DBSPParameter param = closure.parameters[0];
            DBSPTypeTuple paramType = param.getType().to(DBSPTypeRef.class).deref().to(DBSPTypeTuple.class);
            List<DBSPType> fields = Linq.list(paramType.tupFields);
            fields.add(ContainsNow.timestampType());
            DBSPParameter newParam = new DBSPParameter(param.getName(), paramType.makeType(fields).ref());
            this.parameterReplacement = newParam.asVariable();
            this.nowReplacement = newParam.asVariable().deref().field(paramType.size());
            ResolveReferences ref = new ResolveReferences(this.errorReporter, false);
            ref.apply(closure);
            this.refMap = ref.reference;

            this.push(closure);
            DBSPExpression body = this.transform(closure.body);
            this.pop(closure);
            DBSPExpression result = body.closure(newParam);
            this.map(closure, result);
            return VisitDecision.STOP;
        }
    }

    /** Replace map operators that contain now() as an expression with
     * map operators that take an extra field and use that instead of the now() call.
     * Insert a join prior to such operators (which also requires a MapIndex operator.
     * Also inserts a MapIndex operator to index the 'NOW' built-in table.
     * Same for filter operators. */
    static class RewriteNow extends CircuitCloneVisitor {
        // Holds the indexed version of the 'now' operator (indexed with an empty key).
        @Nullable
        DBSPOperator nowIndexed = null;
        final ICompilerComponent compiler;

        public RewriteNow(IErrorReporter reporter, ICompilerComponent compiler) {
            super(reporter, false);
            this.compiler = compiler;
        }

        DBSPOperator createJoin(DBSPUnaryOperator operator) {
            // Index the input
            DBSPType inputType = operator.input().getOutputZSetElementType();
            DBSPVariablePath var = inputType.ref().var();
            DBSPExpression indexFunction = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(),
                    var.deref().applyClone());
            DBSPMapIndexOperator index = new DBSPMapIndexOperator(
                    operator.getNode(), indexFunction.closure(var.asParameter()),
                    TypeCompiler.makeIndexedZSet(new DBSPTypeTuple(), inputType),
                    this.mapped(operator.input()));
            this.addOperator(index);

            // Join with 'indexedNow'
            DBSPVariablePath key = new DBSPTypeTuple().ref().var();
            DBSPVariablePath left = inputType.ref().var();
            DBSPVariablePath right = new DBSPTypeTuple(ContainsNow.timestampType()).ref().var();
            List<DBSPExpression> fields = left.deref().allFields();
            fields.add(right.deref().field(0));
            DBSPTypeZSet joinType = TypeCompiler.makeZSet(new DBSPTypeTuple(
                    Linq.map(fields, DBSPExpression::getType)));
            DBSPExpression joinFunction = new DBSPTupleExpression(fields, false)
                    .closure(key.asParameter(), left.asParameter(), right.asParameter());
            assert nowIndexed != null;
            DBSPOperator result = new DBSPStreamJoinOperator(operator.getNode(), joinType,
                        joinFunction, operator.isMultiset, index, nowIndexed);
            this.addOperator(result);
            return result;
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            ContainsNow cn = new ContainsNow(this.errorReporter);
            DBSPExpression function = operator.getFunction();
            cn.apply(function);
            if (cn.found()) {
                DBSPOperator join = this.createJoin(operator);
                RewriteNowExpression rn = new RewriteNowExpression(this.errorReporter);
                function = rn.apply(function).to(DBSPExpression.class);
                DBSPOperator result = new DBSPMapOperator(operator.getNode(), function, operator.getOutputZSetType(), join);
                this.map(operator, result);
            } else {
                super.postorder(operator);
            }
        }

        @Override
        public void postorder(DBSPFilterOperator operator) {
            ContainsNow cn = new ContainsNow(this.errorReporter);
            DBSPExpression function = operator.getFunction();
            cn.apply(function);
            if (cn.found()) {
                DBSPOperator join = this.createJoin(operator);
                RewriteNowExpression rn = new RewriteNowExpression(this.errorReporter);
                function = rn.apply(function).to(DBSPExpression.class);
                DBSPOperator filter = new DBSPFilterOperator(operator.getNode(), function, join);
                this.addOperator(filter);
                // Drop the extra field
                DBSPVariablePath var = filter.getOutputZSetElementType().ref().var();
                List<DBSPExpression> fields = var.deref().allFields();
                Utilities.removeLast(fields);
                DBSPExpression drop = new DBSPTupleExpression(fields, false).closure(var.asParameter());
                DBSPMapOperator result = new DBSPMapOperator(operator.getNode(), drop, operator.getOutputZSetType(), filter);
                this.map(operator, result);
            } else {
                super.postorder(operator);
            }
        }

        @Override
        public void startVisit(IDBSPOuterNode circuit) {
            super.startVisit(circuit);

            DBSPType timestamp = ContainsNow.timestampType();
            CalciteObject node = circuit.getNode();

            // We are doing this in startVisitor because we want now to be first in topological order,
            // otherwise it may be traversed too late, after nodes that should depend on it
            // (but don't yet, because they think they are calling a function).
            DBSPOperator now;
            boolean useSource = this.compiler.compiler().options.ioOptions.internalNow;
            if (useSource) {
                now = new DBSPNowOperator(circuit.getNode());
            } else {
                // A table followed by a differentiator.
                String tableName = this.compiler.compiler().toCase("NOW");
                now = circuit.to(DBSPCircuit.class).circuit.getInput(tableName);
                if (now == null) {
                    throw new CompilationError("Declaration for table 'NOW' not found in program");
                }
                // Prevent processing it again by this visitor
                this.visited.add(now);
                this.addOperator(now);

                DBSPDifferentiateOperator dNow = new DBSPDifferentiateOperator(circuit.getNode(), now);
                now = dNow;
                dNow.annotations.add(new NoInc());
            }
            this.addOperator(now);
            this.map(now, now, false);

            DBSPVariablePath var = new DBSPTypeTuple(timestamp).ref().var();
            DBSPExpression indexFunction = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(),
                    var.deref().applyClone());
            this.nowIndexed = new DBSPMapIndexOperator(
                    node, indexFunction.closure(var.asParameter()),
                    TypeCompiler.makeIndexedZSet(new DBSPTypeTuple(), new DBSPTypeTuple(timestamp)), now);
            this.nowIndexed.addAnnotation(new AlwaysMonotone());
            this.addOperator(this.nowIndexed);
        }
    }

    public ImplementNow(IErrorReporter reporter, ICompilerComponent compiler) {
        super(reporter);
        boolean removeTable = compiler.compiler().options.ioOptions.internalNow;
        ContainsNow cn = new ContainsNow(reporter);
        CircuitRewriter find = cn.getCircuitVisitor();
        RewriteNow rewriteNow = new RewriteNow(reporter, compiler);
        this.passes.add(find);
        this.passes.add(new Conditional(reporter, rewriteNow, cn::found));
        this.passes.add(new Conditional(reporter, new RemoveNow(reporter, compiler), () -> !cn.found() || removeTable));
        ContainsNow cn0 = new ContainsNow(reporter);
        this.passes.add(new Conditional(reporter,
                new Fail(reporter, "Instances of 'now' have not been replaced"), cn0::found));
    }
}
