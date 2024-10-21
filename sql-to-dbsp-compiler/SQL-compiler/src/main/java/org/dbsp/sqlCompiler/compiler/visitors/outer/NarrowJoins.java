package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Substitution;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Find and remove unused fields in Join operators. */
public class NarrowJoins extends CircuitCloneVisitor {
    public NarrowJoins(IErrorReporter reporter) {
        super(reporter, false);
    }

    /** Rewrite variable and field accesses */
    static class RewriteFields extends InnerRewriteVisitor {
        final Substitution<DBSPParameter, DBSPParameter> newParam;
        // Maps original parameters to their remap tables
        // param.X is remapped to newParam.Y, where newParam is given
        // by the 'newParam' table, and Y is given by fieldRemap[param][X]
        final Map<DBSPParameter, Map<Integer, Integer>> fieldRemap;
        @Nullable
        ReferenceMap refMap;

        protected RewriteFields(IErrorReporter reporter,
                                Substitution<DBSPParameter, DBSPParameter> newParam,
                                Map<DBSPParameter, Map<Integer, Integer>> fieldRemap) {
            super(reporter);
            this.fieldRemap = fieldRemap;
            this.newParam = newParam;
        }

        @Override
        public VisitDecision preorder(DBSPParameter param) {
            DBSPParameter replacement = this.newParam.get(param);
            this.map(param, replacement);
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPVariablePath var) {
            assert this.refMap != null;
            IDBSPDeclaration declaration = this.refMap.getDeclaration(var);
            if (declaration.is(DBSPParameter.class)) {
                DBSPParameter replacement = this.newParam.get(declaration.to(DBSPParameter.class));
                this.map(var, replacement.asVariable());
                return VisitDecision.STOP;
            }
            return super.preorder(var);
        }

        @Override
        public VisitDecision preorder(DBSPFieldExpression expression) {
            // Check for a pattern of the form (*param).x
            // to rewrite them as (*newParam).y
            assert this.refMap != null;
            int field = expression.fieldNo;
            if (expression.expression.is(DBSPDerefExpression.class)) {
                DBSPDerefExpression deref = expression.expression.to(DBSPDerefExpression.class);
                if (deref.expression.is(DBSPVariablePath.class)) {
                    DBSPVariablePath var = deref.expression.to(DBSPVariablePath.class);
                    IDBSPDeclaration declaration = this.refMap.getDeclaration(var);
                    if (declaration.is(DBSPParameter.class)) {
                        Map<Integer, Integer> remap = this.fieldRemap.get(declaration.to(DBSPParameter.class));
                        if (remap != null) {
                            field = Utilities.getExists(remap, field);
                        }
                    }
                    this.push(expression);
                    DBSPExpression source = this.transform(expression.expression);
                    this.pop(expression);
                    this.map(expression, source.field(field));
                    return VisitDecision.STOP;
                }
            }
            return super.preorder(expression);
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            ResolveReferences resolve = new ResolveReferences(this.errorReporter, false);
            this.refMap = resolve.reference;
            resolve.apply(node);
            super.startVisit(node);
        }
    }

    DBSPMapIndexOperator getProjection(CalciteObject node, List<Integer> fields, DBSPOperator input) {
        DBSPType inputType = input.getOutputIndexedZSetType().getKVRefType();
        DBSPVariablePath var = inputType.var();
        List<DBSPExpression> resultFields = Linq.map(fields,
                f -> var.deepCopy().field(1).deref().field(f).applyCloneIfNeeded());
        DBSPRawTupleExpression raw = new DBSPRawTupleExpression(
                var.deepCopy().field(0).deref().applyClone(),
                new DBSPTupleExpression(resultFields, false));
        DBSPClosureExpression projection = raw.closure(var);

        DBSPOperator source = this.mapped(input);
        DBSPTypeIndexedZSet ix = TypeCompiler.makeIndexedZSet(projection.getResultType().to(DBSPTypeRawTuple.class));
        DBSPMapIndexOperator map = new DBSPMapIndexOperator(node, projection, ix, source);
        this.addOperator(map);
        return map;
    }

    boolean processJoin(DBSPBinaryOperator join) {
        Projection projection = new Projection(this.errorReporter);
        projection.apply(join.getFunction());
        if (!projection.hasIoMap()) return false;
        DBSPClosureExpression joinFunction = join.getClosureFunction();
        int leftSize = joinFunction.parameters[1].getType().deref().to(DBSPTypeTupleBase.class).size();
        int rightSize = joinFunction.parameters[2].getType().deref().to(DBSPTypeTupleBase.class).size();

        // Create a projection map for each input which has unused fields
        Projection.IOMap outputMap = projection.getIoMap();
        List<Integer> leftInputs = outputMap.getFieldsOfInput(1);
        List<Integer> rightInputs = outputMap.getFieldsOfInput(2);
        // If all the fields are used there is no point in optimizing this
        if (leftInputs.size() == leftSize && rightInputs.size() == rightSize)
            return false;
        DBSPOperator leftMap = getProjection(join.getNode(), leftInputs, join.left());
        DBSPOperator rightMap = getProjection(join.getNode(), rightInputs, join.right());

        Map<Integer, Integer> leftRemap = new HashMap<>();
        for (int i = 0; i < leftInputs.size(); i++)
            // This "compresses" the indexes in the join function, omitting the ones
            // that are no longer present in the inputs
            leftRemap.put(leftInputs.get(i), i);
        Map<Integer, Integer> rightRemap = new HashMap<>();
        for (int i = 0; i < rightInputs.size(); i++)
            rightRemap.put(rightInputs.get(i), i);

        assert joinFunction.parameters.length == 3;
        Substitution<DBSPParameter, DBSPParameter> subst = new Substitution<>();
        subst.substituteNew(
                joinFunction.parameters[0],
                join.left().getOutputIndexedZSetType().keyType.ref().var().asParameter());
        subst.substitute(
                joinFunction.parameters[1],
                leftMap.getOutputIndexedZSetType().elementType.ref().var().asParameter());
        subst.substitute(
                joinFunction.parameters[2],
                rightMap.getOutputIndexedZSetType().elementType.ref().var().asParameter());

        Map<DBSPParameter, Map<Integer, Integer>> remap = new HashMap<>();
        Utilities.putNew(remap, joinFunction.parameters[1], leftRemap);
        Utilities.putNew(remap, joinFunction.parameters[2], rightRemap);

        RewriteFields rw = new RewriteFields(this.errorReporter, subst, remap);
        DBSPExpression newJoinFunction = rw.apply(join.getFunction()).to(DBSPExpression.class);
        DBSPOperator replacement = join.withFunction(newJoinFunction, join.outputType)
                .withInputs(Linq.list(leftMap, rightMap), true);
        this.map(join, replacement);
        return true;
    }

    @Override
    public void postorder(DBSPStreamJoinOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPJoinOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }
}
