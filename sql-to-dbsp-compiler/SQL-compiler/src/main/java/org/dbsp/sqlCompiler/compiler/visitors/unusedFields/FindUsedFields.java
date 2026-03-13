package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SymbolicInterpreter;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalIncrementExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFailExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTimeAddSub;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Track the fields of parameters used by an expression.
 * For example, given the closure |x| x.1 + x.3, the expression
 * (x.1 + x.3) uses fields 1 and 3 of parameter x.
 * If parameter x has type Tup[4], this can be represented also as
 * [_, X, _, X].
 *
 * <p>The implementation uses abstract interpretation with {@link IUsedFields}
 * being the base class of the abstract value.  In general expression
 * f(e1, e2, e3) will use the union of the fields used by e1, e2, e3.
 * We compute for each expression a value of type {@link IUsedFields},
 * but the final result the user needs is usually a {@link ParameterFieldUse}
 * object, which can be created from the used fields. */
public class FindUsedFields extends SymbolicInterpreter<IUsedFields> {
    final ResolveReferences resolver;

    public FindUsedFields(DBSPCompiler compiler) {
        super(compiler);
        this.resolver = new ResolveReferences(compiler, false);
    }

    @Override
    public void postorder(DBSPVariablePath var) {
        IDBSPDeclaration decl = this.resolver.reference.getDeclaration(var);
        IUsedFields symbolicValue = this.currentValue.get(decl);
        Utilities.enforce(symbolicValue != null);
        this.set(var, symbolicValue);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        Set<IUsedFields> used = new HashSet<>();
        for (DBSPExpression arg: expression.arguments) {
            IUsedFields argUsed = this.get(arg);
            used.add(argUsed);
        }
        IUsedFields set = FieldSet.union(used);
        this.set(expression, set);
    }

    @Override
    public void postorder(DBSPApplyMethodExpression expression) {
        Set<IUsedFields> used = new HashSet<>();
        used.add(this.get(expression.self));
        for (DBSPExpression arg: expression.arguments) {
            IUsedFields argUsed = this.get(arg);
            used.add(argUsed);
        }
        IUsedFields set = FieldSet.union(used);
        this.set(expression, set);
    }

    void copy(DBSPExpression destination, DBSPExpression source) {
        IUsedFields used = this.get(source);
        this.set(destination, used);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        this.copy(expression, expression.source);
    }

    @Override
    public void postorder(DBSPFailExpression expression) {
        this.set(expression, FieldSet.EMPTY);
    }

    @Override
    public void postorder(DBSPConstructorExpression expression) {
        Set<IUsedFields> used = new HashSet<>();
        for (DBSPExpression arg: expression.arguments) {
            IUsedFields argUsed = this.get(arg);
            used.add(argUsed);
        }
        IUsedFields constr = this.get(expression.function);
        used.add(constr);
        IUsedFields set = FieldSet.union(used);
        this.set(expression, set);
    }

    @Override
    public void postorder(DBSPCustomOrdField field) {
        IUsedFields value = this.get(field.expression);
        final IUsedFields result;
        if (value.is(ParameterField.class)) {
            result = value.to(ParameterField.class).field(field.fieldNo);
        } else if (value.is(TupleOfFields.class)) {
            result = value.to(TupleOfFields.class).fields().get(field.fieldNo);
        } else {
            result = value;
        }
        this.set(field, result);
    }

    @Override
    public void postorder(DBSPFieldExpression field) {
        IUsedFields value = this.get(field.expression);
        final IUsedFields result;
        if (value.is(ParameterField.class)) {
            result = value.to(ParameterField.class).field(field.fieldNo);
        } else if (value.is(TupleOfFields.class)) {
            result = value.to(TupleOfFields.class).fields().get(field.fieldNo);
        } else {
            result = value;
        }
        this.set(field, result);
    }

    @Override
    public void postorder(DBSPBorrowExpression expression) {
        IUsedFields value = this.get(expression.expression);
        if (value.is(ParameterField.class)) {
            this.set(expression, new BorrowedField(value.to(ParameterField.class)));
            return;
        }
        this.copy(expression, expression.expression);
    }

    @Override
    public void postorder(DBSPDerefExpression expression) {
        IUsedFields value = this.get(expression.expression);
        if (value.is(BorrowedField.class)) {
            this.set(expression, value.to(BorrowedField.class).field());
            return;
        }
        this.copy(expression, expression.expression);
    }

    @Override
    public void postorder(DBSPUnaryExpression expression) {
        this.copy(expression, expression.source);
    }

    @Override
    public void postorder(DBSPTimeAddSub expression) {
        Set<IUsedFields> both = new HashSet<>();
        both.add(this.get(expression.left));
        both.add(this.get(expression.right));
        IUsedFields union = FieldSet.union(both);
        this.set(expression, union);
    }

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        Set<IUsedFields> both = new HashSet<>();
        both.add(this.get(expression.left));
        both.add(this.get(expression.right));
        IUsedFields union = FieldSet.union(both);
        this.set(expression, union);
    }

    @Override
    public void postorder(DBSPConditionalIncrementExpression expression) {
        Set<IUsedFields> all = new HashSet<>();
        if (expression.condition != null)
            all.add(this.get(expression.condition));
        all.add(this.get(expression.left));
        all.add(this.get(expression.right));
        IUsedFields union = FieldSet.union(all);
        this.set(expression, union);
    }

    @Override
    public void postorder(DBSPGeoPointConstructor expression) {
        Set<IUsedFields> all = new HashSet<>();
        if (expression.left != null)
            all.add(this.get(expression.left));
        if (expression.right != null)
            all.add(this.get(expression.right));
        IUsedFields union = FieldSet.union(all);
        this.set(expression, union);
    }

    @Override
    public void postorder(DBSPIfExpression expression) {
        Set<IUsedFields> all = new HashSet<>();
        all.add(this.get(expression.condition));
        all.add(this.get(expression.positive));
        if (expression.negative != null)
            all.add(this.get(expression.negative));
        IUsedFields union = FieldSet.union(all);
        this.set(expression, union);
    }

    @Override
    public void postorder(DBSPIsNullExpression expression) {
        IUsedFields used = this.get(expression.expression);
        // Special handling for (*var).is_none(), where var refers to a parameter's field which is a tuple.
        if (used.is(ParameterField.class)) {
            ParameterField pf = used.to(ParameterField.class);
            DBSPType type = pf.param().getType();
            if (type.is(DBSPTypeRef.class) && type.deref().is(DBSPTypeTuple.class) && pf.indexes().isEmpty()) {
                this.set(expression, FieldSet.EMPTY);
                return;
            } else if (type.is(DBSPTypeRawTuple.class) && pf.indexes().size() == 1) {
                this.set(expression, FieldSet.EMPTY);
                return;
            } else {
                this.set(expression, pf);
                return;
            }
        }
        this.copy(expression, expression.expression);
    }

    @Override
    public void postorder(DBSPMapExpression expression) {
        Set<IUsedFields> all = new HashSet<>();
        if (expression.keys != null)
            for (DBSPExpression e: expression.keys)
                all.add(this.get(e));
        if (expression.values != null)
            for (DBSPExpression e: expression.values)
                all.add(this.get(e));
        IUsedFields union = FieldSet.union(all);
        this.set(expression, union);
    }

    @Override
    public void postorder(DBSPPathExpression expression) {
        this.set(expression, FieldSet.EMPTY);
    }

    @Override
    public void postorder(DBSPBaseTupleExpression expression) {
        List<IUsedFields> fields = new ArrayList<>();
        if (expression.fields != null) {
            for (DBSPExpression e : expression.fields)
                fields.add(this.get(e));
            IUsedFields tuple = new TupleOfFields(fields);
            this.set(expression, tuple);
        } else {
            this.set(expression, FieldSet.EMPTY);
        }
    }

    @Override
    public void postorder(DBSPSomeExpression expression) {
        this.copy(expression, expression.expression);
    }

    @Override
    public void postorder(DBSPStaticExpression expression) {
        this.set(expression, FieldSet.EMPTY);
    }

    @Override
    public void postorder(DBSPUnsignedWrapExpression expression) {
        this.copy(expression, expression.source);
    }

    @Override
    public void postorder(DBSPUnsignedUnwrapExpression expression) {
        this.copy(expression, expression.source);
    }

    @Override
    public void postorder(DBSPVariantExpression expression) {
        if (expression.value != null)
            this.copy(expression, expression.value);
        else
            this.set(expression, FieldSet.EMPTY);
    }

    @Override
    public void postorder(DBSPArrayExpression expression) {
        if (expression.data != null) {
            Set<IUsedFields> all = new HashSet<>();
            for (DBSPExpression e: expression.data)
                all.add(this.get(e));
            IUsedFields union = FieldSet.union(all);
            this.set(expression, union);
        } else {
            this.set(expression, FieldSet.EMPTY);
        }
    }

    @Override
    public void postorder(DBSPWindowBoundExpression expression) {
        this.copy(expression, expression.representation);
    }

    @Override
    public void postorder(DBSPCloneExpression expression) {
        this.copy(expression, expression.expression);
    }

    @Override
    public void postorder(DBSPLiteral expression) {
        this.set(expression, FieldSet.EMPTY);
    }

    @Override
    public void postorder(DBSPComparatorExpression expression) {
        this.set(expression, FieldSet.EMPTY);
    }

    @Override
    public void postorder(DBSPBlockExpression block) {
        if (block.lastExpression != null) {
            IUsedFields value = this.get(block.lastExpression);
            this.set(block, value);
        }
        super.postorder(block);
    }

    @Override
    public void postorder(DBSPUnwrapExpression expression) {
        this.copy(expression, expression.expression);
    }

    @Override
    public void postorder(DBSPAssignmentExpression expression) {
        this.copy(expression.left, expression.right);
        this.set(expression, FieldSet.EMPTY);
    }

    @Override
    public void postorder(DBSPLetExpression expression) {
        throw new UnimplementedException();
    }

    @Override
    public void postorder(DBSPLetStatement statement) {
        throw new UnimplementedException();
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        super.preorder(expression);
        if (!this.context.isEmpty()) {
            // This means that we are analyzing a closure within another closure; ignore, since
            // nested closures must currently be pure functions
            this.currentValue.popContext();
            this.set(expression, FieldSet.EMPTY);
            return VisitDecision.STOP;
        }
        for (DBSPParameter param: expression.parameters) {
            ParameterField pf = new ParameterField(param, List.of());
            if (param.getType().is(DBSPTypeRawTuple.class)) {
                this.setCurrentValue(param,
                        new TupleOfFields(List.of(
                        new BorrowedField(pf.field(0)),
                        new BorrowedField(pf.field(1)))));
            } else {
                this.setCurrentValue(param, new BorrowedField(pf));
            }
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.resolver.apply(node);
        super.startVisit(node);
    }

    public ParameterFieldUse findUsedFields(DBSPClosureExpression expression) {
        expression = expression.ensureTree(this.compiler).to(DBSPClosureExpression.class);
        this.apply(expression);
        IUsedFields used = this.get(expression.body);
        // Create a map that has all parameters with all fields unused
        ParameterFieldUse empty = new ParameterFieldUse();
        for (var param: expression.parameters) {
            FieldUseMap fum = new FieldUseMap(param.getType(), false);
            empty.set(param, fum);
        }
        ParameterFieldUse found = used.getParameterUse();
        // found may not contain some parameters at all; this union ensures that they are all there
        found.union(empty);
        return found;
    }

    public record AggregateUseMap(IAggregate aggregate, ParameterFieldUse useMap, DBSPParameter parameter) {}

    AggregateUseMap computeUsedFields(LinearAggregate aggregate) {
        DBSPClosureExpression closure = aggregate.map;
        var parameterFieldUse = this.findUsedFields(closure);
        return new AggregateUseMap(
                new LinearAggregate(aggregate.getNode(),
                        closure, aggregate.postProcess, aggregate.emptySetResult),
                parameterFieldUse, closure.parameters[0]);
    }

    AggregateUseMap computeUsedFields(NonLinearAggregate aggregate) {
        DBSPClosureExpression closure = aggregate.increment;
        var parameterFieldUse = this.findUsedFields(closure);
        parameterFieldUse.get(aggregate.increment.parameters[0]).setUsed();
        parameterFieldUse.get(aggregate.increment.parameters[2]).setUsed();
        return new AggregateUseMap(
                new NonLinearAggregate(aggregate.getNode(), aggregate.zero,
                        closure, aggregate.postProcess, aggregate.emptySetResult, aggregate.semigroup),
                parameterFieldUse, closure.parameters[1]);
    }

    AggregateUseMap computeUsedFields(MinMaxAggregate aggregate) {
        DBSPClosureExpression closure = aggregate.comparedValue;
        var parameterFieldUse = this.findUsedFields(closure);
        return new AggregateUseMap(
                new MinMaxAggregate(aggregate.getNode(), aggregate.zero,
                        aggregate.increment.deepCopy().to(DBSPClosureExpression.class),
                        aggregate.emptySetResult, aggregate.semigroup, closure,
                        aggregate.postProcess, aggregate.operation),
                parameterFieldUse, closure.parameters[0]);
    }

    public AggregateUseMap computeUsedFields(IAggregate aggregate) {
        // Maybe I should use a visitor pattern here as well?
        if (aggregate.is(MinMaxAggregate.class))
            // Try this one first, since it extends NonLinearAggregate
            return this.computeUsedFields(aggregate.to(MinMaxAggregate.class));
        else if (aggregate.is(NonLinearAggregate.class))
            return this.computeUsedFields(aggregate.to(NonLinearAggregate.class));
        else if (aggregate.is(LinearAggregate.class))
            return this.computeUsedFields(aggregate.to(LinearAggregate.class));
        throw new InternalCompilerError("Unexpected aggregate " + aggregate);
    }

    public static FieldUseMap computeUsedFields(DBSPClosureExpression closure, DBSPCompiler compiler) {
        Utilities.enforce(closure.parameters.length == 1);
        FindUsedFields fu = new FindUsedFields(compiler);
        var pfm = fu.findUsedFields(closure);
        return pfm.get(closure.parameters[0]);
    }
}
