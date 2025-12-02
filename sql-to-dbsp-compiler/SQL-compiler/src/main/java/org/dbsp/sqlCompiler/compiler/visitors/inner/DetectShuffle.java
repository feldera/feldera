package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Detect whether a closure expression with a single parameter can be expressed as a shuffle of the
 * fields of the input parameter.  (This is a conservative approximation.)
 *
 * <p>For a projection to be described as a shuffle,
 * it cannot contain constant fields, nested field accesses, or any computations.
 * |&a| ((*a).1, (*a).3) is a shuffle [1,3], while
 * |&a| (2, (*a).3, (*a.3).2) is not, for two reasons: constant 2, nested field a.3.2
 * Only makes sense for functions with a single parameter. */
public class DetectShuffle extends SymbolicInterpreter<DetectShuffle.AbstractValue> {
    ResolveReferences resolver;
    @Nullable ParameterValue parameter;

    public DetectShuffle(DBSPCompiler compiler) {
        super(compiler);
        this.resolver = new ResolveReferences(compiler, false);
        this.parameter = null;
    }

    /** Abstract value used by the interpreter */
    interface AbstractValue extends ICastable {}

    /** Value refers to the parameter */
    record ParameterValue(DBSPParameter param) implements AbstractValue {
        DBSPTypeTupleBase getType() { return this.param.getType().deref().to(DBSPTypeTupleBase.class); }
    }

    /** Value refers to a field of a parameter */
    record ParameterFieldPart(ParameterValue param, int field) implements AbstractValue {}

    /** Value refers to a tuple containing fields of a parameter */
    record ShuffleValue(ParameterValue param, List<Integer> fields) implements AbstractValue {
        Shuffle getShuffle() {
            return new ExplicitShuffle(param.getType().size(), fields);
        }
    }

    /** Value when the closure could not be determined to look like a shuffle */
    record NoShuffle() implements AbstractValue {
        static final NoShuffle INSTANCE = new NoShuffle();
    }

    @Override
    public void postorder(DBSPType type) {}

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.resolver.apply(node);
        super.startVisit(node);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression node) {
        if (!this.context.isEmpty()
                || node.parameters.length != 1
                || !node.parameters[0].getType().deref().is(DBSPTypeTupleBase.class)) {
            this.set(node, NoShuffle.INSTANCE);
            return VisitDecision.STOP;
        }
        Utilities.enforce(this.parameter == null);
        this.parameter = new ParameterValue(node.parameters[0]);
        return super.preorder(node);
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        var result = this.get(expression.body);
        this.set(expression, result);
        super.postorder(expression);
    }

    @Override
    public void postorder(DBSPExpression expression) {
        // All unhandled cases
        this.set(expression, NoShuffle.INSTANCE);
    }

    public void tuple(DBSPBaseTupleExpression expression) {
        if (this.parameter == null) {
            this.set(expression, NoShuffle.INSTANCE);
        } else if (expression.fields == null) {
            this.set(expression, new ShuffleValue(this.parameter, Linq.list()));
        } else {
            List<Integer> values = new ArrayList<>();
            for (DBSPExpression field : expression.fields) {
                AbstractValue value = this.get(field);
                if (!value.is(ParameterFieldPart.class)) {
                    this.set(expression, NoShuffle.INSTANCE);
                    return;
                }
                ParameterFieldPart pfp = value.to(ParameterFieldPart.class);
                Utilities.enforce(pfp.param == this.parameter);
                values.add(pfp.field);
            }
            this.set(expression, new ShuffleValue(this.parameter, values));
        }
    }

    @Override
    public void postorder(DBSPTupleExpression expression) {
        this.tuple(expression);
    }

    @Override
    public void postorder(DBSPRawTupleExpression expression) {
        this.tuple(expression);
    }

    @Override
    public void postorder(DBSPCloneExpression expression) {
        AbstractValue val = this.get(expression.expression);
        this.set(expression, val);
    }

    @Override
    public void postorder(DBSPVariablePath expression) {
        IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(expression);
        if (!declaration.is(DBSPParameter.class) || this.parameter == null) {
            this.set(expression, NoShuffle.INSTANCE);
            return;
        }
        DBSPParameter param = declaration.to(DBSPParameter.class);
        if (param == this.parameter.param()) {
            this.set(expression, this.parameter);
            return;
        }
        this.set(expression, NoShuffle.INSTANCE);
    }

    @Override
    public void postorder(DBSPDerefExpression expression) {
        var value = this.get(expression.expression);
        this.set(expression, value);
    }

    AbstractValue field(DBSPExpression source, int fieldNo) {
        var value = this.get(source);
        if (value.is(ParameterValue.class)) {
            var pv = value.to(ParameterValue.class);
            return new ParameterFieldPart(pv, fieldNo);
        }
        return NoShuffle.INSTANCE;
    }

    @Override
    public void postorder(DBSPCustomOrdField field) {
        var result = this.field(field.expression, field.fieldNo);
        this.set(field, result);
    }

    @Override
    public void postorder(DBSPFieldExpression field) {
        var result = this.field(field.expression, field.fieldNo);
        this.set(field, result);
    }

    /** Analyze a closure and return a shuffle if the closure is equivalent to a shuffle of the input fields;
     * return null otherwise. */
    public static @Nullable Shuffle analyze(DBSPCompiler compiler, DBSPClosureExpression expression) {
        DetectShuffle ds = new DetectShuffle(compiler);
        DBSPExpression tree = expression.ensureTree(compiler);
        ds.apply(tree);
        AbstractValue result = ds.get(tree);
        if (result.is(ShuffleValue.class))
            return result.to(ShuffleValue.class).getShuffle();
        return null;
    }
}
