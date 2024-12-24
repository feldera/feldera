package org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Substitution;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SymbolicInterpreter;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.ICastable;
import org.dbsp.util.Utilities;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/** Analyze a closure and find unused fields in its parameters. */
public class FindUnusedFields extends SymbolicInterpreter<FindUnusedFields.SymbolicValue> {
    public record SizedBitSet(int size, BitSet bits) {
        public SizedBitSet(int size) {
            this(size, new BitSet());
        }

        public boolean hasUnusedBits() {
            return this.size > this.bits.cardinality();
        }

        public int cardinality() {
            return this.bits.cardinality();
        }

        public void set(int fieldNo) {
            this.bits.set(fieldNo);
        }

        public void set(int start, int end, boolean b) {
            this.bits.set(start, end, b);
        }

        public boolean get(int i) {
            return this.bits.get(i);
        }

        public void set(int start, int end) {
            this.bits.set(start, end);
        }
    }

    /** Result is deposited here */
    public final Map<DBSPParameter, SizedBitSet> usedFields;
    final ResolveReferences resolver;

    interface SymbolicValue extends ICastable {}

    /** The value of an expression is the specified parameter */
    record ValueIsParameter(DBSPParameter param) implements SymbolicValue {
        public ValueIsDerefParameter deref() {
            return new ValueIsDerefParameter(this.param);
        }
    }

    /** The value of an expression is *param */
    record ValueIsDerefParameter(DBSPParameter param) implements SymbolicValue {
        public ValueIsParameter borrow() {
            return new ValueIsParameter(this.param);
        }
    }

    public FindUnusedFields(DBSPCompiler compiler) {
        super(compiler);
        this.usedFields = new HashMap<>();
        this.resolver = new ResolveReferences(compiler, false);
    }

    public Pair<ParameterFieldRemap, RewriteFields> getFieldRemap() {
        ParameterFieldRemap remap = new ParameterFieldRemap();
        Substitution<DBSPParameter, DBSPParameter> newParam = new Substitution<>();

        for (DBSPParameter param: this.usedFields.keySet()) {
            SizedBitSet bits = this.usedFields.get(param);
            FieldMap fm = new FieldMap(param.getType(), bits);
            remap.setMap(param, fm);
            DBSPType newType = fm.compressedType().ref();
            newParam.substitute(param, newType.var().asParameter());
        }

        RewriteFields rw = new RewriteFields(compiler, newParam, remap);
        return new Pair<>(remap, rw);
    }

    /** True if the closure analyzed can be simplified */
    public boolean foundUnusedFields() {
        for (var e: this.usedFields.entrySet()) {
            DBSPTypeTupleBase type = e.getKey().getType().deref().to(DBSPTypeTupleBase.class);
            SizedBitSet bitset = e.getValue();
            assert type.size() >= bitset.cardinality();
            if (bitset.hasUnusedBits())
                return true;
        }
        return false;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        super.preorder(expression);
        if (!this.context.isEmpty())
            // This means that we are analyzing a closure within another closure.
            throw new InternalCompilerError("Didn't expect nested closures", expression);
        this.resolver.apply(expression);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPParameter param) {
        Utilities.putNew(this.usedFields, param,
                new SizedBitSet(param.getType().deref().to(DBSPTypeTupleBase.class).size()));
        SymbolicValue val = new ValueIsParameter(param);
        this.set(param, val);
        this.setCurrentValue(param, val);
    }

    void used(DBSPExpression expression) {
        SymbolicValue value = this.maybeGet(expression);
        if (value == null)
            return;
        if (value.is(ValueIsDerefParameter.class)) {
            this.markAllFieldsUsed(value.to(ValueIsDerefParameter.class).param);
        }
        if (value.is(ValueIsParameter.class)) {
            this.markAllFieldsUsed(value.to(ValueIsParameter.class).param);
        }
    }

    @Override
    public void postorder(DBSPVariablePath var) {
        IDBSPDeclaration decl = this.resolver.reference.getDeclaration(var);
        SymbolicValue symbolicValue = this.currentValue.get(decl);
        this.maybeSet(var, symbolicValue);
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        this.used(expression.body);
        super.postorder(expression);
    }

    void arguments(DBSPExpression[] arguments) {
        for (DBSPExpression expression: arguments) {
            this.used(expression);
        }
    }

    @Override
    public void postorder(DBSPApplyMethodExpression expression) {
        this.arguments(expression.arguments);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        this.arguments(expression.arguments);
    }

    @Override
    public void postorder(DBSPBorrowExpression expression) {
        SymbolicValue value = this.maybeGet(expression.expression);
        if (value == null)
            return;
        if (value.is(ValueIsDerefParameter.class)) {
            ValueIsDerefParameter vp = value.to(ValueIsDerefParameter.class);
            this.set(expression, vp.borrow());
        }
    }

    @Override
    public void postorder(DBSPDerefExpression expression) {
        SymbolicValue value = this.maybeGet(expression.expression);
        if (value == null)
            return;
        if (value.is(ValueIsParameter.class)) {
            ValueIsParameter vp = value.to(ValueIsParameter.class);
            this.set(expression, vp.deref());
        }
    }

    @Override
    public void postorder(DBSPFieldExpression field) {
        SymbolicValue value = this.maybeGet(field.expression);
        if (value == null)
            return;
        if (value.is(ValueIsDerefParameter.class)) {
            ValueIsDerefParameter vp = value.to(ValueIsDerefParameter.class);
            this.usedFields.get(vp.param).set(field.fieldNo);
        }
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression expression) {
        super.preorder(expression);
        SymbolicValue value = this.maybeGet(expression.initializer);
        this.maybeSet(expression.variable, value);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPLetStatement statement) {
        if (statement.initializer != null) {
            SymbolicValue value = this.maybeGet(statement.initializer);
            this.maybeSet(statement, value);
        }
        super.postorder(statement);
    }

    void markAllFieldsUsed(DBSPParameter param) {
        SizedBitSet set = Utilities.getExists(this.usedFields, param);
        int fields = param.getType().deref().to(DBSPTypeTupleBase.class).size();
        // all fields are used
        set.set(0, fields, true);
    }

    @Override
    public void postorder(DBSPCloneExpression expression) {
        this.used(expression);
    }

    @Override
    public void postorder(DBSPBlockExpression block) {
        if (block.lastExpression != null) {
            SymbolicValue value = this.maybeGet(block.lastExpression);
            this.maybeSet(block, value);
        }
        super.postorder(block);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        this.usedFields.clear();
        this.resolver.startVisit(node);
    }
}
