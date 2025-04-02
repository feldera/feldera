package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Substitution;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SymbolicInterpreter;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyBaseExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.Objects;

/** Analyze a closure and find unused fields in its parameters. */
public class FindUnusedFields extends SymbolicInterpreter<FieldUseMap> {
    /** Result is constructed here.  For each parameter we keep a {@link FieldUseMap}, which is
     * mutated by the dataflow analysis when referenced fields are found.
     * The value of these maps at the end of the analysis is the final result. */
    public final ParameterFieldRemap parameterFieldMap;
    final ResolveReferences resolver;
    @Nullable
    public DBSPClosureExpression closure;

    public FindUnusedFields(DBSPCompiler compiler) {
        super(compiler);
        this.parameterFieldMap = new ParameterFieldRemap();
        this.resolver = new ResolveReferences(compiler, false);
    }

    /** The closure analyzed. If called before analysis, throws. */
    public DBSPClosureExpression getClosure() {
        return Objects.requireNonNull(this.closure);
    }

    /** Create a visitor which will rewrite parameters to only contain the used fields.
     * @param depth: Depth up to which unused fields are eliminated.
     * Note: you cannot mutate the finder; it and the rewrite share state. */
    public RewriteFields getFieldRewriter(int depth) {
        Substitution<DBSPParameter, DBSPParameter> newParam = new Substitution<>();
        for (DBSPParameter param: this.parameterFieldMap.getParameters()) {
            FieldUseMap map = this.parameterFieldMap.get(param);
            DBSPType newType = Objects.requireNonNull(map.compressedType(depth));
            newParam.substitute(param, newType.var().asParameter());
        }
        return new RewriteFields(this.compiler, newParam, this.parameterFieldMap, depth);
    }

    /** Set or change the map for a parameter */
    public void setParameterUseMap(DBSPParameter param, FieldUseMap map) {
        this.parameterFieldMap.changeMap(param, map);
    }

    /** True if the closure analyzed can be simplified.
     * @param depth  Depth at which we are looking for unused fields. */
    public boolean foundUnusedFields(int depth) {
        return this.parameterFieldMap.hasUnusedFields(depth);
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
        // Create an empty FieldUseMap for each parameter of the closure analyzed
        FieldUseMap map = new FieldUseMap(param.getType(), false);
        this.parameterFieldMap.add(param, map);
        this.set(param, map);
        this.setCurrentValue(param, map);
    }

    /** Mark the fields referenced by the specified expression as used */
    void used(DBSPExpression expression) {
        FieldUseMap value = this.maybeGet(expression);
        if (value == null)
            return;
        value.setUsed();
    }

    @Override
    public void postorder(DBSPVariablePath var) {
        IDBSPDeclaration decl = this.resolver.reference.getDeclaration(var);
        FieldUseMap symbolicValue = this.currentValue.get(decl);
        this.maybeSet(var, symbolicValue);
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        this.used(expression.body);
        super.postorder(expression);
    }

    @Override
    public void postorder(DBSPApplyBaseExpression expression) {
        for (DBSPExpression arg: expression.arguments) {
            this.used(arg);
        }
    }

    /** Don't call this function, call findUnusedFields */
    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        return super.apply(node);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        this.used(expression.source);
    }

    @Override
    public void postorder(DBSPUnwrapExpression expression) {
        this.used(expression.expression);
    }

    @Override
    public void postorder(DBSPExpression expression) {
        // Catch-all
        throw new UnimplementedException("Finding unused fields for " + expression);
    }

    @Override
    public void postorder(DBSPConstructorExpression expression) {
        this.used(expression.function);
        for (DBSPExpression e: expression.arguments)
            this.used(e);
    }

    @Override
    public void postorder(DBSPCustomOrdExpression expression) {
        this.used(expression.source);
    }

    @Override
    public void postorder(DBSPCustomOrdField field) {
        FieldUseMap value = this.maybeGet(field.expression);
        if (value == null)
            return;
        FieldUseMap map = value.field(field.fieldNo);
        this.set(field, map);
    }

    @Override
    public void postorder(DBSPBorrowExpression expression) {
        FieldUseMap value = this.maybeGet(expression.expression);
        if (value == null)
            return;
        this.set(expression, value.borrow());
    }

    @Override
    public void postorder(DBSPDerefExpression expression) {
        FieldUseMap value = this.maybeGet(expression.expression);
        if (value == null)
            return;
        this.set(expression, value.deref());
    }

    @Override
    public void postorder(DBSPFieldExpression field) {
        FieldUseMap value = this.maybeGet(field.expression);
        if (value == null)
            return;
        FieldUseMap map = value.field(field.fieldNo);
        this.set(field, map);
    }

    @Override
    public void postorder(DBSPLetExpression expression) {
        this.used(expression.initializer);
    }

    @Override
    public void postorder(DBSPUnaryExpression expression) {
        this.used(expression.source);
    }

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        this.used(expression.left);
        this.used(expression.right);
    }

    @Override
    public void postorder(DBSPLetStatement statement) {
        if (statement.initializer != null)
            this.used(statement.initializer);
    }

    @Override
    public void postorder(DBSPGeoPointConstructor expression) {
        if (expression.left != null)
            this.used(expression.left);
        if (expression.right != null)
            this.used(expression.right);
    }

    @Override
    public void postorder(DBSPIfExpression expression) {
        this.used(expression.condition);
        this.used(expression.positive);
        if (expression.negative != null)
            this.used(expression.negative);
    }

    @Override
    public void postorder(DBSPIsNullExpression expression) {
        this.used(expression.expression);
    }

    @Override
    public void postorder(DBSPMapExpression expression) {
        if (expression.keys != null)
            for (DBSPExpression e: expression.keys)
                this.used(e);
        if (expression.values != null)
            for (DBSPExpression e: expression.values)
                this.used(e);
    }

    @Override
    public void postorder(DBSPPathExpression expression) {}

    @Override
    public void postorder(DBSPBaseTupleExpression expression) {
        if (expression.fields != null) {
            for (DBSPExpression e : expression.fields)
                this.used(e);
        }
    }

    @Override
    public void postorder(DBSPSomeExpression expression) {
        this.used(expression.expression);
    }

    @Override
    public void postorder(DBSPStaticExpression expression) {}

    @Override
    public void postorder(DBSPUnsignedWrapExpression expression) {
        this.used(expression.source);
    }

    @Override
    public void postorder(DBSPUnsignedUnwrapExpression expression) {
        this.used(expression.source);
    }

    @Override
    public void postorder(DBSPVariantExpression expression) {
        if (expression.value != null)
            this.used(expression.value);
    }

    @Override
    public void postorder(DBSPArrayExpression expression) {
        if (expression.data != null) {
            for (DBSPExpression e: expression.data)
                this.used(e);
        }
    }

    @Override
    public void postorder(DBSPWindowBoundExpression expression) {
        this.used(expression.representation);
    }

    @Override
    public void postorder(DBSPAssignmentExpression expression) {
        this.used(expression.right);
    }

    @Override
    public void postorder(DBSPCloneExpression expression) {
        this.used(expression.expression);
    }

    @Override
    public void postorder(DBSPLiteral expression) {}

    @Override
    public void postorder(DBSPComparatorExpression expression) {}

    @Override
    public void postorder(DBSPBlockExpression block) {
        if (block.lastExpression != null) {
            FieldUseMap value = this.maybeGet(block.lastExpression);
            this.maybeSet(block, value);
        }
        super.postorder(block);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        this.parameterFieldMap.clear();
    }

    /** Find unused fields in the closure.  Node: this ensures that the closure is a tree,
     * and returns the tree-shaped equivalent closure too.  Often you want to use that function. */
    public DBSPClosureExpression findUnusedFields(DBSPClosureExpression closure) {
        closure = closure.ensureTree(this.compiler).to(DBSPClosureExpression.class);
        this.closure = closure;
        this.apply(closure);
        return closure;
    }

    public static FieldUseMap computeUsedFields(DBSPClosureExpression closure, DBSPCompiler compiler) {
        assert closure.parameters.length == 1;
        FindUnusedFields fu = new FindUnusedFields(compiler);
        fu.findUnusedFields(closure);
        return fu.parameterFieldMap.get(closure.parameters[0]);
    }
}
