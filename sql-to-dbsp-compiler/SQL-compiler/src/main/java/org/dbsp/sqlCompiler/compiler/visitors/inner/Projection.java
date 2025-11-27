package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.apache.commons.lang3.ArrayUtils;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;

/** Discovers whether a closure is just a projection:
 * selects some fields from the input tuple.
 * A conservative approximation; it does not handle nested tuples. */
public class Projection extends InnerVisitor {
    @Nullable
    public DBSPClosureExpression expression;
    /** Set to true if this is indeed a projection. */
    public boolean isProjection;
    /** Parameters of the analyzed closure. */
    @Nullable public DBSPParameter[] parameters;
    final ResolveReferences resolver;
    /** If true consider casts from a type to the same type non-nullable as noops */
    final boolean allowNoopCasts;
    /** If true, analyze functions that return nested tuples to a bigger depth than 2; these can be projections */
    final boolean allowDeepTuples;

    /** If the description can be described as a shuffle, this is it.
     * For a projection to be described as a shuffle,
     * it cannot contain constant fields, or nested fields.
     * (a.1, a.3) is a shuffle, while
     * (2, a.3, a.3.2) is not.
     * Only makes sense for functions with a single parameter. */
    @Nullable
    List<Integer> shuffle;

    /** A pair containing an input (parameter) number (0, 1, 2, etc.)
     * and an index field in the tuple of the corresponding input .*/
    public record InputAndFieldIndex(int inputIndex, int fieldIndex) {}

    /**
     * A list describing how each output of a projection is computed.
     * This is used to encode projection functions.   For example, the function:
     * |x: Tup2, y: Tup3, z:Tup2| (x.0, y.0, x.1, z.1)
     * is encoded as:
     * 0 -> (0, 0)  first output field comes from x (0), field 0
     * 1 -> (1, 0)  second output field comes from y (0), field 0
     * 2 -> (0, 1)  third output field comes from x (0), field 1
     * 3 -> (2, 1)  fourth output field comes from z (2), field 1
     */
    public record IOMap(List<InputAndFieldIndex> fields) {
        public IOMap() {
            this(new ArrayList<>());
        }

        public void add(int inputIndex, int fieldIndex) {
            this.fields.add(new InputAndFieldIndex(inputIndex, fieldIndex));
        }

        /** The fields of the specified input in the order they are used in the output */
        public List<Integer> getFieldsOfInput(int input) {
            return Linq.map(Linq.where(this.fields, f -> f.inputIndex() == input), InputAndFieldIndex::fieldIndex);
        }

        public int size() {
            return this.fields.size();
        }

        /** If this input field is used as an output, return the first output using it.
         * Otherwise, return -1. */
        public int firstOutputField(int input, int field) {
            InputAndFieldIndex ix = new InputAndFieldIndex(input, field);
            return this.fields.indexOf(ix);
        }
    }

    /** A list indexed by output number.  For each output, the list
     * contains the input parameter index, and the field index, if the analyzed
     * function is a simple projection. */
    @Nullable
    IOMap ioMap;

    void notShuffle() {
        this.shuffle = null;
        this.ioMap = null;
    }

    VisitDecision notProjection() {
        this.notShuffle();
        this.isProjection = false;
        this.parameters = null;
        return VisitDecision.STOP;
    }

    public Projection(DBSPCompiler compiler, boolean allowNoopCasts, boolean allowDeepTuples) {
        super(compiler);
        this.isProjection = true;
        this.ioMap = new IOMap();
        this.shuffle = new ArrayList<>();
        this.resolver = new ResolveReferences(compiler, false);
        this.allowNoopCasts = allowNoopCasts;
        this.allowDeepTuples = allowDeepTuples;
    }

    public Projection(DBSPCompiler compiler) {
        this(compiler, false, false);
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        // Any other expression makes this not be a projection.
        return this.notProjection();
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression expression) {
        if (!expression.contents.isEmpty()) {
            // Too hard.  Give up.
            return this.notProjection();
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement statement) {
        return this.notProjection();
    }

    /** Which parameter of the current function is referenced by this variable?
     * @return -1 if there is no such parameter.
     */
    int getParameterIndex(DBSPVariablePath var) {
        IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
        if (!declaration.is(DBSPParameter.class)) {
            return -1;
        }
        DBSPParameter param = declaration.to(DBSPParameter.class);
        return ArrayUtils.indexOf(this.parameters, param);
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        // Allow two patterns: *param (when the input is a ZSet)
        // and *(param.0) (when the input is an indexed ZSet).
        boolean legal = expression.expression.is(DBSPVariablePath.class);
        if (!legal)
            legal = expression.expression.is(DBSPFieldExpression.class) &&
                    expression.expression.to(DBSPFieldExpression.class)
                            .expression.is(DBSPVariablePath.class);
        if (!legal) {
            this.notShuffle();
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath var) {
        if (this.getParameterIndex(var) < 0) {
            return this.notProjection();
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        DBSPType type = expression.getType();
        if (!expression.source.getType().withMayBeNull(true)
                .sameType(type.withMayBeNull(true)) ||
                !this.allowNoopCasts) {
            // A cast which only changes nullability is
            // considered an identity function
            return this.notProjection();
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression field) {
        if (!field.expression.is(DBSPDerefExpression.class) &&
            !field.expression.is(DBSPUnwrapCustomOrdExpression.class)) {
            this.notShuffle();
            return VisitDecision.CONTINUE;
        }
        if (this.shuffle != null)
            this.shuffle.add(field.fieldNo);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPUnwrapCustomOrdExpression field) {
        if (!field.expression.is(DBSPDerefExpression.class)) {
            this.notShuffle();
            return VisitDecision.CONTINUE;
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPCustomOrdField field) {
        if (!field.expression.is(DBSPVariablePath.class)) {
            this.notProjection();
            return VisitDecision.CONTINUE;
        }
        if (this.shuffle != null)
            this.shuffle.add(field.fieldNo);
        return VisitDecision.CONTINUE;
    }

    void field(DBSPExpression source, int field) {
        if (this.ioMap == null)
            return;

        // Extract the variable from patterns of the form var.field or (*var).field
        DBSPVariablePath var = null;
        if (source.is(DBSPDerefExpression.class))
            source = source.to(DBSPDerefExpression.class).expression;
        if (source.is(DBSPVariablePath.class))
            var = source.to(DBSPVariablePath.class);
        if (var == null) {
            this.notProjection();
            return;
        }
        int paramIndex = this.getParameterIndex(var);
        if (paramIndex >= 0) {
            this.ioMap.add(paramIndex, field);
        } else {
            this.notProjection();
        }
    }

    @Override
    public void postorder(DBSPFieldExpression field) {
        this.field(field.expression, field.fieldNo);
    }

    @Override
    public void postorder(DBSPCustomOrdField field) {
        this.field(field.expression, field.fieldNo);
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPUnwrapExpression expression) { return VisitDecision.CONTINUE; }

    VisitDecision checkNestedTuples(DBSPBaseTupleExpression expression) {
        if (expression.fields != null) {
            // If this creates a nested tuple, give up.
            // Note: this fundamentally relies on the analyzed function shape looking like
            // TupN::new(expr0, expr1, expr2).
            // For a shuffle each expr should look like a parameter field reference.
            for (DBSPExpression field : expression.fields)
                if (field.type.is(DBSPTypeTupleBase.class)) {
                    this.notShuffle();
                    return VisitDecision.STOP;
                }
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        return this.checkNestedTuples(expression);
    }

    @Override
    public VisitDecision preorder(DBSPRawTupleExpression expression) {
        return this.checkNestedTuples(expression);
    }

    public VisitDecision preorder(DBSPLiteral expression) {
        this.notShuffle();
        return VisitDecision.CONTINUE;
    }

    /** True if the specified type is a tuple with non-tuple fields */
    boolean isSimpletuple(DBSPType type) {
        if (!type.is(DBSPTypeTupleBase.class))
            return false;
        DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
        if (tuple.is(DBSPTypeRawTuple.class)) {
            // This is an indexed Z-set, expect both key and value to be simple
            if (tuple.size() != 2)
                return false;

            for (DBSPType field : tuple.tupFields) {
                if (!field.is(DBSPTypeTuple.class))
                    return false;
                for (DBSPType fField : field.to(DBSPTypeTuple.class).tupFields)
                    if (fField.is(DBSPTypeTupleBase.class))
                        return false;
            }
        } else {
            for (DBSPType field: tuple.tupFields)
                if (field.is(DBSPTypeTupleBase.class))
                    return false;
        }
        return true;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (!this.context.isEmpty())
            // We only allow closures in the outermost context.
            return this.notProjection();
        this.expression = expression;
        if (expression.parameters.length == 0)
            return this.notProjection();
        // Currently skip expressions that return nested tuples
        if (!this.isSimpletuple(expression.body.getType())) {
            if (!this.allowDeepTuples) {
                return this.notProjection();
            }
        }

        this.parameters = expression.parameters;
        return VisitDecision.CONTINUE;
    }

    public Shuffle getShuffle() {
        Utilities.enforce(this.isProjection);
        Utilities.enforce(this.parameters != null);
        DBSPType type = this.parameters[0].getType();
        int size = type.deref().to(DBSPTypeTupleBase.class).size();
        return new ExplicitShuffle(size, Objects.requireNonNull(this.shuffle));
    }

    /** Compose this projection with a constant expression.
     * @param before Constant expression.
     * @return A new constant expression. */
    public DBSPExpression applyAfter(DBSPZSetExpression before) {
        Objects.requireNonNull(this.expression);

        Map<DBSPExpression, Long> result = new HashMap<>();
        InnerPasses inner = new InnerPasses(
                new BetaReduction(this.compiler),
                new Simplify(this.compiler)
        );

        DBSPType elementType = this.expression.getResultType();
        for (Map.Entry<DBSPExpression, Long> entry: before.data.entrySet()) {
            DBSPExpression row = entry.getKey();
            DBSPExpression apply = this.expression.call(row.borrow());
            DBSPExpression simplified = inner.apply(apply).to(DBSPExpression.class);
            result.put(simplified, entry.getValue());
        }
        return new DBSPZSetExpression(result, elementType);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.resolver.apply(node);
        super.startVisit(node);
        this.ioMap = new IOMap();
        this.shuffle = new ArrayList<>();
        this.isProjection = true;
    }

    @Override
    public void endVisit() {
        if (this.ioMap != null && this.expression != null) {
            DBSPTypeTupleBase bodyType = this.expression.body.getType().to(DBSPTypeTupleBase.class);
            int iomapSize = this.ioMap.fields().size();
            if (bodyType.is(DBSPTypeRawTuple.class)) {
                Utilities.enforce(bodyType.tupFields.length == 2);
                int totalSize = bodyType.tupFields[0].to(DBSPTypeTupleBase.class).size() +
                        bodyType.tupFields[1].to(DBSPTypeTupleBase.class).size();
                Utilities.enforce(iomapSize == totalSize,
                        () -> "IOMap has " + iomapSize + " fields, but expected " + totalSize);
            } else {
                Utilities.enforce(iomapSize == bodyType.size(),
                        () -> "IOMap has " + iomapSize + " fields, but expected " + bodyType.size());
            }
        }
    }

    public boolean isShuffle() {
        Utilities.enforce(Objects.requireNonNull(this.parameters).length == 1);
        return this.shuffle != null;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean hasIoMap() {
        return this.isProjection && this.ioMap != null;
    }

    /** @return The IOMap, if the analyzed
     * function is a projection. */
    public IOMap getIoMap() {
        return Objects.requireNonNull(this.ioMap);
    }
}
