package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A DBSP simple operator, which applies a function to the inputs and produces a single output.
 * On the naming of the operator classes:
 * Each operator has an "operation" field. This one corresponds to the
 * Rust Stream method that is invoked to implement the operator.
 * Some operators have "Stream" in their name.  These usually correspond
 * to a Rust method starting with "stream_*".
 * Some operators compute correctly both over deltas and aver whole sets, e.g. Map.
 */
public abstract class DBSPSimpleOperator extends DBSPOperator
        implements IHasType, IDBSPOuterNode {
    /** Operation that is invoked on inputs; corresponds to a DBSP operator name, e.g., join. */
    public final String operation;
    /** Computation invoked by the operator, usually a closure. */
    @Nullable
    public final DBSPExpression function;
    /** Type of output produced. */
    public final DBSPType outputType;
    /** True if the output of the operator is a multiset.  Conservative approximation;
     * if this is 'false', it is surely false.  If it is true, the output may still be a set. */
    public final boolean isMultiset;
    /** True if the operator contains an integrator */
    public final boolean containsIntegrator;

    protected DBSPSimpleOperator(CalciteRelNode node, String operation,
                                 @Nullable DBSPExpression function, DBSPType outputType,
                                 boolean isMultiset, @Nullable String comment,
                                 boolean containsIntegrator) {
        super(node, comment);
        this.operation = operation;
        this.function = function;
        this.outputType = outputType;
        this.isMultiset = isMultiset;
        this.containsIntegrator = containsIntegrator;
        if (!operation.startsWith("waterline") &&
                !operation.startsWith("apply") &&
                !operation.startsWith("transaction_delay") &&
                !outputType.is(DBSPTypeZSet.class) &&
                !outputType.is(DBSPTypeIndexedZSet.class))
            throw new InternalCompilerError("Operator " + operation +
                    " output type is unexpected " + outputType);
    }

    public CalciteObject getFunctionNode() {
        if (this.function == null)
            return CalciteObject.EMPTY;
        return this.getFunction().getNode();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        visitor.property("outputType");
        this.outputType.accept(visitor);
        if (this.function != null) {
            visitor.property("function");
            this.function.accept(visitor);
        }
    }

    @Override
    public boolean hasOutput(int outputNumber) {
        return outputNumber == 0;
    }

    public DBSPType outputType() {
        return this.outputType;
    }

    public OutputPort outputPort() {
        return new OutputPort(this, 0);
    }

    public DBSPSimpleOperator(CalciteRelNode node, String operation,
                              @Nullable DBSPExpression function,
                              DBSPType outputType, boolean isMultiset,
                              boolean containsIntegrator) {
        this(node, operation, function, outputType, isMultiset, null, containsIntegrator);
    }

    public DBSPSimpleOperator copyAnnotations(DBSPSimpleOperator source) {
        if (source != this)
            this.annotations.replace(source.annotations);
        return this;
    }

    /**
     * Check that the result type of function is the same as expected.
     * @param function  An expression with a function type.
     * @param expected  Type expected to be returned by the function. */
    public void checkResultType(DBSPExpression function, DBSPType expected) {
        if (function.getType().is(DBSPTypeAny.class))
            return;
        DBSPType type = function.getType().to(DBSPTypeFunction.class).resultType;
        if (!expected.sameType(type))
            throw new InternalCompilerError(this + ": Expected function to return\n" + expected +
                    " but it returns\n" + type, this);
    }

    /** Return a version of this operator with the specified parts replaced.
     * @param function    Function to use for operator.
     * @param outputType  New operator output type.
     * @param inputs      New inputs.
     * @param force       If false and all fields are the same, return original. */
    @CheckReturnValue
    public abstract DBSPOperator with(
            @Nullable DBSPExpression function,
            DBSPType outputType,
            List<OutputPort> inputs,
            boolean force);

    @CheckReturnValue
    public DBSPOperator withFunction(
            @Nullable DBSPExpression function, DBSPType outputType) {
        return this.with(function, outputType, this.inputs, false);
    }

    protected void checkParameterCount(DBSPExpression function, int expected) {
        DBSPClosureExpression closure = function.to(DBSPClosureExpression.class);
        Utilities.enforce(closure.parameters.length == expected);
    }

    public DBSPTypeIndexedZSet getOutputIndexedZSetType() {
        return this.outputPort().getOutputIndexedZSetType();
    }

    public DBSPTypeZSet getOutputZSetType() {
        return this.outputPort().getOutputZSetType();
    }

    public DBSPType getOutputZSetElementType() {
        return this.outputPort().getOutputZSetElementType();
    }

    /**
     * Check that the specified source operator produces a ZSet/IndexedZSet with element types that can be fed
     * to the specified function.
     *
     * @param function Function with a single argument.
     * @param source   Source operator producing the arg input to function.
     */
    static void checkArgumentFunctionType(DBSPExpression function, OutputPort source) {
        if (function.getType().is(DBSPTypeAny.class))
            return;
        DBSPType sourceElementType;
        DBSPTypeZSet zSet = source.outputType().as(DBSPTypeZSet.class);
        DBSPTypeIndexedZSet iZSet = source.outputType().as(DBSPTypeIndexedZSet.class);
        if (zSet != null) {
            sourceElementType = zSet.elementType.ref();
        } else if (iZSet != null) {
            sourceElementType = iZSet.getKVRefType();
        } else {
            throw new InternalCompilerError(
                    "Source " + source + " does not produce an (Indexed)ZSet, but "
                    + source);
        }
        DBSPTypeFunction funcType = function.getType().to(DBSPTypeFunction.class);
        DBSPType argType = funcType.parameterTypes[0];
        if (argType.is(DBSPTypeAny.class))
            return;
        if (!sourceElementType.sameType(argType))
            throw new InternalCompilerError("Expected function to accept\n" + sourceElementType +
                    " as argument, but it expects\n" + funcType.parameterTypes[0]);
    }

    @Override
    public DBSPType getType() {
        return this.outputType;
    }

    public DBSPExpression getFunction() {
        return Objects.requireNonNull(this.function);
    }

    public DBSPClosureExpression getClosureFunction() {
        return this.getFunction().to(DBSPClosureExpression.class);
    }

    /** Return a version of this operator with the inputs replaced.
     * @param newInputs  Inputs to use instead of the old ones.
     * @param force      If true always return a new operator.
     *                   If false and the inputs are the same this may return this.
     */
    @Override
    public final DBSPOperator withInputs(List<OutputPort> newInputs, boolean force) {
        return this.with(this.function, this.outputType, newInputs, force);
    }

    protected boolean mustReplace(boolean force, @Nullable DBSPExpression function, List<OutputPort> inputs, DBSPType outputType) {
        return force ||
                function != this.function ||
                this.inputsDiffer(inputs) ||
                !this.outputType.sameType(outputType);
    }

    public String getOutputName() {
        return this.getNodeName(false);
    }

    @Override
    public String toString() {
        return this.getClass()
                .getSimpleName()
                .replace("DBSP", "")
                .replace("Operator", "")
                + " " + this.getIdString()
                + (this.comment != null ? this.comment : "");
    }

    public SourcePositionRange getSourcePosition() {
        return this.getNode().getPositionRange();
    }

    IIndentStream writeComments(IIndentStream builder, @Nullable String comment) {
        if (comment == null)
            return builder;
        String[] parts = comment.split("\n");
        parts = Linq.map(parts, p -> "// " + p, String.class);
        return builder.intercalate("\n", parts);
    }

    IIndentStream writeComments(IIndentStream builder) {
        return this.writeComments(builder,
                this.getClass().getSimpleName() + " " + this.getIdString() +
                (this.comment != null ? "\n" + this.comment : ""));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        this.writeComments(builder)
                .append("let ")
                .append(this.getNodeName(false))
                .append(" = ");
        if (!this.inputs.isEmpty())
            builder.append(this.inputs.get(0).getName(false))
                    .append(".");
        builder.append(this.operation)
                .append("(");
        for (int i = 1; i < this.inputs.size(); i++) {
            if (i > 1)
                builder.append(",");
            builder.append("&")
                    .append(this.inputs.get(i).getName(false));
        }
        if (this.function != null) {
            if (this.inputs.size() > 1)
                builder.append(", ");
            builder.append(this.function);
        }
        return builder.append(");");
    }

    /** True if this is equivalent with the other operator,
     * which means that common-subexpression elimination can replace this with 'other'.
     * This implies that all inputs are the same, and the computed functions are the same. */
    @Override
    public boolean equivalent(DBSPOperator other) {
        // Default implementation
        DBSPSimpleOperator simple = other.as(DBSPSimpleOperator.class);
        if (simple == null)
            return false;
        if (!this.operation.equals(simple.operation))
            return false;
        if (!this.sameInputs(simple))
            return false;
        return EquivalenceContext.equiv(this.function, simple.function);
    }

    @Override
    public DBSPType outputType(int outputNo) {
        Utilities.enforce(outputNo == 0);
        return this.outputType;
    }

    @Override
    public boolean isMultiset(int outputNo) {
        Utilities.enforce(outputNo == 0);
        return this.isMultiset;
    }

    @Override
    public int outputCount() {
        return 1;
    }

    record CommonInfo(
            @Nullable DBSPExpression function,
            DBSPType outputType,
            boolean isMultiset,
            List<OutputPort> inputs,
            Annotations annotations) {
        public DBSPExpression getFunction() {
            return Objects.requireNonNull(this.function);
        }

        public DBSPClosureExpression getClosureFunction() {
            return this.getFunction().to(DBSPClosureExpression.class);
        }

        public DBSPTypeZSet getZsetType() {
            return this.outputType.to(DBSPTypeZSet.class);
        }

        public DBSPTypeIndexedZSet getIndexedZsetType() {
            return this.outputType.to(DBSPTypeIndexedZSet.class);
        }

        public OutputPort getInput(int index) {
            return this.inputs.get(index);
        }
    }

    static CommonInfo commonInfoFromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType outputType = fromJsonInner(node, "outputType", decoder, DBSPType.class);
        DBSPExpression function = null;
        if (node.has("function"))
            function = fromJsonInner(node, "function", decoder, DBSPExpression.class);
        boolean isMultiset = Utilities.getBooleanProperty(node, "isMultiset");
        List<OutputPort> inputs = Linq.list(Linq.map(
                Utilities.getProperty(node, "inputs").elements(),
                e -> OutputPort.fromJson(e, decoder)));
        Annotations annotations = Annotations.fromJson(Utilities.getProperty(node, "annotations"));
        return new CommonInfo(function, outputType, isMultiset, inputs, annotations);
    }
}
