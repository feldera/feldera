package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;

import java.util.List;

/** These operators have *two* outputs: a regular stream output
 * and an error output */
public abstract class DBSPOperatorWithError extends DBSPOperator {
    public final String operation;
    protected final DBSPType outputType;
    protected final DBSPType errorType;
    public final DBSPClosureExpression function;
    public final DBSPClosureExpression error;
    public final DBSPTypeStream outputStreamType;
    public final DBSPTypeStream errorStreamType;

    protected DBSPOperatorWithError(CalciteRelNode node, String operation, DBSPType outputType, DBSPType errorType,
                                    DBSPClosureExpression function, DBSPClosureExpression error) {
        super(node);
        this.operation = operation;
        this.outputType = outputType;
        this.errorType = errorType;
        this.function = function;
        this.error = error;
        this.outputStreamType = new DBSPTypeStream(this.outputType);
        this.errorStreamType = new DBSPTypeStream(this.errorType);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        visitor.property("errorType");
        this.errorType.accept(visitor);
        visitor.property("error");
        this.error.accept(visitor);
        visitor.property("outputType");
        this.outputType.accept(visitor);
        visitor.property("function");
        this.function.accept(visitor);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!other.is(DBSPOperatorWithError.class))
            return false;
        DBSPOperatorWithError oe = other.to(DBSPOperatorWithError.class);
        if (!this.sameInputs(oe) ||
            !this.outputType.sameType(oe.outputType) &&
                    !this.errorType.sameType(oe.errorType))
            return false;
        return EquivalenceContext.equiv(this.function, oe.function) &&
                EquivalenceContext.equiv(this.error, oe.error);
    }

    public DBSPOperatorWithError copyAnnotations(DBSPOperator source) {
        if (source != this)
            this.annotations.replace(source.annotations);
        return this;
    }

    @Override
    public DBSPType outputType(int outputNo) {
        if (outputNo == 0)
            return this.outputType;
        else if (outputNo == 1)
            return this.errorType;
        throw new InternalCompilerError("No output " + outputNo);
    }

    @Override
    public boolean isMultiset(int outputNumber) {
        return true;
    }

    @Override
    public String getOutputName(int outputNumber) {
        String name = this.getNodeName();
        return name + "_" + outputNumber;
    }

    @Override
    public int outputCount() {
        return 2;
    }

    @Override
    public DBSPType streamType(int outputNumber) {
        if (outputNumber == 0)
            return this.outputStreamType;
        else if (outputNumber == 1)
            return this.errorStreamType;
        throw new InternalCompilerError("No output " + outputNumber);
    }
    
    public abstract DBSPOperatorWithError withInputs(List<OutputPort> sources, boolean force);
}
