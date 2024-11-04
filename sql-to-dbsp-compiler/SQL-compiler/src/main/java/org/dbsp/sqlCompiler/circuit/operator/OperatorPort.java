package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.graph.Port;

import java.util.Objects;

public class OperatorPort extends Port<DBSPOperator> {
    public OperatorPort(DBSPOperator operator, int port) {
        super(operator, port);
    }

    public OperatorPort(Port<DBSPOperator> dest) {
        super(dest.node(), dest.port());
    }

    public String getOutputName() {
        return this.node().to(DBSPSimpleOperator.class).getOutputName();
    }

    public DBSPTypeZSet getOutputZSetType() { return this.outputType().to(DBSPTypeZSet.class); }

    public DBSPTypeIndexedZSet getOutputIndexedZSetType() {
        return this.outputType().to(DBSPTypeIndexedZSet.class);
    }

    public DBSPType getOutputZSetElementType() {
        return this.getOutputZSetType().elementType;
    }

    /** If the output is a ZSet it returns the element type.
     * If the output is an IndexedZSet it returns the tuple (keyType, elementType).
     * If the output is something else, it returns its type.
     * (The last can happen for apply nodes, after insertion of limiters). */
    public DBSPType getOutputRowType() {
        if (this.outputType().is(DBSPTypeZSet.class))
            return this.getOutputZSetElementType();
        if (this.outputType().is(DBSPTypeIndexedZSet.class))
            return this.getOutputIndexedZSetType().getKVType();
        return this.outputType();
    }

    public boolean isMultiset() {
        return this.node().to(DBSPSimpleOperator.class).isMultiset;
    }

    public DBSPType outputType() {
        return this.node().to(DBSPSimpleOperator.class).outputType;
    }

    public DBSPSimpleOperator simpleNode() {
        return this.node().to(DBSPSimpleOperator.class);
    }

    @Override
    public String toString() {
        if (this.node().is(DBSPSimpleOperator.class))
            return this.node().toString();
        return this.node() + ":" + this.port();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof OperatorPort op))
            return false;
        return this.node() == op.node() && this.port() == op.port();
    }

    // HashCode inherited from base class
}
