package org.dbsp.sqlCompiler.circuit;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;

/** An output port of an operator. */
public class OutputPort {
    public final DBSPOperator operator;
    public final int outputNumber;

    /** Create an OutputPort.
     *
     * @param operator  Operator whose port is represented.
     * @param port      Output number.
     */
    public OutputPort(DBSPOperator operator, int port) {
        this.operator = operator;
        this.outputNumber = port;
    }

    public OutputPort(Port<DBSPOperator> dest) {
        this(dest.node(), dest.port());
    }

    @Nullable
    public static OutputPort fromJson(JsonNode node, JsonDecoder decoder) {
        if (node.isNull())
            return null;
        int outputNumber = Utilities.getIntProperty(node, "outputNumber");
        DBSPOperator operator = DBSPNode.fromJsonOuter(node, "operator", decoder, DBSPOperator.class);
        return new OutputPort(operator, outputNumber);
    }

    public DBSPOperator node() {
        return this.operator;
    }

    public int port() {
        return this.outputNumber;
    }

    public String getNodeName(boolean preferHash) {
        return this.node().getNodeName(preferHash);
    }

    public String getOutputName() {
        if (this.isSimpleNode())
            return this.getNodeName(false);
        return this.getNodeName(false) + "." + this.outputNumber;
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
        return this.node().isMultiset(this.outputNumber);
    }

    public DBSPType outputType() {
        return this.node().outputType(this.outputNumber);
    }

    public DBSPSimpleOperator simpleNode() {
        return this.node().to(DBSPSimpleOperator.class);
    }

    public <T extends DBSPSimpleOperator> T to(Class<T> clazz) {
        return this.node().to(clazz);
    }

    @Override
    public String toString() {
        if (this.node().is(DBSPSimpleOperator.class))
            return this.node().toString();
        return this.node().id + ":" + this.port();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OutputPort that = (OutputPort) o;
        return outputNumber == that.outputNumber && operator.equals(that.operator);
    }

    @Override
    public int hashCode() {
        int result = operator.hashCode();
        result = 31 * result + outputNumber;
        return result;
    }

    public boolean isSimpleNode() {
        return this.node().is(DBSPSimpleOperator.class);
    }

    public void asJson(ToJsonOuterVisitor visitor) {
        visitor.stream.beginObject()
                .label("outputNumber").append(this.outputNumber)
                .label("operator");
        this.operator.accept(visitor);
        visitor.stream.endObject();
    }

    public String asJson() {
        return "{ \"node\": " + this.operator.id + ", \"output\": " + this.outputNumber + " }";
    }
}
