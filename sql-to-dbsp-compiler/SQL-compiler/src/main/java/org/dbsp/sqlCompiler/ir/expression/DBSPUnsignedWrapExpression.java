package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** An unsigned wrap expression just wraps a signed integer into an unsigned one
 * preserving order. */
public final class DBSPUnsignedWrapExpression extends DBSPExpression {
    public static class TypeSequence {
        public final DBSPType dataType;
        public final DBSPTypeInteger dataConvertedType;
        public final DBSPTypeInteger intermediateType;
        public final DBSPTypeInteger unsignedType;

        TypeSequence(DBSPType dataType) {
            this.dataType = dataType;
            this.dataConvertedType = getInitialIntegerType(this.dataType);
            if (!this.dataConvertedType.signed) {
                throw new InternalCompilerError("The data type encoding " + dataType +
                        " must be signed, but it is " + this.dataConvertedType);
            } else {
                int width = this.dataConvertedType.getWidth();
                if (width > 64)
                    // This may cause runtime overflows, but this is the best we can do
                    width = 64;
                this.intermediateType = new DBSPTypeInteger(dataType.getNode(), width * 2, true, false);
                this.unsignedType = new DBSPTypeInteger(dataType.getNode(), width * 2, false, false);
            }
        }

        static DBSPTypeInteger getResultType(DBSPType dataType) {
            TypeSequence seq = new TypeSequence(dataType);
            return seq.unsignedType;
        }

        // How each SQL type that may be used in a sorting key is converted into a signed value
        static DBSPTypeInteger getInitialIntegerType(DBSPType sourceType) {
            return switch (sourceType.code) {
                case INT8, INT16, INT32, INT64, INT128 -> sourceType.withMayBeNull(false).to(DBSPTypeInteger.class);
                case DATE -> new DBSPTypeInteger(sourceType.getNode(), 32, true, false);
                case TIMESTAMP, TIME -> new DBSPTypeInteger(sourceType.getNode(), 64, true, false);
                default -> throw new InternalCompilerError("Not yet supported wrappers for " + sourceType, sourceType.getNode());
            };
        }
    }

    public final DBSPExpression source;
    public final TypeSequence sequence;
    public final boolean nullsLast;
    public final boolean ascending;

    /** Create an expression that converts a value to an unsigned representation.
     * @param node          Calcite node
     * @param source        Expression that is being converted
     * @param ascending     True if sorting ascending
     * @param nullsLast     If true nulls compare last */
    public DBSPUnsignedWrapExpression(
            CalciteObject node, DBSPExpression source, boolean ascending, boolean nullsLast) {
        // This allocates two sequences, but I don't know how to avoid that.
        super(node, TypeSequence.getResultType(source.getType()));
        this.source = source;
        this.sequence = new TypeSequence(source.getType());
        this.nullsLast = nullsLast;
        this.ascending = ascending;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.source.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPUnsignedWrapExpression o = other.as(DBSPUnsignedWrapExpression.class);
        if (o == null)
            return false;
        return this.source == o.source
                && this.ascending == o.ascending && this.nullsLast == o.nullsLast;
    }

    public String getMethod() {
        return this.source.getType().mayBeNull ? "from_option" : "from_signed";
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("UnsignedWrapper")
                .append("::")
                .append(this.getMethod())
                .append("(")
                .append(source)
                .append(", ")
                .append(this.ascending)
                .append(", ")
                .append(this.nullsLast)
                .append(")");
    }

    public DBSPType getSourceType() {
        return this.source.getType();
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUnsignedWrapExpression(
                this.getNode(), this.source.deepCopy(), this.ascending, this.nullsLast);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPUnsignedWrapExpression otherExpression = other.as(DBSPUnsignedWrapExpression.class);
        if (otherExpression == null)
            return false;
        return this.ascending == otherExpression.ascending &&
                this.nullsLast == otherExpression.nullsLast &&
                context.equivalent(this.source, otherExpression.source);
    }

    public DBSPExpression replaceSource(DBSPExpression source) {
        return new DBSPUnsignedWrapExpression(this.getNode(), source, this.ascending, this.nullsLast);
    }

    @SuppressWarnings("unused")
    public static DBSPUnsignedWrapExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression source = fromJsonInner(node, "source", decoder, DBSPExpression.class);
        boolean ascending = Utilities.getBooleanProperty(node, "ascending");
        boolean nullsLast = Utilities.getBooleanProperty(node, "nullsLast");
        return new DBSPUnsignedWrapExpression(CalciteObject.EMPTY, source, ascending, nullsLast);
    }
}

