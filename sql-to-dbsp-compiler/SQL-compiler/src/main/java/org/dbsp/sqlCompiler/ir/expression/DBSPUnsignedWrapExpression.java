package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IIndentStream;

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
                this.intermediateType = this.dataConvertedType;
                this.unsignedType = this.dataConvertedType;
            } else {
                int width = this.dataConvertedType.getWidth();
                this.intermediateType = new DBSPTypeInteger(dataType.getNode(), width * 2, true, false);
                this.unsignedType = new DBSPTypeInteger(dataType.getNode(), width * 2, false, false);
            }
        }

        static DBSPTypeInteger getResultType(DBSPType dataType) {
            TypeSequence seq = new TypeSequence(dataType);
            return seq.unsignedType;
        }

        static DBSPTypeInteger getInitialIntegerType(DBSPType sourceType) {
            return switch (sourceType.code) {
                case INT8, INT16, INT32, INT64 -> sourceType.setMayBeNull(false).to(DBSPTypeInteger.class);
                case DATE -> new DBSPTypeInteger(sourceType.getNode(), 32, true, false);
                case TIMESTAMP -> new DBSPTypeInteger(sourceType.getNode(), 64, true, false);
                case TIME -> new DBSPTypeInteger(sourceType.getNode(), 64, false, false);
                default -> throw new UnimplementedException(sourceType.getNode());
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
        this.source.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
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
}
