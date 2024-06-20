package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Represents a (constant) map described by its elements. */
public final class DBSPMapLiteral extends DBSPLiteral {
    // Both lists must have the same length
    @Nullable
    public final List<DBSPExpression> keys;
    @Nullable
    public final List<DBSPExpression> values;
    public final DBSPTypeMap mapType;

    public DBSPMapLiteral(DBSPType keyType, DBSPType valueType, boolean mayBeNull) {
        super(CalciteObject.EMPTY, new DBSPTypeMap(keyType, valueType, mayBeNull), false);
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        this.mapType = this.getType().to(DBSPTypeMap.class);
    }

    public static List<DBSPExpression> getKeys(List<DBSPExpression> data) {
        ArrayList<DBSPExpression> result = new ArrayList<>();
        for (int i = 0; i < data.size(); i += 2)
            result.add(data.get(i));
        return result;
    }

    public static List<DBSPExpression> getValues(List<DBSPExpression> data) {
        ArrayList<DBSPExpression> result = new ArrayList<>();
        for (int i = 1; i < data.size(); i += 2)
            result.add(data.get(i));
        return result;
    }

    public DBSPMapLiteral(DBSPTypeMap mapType, List<DBSPExpression> data) {
        this(mapType, getKeys(data), getValues(data));
    }

    public DBSPMapLiteral(DBSPTypeMap mapType, @Nullable List<DBSPExpression> keys, @Nullable List<DBSPExpression> values) {
        super(CalciteObject.EMPTY, mapType, mapType.mayBeNull);
        this.mapType = this.getType().to(DBSPTypeMap.class);
        if (keys == null) {
            this.keys = null;
        } else {
            this.keys = new ArrayList<>();
            assert values != null;
            assert keys.size() == values.size();
            for (DBSPExpression e : keys) {
                if (!e.getType().sameType(this.getKeyType()))
                    throw new InternalCompilerError("Not all keys of map have the same type:" +
                            e.getType() + " vs " + this.getKeyType(), this);
                this.keys.add(e);
            }
        }
        if (values == null) {
            this.values = null;
        } else {
            this.values = new ArrayList<>();
            for (DBSPExpression e : values) {
                if (!e.getType().sameType(this.getValueType()))
                    throw new InternalCompilerError("Not all values of map have the same type:" +
                            e.getType() + " vs " + this.getValueType(), this);
                this.values.add(e);
            }
        }
    }

    public DBSPType getKeyType() {
        return this.mapType.getKeyType();
    }

    public DBSPType getValueType() {
        return this.mapType.getValueType();
    }

    public int size() {
        return Objects.requireNonNull(this.keys).size();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        if (this.keys != null) {
            assert this.values != null;
            for (int i = 0; i < this.keys.size(); i++) {
                DBSPExpression key = this.keys.get(i);
                key.accept(visitor);
                DBSPExpression value = this.values.get(i);
                value.accept(visitor);
            }
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPMapLiteral(this.getType().setMayBeNull(mayBeNull).to(DBSPTypeMap.class),
                this.keys, this.values);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPMapLiteral that = (DBSPMapLiteral) o;
        if (!Objects.equals(this.keys, that.keys)) return false;
        if (!Objects.equals(this.values, that.values)) return false;
        return mapType.equals(that.mapType);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.keys == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")")
                    .append("null");
        builder.append("BTreeMap::from([")
                .increase();
        assert this.values != null;
        for (int i = 0; i < keys.size(); i++) {
            builder.append(this.keys.get(i))
                    .append(", ")
                    .append(this.values.get(i));
        }
        return builder.append("])")
                .decrease()
                .newline();
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPMapLiteral(this.mapType,
                this.keys != null ? Linq.map(this.keys, DBSPExpression::deepCopy) : null,
                this.values != null ? Linq.map(this.values, DBSPExpression::deepCopy) : null);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPMapLiteral otherExpression = other.as(DBSPMapLiteral.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.keys, otherExpression.keys) &&
                context.equivalent(this.values, otherExpression.values);
    }
}
