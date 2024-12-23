package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.ToIndentableString;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public final class DBSPZSetExpression extends DBSPExpression
        implements IDBSPContainer, ToIndentableString, ISameValue {
    public final Map<DBSPExpression, Long> data;
    public final DBSPType elementType;

    static DBSPType getType(DBSPExpression... data) {
        if (data.length == 0)
            throw new RuntimeException("This constructor cannot be used to build an empty set;" +
                    " you need to specify the type");
        return data[0].getType();
    }

    public boolean isConstant() {
        return Linq.all(this.data.keySet(), DBSPExpression::isConstantLiteral);
    }

    /**
     * Create a ZSet literal from a set of data values.
     *
     * @param data Data to insert in zset - cannot be empty, since
     *             it is used to extract the zset type.
     *             To create empty zsets use the constructor
     *             with just a type argument.
     */
    public DBSPZSetExpression(DBSPExpression... data) {
        super(CalciteObject.EMPTY, new DBSPTypeZSet(getType(data)));
        this.elementType = data[0].getType();
        this.data = new HashMap<>();
        for (DBSPExpression e : data) {
            if (!e.getType().sameType(data[0].getType()))
                throw new RuntimeException("Cannot add value " + e +
                        "\nNot all values of set have the same type:" +
                        e.getType() + " vs " + data[0].getType());
            this.add(e);
        }
    }

    public DBSPZSetExpression(Map<DBSPExpression, Long> data, DBSPType elementType) {
        super(CalciteObject.EMPTY, new DBSPTypeZSet(elementType));
        this.data = data;
        this.elementType = elementType;
    }

    /** Creates an empty zset with the specified element type. */
    public DBSPZSetExpression(DBSPType elementType) {
        super(CalciteObject.EMPTY, new DBSPTypeZSet(elementType));
        this.elementType = elementType;
        this.data = new HashMap<>();
    }

    /**
     * Creates an empty zset with the specified type.
     */
    public static DBSPZSetExpression emptyWithElementType(DBSPType elementType) {
        return new DBSPZSetExpression(elementType);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public DBSPZSetExpression clone() {
        return new DBSPZSetExpression(new HashMap<>(this.data), this.elementType);
    }

    public DBSPType getElementType() {
        return this.elementType;
    }

    public DBSPZSetExpression add(DBSPExpression expression) {
        return this.add(expression, 1);
    }

    public DBSPZSetExpression map(Function<DBSPExpression, DBSPExpression> map, DBSPType elementType) {
        DBSPZSetExpression result = DBSPZSetExpression.emptyWithElementType(elementType);
        for (Map.Entry<DBSPExpression, Long> entry : this.data.entrySet()) {
            DBSPExpression converted = map.apply(entry.getKey());
            result.add(converted, entry.getValue());
        }
        return result;
    }

    public DBSPZSetExpression add(DBSPExpression expression, long weight) {
        // We expect the expression to be a constant value (a literal)
        if (expression.getType().code != this.getElementType().code)
            throw new InternalCompilerError("Added element type " +
                    expression.getType() + " does not match zset type " + this.getElementType(), expression);
        if (this.data.containsKey(expression)) {
            long oldWeight = this.data.get(expression);
            long newWeight = weight + oldWeight;
            if (newWeight == 0)
                this.data.remove(expression);
            else
                this.data.put(expression, weight + oldWeight);
            return this;
        }
        this.data.put(expression, weight);
        return this;
    }

    public DBSPZSetExpression add(DBSPZSetExpression other) {
        if (!this.elementType.sameType(other.elementType))
            throw new InternalCompilerError("Added zsets do not have the same type " +
                    this.getElementType() + " vs " + other.getElementType(), this.elementType);
        other.data.forEach(this::add);
        return this;
    }

    public DBSPZSetExpression addUsingCast(DBSPZSetExpression other) {
        other.data.forEach(this::addUsingCast);
        return this;
    }

    public DBSPExpression castRecursive(DBSPExpression expression, DBSPType type) {
        if (type.is(DBSPTypeBaseType.class)) {
            return expression.cast(type);
        } else if (type.is(DBSPTypeVec.class)) {
            DBSPTypeVec vec = type.to(DBSPTypeVec.class);
            DBSPVecExpression vecLit = expression.to(DBSPVecExpression.class);
            if (vecLit.data == null) {
                return new DBSPVecExpression(type, type.mayBeNull);
            }
            List<DBSPExpression> fields = Linq.map(vecLit.data, e -> castRecursive(e, vec.getElementType()));
            return new DBSPVecExpression(expression.getNode(), type, fields);
        } else if (type.is(DBSPTypeTupleBase.class)) {
            DBSPTypeTupleBase tuple = this.elementType.to(DBSPTypeTupleBase.class);
            DBSPExpression[] fields = new DBSPExpression[tuple.size()];
            for (int i = 0; i < tuple.size(); i++) {
                fields[i] = this.castRecursive(expression.field(i).simplify(), tuple.tupFields[i]);
            }
            return tuple.makeTuple(fields);
        } else {
            throw new InternalCompilerError("Casting expressions of type " + type);
        }
    }

    public void addUsingCast(DBSPExpression row, Long weight) {
        DBSPExpression toAdd = this.castRecursive(row, this.elementType);
        this.add(toAdd, weight);
    }

    public DBSPZSetExpression negate() {
        DBSPZSetExpression result = DBSPZSetExpression.emptyWithElementType(this.elementType);
        for (Map.Entry<DBSPExpression, Long> entry : data.entrySet()) {
            result.add(entry.getKey(), -entry.getValue());
        }
        return result;
    }

    public int size() {
        return this.data.size();
    }

    public DBSPZSetExpression minus(DBSPZSetExpression sub) {
        DBSPZSetExpression result = this.clone();
        result.add(sub.negate());
        return result;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        for (DBSPExpression expr: this.data.keySet())
            expr.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPZSetExpression otherZset = other.as(DBSPZSetExpression.class);
        if (otherZset == null)
            return false;
        if (this.size() != otherZset.size())
            return false;
        for (var entry: this.data.entrySet()) {
            DBSPExpression expr = entry.getKey();
            if (!otherZset.data.containsKey(expr))
                return false;
            if (!Objects.equals(entry.getValue(), otherZset.data.get(expr)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return this.data.hashCode();
    }

    public boolean isEmpty() {
        return this.data.isEmpty();
    }

    public DBSPZSetExpression deepCopy() {
        Map<DBSPExpression, Long> newData = new HashMap<>();
        for (Map.Entry<DBSPExpression, Long> d : this.data.entrySet()) {
            newData.put(d.getKey().deepCopy(), d.getValue());
        }
        return new DBSPZSetExpression(newData, this.elementType);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        throw new UnimplementedException();
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPZSetExpression that = (DBSPZSetExpression) o;
        if (!this.type.sameType(that.type)) return false;
        return this.minus(that).size() == 0;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("zset!(");
        boolean first = true;
        for (Map.Entry<DBSPExpression, Long> e : this.data.entrySet()) {
            if (!first)
                builder.newline();
            first = false;
            builder.append(e.getKey());
            builder.append(" => ")
                    .append(e.getValue())
                    .append(",");
        }
        return builder.append(")");
    }
}