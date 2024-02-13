/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.IDBSPContainer;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.ToIndentableString;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Represents a (constant) ZSet described by its elements.
 * A ZSet is a map from tuples to integer weights.
 * In general weights should not be zero.
 * TODO: check for weight overflow?
 */
public class DBSPZSetLiteral extends DBSPLiteral implements IDBSPContainer {
    /**
     * The contents of a ZSet - everything except the Weight type.
     */
    public static class Contents implements ToIndentableString {
        public final Map<DBSPExpression, Long> data;
        public final DBSPType elementType;
        /**
         * Create a ZSet literal from a set of data values.
         * @param data Data to insert in zset - cannot be empty, since
         *             it is used to extract the zset type.
         *             To create empty zsets use the constructor
         *             with just a type argument.
         */
        public Contents(DBSPExpression... data) {
            if (data.length == 0)
                throw new RuntimeException("This constructor cannot be used to build an empty set; you need to specify the type");
            // value 0 is not used
            this.elementType = data[0].getType();
            this.data = new HashMap<>();
            for (DBSPExpression e: data) {
                if (!e.getType().sameType(data[0].getType()))
                    throw new RuntimeException("Cannot add value " + e +
                            "\nNot all values of set have the same type:" +
                            e.getType() + " vs " + data[0].getType());
                this.add(e);
            }
        }

        public Contents(Map<DBSPExpression, Long> data, DBSPType elementType) {
            this.data = data;
            this.elementType = elementType;
        }

        /**
         * Creates an empty zset with the specified element type.
         */
        Contents(DBSPType elementType) {
            this.elementType = elementType;
            this.data = new HashMap<>();
        }

        /**
         * Creates an empty zset with the specified type.
         */
        public static Contents emptyWithElementType(DBSPType elementType) {
            return new Contents(elementType);
        }

        @SuppressWarnings("MethodDoesntCallSuperMethod")
        public Contents clone() {
            return new Contents(new HashMap<>(this.data), this.elementType);
        }

        public DBSPType getElementType() {
            return this.elementType;
        }

        public Contents add(DBSPExpression expression) {
            return this.add(expression, 1);
        }

        public Contents map(Function<DBSPExpression, DBSPExpression> map, DBSPType elementType) {
            Contents result = Contents.emptyWithElementType(elementType);
            for (Map.Entry<DBSPExpression, Long> entry: this.data.entrySet()) {
                DBSPExpression converted = map.apply(entry.getKey());
                result.add(converted, entry.getValue());
            }
            return result;
        }

        public Contents add(DBSPExpression expression, long weight) {
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

        public Contents add(Contents other) {
            if (!this.elementType.sameType(other.elementType))
                throw new InternalCompilerError("Added zsets do not have the same type " +
                        this.getElementType() + " vs " + other.getElementType(), this.elementType);
            other.data.forEach(this::add);
            return this;
        }

        public Contents negate() {
            Contents result = Contents.emptyWithElementType(this.elementType);
            for (Map.Entry<DBSPExpression, Long> entry: data.entrySet()) {
                result.add(entry.getKey(), -entry.getValue());
            }
            return result;
        }

        public int size() {
            return this.data.size();
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{ ");
            boolean first = true;
            for (Map.Entry<DBSPExpression, Long> e: data.entrySet()) {
                if (!first)
                    builder.append(", ");
                first = false;
                builder.append(e.getKey())
                        .append(" => ")
                        .append(e.getValue());
            }
            builder.append("}");
            return builder.toString();
        }

        public Contents minus(Contents sub) {
            Contents result = this.clone();
            result.add(sub.negate());
            return result;
        }

        @Override
        public int hashCode() {
            return this.data.hashCode();
        }

        public boolean sameValue(DBSPZSetLiteral.Contents other) {
            return this.minus(other).size() == 0;
        }

        @Override
        public IIndentStream toString(IIndentStream builder) {
            boolean first = true;
            for (Map.Entry<DBSPExpression, Long> e: this.data.entrySet()) {
                if (!first)
                    builder.newline();
                first = false;
                builder.append(e.getKey());
                builder.append(" => ")
                        .append(e.getValue())
                        .append(",");
            }
            return builder;
        }

        public boolean isEmpty() {
            return this.data.isEmpty();
        }

        public Contents deepCopy() {
            Map<DBSPExpression, Long> newData = new HashMap<>();
            for (Map.Entry<DBSPExpression, Long> d: this.data.entrySet()) {
                newData.put(d.getKey().deepCopy(), d.getValue());
            }
            return new Contents(newData, this.elementType);
        }
    }

    public final DBSPTypeZSet zsetType;
    public final Contents data;

    public DBSPZSetLiteral(CalciteObject node, DBSPType type, Contents contents) {
        super(node, type, false);
        this.data = contents;
        this.zsetType = this.getType().to(DBSPTypeZSet.class);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPZSetLiteral(this.getNode(), this.type, this.data.deepCopy());
    }

    public DBSPZSetLiteral(DBSPType zsetType) {
        this(zsetType.getNode(), zsetType, new Contents(zsetType.to(DBSPTypeZSet.class).elementType));
    }

    public DBSPZSetLiteral(DBSPType weightType, Contents contents) {
        this(CalciteObject.EMPTY, TypeCompiler.makeZSet(contents.elementType, weightType), contents);
    }

    public DBSPZSetLiteral(DBSPType weightType, DBSPExpression... data) {
        this(CalciteObject.EMPTY, TypeCompiler.makeZSet(data[0].getType(), weightType), new Contents(data));
    }

    public DBSPZSetLiteral(DBSPType elementType, DBSPType weightType) {
        this(CalciteObject.EMPTY, TypeCompiler.makeZSet(elementType, weightType), new Contents(elementType));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        for (DBSPExpression expr: this.data.data.keySet())
            expr.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    public Contents getContents() {
        return this.data;
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        if (mayBeNull)
            throw new InternalCompilerError("Nullable zset");
        return this;
    }

    @Override
    public void add(DBSPExpression expression) {
        this.data.add(expression);
    }

    public void add(DBSPExpression expression, long weight) {
        this.data.add(expression, weight);
    }

    public int size() {
        return this.data.size();
    }

    public DBSPType getElementType() {
        return this.zsetType.elementType;
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPZSetLiteral that = (DBSPZSetLiteral) o;
        if (!this.zsetType.sameType(that.zsetType)) return false;
        return this.data.sameValue(that.data);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("zset!(")
                .append(this.data)
                .append(")");
    }
}
