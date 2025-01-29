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

package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.Arrays;
import java.util.Objects;

/** User-defined generic type with type arguments. */
public class DBSPTypeUser extends DBSPType {
    public final String name;
    public final DBSPType[] typeArgs;

    public DBSPTypeUser(CalciteObject node, DBSPTypeCode code, String name, boolean mayBeNull, DBSPType... typeArgs) {
        super(node, code, mayBeNull);
        this.name = name;
        this.typeArgs = typeArgs;
    }

    @Override
    public boolean hasCopy() {
        return false;
    }

    public DBSPType getTypeArg(int index) {
        return this.typeArgs[index];
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeUser(this.getNode(), this.code, this.name, mayBeNull, this.typeArgs);
    }

    @Override
    public DBSPExpression defaultValue() {
        throw new UnimplementedException();
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (!this.sameNullability(other)) return false;
        DBSPTypeUser type = other.as(DBSPTypeUser.class);
        if (type == null) return false;
        return this.name.equals(type.name) &&
                Linq.same(this.typeArgs, type.typeArgs);
    }

    @Override
    public int getToplevelFieldCount() {
        return 1;
    }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        if (!type.is(DBSPTypeUser.class))
            return false;
        DBSPTypeUser other = type.to(DBSPTypeUser.class);
        if (!this.name.equals(other.name))
            return false;
        return DBSPType.sameTypes(this.typeArgs, other.typeArgs);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), name);
        result = Objects.hash(result, Arrays.hashCode(typeArgs));
        return result;
    }

    // Do not forget to override this function in subclasses, even
    // if the implementation is identical.
    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        for (DBSPType type: this.typeArgs)
            type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append(this.name);
        if (this.typeArgs.length > 0) {
            builder.append("<")
                    .join(", ", this.typeArgs)
                    .append(">");
        }
        if (this.mayBeNull)
            builder.append("?");
        return builder;
    }

    /** Generate a constructor for this type with the signature:
     * (Self)::method(arguments)
     * @param method     Method to invoke on this type
     * @param arguments  Arguments to pass to constructor
     * @return           An expression which invokes the constructor
     */
    public DBSPExpression constructor(String method, DBSPExpression... arguments) {
        return new DBSPConstructorExpression(
                new DBSPPathExpression(this, new DBSPPath(this.name, method)),
                this,
                arguments);
    }

    /** Generate a constructor for this type with the signature:
     * (Self)::new(arguments)
     * @param arguments  Arguments to pass to constructor
     * @return           An expression which invokes the constructor
     */
    public DBSPExpression constructor(DBSPExpression... arguments) {
        return this.constructor("new", arguments);
    }
}
