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

package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.util.IIndentStream;

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

    public DBSPType getTypeArg(int index) {
        return this.typeArgs[index];
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeUser(this.getNode(), this.code, this.name, mayBeNull, this.typeArgs);
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
}
