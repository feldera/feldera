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

import org.dbsp.sqlCompiler.ir.InnerVisitor;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/**
 * User-defined generic type with type arguments.
 */
public class DBSPTypeUser extends DBSPType {
    public final String name;
    public final DBSPType[] typeArgs;

    public DBSPTypeUser(@Nullable Object node, String name, boolean mayBeNull, DBSPType... typeArgs) {
        super(node, mayBeNull);
        this.name = name;
        this.typeArgs = typeArgs;
    }

    public DBSPType getTypeArg(int index) {
        return this.typeArgs[index];
    }

    public String getName() {
        return this.name;
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeUser(this.getNode(), this.name, mayBeNull, this.typeArgs);
    }

    @Override
    public boolean sameType(@Nullable DBSPType type) {
        if (!super.sameType(type))
            return false;
        assert type != null;
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
        if (!visitor.preorder(this)) return;
        visitor.push(this);
        for (DBSPType type: this.typeArgs)
            type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }
}
