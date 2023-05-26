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

public class DBSPTypeFunction extends DBSPType {
    @Nullable
    public final DBSPType resultType;
    public final DBSPType[] argumentTypes;

    public DBSPTypeFunction(@Nullable DBSPType resultType, DBSPType... argumentTypes) {
        super(false);
        this.resultType = resultType;
        this.argumentTypes = argumentTypes;
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.resultType != null)
            this.resultType.accept(visitor);
        for (DBSPType arg: this.argumentTypes)
            arg.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), resultType);
        result = 31 * result + Arrays.hashCode(argumentTypes);
        return result;
    }

    @Override
    public boolean sameType(@Nullable DBSPType type) {
        if (!super.sameType(type))
            return false;
        assert type != null;
        if (!type.is(DBSPTypeFunction.class))
            return false;
        DBSPTypeFunction other = type.to(DBSPTypeFunction.class);
        if (!DBSPType.sameType(this.resultType, other.resultType))
            return false;
        return DBSPType.sameTypes(this.argumentTypes, other.argumentTypes);
    }
}
