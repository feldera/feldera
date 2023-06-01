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

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPIntervalMillisLiteral extends DBSPLiteral {
    @Nullable public final Long value;

    public DBSPIntervalMillisLiteral() {
        this(null, DBSPTypeMillisInterval.NULLABLE_INSTANCE, null);
    }

    public DBSPIntervalMillisLiteral(@Nullable Object node, DBSPType type, @Nullable Long value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPIntervalMillisLiteral(long value, boolean mayBeNull) {
        this(null, DBSPTypeMillisInterval.INSTANCE.setMayBeNull(mayBeNull), value);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPIntervalMillisLiteral that = (DBSPIntervalMillisLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return new DBSPIntervalMillisLiteral(this.getNode(),
                this.getNonVoidType().setMayBeNull(false), Objects.requireNonNull(this.value));
    }
}
