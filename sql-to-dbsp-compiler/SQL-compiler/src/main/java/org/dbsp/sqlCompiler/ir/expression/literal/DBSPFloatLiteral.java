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

import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeFloat;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPFloatLiteral extends DBSPFPLiteral {
    @Nullable
    public final Float value;

    public DBSPFloatLiteral(@Nullable Object node, DBSPType type, @Nullable Float value, boolean raw) {
        super(node, type, value, raw);
        this.value = value;
    }

    @SuppressWarnings("unused")
    public DBSPFloatLiteral() {
        this(null, true);
    }

    public DBSPFloatLiteral(float value) {
        this(value, false);
    }

    public DBSPFloatLiteral(@Nullable Float f, boolean nullable) {
        this(f, nullable, false);
    }

    protected DBSPFloatLiteral(@Nullable Float f, boolean nullable, boolean raw) {
        this(null, DBSPTypeFloat.INSTANCE.setMayBeNull(nullable), f, raw);
        if (f == null && !nullable)
            throw new RuntimeException("Null value with non-nullable type");
    }

    public DBSPFloatLiteral raw() {
        return new DBSPFloatLiteral(this.value, this.getType().mayBeNull, true);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return new DBSPFloatLiteral(Objects.requireNonNull(this.value), false, this.raw);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPFloatLiteral that = (DBSPFloatLiteral) o;
        return Objects.equals(value, that.value);
    }
}
