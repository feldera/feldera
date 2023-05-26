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
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeStr;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPStrLiteral extends DBSPLiteral {
    @Nullable
    public final String value;
    public final boolean raw;

    public DBSPStrLiteral(@Nullable Object node, DBSPType type, @Nullable String value, boolean raw) {
        super(node, type, value);
        this.value = value;
        this.raw = raw;
    }

    public DBSPStrLiteral() {
        this(null, true, false);
    }

    public DBSPStrLiteral(String value) {
        this(value, false, false);
    }

    public DBSPStrLiteral(@Nullable String value, boolean nullable, boolean raw) {
        this(null, DBSPTypeStr.INSTANCE.setMayBeNull(nullable), value, raw);
        if (value == null && !nullable)
            throw new RuntimeException("Null value with non-nullable type");
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return new DBSPStrLiteral(Objects.requireNonNull(this.value), false, this.raw);
    }
}
