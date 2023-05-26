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

package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This corresponds to the Calcite 'SYMBOL' type,
 * which is the type of various keywords that appear in SQL expressions.
 * It should never surface in code.
 */
public class DBSPTypeKeyword extends DBSPTypeBaseType {
    public static final DBSPTypeKeyword INSTANCE =new DBSPTypeKeyword();

    protected DBSPTypeKeyword() {
        super(null, false);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        throw new UnsupportedException(this);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        throw new UnsupportedException(this);
    }

    @Override
    public String shortName() {
        throw new UnsupportedException(this);
    }

    @Override
    public DBSPLiteral defaultValue() {
        throw new UnsupportedException("Default value for type 'keyword'");
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 7);
    }

    @Override
    public boolean sameType(@Nullable DBSPType other) {
        if (!super.sameType(other))
            return false;
        assert other != null;
        return other.is(DBSPTypeKeyword.class);
    }
}
