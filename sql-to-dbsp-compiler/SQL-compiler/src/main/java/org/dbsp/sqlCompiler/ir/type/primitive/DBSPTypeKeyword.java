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

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;

import java.util.Objects;

/** This corresponds to the Calcite 'SYMBOL' type,
 * which is the type of various keywords that appear in SQL expressions.
 * It should never surface in code. */
public class DBSPTypeKeyword extends DBSPTypeBaseType {
    DBSPTypeKeyword() {
        super(CalciteObject.EMPTY, DBSPTypeCode.KEYWORD, false);
    }

    public static final DBSPTypeKeyword INSTANCE = new DBSPTypeKeyword();

    @Override
    public void accept(InnerVisitor visitor) {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public DBSPExpression defaultValue() {
        throw new UnsupportedException("Default value for type 'keyword'", this.getNode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 7);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeKeyword.class);
    }
}
