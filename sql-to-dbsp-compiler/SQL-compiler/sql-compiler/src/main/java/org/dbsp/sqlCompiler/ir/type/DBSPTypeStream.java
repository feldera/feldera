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

import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.util.IIndentStream;

import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.STREAM;

/**
 * A type of the form 'Stream<_, elementType>'
 */
public class DBSPTypeStream extends DBSPType {
    public final DBSPType elementType;

    public DBSPTypeStream(DBSPType elementType) {
        super(elementType.getNode(), STREAM, elementType.mayBeNull);
        this.elementType = elementType;
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        this.elementType.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.elementType.hashCode());
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        DBSPTypeStream oRef = other.as(DBSPTypeStream.class);
        if (oRef == null)
            return false;
        return this.elementType.sameType(oRef.elementType);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("stream<")
                .append(this.elementType)
                .append(">");
    }
}
