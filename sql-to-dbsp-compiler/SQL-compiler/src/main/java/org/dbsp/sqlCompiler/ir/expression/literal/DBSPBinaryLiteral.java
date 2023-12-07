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

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/** A byte array literal */
public class DBSPBinaryLiteral extends DBSPLiteral {
    @Nullable
    public final byte[] value;

    @SuppressWarnings("unused")
    public DBSPBinaryLiteral() {
        this(null);
    }

    public DBSPBinaryLiteral(@Nullable byte[] value) {
        this(CalciteObject.EMPTY, new DBSPTypeBinary(CalciteObject.EMPTY, value == null), value);
    }

    public DBSPBinaryLiteral(@Nullable byte[] value, boolean mayBeNull) {
        this(CalciteObject.EMPTY, new DBSPTypeBinary(CalciteObject.EMPTY, mayBeNull), value);
    }

    public DBSPBinaryLiteral(CalciteObject node, DBSPType type, @Nullable byte[] value) {
        super(node, type, value == null);
        this.value = value;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPBinaryLiteral(this.getNode(), this.type, this.value);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPBinaryLiteral that = (DBSPBinaryLiteral) o;
        return Arrays.equals(value, that.value);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPBinaryLiteral(this.checkIfNull(this.value, mayBeNull), mayBeNull);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")null");
        else
            return builder.append(Arrays.toString(Objects.requireNonNull(this.value)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(this.value));
    }
}
