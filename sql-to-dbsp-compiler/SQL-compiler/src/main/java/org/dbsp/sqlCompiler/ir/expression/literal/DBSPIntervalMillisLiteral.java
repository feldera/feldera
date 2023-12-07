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
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPIntervalMillisLiteral extends DBSPLiteral {
    @Nullable public final Long value;

    public DBSPIntervalMillisLiteral() {
        this(CalciteObject.EMPTY, new DBSPTypeMillisInterval(CalciteObject.EMPTY, true), null);
    }

    public DBSPIntervalMillisLiteral(CalciteObject node, DBSPType type, @Nullable Long value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPIntervalMillisLiteral(long value, boolean mayBeNull) {
        this(CalciteObject.EMPTY, new DBSPTypeMillisInterval(CalciteObject.EMPTY, mayBeNull), value);
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
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPIntervalMillisLiteral that = (DBSPIntervalMillisLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPIntervalMillisLiteral(this.getNode(),
                this.getType().setMayBeNull(mayBeNull), this.checkIfNull(this.value, mayBeNull));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("(")
                .append(this.type)
                .append(")");
        if (this.value != null)
            return builder.append(this.value.toString());
        return builder.append("null");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPIntervalMillisLiteral(this.getNode(), this.type, this.value);
    }
}
