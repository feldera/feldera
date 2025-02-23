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

package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/** For now only support simple enums, with no additional arguments. */
@NonCoreIR
public final class DBSPEnumValue extends DBSPExpression {
    public final String enumName;
    public final String constructor;

    public DBSPEnumValue(String enumName, String constructor) {
        super(CalciteObject.EMPTY, new DBSPTypeUser(CalciteObject.EMPTY, USER, enumName, false));
        this.enumName = enumName;
        this.constructor = constructor;
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
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPEnumValue o = other.as(DBSPEnumValue.class);
        if (o == null)
            return false;
        return this.enumName.equals(o.enumName) &&
                this.constructor.equals(o.constructor);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.enumName)
                .append("::")
                .append(this.constructor);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPEnumValue(this.enumName, this.constructor);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPEnumValue otherExpression = other.as(DBSPEnumValue.class);
        if (otherExpression == null)
            return false;
        return this.enumName.equals(otherExpression.enumName) &&
                this.constructor.equals(otherExpression.constructor);
    }

    @SuppressWarnings("unused")
    public static DBSPEnumValue fromJson(JsonNode node, JsonDecoder decoder) {
        String enumName = Utilities.getStringProperty(node, "enumName");
        String constructor = Utilities.getStringProperty(node, "constructor");
        return new DBSPEnumValue(enumName, constructor);
    }
}
