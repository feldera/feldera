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
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.IIndentStream;

public abstract class DBSPTypeBaseType extends DBSPType {
    protected DBSPTypeBaseType(CalciteObject node, DBSPTypeCode code, boolean mayBeNull) {
        super(node, code, mayBeNull);
    }

    public String shortName() {
        return this.code.shortName;
    }

    public String getRustString() {
        return this.code.rustName;
    }

    @Override
    public DBSPClosureExpression caster(DBSPType to, boolean safe) {
        DBSPVariablePath var = this.var();
        return var.applyCloneIfNeeded().cast(this.getNode(), to, safe).closure(var);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.shortName())
                .append(this.mayBeNull ? "?" : "");
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (!this.sameNullability(other)) return false;
        DBSPTypeBaseType type = other.as(DBSPTypeBaseType.class);
        if (type == null) return false;
        return this.code == type.code;
    }

    @Override
    public int getToplevelFieldCount() {
        return 1;
    }
}
