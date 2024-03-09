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

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.ZSET;

public class DBSPTypeZSet extends DBSPTypeUser implements ICollectionType {
    public final DBSPType elementType;

    public DBSPTypeZSet(CalciteObject node, DBSPType elementType) {
        super(node, ZSET, "WSet", false, elementType);
        this.elementType = elementType;
        assert !elementType.is(DBSPTypeZSet.class);
        assert !elementType.is(DBSPTypeIndexedZSet.class);
    }

    public DBSPTypeZSet(DBSPType elementType) {
        this(elementType.getNode(), elementType);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.elementType.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPType getElementType() {
        return this.elementType;
    }

    // sameType and hashCode inherited from TypeUser
}
