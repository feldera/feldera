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

package org.dbsp.sqlCompiler.ir.path;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

public class DBSPPath extends DBSPNode implements IDBSPInnerNode {
    public final DBSPPathSegment[] components;

    public DBSPPath(DBSPPathSegment... components) {
        super(CalciteObject.EMPTY);
        this.components = components;
    }

    public DBSPExpression toExpression() {
        return DBSPTypeAny.getDefault().path(this);
    }

    public DBSPPath(String... components) {
        this(Linq.map(components, DBSPSimplePathSegment::new, DBSPSimplePathSegment.class));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.startArrayProperty("components");
        int index = 0;
        for (DBSPPathSegment path: this.components) {
            visitor.propertyIndex(index);
            index++;
            path.accept(visitor);
        }
        visitor.endArrayProperty("components");
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPPath o = other.as(DBSPPath.class);
        if (o == null)
            return false;
        return Linq.same(this.components, o.components);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.join("::", this.components);
    }

    public boolean equivalent(DBSPPath other) {
        if (this.components.length != other.components.length)
            return false;
        for (int i = 0; i < this.components.length; i++)
            if (!this.components[i].equivalent(other.components[i]))
                return false;
        return true;
    }

    public String asString() {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (var segment: this.components) {
            if (!first)
                builder.append("::");
            first = false;
            builder.append(segment.asString());
        }
        return builder.toString();
    }
}
