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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;

public class DBSPSimplePathSegment extends DBSPPathSegment {
    public final String identifier;
    public final DBSPType[] genericArgs;

    public DBSPSimplePathSegment(String identifier, DBSPType... genericArgs) {
        super(CalciteObject.EMPTY);
        this.identifier = identifier;
        this.genericArgs = genericArgs;
        for (DBSPType type: genericArgs)
            assert type != null;
    }

    public String asString() {
        return this.identifier;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.startArrayProperty("genericArgs");
        int index = 0;
        for (DBSPType arg : this.genericArgs) {
            visitor.propertyIndex(index);
            index++;
            arg.accept(visitor);
        }
        visitor.endArrayProperty("genericArgs");
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPSimplePathSegment o = other.as(DBSPSimplePathSegment.class);
        if (o == null)
            return false;
        return this.identifier.equals(o.identifier) &&
                Linq.same(this.genericArgs, o.genericArgs);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append(this.identifier);
        if (this.genericArgs.length > 0)
            builder.append("<")
                    .join(", ", this.genericArgs)
                    .append(">");
        return builder;
    }

    @Override
    public boolean equivalent(DBSPPathSegment other) {
        DBSPSimplePathSegment otherSegment = other.as(DBSPSimplePathSegment.class);
        if (otherSegment == null)
            return false;
        if (!this.identifier.equals(otherSegment.identifier))
            return false;
        if (this.genericArgs.length != otherSegment.genericArgs.length)
            return false;
        for (int i = 0; i < this.genericArgs.length; i++)
            if (!this.genericArgs[i].sameType(otherSegment.genericArgs[i]))
                return false;
        return true;
    }

    @SuppressWarnings("unused")
    public static DBSPSimplePathSegment fromJson(JsonNode node, JsonDecoder decoder) {
        String identifier = Utilities.getStringProperty(node, "identifier");
        List<DBSPType> genericArgs = fromJsonInnerList(node, "genericArgs", decoder, DBSPType.class);
        return new DBSPSimplePathSegment(identifier, genericArgs.toArray(new DBSPType[0]));
    }
}
