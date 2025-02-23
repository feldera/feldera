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

package org.dbsp.sqlCompiler.ir.pattern;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

@NonCoreIR
public final class DBSPIdentifierPattern extends DBSPPattern {
    public final String identifier;
    public final boolean mutable;

    public DBSPIdentifierPattern(String identifier, boolean mutable) {
        super(CalciteObject.EMPTY);
        this.identifier = identifier;
        this.mutable = mutable;
    }

    public DBSPIdentifierPattern(String identifier) {
        this(identifier, false);
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
        DBSPIdentifierPattern o = other.as(DBSPIdentifierPattern.class);
        if (o == null)
            return false;
        return this.identifier.equals(o.identifier) && this.mutable == o.mutable;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.mutable ? "mut " : "")
                .append(this.identifier);
    }

    @SuppressWarnings("unused")
    public static DBSPIdentifierPattern fromJson(JsonNode node, JsonDecoder decoder) {
        String identifier = Utilities.getStringProperty(node, "identifier");
        boolean mutable = Utilities.getBooleanProperty(node, "mutable");
        return new DBSPIdentifierPattern(identifier, mutable);
    }
}
