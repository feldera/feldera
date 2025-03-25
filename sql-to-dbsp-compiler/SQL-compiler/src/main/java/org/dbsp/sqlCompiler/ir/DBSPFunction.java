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

package org.dbsp.sqlCompiler.ir;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** A (Rust) function.  If the body is null, this is just a function declaration. */
@NonCoreIR
public final class DBSPFunction extends DBSPNode
        implements IHasType, IDBSPDeclaration, IDBSPInnerNode {
    public final String name;
    public final List<DBSPParameter> parameters;
    public final DBSPType returnType;
    @Nullable
    public final DBSPExpression body;
    public final List<String> annotations;
    public final DBSPTypeFunction type;

    public DBSPFunction(String name, List<DBSPParameter> parameters,
                        DBSPType returnType, @Nullable DBSPExpression body,
                        List<String> annotations) {
        super(CalciteObject.EMPTY);
        this.name = name;
        this.parameters = parameters;
        this.returnType = returnType;
        this.body = body;
        this.annotations = annotations;
        DBSPType[] argTypes = new DBSPType[parameters.size()];
        for (int i = 0; i < argTypes.length; i++)
            argTypes[i] = parameters.get(i).getType();
        this.type = new DBSPTypeFunction(returnType, argTypes);
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("returnType");
        this.returnType.accept(visitor);
        visitor.startArrayProperty("parameters");
        int index = 0;
        for (DBSPParameter argument: this.parameters) {
            visitor.propertyIndex(index);
            index++;
            argument.accept(visitor);
        }
        visitor.endArrayProperty("parameters");
        if (this.body != null) {
            visitor.property("body");
            this.body.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPExpression getReference() {
        return new DBSPPathExpression(this.type, new DBSPPath(this.name));
    }

    public DBSPExpression call(DBSPExpression... arguments) {
        return new DBSPApplyExpression(this.getReference(), arguments);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPFunction o = other.as(DBSPFunction.class);
        if (o == null)
            return false;
        return this.name.equals(o.name) &&
            Linq.same(this.parameters, o.parameters) &&
            this.returnType == o.returnType &&
            this.body == o.body &&
            Linq.sameStrings(this.annotations, o.annotations) &&
                this.type.sameType(o.type);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("fn ")
                .append(this.name)
                .append("(")
                .joinI(", ", this.parameters)
                .append(") -> ")
                .append(this.returnType);
        if (this.body != null)
            builder
                .append(" ")
                .append(this.body);
        return builder;
    }

    @SuppressWarnings("unused")
    public static DBSPFunction fromJson(JsonNode node, JsonDecoder decoder) {
        String name = Utilities.getStringProperty(node, "name");
        List<DBSPParameter> parameters = fromJsonInnerList(node, "parameters", decoder, DBSPParameter.class);
        DBSPType returnType = fromJsonInner(node, "returnType", decoder, DBSPType.class);
        DBSPExpression body = fromJsonInner(node, "body", decoder, DBSPExpression.class);
        List<String> annotations = Linq.list(
                Linq.map(Utilities.getProperty(node, "annotations").elements(),
                JsonNode::asText));
        return new DBSPFunction(name, parameters, returnType, body, annotations);
    }
}
