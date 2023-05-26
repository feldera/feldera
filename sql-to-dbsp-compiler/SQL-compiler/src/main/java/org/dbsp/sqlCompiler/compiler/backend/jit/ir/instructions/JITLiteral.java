/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITScalarType;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFloatLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Unimplemented;

public class JITLiteral extends JITValue {
    public final DBSPLiteral literal;
    public final JITScalarType type;

    public JITLiteral(DBSPLiteral literal, JITScalarType type) {
        this.literal = literal;
        this.type = type;
    }

    public boolean mayBeNull() {
        return this.literal.getNonVoidType().mayBeNull;
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ObjectNode value;
        if (this.mayBeNull()) {
            if (this.literal.isNull) {
                result.set("Nullable", NullNode.getInstance());
                return result;
            } else {
                value = result.putObject("Nullable");
            }
        } else {
            value = result.putObject("NonNull");
        }
        value.set(this.type.toString(), this.getValueAsJson());
        return result;
    }

    @SuppressWarnings("DataFlowIssue")
    public BaseJsonNode getValueAsJson() {
        boolean isNull = this.literal.isNull;
        if (this.literal.is(DBSPI32Literal.class)) {
            return isNull ? new IntNode(0) : new IntNode(this.literal.to(DBSPI32Literal.class).value);
        } else if (this.literal.is(DBSPI64Literal.class)) {
            return isNull ? new LongNode(0) : new LongNode(this.literal.to(DBSPI64Literal.class).value);
        } else if (this.literal.is(DBSPStringLiteral.class)) {
            return isNull ? new TextNode("") : new TextNode(this.literal.to(DBSPStringLiteral.class).value);
        } else if (this.literal.is(DBSPBoolLiteral.class)) {
            return isNull ? BooleanNode.valueOf(false) : BooleanNode.valueOf(this.literal.to(DBSPBoolLiteral.class).value);
        } else if (this.literal.is(DBSPDoubleLiteral.class)) {
            return isNull ? new DoubleNode(0.0) : new DoubleNode(this.literal.to(DBSPDoubleLiteral.class).value);
        } else if (this.literal.is(DBSPFloatLiteral.class)) {
            return isNull ? new FloatNode(0.0F) : new FloatNode(this.literal.to(DBSPFloatLiteral.class).value);
        } else {
            throw new Unimplemented(this.literal);
        }
    }

    public boolean isNull() {
        return this.literal.isNull;
    }

    @Override
    public String toString() {
        return this.literal.toString();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.literal.toString());
    }
}
