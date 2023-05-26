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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITBoolType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITScalarType;
import org.dbsp.util.IIndentStream;

/**
 * An instruction that returns a constant value.
 */
public class JITConstantInstruction extends JITInstruction {
    public final JITScalarType type;
    public final JITLiteral value;
    /**
     * Are we returning the value of the literal or its "isNull" flag?
     * 'True' means we are returning the value.
     */
    public final boolean valueOrNull;

    public JITConstantInstruction(long id, JITScalarType type, JITLiteral literal, boolean valueOrNull) {
        super(id, "Constant");
        this.type = type;
        this.value = literal;
        this.valueOrNull = valueOrNull;
    }

    @Override
    protected BaseJsonNode instructionAsJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        if (this.valueOrNull) {
            result.set(this.type.toString(), this.value.getValueAsJson());
        } else {
            result.put(JITBoolType.INSTANCE.toString(), this.value.isNull());
        }
        return result;
    }

    @Override
    public String toString() {
        return "Constant " + this.value;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return super.toString(builder)
                .append(" ")
                .append(this.value);
    }
}
