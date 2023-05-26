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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.IJITId;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITReference;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.StringPrintStream;

public abstract class JITInstruction extends JITNode implements IJITId {
    public final long id;
    public final String name;
    public final String comment;

    public JITInstruction(long id, String name, String comment) {
        this.id = id;
        this.name = name;
        this.comment = comment;
    }

    public JITInstruction(long id, String name) {
        this(id, name, "");
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public JITReference getReference() {
        return this.getInstructionReference();
    }

    @Override
    public BaseJsonNode asJson() {
        ArrayNode result = jsonFactory().createArrayNode();
        result.add(this.id);
        ObjectNode description = result.addObject();
        description.set(this.name, this.instructionAsJson());
        return result;
    }

    protected abstract BaseJsonNode instructionAsJson();

    public JITInstructionRef getInstructionReference() {
        return new JITInstructionRef(this.id);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (!this.comment.isEmpty())
            builder.append("# ").append(this.comment).newline();
        return builder.append(this.id)
                .append(" ")
                .append(this.name);
    }

    @Override
    public String toString() {
        StringPrintStream sps = new StringPrintStream();
        IndentStream str = new IndentStream(sps.getPrintStream());
        this.toString(str);
        return sps.toString();
    }
}
