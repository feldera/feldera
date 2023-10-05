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
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.util.IIndentStream;

public class JITSetNullInstruction extends JITInstruction {
    public final JITInstructionRef target;
    public final JITRowType targetType;
    public final int column;
    public final JITInstructionRef source;

    public JITSetNullInstruction(long id, JITInstructionRef target, JITRowType targetType,
                                 int column, JITInstructionRef source) {
        super(id, "SetNull");
        this.target = target;
        this.targetType = targetType;
        this.column = column;
        this.source = source;
    }

    @Override
    protected BaseJsonNode instructionAsJson() {
        // "SetNull": {
        //  "target": 2,
        //  "target_layout": 3,
        //  "column": 0,
        //  "is_null": {
        //    "Expr": 4 }}
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("target", this.target.getId());
        result.put("target_layout", this.targetType.getId());
        result.put("column", this.column);
        ObjectNode source = result.putObject("is_null");
        source.put("Expr", this.source.getId());
        return result;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return super.toString(builder)
                .append(" ")
                .append(this.target)
                .append("[")
                .append(this.column)
                .append("]=")
                .append(this.source);
    }

    @Override
    public boolean same(JITInstruction other) {
        // Two instructions that setnull are never considered to be equivalent.
        return false;
    }
}
