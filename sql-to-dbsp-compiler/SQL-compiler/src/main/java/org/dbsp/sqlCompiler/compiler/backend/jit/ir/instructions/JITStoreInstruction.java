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
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITScalarType;
import org.dbsp.util.IIndentStream;

public class JITStoreInstruction extends JITInstruction {
    public final JITInstructionRef target;
    public final JITRowType targetType;
    public final int column;
    public final JITInstructionRef source;
    public final JITScalarType valueType;

    public JITStoreInstruction(long id, JITInstructionRef target, JITRowType targetType,
                               int column, JITInstructionRef source, JITScalarType valueType) {
        super(id, "Store");
        this.target = target;
        this.targetType = targetType;
        this.column = column;
        this.source = source;
        this.valueType = valueType;
    }

    @Override
    protected BaseJsonNode instructionAsJson() {
        // "Store": {
        //   "target": 2,
        //   "target_layout": 3,
        //   "column": 0,
        //   "value": {
        //     "Expr": 3
        //   },
        //   "value_type": "I32"
        // }
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("target", this.target.getId());
        result.put("target_layout", this.targetType.getId());
        result.put("column", this.column);
        ObjectNode value = result.putObject("value");
        value.put("Expr", this.source.getId());
        result.put("value_type", this.valueType.toString());
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
}
