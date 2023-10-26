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

public class JITLoadInstruction extends JITInstruction {
    public final JITInstructionRef source;
    public final JITRowType sourceType;
    public final int column;
    public final JITScalarType resultType;

    public JITLoadInstruction(long id, JITInstructionRef source, JITRowType sourceType,
                              int column, JITScalarType resultType, String comment) {
        super(id, "Load", comment);
        this.source = source;
        this.sourceType = sourceType;
        this.column = column;
        this.resultType = resultType;
    }

    @Override
    public BaseJsonNode instructionAsJson() {
        // "Load": {
        //   "source": 1,
        //   "source_layout": 2,
        //   "column": 1,
        //   "column_type": "I32"
        // }
        ObjectNode load = jsonFactory().createObjectNode();
        load.put("source", this.source.getId());
        load.put("source_layout", this.sourceType.getId());
        load.put("column", this.column);
        load.put("column_type", resultType.toString());
        return load;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return super.toString(builder)
                .append(" ")
                .append(this.source)
                .append("[")
                .append(this.column)
                .append("]");
    }

    @Override
    public boolean same(JITInstruction other) {
        JITLoadInstruction ol = other.as(JITLoadInstruction.class);
        if (ol == null)
            return false;
        return this.source.equals(ol.source) &&
                this.column == ol.column &&
                this.sourceType.sameType(ol.sourceType);
    }
}
