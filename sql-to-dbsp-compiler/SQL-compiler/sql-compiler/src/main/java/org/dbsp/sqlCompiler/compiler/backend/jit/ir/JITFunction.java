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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBlock;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITScalarType;
import org.dbsp.util.IIndentStream;

import java.util.List;

public class JITFunction extends JITNode {
    public final List<JITParameter> parameters;
    public final List<JITBlock> blocks;
    public final JITScalarType resultType;

    public JITFunction(List<JITParameter> parameters, List<JITBlock> blocks,
                       JITScalarType resultType) {
        this.parameters = parameters;
        this.blocks = blocks;
        this.resultType = resultType;
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ArrayNode params = result.putArray("args");
        for (JITParameter param: this.parameters) {
            params.add(param.asJson());
        }
        result.set("ret", this.resultType.asJson());
        result.put("entry_block", this.blocks.get(0).getId());
        ObjectNode blocks = result.putObject("blocks");
        for (JITBlock block: this.blocks) {
            blocks.set(Long.toString(block.id), block.asJson());
        }
        return result;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(")
                .joinI(", ", this.parameters)
                .append(")")
                .newline()
                .joinI(System.lineSeparator(), this.blocks);
    }
}
