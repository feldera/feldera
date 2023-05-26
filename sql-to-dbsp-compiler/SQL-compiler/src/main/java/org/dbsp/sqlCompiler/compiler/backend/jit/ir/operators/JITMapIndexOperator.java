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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITFunction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITKVType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;

import java.util.List;

public class JITMapIndexOperator extends JITOperator {
    public final JITRowType inputType;
    public final JITKVType kvType;

    public JITMapIndexOperator(long id, JITKVType kvType, JITRowType inputType,
                               List<JITOperatorReference> inputs, JITFunction function) {
        // A MapIndex operator compiles to a Map representation that
        // returns elements with type "Map" instead of "Set"
        super(id, "Map", "map_fn", kvType, inputs, function, null);
        this.inputType = inputType;
        this.kvType = kvType;
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = (ObjectNode)super.asJson();
        this.kvType.addDescriptionTo(result, "output_layout");
        this.inputType.addDescriptionTo(result, "input_layout");
        return result;
    }
}
