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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstructionRef;
import org.dbsp.util.IIndentStream;

public class JITReturnTerminator extends JITBlockTerminator {
    public final JITInstructionRef retVal;

    public JITReturnTerminator(JITInstructionRef retVal) {
        this.retVal = retVal;
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ObjectNode ret = result.putObject("Return");
        ObjectNode retValue = ret.putObject("value");
        if (this.retVal.isValid()) {
            retValue.put("Expr", this.retVal.getId());
        } else {
            retValue.put("Imm", "Unit");
        }
        return result;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("return ")
                .append(this.retVal);
    }
}
