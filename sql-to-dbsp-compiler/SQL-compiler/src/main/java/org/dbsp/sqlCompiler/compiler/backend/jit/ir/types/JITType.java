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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir.types;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.IIndentStream;

public abstract class JITType extends JITNode {
    public final DBSPTypeCode code;

    protected JITType(DBSPTypeCode code) {
        this.code = code;
    }

    public abstract boolean isScalarType();

    public abstract BaseJsonNode asJsonReference();

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.toString());
    }

    /**
     * True if these two types are equivalent: same fields, same types.
     */
    public abstract boolean sameType(JITType other);
}
