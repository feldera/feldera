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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.*;

/**
 * Base class for all JIT IR classes.
 */
public abstract class JITNode implements ICastable, ToIndentableString {
    public BaseJsonNode asJson() {
        throw new UnimplementedException("Should be overridden in all subclasses");
    }

    private static final ObjectMapper topMapper = new ObjectMapper();

    public static ObjectMapper jsonFactory() {
        return topMapper;
    }

    /**
     * Compact textual representation of the JIT element.
     * @param builder Build the representation here.
     */
    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder;
    }

    /**
     * Convert the program into an assembly-like output.
     */
    public String toAssembly() {
        StringPrintStream str = new StringPrintStream();
        IndentStream stream = new IndentStream(str.getPrintStream());
        stream.append(this);
        return str.toString();
    }
}
