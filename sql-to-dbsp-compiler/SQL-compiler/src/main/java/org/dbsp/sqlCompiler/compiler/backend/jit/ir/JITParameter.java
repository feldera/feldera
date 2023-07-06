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

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.util.IIndentStream;

public class JITParameter extends JITReference {
    public enum Direction {
        IN("input"),
        OUT("output"),
        INOUT("inout");

        public final String direction;

        Direction(String direction) {
            this.direction = direction;
        }

        @Override
        public String toString() {
            return this.direction;
        }
    }

    public final String originalName;  // not used in code generation.
    public final Direction direction;
    public final JITRowType type;
    public final boolean mayBeNull;    // not used in code generation
    /**
     * If true this is a field of a flattened Tuple type.
     */
    public final boolean flattenedType;

    public JITParameter(long id, String name, Direction direction, JITRowType type,
                        boolean mayBeNull, boolean flattenedType) {
        super(id);
        this.direction = direction;
        this.originalName = name;
        this.type = type;
        this.mayBeNull = mayBeNull;
        this.flattenedType = flattenedType;
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("id", this.id);
        result.put("layout", this.type.getId());
        result.put("flags", this.direction.toString());
        return result;
    }

    @Override
    public String toString() {
        return this.originalName + " " + this.id + " " +
                this.direction + ":" + this.type + (this.flattenedType ? " (flattened)" : "");
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.toString());
    }
}
