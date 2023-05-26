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

import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;

import javax.annotation.Nullable;

/**
 * We represent each scalar value as a pair of ids:
 * - the id of the instruction producing the actual value
 * - the id of the instruction producing the Boolean is_null flag.
 *   The latter is invalid if the value is not nullable.
 *   Notice that the two instructions do not have to be in the same basic block.
 */
public class JITInstructionPair extends JITNode {
    public final JITInstructionRef value;
    public final JITInstructionRef isNull;

    public JITInstructionPair(JITInstructionRef value, JITInstructionRef isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public JITInstructionPair(JITInstructionRef value) {
        this(value, new JITInstructionRef());
    }

    public JITInstructionPair(JITInstruction value, @Nullable JITInstruction isNull) {
        this(value.getInstructionReference(),
                isNull == null ? new JITInstructionRef() : isNull.getInstructionReference());
    }

    public JITInstructionPair(JITInstruction value) {
        this(value.getInstructionReference(), new JITInstructionRef());
    }

    @Override
    public String toString() {
        return this.value + ":" + this.isNull;
    }

    public boolean hasNull() {
        return this.isNull.isValid();
    }
}
