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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.IJITId;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITReference;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstructionRef;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITType;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class JITBlock extends JITNode implements IJITId {
    final List<JITBlockParameter> parameters;
    final List<JITInstruction> instructions;
    /** Terminator should never be null, but it is set later. */
    @Nullable
    JITBlockTerminator terminator;
    public final long id;

    public JITBlock(long id) {
        this.id = id;
        this.instructions = new ArrayList<>();
        this.parameters = new ArrayList<>();
        this.terminator = null;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return "Block " + this.id;
    }

    @Override
    public JITReference getReference() {
        return this.getBlockReference();
    }

    public JITBlockReference getBlockReference() { return new JITBlockReference(this.id); }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("id", this.id);
        ArrayNode body = result.putArray("body");
        for (JITInstruction i: this.instructions) {
            body.add(i.asJson());
        }
        if (this.terminator == null)
            throw new InternalCompilerError("Block without terminator", this);
        result.set("terminator", this.terminator.asJson());
        ArrayNode params = result.putArray("params");
        for (JITBlockParameter param: this.parameters) {
            params.add(param.asJson());
        }
        return result;
    }

    public JITInstructionRef add(JITInstruction instruction) {
        // If an equivalent instruction already exists, return that one.
        // This is a quadratic cost, if it becomes too expensive we can curtail it for large blocks.
        if (this.terminator != null)
            throw new InternalCompilerError("Block already terminated while adding instruction ", instruction);
        for (JITInstruction existing: this.instructions) {
            // It's probably best to compare in this direction
            if (instruction.same(existing))
                return existing.getInstructionReference();
        }
        this.instructions.add(instruction);
        return instruction.getInstructionReference();
    }

    public JITInstructionRef addParameter(JITReference instruction, JITType type) {
        this.parameters.add(new JITBlockParameter(instruction, type));
        return new JITInstructionRef(instruction.id);
    }

    public void terminate(JITBlockTerminator terminator) {
        if (this.terminator != null)
            throw new InternalCompilerError("Block already terminated", this.terminator);
        this.terminator = terminator;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("block ")
                .append(this.getId())
                .append("(")
                .joinI(", ", this.parameters)
                .append(")")
                .increase()
                .intercalateI(System.lineSeparator(), this.instructions);
        if (this.terminator != null)
            // Terminator may be null while debugging
            builder.append(this.terminator);
        return builder.decrease();
    }

    public JITBlockDestination createDestination() {
        return this.getBlockReference().createDestination();
    }
}
