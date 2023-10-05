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
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITType;
import org.dbsp.util.IIndentStream;

public class JITBinaryInstruction extends JITInstruction {
    public enum Operation {
        ADD("Add"),
        SUB("Sub"),
        MUL("Mul"),
        DIV("Div"),
        EQ("Eq"),
        NEQ("Neq"),
        LT("LessThan"),
        GT("GreaterThan"),
        LTE("LessThanOrEqual"),
        GTE("GreaterThanOrEqual"),
        AND("And"),
        OR("Or"),
        XOR("Xor"),
        MAX("Max"),
        MIN("Min")
        ;

        private final String text;

        Operation(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return this.text;
        }
    }

    public final Operation operation;
    public final JITInstructionRef left;
    public final JITInstructionRef right;
    public final JITType type;

    public JITBinaryInstruction(long id, Operation operation,
                                JITInstructionRef left,
                                JITInstructionRef right,
                                JITType type, String comment) {
        super(id, "BinOp", comment);
        this.operation = operation;
        this.right = right;
        this.left = left;
        this.type = type;
    }

    @Override
    public BaseJsonNode instructionAsJson() {
        // "BinOp": {
        //   "lhs": 4,
        //   "rhs": 5,
        //   "operand_ty": "I64",
        //   "kind": "GreaterThan"
        // }
        ObjectNode result = JITNode.jsonFactory().createObjectNode();
        result.put("lhs", this.left.getId());
        result.put("rhs", this.right.getId());
        result.put("kind", this.operation.text);
        result.put("operand_ty", type.toString());
        return result;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return this.commentToString(builder)
                .append(this.id)
                .append(" ")
                .append(this.left)
                .append(" ")
                .append(this.operation.text)
                .append(" ")
                .append(this.right);
    }

    @Override
    public boolean same(JITInstruction other) {
        JITBinaryInstruction ob = other.as(JITBinaryInstruction.class);
        if (ob == null)
            return false;
        return this.left.equals(ob.left) &&
                this.right.equals(ob.right) &&
                this.operation.equals(ob.operation);
    }
}
