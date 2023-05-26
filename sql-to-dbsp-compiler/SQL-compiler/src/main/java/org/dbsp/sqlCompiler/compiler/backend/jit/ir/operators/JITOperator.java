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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.IJITId;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITFunction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITReference;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.IJitKvOrRowType;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.List;

public abstract class JITOperator extends JITNode implements IJITId {
    public final long id;
    public final String name;
    public final String functionName;
    public final IJitKvOrRowType type;
    public final List<JITOperatorReference> inputs;
    @Nullable
    public final JITFunction function;
    @Nullable
    public final String comment;

    protected JITOperator(long id, String name, String functionName,
                          IJitKvOrRowType type, List<JITOperatorReference> inputs,
                          @Nullable
                          JITFunction function,
                          @Nullable String comment) {
        this.id = id;
        this.name = name;
        this.functionName = functionName;
        this.type = type;
        this.inputs = inputs;
        this.function = function;
        this.comment = comment;
    }

    @Override
    public long getId() {
        return this.id;
    }

    // Indexed with the number of inputs of the operator.
    static final String[][] OPERATOR_INPUT_NAMES = new String[][] {
            {},
            { "input" },
            { "lhs", "rhs" },
    };

    @Override
    public JITReference getReference() {
        return new JITReference(this.id);
    }

    void addInputs(ObjectNode node) {
        String[] names = OPERATOR_INPUT_NAMES[this.inputs.size()];
        int index = 0;
        for (JITOperatorReference sources: this.inputs) {
            String name = names[index++];
            node.put(name, sources.getId());
        }
    }

    void addComment(ObjectNode node, @Nullable String comment) {
        if (comment != null)
            node.put("comment", comment);
    }

    /**
     * Standard serialization of this operator as JSON.
     * Cannot be overloaded, unlike asJson below.
     */
    private BaseJsonNode stdAsJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ObjectNode data = result.putObject(this.name);
        this.addInputs(data);
        if (this.function != null) {
            data.set(this.functionName, function.asJson());
        }
        this.addComment(data, this.comment);
        return result;
    }

    /**
     * Implementation of asJson which also adds the object layout information.
     */
    public BaseJsonNode asJsonWithLayout() {
        BaseJsonNode result = this.stdAsJson();
        ObjectNode inner = this.getInnerObject(result);
        this.type.addDescriptionTo(inner, "layout");
        return result;
    }

    @Override
    public BaseJsonNode asJson() {
        return this.stdAsJson();
    }

    /**
     * Given the result returned by asJson, reach within
     * it and return the field with name "Name", called
     * "data" in the above function.
     */
    ObjectNode getInnerObject(BaseJsonNode node) {
        if (!(node instanceof ObjectNode))
            throw new RuntimeException("Expected an Object");
        JsonNode result = node.get(this.name);
        if (!(result instanceof ObjectNode))
            throw new RuntimeException("Expected the field " + this.name + " to be an Object");
        return (ObjectNode)result;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append(this.getId())
                .append(" ")
                .append(this.name)
                .append("(")
                .joinI(", ", this.inputs)
                .append(")");
        if (this.function != null) {
            builder.increase()
                    .append(this.function)
                    .decrease();
        }
        return builder;
    }
}
