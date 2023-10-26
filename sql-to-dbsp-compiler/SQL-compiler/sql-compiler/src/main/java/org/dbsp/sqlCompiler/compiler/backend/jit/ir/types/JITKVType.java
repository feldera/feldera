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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;

/**
 * A pair type with two fields: a key type and a value type.
 */
@SuppressWarnings("GrazieInspection")
public class JITKVType extends JITType implements IJitKvOrRowType {
    final JITRowType key;
    final JITRowType value;

    public JITKVType(JITRowType key, JITRowType value) {
        super(DBSPTypeCode.KV);
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean isScalarType() {
        return false;
    }

    @Override
    public BaseJsonNode asJsonReference() {
        throw new UnsupportedException(CalciteObject.EMPTY);
    }

    @Override
    public boolean sameType(JITType other) {
        JITKVType ok = other.as(JITKVType.class);
        if (ok == null)
            return false;
        return this.key.sameType(ok.key) &&
                this.value.sameType(ok.value);
    }

    @Override
    public void addDescriptionTo(ObjectNode parent, String label) {
        ObjectNode map = parent.putObject(label);
        ArrayNode array = map.putArray("Map");
        array.add(this.key.getId());
        array.add(this.value.getId());
    }
}
