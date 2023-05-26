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

package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

public class DBSPCircuit extends DBSPNode implements IDBSPOuterNode {
    public final DBSPPartialCircuit circuit;
    public final String name;

    public DBSPCircuit(DBSPPartialCircuit circuit, String name) {
        super(null);
        this.circuit = circuit;
        this.name = name;
    }

    @Nullable
    @Override
    public Object getNode() {
        return this.circuit.getNode();
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (!visitor.preorder(this)) return;
        this.circuit.accept(visitor);
        visitor.postorder(this);
    }

    public int getOutputCount() {
        return this.circuit.getOutputCount();
    }

    public DBSPType getOutputType(int i) {
        return this.circuit.getOutputType(i);
    }

    public List<String> getInputTables() {
        return this.circuit.getInputTables();
    }

    public DBSPCircuit rename(String name) {
        return new DBSPCircuit(this.circuit, name);
    }
}
