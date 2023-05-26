/*
 * Copyright 2022 VMware, Inc.
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

package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.InnerVisitor;

/**
 * Represents the type of a Rust Vec as a TypeUser.
 */
@SuppressWarnings("GrazieInspection")
public class DBSPTypeVec extends DBSPTypeUser implements ICollectionType {
    public DBSPTypeVec(DBSPType vectorElementType) {
        super(null, "Vec", false, vectorElementType);
    }

    public DBSPType getElementType() {
        return this.getTypeArg(0);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        for (DBSPType type: this.typeArgs)
            type.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public boolean hasCopy() {
        return false;
    }

    // sameType and hashCode inherited from TypeUser.
}
