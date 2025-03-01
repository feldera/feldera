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
 *
 *
 */

package org.dbsp.util;

import java.io.IOException;

public class IndentStream implements IIndentStream {
    private Appendable stream;
    int indent = 0;
    int amount = 4;
    boolean emitIndent = false;

    public IndentStream(Appendable appendable) {
        this.stream = appendable;
    }

    /** Set the output stream.
     * @return The previous output stream. */
    public Appendable setOutputStream(Appendable appendable) {
        Appendable result = this.stream;
        this.stream = appendable;
        return result;
    }

    /** Set the indent amount.  If less or equal to 0, newline will have no effect. */
    public IIndentStream setIndentAmount(int amount) {
        assert amount >= 0;
        this.amount = amount;
        return this;
    }

    @Override
    public IIndentStream appendChar(char c) {
        try {
            if (c == '\n') {
                if (this.amount > 0) {
                    this.stream.append(c);
                    this.emitIndent = true;
                }
                return this;
            }
            if (this.emitIndent && !Character.isSpaceChar(c)) {
                this.emitIndent = false;
                for (int in = 0; in < this.indent; in++)
                    this.stream.append(' ');
            }
            this.stream.append(c);
            return this;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public IIndentStream increase() {
        this.indent += Math.max(this.amount, 0);
        return this.newline();
    }

    @Override
    public IIndentStream decrease() {
        this.indent -= Math.max(this.amount, 0);
        if (this.indent < 0)
            throw new RuntimeException("Negative indent");
        return this;
    }

    @Override
    public String toString() {
        return this.stream.toString();
    }
}
