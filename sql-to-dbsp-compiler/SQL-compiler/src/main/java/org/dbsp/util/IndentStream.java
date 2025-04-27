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
import java.util.HashMap;
import java.util.Map;

public class IndentStream implements IIndentStream {
    private Appendable stream;
    int spaces = 0;
    int amount = 4;
    String indentAdd = "";
    String indentString = "";
    Map<Integer, String> indentStringCache = new HashMap<>();
    boolean emitIndent = false;

    public IndentStream(Appendable appendable) {
        this.stream = appendable;
        this.setIndentAmount(this.amount);
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
        Utilities.enforce(amount >= 0);
        this.amount = amount;
        this.indentAdd = " ".repeat(amount);
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
                this.appendFast(this.indentString);
            }
            this.stream.append(c);
            return this;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** Append a string that does not contain newlines */
    @Override
    public IIndentStream appendFast(String s) {
        try {
            if (s.isEmpty())
                return this;
            if (this.emitIndent) {
                this.emitIndent = false;
                this.stream.append(this.indentString);
            }
            this.stream.append(s);
            return this;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public IIndentStream increase() {
        this.spaces += Math.max(this.amount, 0);
        if (this.indentStringCache.containsKey(this.spaces)) {
            this.indentString = this.indentStringCache.get(this.spaces);
        } else {
            this.indentString += this.indentAdd;
            this.indentStringCache.put(this.spaces, this.indentString);
            Utilities.enforce(this.spaces == this.indentString.length());
        }
        return this.newline();
    }

    @Override
    public IIndentStream decrease() {
        this.spaces -= Math.max(this.amount, 0);
        if (this.spaces < 0)
            throw new RuntimeException("Negative indent");
        if (this.indentStringCache.containsKey(this.spaces)) {
            this.indentString = this.indentStringCache.get(this.spaces);
        } else {
            this.indentString = this.indentString.substring(0, this.spaces);
            this.indentStringCache.put(this.spaces, this.indentString);
            Utilities.enforce(this.spaces == this.indentString.length());
        }
        return this;
    }

    @Override
    public String toString() {
        return this.stream.toString();
    }
}
