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

package org.dbsp.util;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SuppressWarnings("UnusedReturnValue")
public interface IIndentStream {
    IIndentStream appendChar(char c);

    default IIndentStream append(String string) {
        if (string.equals(System.lineSeparator()))
            return this.appendChar('\n');
        if (!string.contains("\n"))
            return this.appendFast(string);
        String[] parts = string.split("\n", -1);
        boolean first = true;
        for (String part: parts) {
            if (!first)
                this.newline();
            first = false;
            this.appendFast(part);
        }
        return this;
    }

    /** Append a string that does not contain a newline */
    IIndentStream appendFast(String string);

    default IIndentStream append(boolean b) {
        return this.appendFast(Boolean.toString(b));
    }

    default <T extends ToIndentableString> IIndentStream append(T value) {
        value.toString(this);
        return this;
    }

    default IIndentStream append(int value) {
        this.appendFast(Integer.toString(value));
        return this;
    }

    default IIndentStream append(long value) {
        this.appendFast(Long.toString(value));
        return this;
    }

    /** For lazy evaluation of the argument. */
    default IIndentStream appendSupplier(Supplier<String> supplier) {
        return this.append(supplier.get());
    }

    default IIndentStream joinS(String separator, Collection<String> data) {
        boolean first = true;
        for (String d: data) {
            if (!first)
                this.append(separator);
            first = false;
            this.append(d);
        }
        return this;
    }

    default <T extends ToIndentableString> IIndentStream joinI(String separator, Collection<T> data) {
        boolean first = true;
        for (ToIndentableString d: data) {
            if (!first)
                this.append(separator);
            first = false;
            this.append(d);
        }
        return this;
    }

    default IIndentStream join(String separator, String[] data) {
        boolean first = true;
        for (String d: data) {
            if (!first)
                this.append(separator);
            first = false;
            this.append(d);
        }
        return this;
    }

    default IIndentStream join(String separator, Stream<String> data) {
        final boolean[] first = {true};
        data.forEach(d -> {
            if (!first[0])
                this.append(separator);
            first[0] = false;
            this.append(d);
        });
        return this;
    }

    default <T> IIndentStream join(String separator, T[] data, Function<T, String> generator) {
        boolean first = true;
        for (T d: data) {
            if (!first)
                this.append(separator);
            first = false;
            this.append(generator.apply(d));
        }
        return this;
    }
    
    default <T> IIndentStream intercalate(String separator, T[] data, Function<T, String> generator) {
        for (T d: data) {
            this.append(generator.apply(d));
            this.append(separator);
        }
        return this;
    }

    default <T extends ToIndentableString> IIndentStream join(String separator, T[] data) {
        boolean first = true;
        for (ToIndentableString d: data) {
            if (!first)
                this.append(separator);
            first = false;
            this.append(d);
        }
        return this;
    }

    default IIndentStream join(String separator, Collection<String> data) {
        boolean first = true;
        for (String d: data) {
            if (!first)
                this.append(separator);
            first = false;
            this.append(d);
        }
        return this;
    }

    default IIndentStream joinSupplier(String separator, Supplier<Collection<String>> data) {
        return this.join(separator, data.get());
    }

    default IIndentStream intercalate(String separator, Collection<String> data) {
        for (String d: data) {
            this.append(d);
            this.append(separator);
        }
        return this;
    }

    default IIndentStream intercalate(String separator, String[] data) {
        for (String d: data) {
            this.append(d);
            this.append(separator);
        }
        return this;
    }

    default IIndentStream intercalateS(String separator, Collection<String> data) {
        for (String d: data) {
            this.append(d);
            this.append(separator);
        }
        return this;
    }

    default <T extends ToIndentableString>
    IIndentStream intercalateI(String separator, Collection<T> data) {
        for (T d: data) {
            this.append(d);
            this.append(separator);
        }
        return this;
    }

    default <T extends ToIndentableString> IIndentStream intercalateI(String separator, T[] data) {
        for (T d: data) {
            this.append(d);
            this.append(separator);
        }
        return this;
    }

    default IIndentStream newline()  {
        return this.appendChar('\n');
    }

    /** Increase indentation and emit a newline. */
    IIndentStream increase();
    IIndentStream decrease();

    default IIndentStream appendJsonLabelAndColon(String label) {
        return this.append("\"").append(Utilities.escapeDoubleQuotes(label)).append("\": ");
    }

    /** Given data composed of multiple lines, append each line
     * indented with the current amount */
    default IIndentStream appendIndentedStrings(String data) {
        String[] lines = data.split(System.lineSeparator());
        this.join(System.lineSeparator(), lines);
        return this;
    }
}
