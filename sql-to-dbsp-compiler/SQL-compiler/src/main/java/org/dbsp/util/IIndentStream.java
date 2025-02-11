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
    IIndentStream append(String string);
    IIndentStream append(boolean b);
    <T extends ToIndentableString> IIndentStream append(T value);
    IIndentStream append(int value);
    IIndentStream append(long value);
    /** For lazy evaluation of the argument. */
    IIndentStream appendSupplier(Supplier<String> supplier);
    IIndentStream joinS(String separator, Collection<String> data);
    <T extends ToIndentableString> IIndentStream joinI(String separator, Collection<T> data);
    IIndentStream join(String separator, String[] data);
    IIndentStream join(String separator, Stream<String> data);
    <T> IIndentStream join(String separator, T[] data, Function<T, String> generator);
    <T> IIndentStream intercalate(String separator, T[] data, Function<T, String> generator);
    <T extends ToIndentableString> IIndentStream join(String separator, T[] data);
    IIndentStream join(String separator, Collection<String> data);
    IIndentStream joinSupplier(String separator, Supplier<Collection<String>> data);
    IIndentStream intercalate(String separator, Collection<String> data);
    IIndentStream intercalate(String separator, String[] data);
    IIndentStream intercalateS(String separator, Collection<String> data);
    <T extends ToIndentableString> IIndentStream intercalateI(String separator, Collection<T> data);
    <T extends ToIndentableString> IIndentStream intercalateI(String separator, T[] data);
    IIndentStream newline();
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
