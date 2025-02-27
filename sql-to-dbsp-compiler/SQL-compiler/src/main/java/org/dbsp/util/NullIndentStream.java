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

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** Drops all data written. */
public class NullIndentStream implements IIndentStream {
    public NullIndentStream() {}

    @Override
    public IIndentStream appendChar(char c) {
        return this;
    }

    @Override
    public IIndentStream append(String string) {
        return this;
    }

    @Override
    public IIndentStream append(boolean b) {
        return this;
    }

    @Override
    public <T extends ToIndentableString> IIndentStream append(T value) {
        return this;
    }

    @Override
    public IIndentStream append(int value) {
        return this;
    }

    @Override
    public IIndentStream append(long value) {
        return this;
    }

    @Override
    public IIndentStream appendSupplier(Supplier<String> supplier) {
        return this;
    }

    @Override
    public IIndentStream joinS(String separator, Collection<String> data) {
        return this;
    }

    @Override
    public <T extends ToIndentableString>
    IIndentStream joinI(String separator, Collection<T> data) {
        return this;
    }

    @Override
    public <T> IIndentStream join(String separator, T[] data, Function<T, String> generator) {
        return this;
    }

    @Override
    public <T> IIndentStream intercalate(String separator, T[] data, Function<T, String> generator) {
        return this;
    }

    @Override
    public IIndentStream join(String separator, String[] data) {
        return this;
    }

    @Override
    public IIndentStream join(String separator, Stream<String> data) {
        return this;
    }

    @Override
    public <T extends ToIndentableString> IIndentStream join(String separator, T[] data) {
        return this;
    }

    @Override
    public IIndentStream join(String separator, Collection<String> data) {
        return this;
    }

    @Override
    public IIndentStream joinSupplier(String separator, Supplier<Collection<String>> data) {
        return this;
    }

    @Override
    public IIndentStream intercalate(String separator, Collection<String> data) {
        return this;
    }

    @Override
    public IIndentStream intercalate(String separator, String[] data) {
        return this;
    }

    @Override
    public IIndentStream intercalateS(String separator, Collection<String> data) {
        return this;
    }

    @Override
    public <T extends ToIndentableString>
    IIndentStream intercalateI(String separator, Collection<T> data) {
        return this;
    }

    @Override
    public <T extends ToIndentableString> IIndentStream intercalateI(String separator, T[] data) {
        return this;
    }

    @Override
    public IIndentStream newline() {
        return this;
    }

    @Override
    public IIndentStream increase() {
        return this;
    }

    @Override
    public IIndentStream decrease() {
        return this;
    }

    @Override
    public String toString() {
        return "";
    }
}
