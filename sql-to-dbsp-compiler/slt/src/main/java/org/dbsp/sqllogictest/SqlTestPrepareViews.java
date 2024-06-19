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

package org.dbsp.sqllogictest;

import net.hydromatic.sqllogictest.SltSqlStatement;
import org.dbsp.util.Utilities;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A set of SQL statements create tables. */
public class SqlTestPrepareViews {
    public final Map<String, SltSqlStatement> viewDefinition = new LinkedHashMap<>();
    static final Pattern createRegex = Pattern.compile("^create\\s+view\\s+(\\w+)");
    static final Pattern dropRegex = Pattern.compile("drop\\s+view\\s+(\\w+)");

    static String getView(Pattern pattern, SltSqlStatement statement) {
        Matcher m = pattern.matcher(statement.statement.toLowerCase());
        if (!m.find())
            throw new RuntimeException("Could not figure out view in " + statement);
        return m.group(1);
    }

    public void add(SltSqlStatement statement) {
        String view = getView(createRegex, statement);
        Utilities.putNew(this.viewDefinition, view, statement);
    }

    public void clear() {
        this.viewDefinition.clear();
    }

    public void remove(SltSqlStatement statement) {
        String view = getView(dropRegex, statement);
        this.viewDefinition.remove(view);
    }

    public Iterable<? extends SltSqlStatement> definitions() {
        return this.viewDefinition.values();
    }
}
