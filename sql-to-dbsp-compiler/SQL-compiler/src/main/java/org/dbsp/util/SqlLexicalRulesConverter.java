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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import org.apache.calcite.config.Lex;

import java.util.ArrayList;
import java.util.List;

public class SqlLexicalRulesConverter implements IStringConverter<Lex> {
    static final private List<String> legalDialects = new ArrayList<>();

    static {
        legalDialects.add("mysql");
        legalDialects.add("oracle");
        legalDialects.add("big_query");
    }

    @Override
    public Lex convert(String s) {
        return switch (s.toLowerCase()) {
            case "mysql" -> Lex.MYSQL;
            case "oracle" -> Lex.ORACLE;
            case "big_query" -> Lex.BIG_QUERY;
            default -> throw new ParameterException("Illegal value " + Utilities.singleQuote(s)
                                                    + " for SQL lexical rules; legal values are: " + legalDialects);
        };
    }
}
