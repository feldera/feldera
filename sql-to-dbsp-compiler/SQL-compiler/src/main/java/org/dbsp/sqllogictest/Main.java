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

import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.TestStatistics;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerPasses;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqllogictest.executors.DBSPExecutor;
import org.dbsp.sqllogictest.executors.DbspJdbcExecutor;
import org.dbsp.sqllogictest.executors.JitDbspExecutor;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Execute all SqlLogicTest tests.
 */
public class Main {
    @SuppressWarnings("SpellCheckingInspection")
    public static void main(String[] argv) throws IOException {
        List<String> files = Linq.list(
                "test/index/between/100/slt_good_3.test"
                /*
                "select1.test"
                "select2.test",
                "select3.test",
                "select4.test",
                "select5.test",
                "random/select",
                "random/aggregates",
                "random/groupby",
                "random/expr",
                "index/commute",
                "index/orderby",
                "index/between",
                "index/view/",
                "index/in",      
                "index/delete",  
                "index/commute", 
                "index/orderby_nosort", 
                "index/random",  
                "evidence"
                 */
        );

        Logger.INSTANCE.setLoggingLevel(Passes.class, 2);
        Logger.INSTANCE.setLoggingLevel(InnerPasses.class, 2);
        String[] args = {
                "-v", "-x",
                "-e", "jit",      // executor
        };
        if (argv.length > 0) {
            args = argv;
        } else {
            List<String> a = new ArrayList<>();
            a.addAll(Linq.list(args));
            a.addAll(files);
            args = a.toArray(new String[0]);
        }
        System.out.println(Arrays.toString(args));
        OptionsParser parser = new OptionsParser(true, System.out, System.err);
        DBSPExecutor.register(parser);
        DbspJdbcExecutor.register(parser);
        JitDbspExecutor.register(parser);
        TestStatistics results = net.hydromatic.sqllogictest.Main.execute(parser, args);
        results.printStatistics(System.out);
    }
}
