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
import org.dbsp.sqllogictest.executors.DBSPExecutor;
import org.dbsp.sqllogictest.executors.DbspJdbcExecutor;
import org.dbsp.util.Linq;

import java.io.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Execute some or all of SqlLogicTest tests. */
public class Main {
    static final String rustDirectory = "./temp/src/";
    public static final String testFileName = "lib";

    public static String getAbsoluteRustDirectory() {
        String wd = System.getProperty("user.dir");
        return wd + "/" + rustDirectory;
    }

    /** Executes a few test files based on the day of the month */
    public static void rotateTests() throws IOException, ClassNotFoundException {
        Set<String> tests = net.hydromatic.sqllogictest.Main.getTestList();
        // Number of days required to run all tests
        final int rotationPeriodInDays = 365;
        int testsPerDay = (tests.size() + rotationPeriodInDays - 1) / rotationPeriodInDays;
        LocalDate date = LocalDate.now();
        final int day = date.getDayOfYear() % rotationPeriodInDays;
        final int firstTest = testsPerDay * (day - 1);
        final int lastTest = firstTest + testsPerDay;

        List<String> list = new ArrayList<>(tests);
        list.sort(String::compareTo);
        List<String> toRun = list.subList(firstTest, lastTest);
        String[] args = new String[] { "-v", "-x", "-inc", "-e", "hybrid" };

        String wd = System.getProperty("user.dir");
        File directory = new File(wd + "/..").getAbsoluteFile();
        System.setProperty("user.dir", directory.getAbsolutePath());
        wd = System.getProperty("user.dir");
        System.out.println("working directory is " + wd);

        String[] argv = new String[args.length + toRun.size()];
        System.arraycopy(args, 0, argv, 0, args.length);
        for (int i = 0; i < toRun.size(); i++)
            argv[i + args.length] = toRun.get(i);
        main(argv);
    }

    @SuppressWarnings("SpellCheckingInspection")
    public static void main(String[] argv) throws IOException, ClassNotFoundException {
        Class.forName("org.hsqldb.jdbcDriver");
        List<String> files = Linq.list(
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

        String[] args = {
                "-v", "-x", "-inc",
                "-e", "hybrid",      // executor
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
        // Used for debugging: how many tests to skip from the first file
        AtomicReference<Integer> skip = new AtomicReference<>();
        skip.set(0);
        parser.registerOption("-skip", "skipCount", "How many tests to skip (for debugging)", o -> {
            skip.set(Integer.parseInt(o));
            return true;
        });
        DBSPExecutor.register(parser, skip);
        DbspJdbcExecutor.register(parser, skip);
        TestStatistics results = net.hydromatic.sqllogictest.Main.execute(parser, args);
        results.printStatistics(System.out);
    }
}
