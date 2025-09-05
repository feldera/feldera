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
        List<String> list = new ArrayList<>(tests);
        list = Linq.where(list, n -> !n.contains("evidence"));
        // This should have 610 tests
        list.sort(String::compareTo);

        LocalDate date = LocalDate.now();
        // Number of days required to run all tests.
        // This period amounts to exactly 2 tests per day
        final int rotationPeriodInDays = 305;
        final int day = date.getDayOfYear() % rotationPeriodInDays;

        List<String> toRun = new ArrayList<>();
        for (int i = 0; i < (list.size() + rotationPeriodInDays - 1) / rotationPeriodInDays; i++) {
            int index = i * rotationPeriodInDays + day;
            if (index < list.size())
                toRun.add(list.get(index));
        }
        String[] args = new String[] { "-v", "-x", "-inc", "-e", "hybrid", "-b", "skip.txt" };

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

    /** Executes one SLT program, to make sure everything is fine */
    public static void quick() throws IOException, ClassNotFoundException {
        String[] args = new String[] { "-v", "-x", "-inc", "-e", "hybrid", "-skip", "1",
                "test/random/expr/slt_good_102.test" };

        String wd = System.getProperty("user.dir");
        File directory = new File(wd + "/..").getAbsoluteFile();
        System.setProperty("user.dir", directory.getAbsolutePath());
        wd = System.getProperty("user.dir");
        System.out.println("working directory is " + wd);
        main(args);
    }

    @SuppressWarnings("SpellCheckingInspection")
    public static void main(String[] argv) throws IOException, ClassNotFoundException {
        Class.forName("org.hsqldb.jdbcDriver");
        List<String> files = Linq.list(
                // "test/random/expr/slt_good_102.test"
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
                "-e", "hybrid", "-b", "slt/skip.txt",
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
        System.out.println("WD: " + System.getProperty("user.dir"));
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
