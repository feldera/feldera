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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.TimeString;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class Utilities {
    private Utilities() {}

    /**
     * Escape special characters in a string.
     */
    public static String escape(String value) {
        StringBuilder builder = new StringBuilder();
        final int length = value.length();
        for (int offset = 0; offset < length; ) {
            final int c = value.codePointAt(offset);
            if (c == '\'')
                builder.append("\\'");
            else if (c == '\\')
                builder.append("\\\\");
            else if (c == '\"' )
                builder.append("\\\"");
            else if (c == '\r' )
                builder.append("\\r");
            else if (c == '\n' )
                builder.append("\\n");
            else if (c == '\t' )
                builder.append("\\t");
            else if (c < 32 || c >= 127) {
                builder.append("\\u{");
                builder.append(String.format("%04x", c));
                builder.append("}");
            } else
                builder.append((char)c);
            offset += Character.charCount(c);
        }
        return builder.toString();
    }

    public static <K, V, W> LinkedHashMap<K, W> mapValues(Map<K, V> map, Function<V, W> transform) {
        LinkedHashMap<K, W> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry: map.entrySet()) {
            result.put(entry.getKey(), transform.apply(entry.getValue()));
        }
        return result;
    }

    /**
     * Escape special characters in a string.
     */
    public static String escapeDoubleQuotes(String value) {
        StringBuilder builder = new StringBuilder();
        final int length = value.length();
        for (int offset = 0; offset < length; ) {
            final int c = value.codePointAt(offset);
            if (c == '\"' )
                builder.append("\\\"");
            else
                builder.append((char)c);
            offset += Character.charCount(c);
        }
        return builder.toString();
    }

    /**
     * Detects if dot is installed.
     */
    public static boolean isDotInstalled() {
        try {
            runProcess(".", "dot", "-V");
            return true;
        } catch (Exception unused) {
            return false;
        }
    }

    /**
     * Add double quotes around string and escape symbols that need it.
     */
    public static String doubleQuote(String value) {
         return "\"" + escape(value) + "\"";
     }

    /**
     * Just adds single quotes around a string.  No escaping is performed.
     */
     public static String singleQuote(@Nullable String other) {
         return "'" + other + "'";
     }

    /**
     * put something in a hashmap that is supposed to be new.
     * @param map    Map to insert in.
     * @param key    Key to insert in map.
     * @param value  Value to insert in map.
     * @return       The inserted value.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static <K, V, VE extends V> VE putNew(Map<K, V> map, K key, VE value) {
        V previous = map.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
        if (previous != null)
            throw new RuntimeException("Key " + key + " already mapped to " + previous + " when adding " + value);
        return value;
    }

    public static void showResultSet(ResultSet result, PrintStream out)
            throws SQLException {
        int columnCount = result.getMetaData().getColumnCount();
        while (result.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1)
                    out.print(", ");
                out.print(result.getString(i));
            }
            out.println();
        }
    }

    public static String readFile(Path filename) throws IOException {
        List<String> lines = Files.readAllLines(filename);
        return String.join(System.lineSeparator(), lines);
    }

    public static void writeFile(Path filename, String contents) throws IOException {
        try (FileWriter writer = new FileWriter(filename.toFile())) {
            writer.write(contents);
        }
    }

    /**
     * Get a value that must exist in a map.
     * @param map  Map to look for.
     * @param key  Key the value is indexed with.
     */
    public static <K, V> V getExists(Map<K, V> map, K key) {
        V result = map.get(key);
        if (result == null)
            throw new RuntimeException("Key '" + key + "' does not exist in map");
        return result;
    }

    /**
     * True when a simple identifier is quoted.
     */
    public static boolean identifierIsQuoted(SqlIdentifier id) {
        // Heuristic: the name is quoted if it's shorter than the position would indicate.
        SqlParserPos parserPosition = id.getParserPosition();
        int posLen = parserPosition.getEndColumnNum() - parserPosition.getColumnNum();
        int len = id.getSimple().length();
        return posLen > len;
    }

    /**
     * Remove a value that must exist in a map.
     * @param map  Map to look for.
     * @param key  Key the value is indexed with.
     */
    @SuppressWarnings("unused")
    public static <K, V> V removeExists(Map<K, V> map, K key) {
        V result = map.remove(key);
        if (result == null)
            throw new RuntimeException("Key " + key + " does not exist in map");
        return result;
    }

    public static <T> T removeLast(List<T> data) {
        if (data.isEmpty())
            throw new RuntimeException("Removing from empty list");
        return data.remove(data.size() - 1);
    }

    public static <T> T[] arraySlice(T[] data, int start, int endExclusive) {
        if (endExclusive > data.length)
            throw new RuntimeException("Slice larger than array " + endExclusive + " vs " + data.length);
        return Arrays.copyOfRange(data, start, endExclusive);
    }

    public static <T> T[] arraySlice(T[] data, int start) {
        return Utilities.arraySlice(data, start, data.length);
    }

    /**
     * Run a process.
     * @param directory    Working directory for process.
     * @param environment  A map of values for environment variables.
     * @param commands     Command and arguments to execute.
     */
    public static void runProcess(String directory, Map<String, String> environment, String[] commands)
            throws IOException, InterruptedException {
        File out = File.createTempFile("out", ".tmp", new File("."));
        out.deleteOnExit();
        ProcessBuilder processBuilder = new ProcessBuilder()
                .command(commands)
                .directory(new File(directory))
                // If this is called from a JUNIT test the output
                // of the process interferes with the surefire plugin communication,
                // so we need to redirect the output.
                .redirectOutput(out)
                .redirectError(out);
        Map<String, String> env = processBuilder.environment();
        env.putAll(environment);
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            // Only print process output if an error occurred.
            List<String> strings = Files.readAllLines(out.toPath());
            for (String s: strings)
                System.out.println(s);
        }
        if (exitCode != 0) {
            throw new RuntimeException("Process failed with exit code " + exitCode);
        }
    }

    public static void runProcess(String directory, String... commands) throws IOException, InterruptedException {
        runProcess(directory, new HashMap<>(), commands);
    }

    static void compile(String directory, boolean quiet, String... extraArgs) throws IOException, InterruptedException {
        List<String> args = new ArrayList<>();
        args.add("cargo");
        args.add("test");
        args.addAll(Arrays.asList(extraArgs));
        if (quiet) {
            args.add("-q");
        } else {
            args.add("--");
            args.add("--show-output");
        }
        runProcess(directory, args.toArray(new String[0]));
    }

    static final boolean retry = false;
    public static void compileAndTestRust(String directory, boolean quiet, String... extraArgs)
            throws IOException, InterruptedException {
        try {
           compile(directory, quiet, extraArgs);
        } catch (RuntimeException ex) {
            if (!retry)
                throw ex;
            // Sometimes the rust compiler crashes; retry.
            runProcess(directory, "cargo", "clean");
            compile(directory, quiet, extraArgs);
        }
    }

    public static <T> T last(List<T> data) {
        if (data.isEmpty())
            throw new RuntimeException("Extracting last element from empty list");
        return data.get(data.size() - 1);
    }

    public static boolean isLegalRustIdentifier(String identifier) {
        if (identifier.isEmpty())
            return false;
        boolean first = true;
        final int length = identifier.length();
        for (int offset = 0; offset < length; ) {
            int codepoint = identifier.codePointAt(offset);
            if (first) {
                if (!Character.isLetter(codepoint) && codepoint != '_')
                    return false;
            } else if (!Character.isLetterOrDigit(codepoint) && codepoint != '_') {
                return false;
            }
            first = false;
            offset += Character.charCount(codepoint);
        }
        return true;
    }

    public static long timeStringToNanoseconds(TimeString ts) {
        // TimeString has a pretty strict format
        String v = ts.toString();
        long time = Integer.parseInt(v.substring(0, 2));
        int m = Integer.parseInt(v.substring(3, 5));
        time = time * 60 + m;
        int s = Integer.parseInt(v.substring(6, 8));
        time = time * 60 + s;
        long ns = 0;
        if (v.length() > 9) {
            String tail = v.substring(9);
            tail = tail + "000000000";
            tail = tail.substring(0, 9);
            ns = Long.parseLong(tail);
        }
        time = time * 1_000_000_000 + ns;
        return time;
    }

    public static String trimRight(String value) {
        return value.replaceAll("[ ]*$", "");
    }
}
