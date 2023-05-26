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

import javax.annotation.Nullable;
import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Utilities {
    private Utilities() {}

    /**
     * Generate a rust file which does nothing but includes the specified modules.
     * @param file     File to write to.
     * @param modules  List of modules to include.
     */
    public static void writeRustLib(String file, List<String> modules) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(file);
        writer.println("// automatically-generated file");
        for (String module: modules) {
            writer.println("mod " + module + ";");
        }
        writer.close();
    }

    /**
     * Add double quotes around string and escape symbols that need it.
     */
    public static String doubleQuote(String value) {
         StringBuilder builder = new StringBuilder();
         builder.append("\"");
         for (char c : value.toCharArray()) {
             if (c == '\'')
                 builder.append("\\'");
             else if (c == '\"' )
                 builder.append("\\\"");
             else if (c == '\r' )
                 builder.append("\\r");
             else if (c == '\n' )
                 builder.append("\\n");
             else if (c == '\t' )
                 builder.append("\\t");
             else if (c < 32 || c >= 127 )
                 builder.append( String.format( "\\u%04x", (int)c ) );
             else
                 builder.append(c);
         }
         builder.append("\"");
         return builder.toString();
     }

    /**
     * Just adds single quotes around a string.  No escaping is performed.
     */
     public static String singleQuote(String other) {
         return "'" + other + "'";
     }

    /**
     * put something in a hashmap that is supposed to be new.
     * @param map    Map to insert in.
     * @param key    Key to insert in map.
     * @param value  Value to insert in map.
     * @return       The inserted value.
     */
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

    /**
     * Get a value that must exist in a map.
     * @param map  Map to look for.
     * @param key  Key the value is indexed with.
     */
    public static <K, V> V getExists(Map<K, V> map, K key) {
        V result = map.get(key);
        if (result == null)
            throw new RuntimeException("Key " + key + " does not exist in map");
        return result;
    }

    @Nullable
    public static String getFileExtension(String filename) {
        int i = filename.lastIndexOf('.');
        if (i > 0)
            return filename.substring(i+1);
        return null;
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

    public static void runProcess(String directory, String... commands) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
        processBuilder.directory(new File(directory));
        processBuilder.inheritIO();
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0)
            throw new RuntimeException("Process failed with exit code " + exitCode);
    }

    static final boolean retry = false;
    public static void compileAndTestRust(String directory, boolean quiet)
            throws IOException, InterruptedException {
        try {
            if (quiet)
                runProcess(directory, "cargo", "test", "-q");
            else
                runProcess(directory, "cargo", "test", "--", "--show-output");
        } catch (RuntimeException ex) {
            if (!retry)
                throw ex;
            // Sometimes the rust compiler crashes; retry.
            runProcess(directory, "cargo", "clean");
            if (quiet)
                runProcess(directory, "cargo", "test", "-q");
            else
                runProcess(directory, "cargo", "test", "--", "--show-output");
        }
    }

    @SuppressWarnings("SpellCheckingInspection")
    private static final char[] hexCode = "0123456789abcdef".toCharArray();

    public static String toHex(byte[] data) {
        StringBuilder r = new StringBuilder(data.length * 2);
        for (byte b : data) {
            r.append(hexCode[(b >> 4) & 0xF]);
            r.append(hexCode[(b & 0xF)]);
        }
        return r.toString();
    }

    public static List<String> toList(@Nullable String comment) {
        if (comment == null)
            return Linq.list();
        return Linq.list(comment.split("\n"));
    }

    public static void compileAndTestJit(String directory, File jsonFile) throws IOException, InterruptedException {
        runProcess(directory, "cargo", "run", "-p", "dataflow-jit",
                "--bin", "dataflow-jit", "--features", "binary", "--", "validate", jsonFile.getAbsolutePath());
    }
}
