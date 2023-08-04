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

package org.dbsp.sqlCompiler.compiler;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.apache.calcite.config.Lex;
import org.dbsp.util.SqlLexicalRulesConverter;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Packages options for a compiler from SQL to Rust.
 */
@SuppressWarnings("CanBeFinal")
// These fields cannot be final, since JCommander writes them through reflection.
public class CompilerOptions {
    /**
     * Options for the optimizer.
     */
    @SuppressWarnings("CanBeFinal")
    public static class Optimizer {
        /**
         * If true the compiler should generate an incremental streaming circuit.
         */
        @Parameter(names = "-i", description = "Generate an incremental circuit")
        public boolean incrementalize = false;
        @Parameter(names = "-O", description = "Optimization level (0, 1, or 2)")
        public int optimizationLevel = 2;
        /**
         * Useful for development
         */
        public boolean throwOnError = false;
        @Parameter(names = "-alltables", description = "Generate an input for each CREATE TABLE, even if the table is not used by any view")
        public boolean generateInputForEveryTable = false;

        /**
         * Only compare fields that matter.
         */
        public boolean same(Optimizer optimizer) {
            return incrementalize == optimizer.incrementalize;
        }

        @Override
        public String toString() {
            return "Optimizer{" +
                    "incrementalize=" + incrementalize +
                    ", optimizationLevel=" + optimizationLevel +
                    ", throwOnError=" + throwOnError +
                    ", generateInputForEveryTable=" + generateInputForEveryTable +
                    '}';
        }
    }

    /**
     * Options related to input and output.
     */
    @SuppressWarnings("CanBeFinal")
    public static class IO {
        @DynamicParameter(names = "-T",
                description = "Specify logging level for a class (can be repeated)")
        public Map<String, String> loggingLevel = new HashMap<>();
        @Parameter(names="-o", description = "Output file; stdout if null")
        @Nullable
        public String outputFile = null;
        @Parameter(names = "-j", description = "Emit the JIT JSON representation instead of Rust")
        public boolean jit = false;
        @Parameter(names = "-jpg", description = "Emit a jpg image of the circuit instead of Rust")
        public boolean emitJpeg = false;
        @Parameter(names = "-png", description = "Emit a png image of the circuit instead of Rust")
        public boolean emitPng = false;
        @Parameter(names = "-je", description = "Emit error messages as a JSON array to stderr")
        public boolean emitJsonErrors = false;
        @Parameter(names = "-js", description = "Emit a JSON file containing the schema of all views and tables involved")
        @Nullable
        public String emitJsonSchema = null;
        @Parameter(names = "-q", description = "Quiet: do not print warnings")
        public boolean quiet = false;
        @Parameter(description = "Input file to compile", required = true)
        @Nullable
        public String inputFile = null;
        @Parameter(names = "-f", description = "Name of function to generate")
        public String functionName = "circuit";
        @Parameter(names = "-d", description = "SQL syntax dialect used",
                   converter = SqlLexicalRulesConverter.class)
        public Lex lexicalRules;

        IO() {
            this.lexicalRules = Lex.ORACLE;
        }

        /**
         * Only compare fields that matter.
         */
        public boolean same(IO io) {
            if (jit != io.jit) return false;
            return lexicalRules == io.lexicalRules;
        }

        @Override
        public String toString() {
            return "IO{" +
                    "outputFile='" + outputFile + '\'' +
                    ", jit=" + jit +
                    ", emitJpeg=" + emitJpeg +
                    ", emitPng=" + emitPng +
                    ", emitJsonErrors=" + emitJsonErrors +
                    ", emitJsonSchema='" + emitJsonSchema + '\'' +
                    ", inputFile='" + inputFile + '\'' +
                    ", functionName='" + functionName + '\'' +
                    ", lexicalRules=" + lexicalRules +
                    '}';
        }
    }

    @Parameter(names = {"-h", "--help", "-?"}, help=true, description = "Show this message and exit")
    public boolean help;
    @ParametersDelegate
    public IO ioOptions = new IO();
    @ParametersDelegate
    public Optimizer optimizerOptions = new Optimizer();

    public boolean same(CompilerOptions other) {
        if (!ioOptions.same(other.ioOptions)) return false;
        return optimizerOptions.same(other.optimizerOptions);
    }

    @Override
    public int hashCode() {
        int result = (help ? 1 : 0);
        result = 31 * result + ioOptions.hashCode();
        result = 31 * result + optimizerOptions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CompilerOptions{" +
                "help=" + help +
                ", ioOptions=" + ioOptions +
                ", optimizerOptions=" + optimizerOptions +
                '}';
    }

    public CompilerOptions() {}
}
