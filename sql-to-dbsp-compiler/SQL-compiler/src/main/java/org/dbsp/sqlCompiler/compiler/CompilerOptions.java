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
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Packages options for a compiler from SQL to Rust.
 */
@SuppressWarnings("CanBeFinal")
// These fields cannot be final, since JCommander writes them through reflection.
public class CompilerOptions {
    /** Options related to the language compiled. */
    @SuppressWarnings("CanBeFinal")
    public static class Language {
        /** If true the compiler should generate an incremental streaming circuit. */
        @Parameter(names = "-i", description = "Generate an incremental circuit")
        public boolean incrementalize = false;
        @Parameter(names = "-O", description = "Optimization level (0, 1, or 2)")
        public int optimizationLevel = 2;
        /**
         * Useful for development
         */
        public boolean throwOnError = false;
        @Parameter(names = "--alltables", description = "Generate an input for each CREATE TABLE, even if the table is not used by any view")
        public boolean generateInputForEveryTable = false;
        @Parameter(names = "--ignoreOrder",
                description = "Ignore ORDER BY clauses at the end")
        public boolean ignoreOrderBy = false;
        @Parameter(names = "--outputsAreSets",
                description = "Ensure that outputs never contain duplicates")
        public boolean outputsAreSets = false;
        @Parameter(names = "-d", description = "SQL syntax dialect used",
                converter = SqlLexicalRulesConverter.class)
        public Lex lexicalRules = Lex.ORACLE;
        @Parameter(names = "--lenient",
                description = "Lenient SQL validation.  If true it allows duplicate column names in a view")
        public boolean lenient = false;
        @Parameter(names = "--unquotedCasing",
                description = "How unquoted identifiers are treated.  Choices are: 'upper', 'lower', 'unchanged'")
        public String unquotedCasing = "upper";

        public boolean same(Language language) {
            // Only compare fields that matter.
            return this.incrementalize == language.incrementalize &&
                    this.ignoreOrderBy == language.ignoreOrderBy &&
                    this.outputsAreSets == language.outputsAreSets &&
                    this.lexicalRules.equals(language.lexicalRules);
        }

        @Override
        public String toString() {
            return "Language{" +
                    "incrementalize=" + this.incrementalize +
                    ", ignoreOrderBy=" + this.ignoreOrderBy +
                    ", outputsAreSets=" + this.outputsAreSets +
                    ", optimizationLevel=" + this.optimizationLevel +
                    ", throwOnError=" + this.throwOnError +
                    ", generateInputForEveryTable=" + this.generateInputForEveryTable +
                    ", lexicalRules=" + this.lexicalRules +
                    ", lenient=" + this.lenient +
                    ", unquotedCasing=" + this.unquotedCasing +
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
        public String outputFile = "";
        @Parameter(names = "--udf", description = "Specify a Rust file containing implementations of user-defined functions")
        public String udfs = "";
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
        @Parameter(names = "-v", description = "Output verbosity")
        public int verbosity = 0;
        @Parameter(names = "--handles", description = "Use handles (true) or Catalog (false) in the emitted Rust code")
        public boolean emitHandles = false;
        @Parameter(names = "--jdbcSource", description = "Connection string to a database that contains table metadata")
        public String metadataSource = "";

        /** Only compare fields that matter. */
        public boolean same(IO other) {
            return this.emitHandles == other.emitHandles;
        }

        @Override
        public String toString() {
            return "IO{" +
                    "outputFile=" + Utilities.singleQuote(this.outputFile) +
                    ", metadataSource=" + this.metadataSource +
                    ", emitJpeg=" + this.emitJpeg +
                    ", emitHandles=" + this.emitHandles +
                    ", emitPng=" + this.emitPng +
                    ", emitJsonErrors=" + this.emitJsonErrors +
                    ", emitJsonSchema=" + Utilities.singleQuote(this.emitJsonSchema) +
                    ", inputFile=" + Utilities.singleQuote(this.inputFile) +
                    ", functionName=" + Utilities.singleQuote(this.functionName) +
                    ", verbosity=" + this.verbosity +
                    '}';
        }
    }

    @Parameter(names = {"-h", "--help", "-?"}, help=true, description = "Show this message and exit")
    public boolean help;
    @ParametersDelegate
    public IO ioOptions = new IO();
    @ParametersDelegate
    public Language languageOptions = new Language();

    public boolean same(CompilerOptions other) {
        if (!ioOptions.same(other.ioOptions)) return false;
        return languageOptions.same(other.languageOptions);
    }

    @Override
    public int hashCode() {
        int result = (help ? 1 : 0);
        result = 31 * result + ioOptions.hashCode();
        result = 31 * result + languageOptions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CompilerOptions{" +
                "help=" + help +
                ", ioOptions=" + ioOptions +
                ", optimizerOptions=" + languageOptions +
                '}';
    }

    public CompilerOptions() {}
}
