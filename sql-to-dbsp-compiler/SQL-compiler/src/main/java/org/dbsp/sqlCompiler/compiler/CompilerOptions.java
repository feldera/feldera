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
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.IDiff;
import org.dbsp.util.IValidate;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Command-line options for the SQL compiler */
@SuppressWarnings("CanBeFinal")
// These fields cannot be final, since JCommander writes them through reflection.
public class CompilerOptions implements IDiff<CompilerOptions>, IValidate {
    /** Options related to the language compiled. */
    @SuppressWarnings("CanBeFinal")
    public static class Language implements IDiff<Language>, IValidate {
        /** If true the compiler should generate an incremental streaming circuit. */
        @Parameter(names = "-i", description = "Generate an incremental circuit")
        public boolean incrementalize = false;
        @Parameter(names = "-O", description = "Optimization level (0, 1, or 2)")
        public int optimizationLevel = 2;
        /** Useful for development */
        public boolean throwOnError = false;
        @Parameter(names = "--alltables", description = "Generate an input for each CREATE TABLE, even if the table is not used by any view")
        public boolean generateInputForEveryTable = false;
        @Parameter(names = "--ignoreOrder",
                description = "Ignore ORDER BY clauses at the end")
        public boolean ignoreOrderBy = false;
        @Parameter(names = "--outputsAreSets",
                description = "Ensure that outputs never contain duplicates")
        public boolean outputsAreSets = false;
        @Parameter(names = "--streaming",
                description = "Compiling a streaming program, where only inserts are allowed")
        public boolean streaming = false;
        @Parameter(names = "--lenient",
                description = "Lenient SQL validation.  If true it allows duplicate column names in a view.")
        public boolean lenient = false;
        @Parameter(names = "--no-restrict-io",
                description = "Do not restrict the types of columns allowed in tables and views")
        public boolean unrestrictedIOTypes = false;
        @Parameter(names = "--unaryPlusNoop",
                description = "Compile unary plus into a no-operation; similar to sqlite")
        public boolean unaryPlusNoop = false;

        public boolean same(Language language) {
            // Only compare fields that matter.
            return this.incrementalize == language.incrementalize &&
                    this.ignoreOrderBy == language.ignoreOrderBy &&
                    this.outputsAreSets == language.outputsAreSets &&
                    this.unaryPlusNoop == language.unaryPlusNoop;
        }

        @Override
        public String toString() {
            return "Language{" +
                    "\n\tincrementalize=" + this.incrementalize +
                    ",\n\tignoreOrderBy=" + this.ignoreOrderBy +
                    ",\n\toutputsAreSets=" + this.outputsAreSets +
                    ",\n\toptimizationLevel=" + this.optimizationLevel +
                    ",\n\tthrowOnError=" + this.throwOnError +
                    ",\n\tgenerateInputForEveryTable=" + this.generateInputForEveryTable +
                    ",\n\tunrestrictedIOTypes=" + this.unrestrictedIOTypes +
                    ",\n\tunaryPlusNoop=" + this.unaryPlusNoop +
                    ",\n\tlenient=" + this.lenient +
                    '}';
        }

        @Override
        public boolean validate(IErrorReporter reporter) {
            return true;
        }

        @Override
        public String diff(Language other) {
            if (this.same(other))
                return "";
            StringBuilder result = new StringBuilder();
            result.append("Language{");
            if (this.incrementalize != other.incrementalize)
                result.append("incrementalize=")
                        .append(this.incrementalize)
                        .append("!=")
                        .append(other.incrementalize)
                        .append(System.lineSeparator());
            if (this.ignoreOrderBy != other.ignoreOrderBy)
                result.append(", ignoreOrderBy=")
                        .append(this.ignoreOrderBy)
                        .append("!=")
                        .append(other.ignoreOrderBy)
                        .append(System.lineSeparator());
            if (this.outputsAreSets != other.outputsAreSets)
                result.append(", outputsAreSets=")
                        .append(this.outputsAreSets)
                        .append("!=")
                        .append(other.outputsAreSets)
                        .append(System.lineSeparator());
            if (this.unaryPlusNoop != other.unaryPlusNoop)
                result.append(", unaryPlusNoop=")
                        .append(this.unaryPlusNoop)
                        .append("!=")
                        .append(other.unaryPlusNoop)
                        .append(System.lineSeparator());
            result.append("}")
                    .append(System.lineSeparator());
            return result.toString();
        }
    }

    public String canonicalName(ProgramIdentifier name) {
        return this.canonicalName(name.name(), name.isQuoted());
    }

    public String canonicalName(String name, boolean nameIsQuoted) {
        if (nameIsQuoted)
            return name;
        Casing casing = Casing.TO_LOWER;
        return SqlParserUtil.toCase(name, casing);
    }

    @Override
    public String diff(CompilerOptions other) {
        return this.languageOptions.diff(other.languageOptions) +
                this.ioOptions.diff(other.ioOptions);
    }

    /** Options related to input and output. */
    @SuppressWarnings("CanBeFinal")
    public static class IO implements IDiff<IO>, IValidate {
        @DynamicParameter(names = "-T",
                description = "Specify logging level for a class (can be repeated)")
        public Map<String, String> loggingLevel = new HashMap<>();
        @Parameter(names = "--noRust", description = "Do not generate Rust output files")
        public boolean noRust = false;
        @Parameter(names = "--enterprise", description = "Generate code supporting enterprise features")
        public boolean enterprise = false;
        @Parameter(names="-o", description = "Output file; stdout if not specified")
        public String outputFile = "";
        @Parameter(names="--errors", description = "Error output file; stderr if not specified")
        public String errorFile = "";
        @Parameter(names = {"--jpg", "-jpg"}, description = "Emit a jpg image of the circuit instead of Rust")
        public boolean emitJpeg = false;
        @Parameter(names = {"--png", "-png"}, description = "Emit a png image of the circuit instead of Rust")
        public boolean emitPng = false;
        @Nullable @Parameter(names = "--plan", description = "Emit the Calcite plan of the program in the specified JSON file")
        public String emitPlan = null;
        @Nullable @Parameter(names = "--dataflow", description = "Emit the Dataflow graph of the program in the specified JSON file")
        public String emitDataflow = null;
        @Parameter(names = {"--je", "-je"}, description = "Emit error messages as a JSON array to the error output")
        public boolean emitJsonErrors = false;
        @Parameter(names = {"--js", "-js"},
                description = "Emit a JSON file containing the schema of all views and tables in the specified file.")
        @Nullable
        public String emitJsonSchema = null;
        @Parameter(names = "-q", description = "Quiet: do not print warnings")
        public boolean quiet = false;
        @Parameter(description = "Input file to compile", required = true)
        @Nullable
        public String inputFile = null;
        @Parameter(names = "-v", description = "Output verbosity")
        public int verbosity = 0;
        @Parameter(names = "--handles",
                description = "Use handles (true) or Catalog (false) in the emitted Rust code")
        public boolean emitHandles = false;
        @Parameter(names = "--jdbcSource",
                description = "Connection string to a database that contains table metadata")
        public String metadataSource = "";
        @Parameter(names = "--nowstream",
                description = "Implement NOW as a stream (true) or as an internal operator (false)")
        public boolean nowStream = false;
        @Parameter(names = "--sqlnames", hidden = true,
                description = "Use the table names as identifiers in the generated code")
        public boolean sqlNames = false;
        @Parameter(names = "--trimInputs", description = "Do not ingest unused fields of input tables")
        public boolean trimInputs = false;
        @Parameter(names = "--raw", hidden = true,
                description = "Do not generate any internal tables (ERROR, NOW, etc).")
        public boolean raw = false;
        @Parameter(names = "--crates", description = "Followed by a program name. Generates code using multiple crates; " +
                "`outputFile` is interpreted as a directory.")
        public String crates = "";
        @Parameter(hidden = true, names = "--input_circuit",
                description = "Do not process the circuit, return immediately after creation.  Used for testing")
        public boolean inputCircuit = false;
        @Parameter(hidden = true, names = "--skip_calcite_optimization",
                description = "Calcite optimizer steps whose names match this regex are not applied.  Used for testing")
        public String skipCalciteOptimizations = "";

        /** Only compare fields that matter. */
        public boolean same(IO other) {
            return this.emitHandles == other.emitHandles &&
                    this.trimInputs == other.trimInputs;
        }

        public boolean multiCrates() {
            return !this.crates.isEmpty();
        }

        @Override
        public boolean validate(IErrorReporter reporter) {
            if (this.emitJpeg && this.emitPng) {
                reporter.reportError(SourcePositionRange.INVALID, "Invalid options",
                        "Options -png and -jpg cannot be used at the same time");
                return false;
            }
            if (this.noRust && !this.outputFile.isEmpty()) {
                reporter.reportWarning(SourcePositionRange.INVALID, "Invalid options",
                        "Options --nooutput and -o used at the same time");
            }
            return true;
        }

        @Override
        public String toString() {
            return "IO{" +
                    "\n\toutputFile=" + Utilities.singleQuote(this.outputFile) +
                    ",\n\tmetadataSource=" + this.metadataSource +
                    ",\n\terrorFile=" + Utilities.singleQuote(this.errorFile) +
                    ",\n\temitHandles=" + this.emitHandles +
                    ",\n\temitJpeg=" + this.emitJpeg +
                    ",\n\temitPng=" + this.emitPng +
                    ",\n\temitPlan=" + this.emitPlan +
                    ",\n\temitJsonErrors=" + this.emitJsonErrors +
                    ",\n\temitJsonSchema=" + Utilities.singleQuote(this.emitJsonSchema) +
                    ",\n\tinputFile=" + Utilities.singleQuote(this.inputFile) +
                    ",\n\ttrimInputs=" + this.trimInputs +
                    ",\n\tverbosity=" + this.verbosity +
                    ",\n\tquiet=" + this.quiet +
                    ",\n\tnoRust=" + this.noRust +
                    '}';
        }

        @Override
        public String diff(IO other) {
            if (this.same(other))
                return "";
            return "IO{" +
                    (this.emitHandles != other.emitHandles ? ".emitHandles=" +
                            this.emitHandles + "!=" + other.emitHandles: "")
                    + (this.trimInputs != other.trimInputs ? ".trimInputs=" +
                            this.trimInputs + "!=" + other.trimInputs: "") +
                    "}";
        }
    }

    @Parameter(names = {"-h", "--help", "-?"}, help=true, description = "Show this message and exit")
    public boolean help;
    @ParametersDelegate
    public IO ioOptions = new IO();
    @ParametersDelegate
    public Language languageOptions = new Language();

    public boolean same(CompilerOptions other) {
        if (!this.ioOptions.same(other.ioOptions)) return false;
        return this.languageOptions.same(other.languageOptions);
    }

    @Override
    public int hashCode() {
        int result = (this.help ? 1 : 0);
        result = 31 * result + this.ioOptions.hashCode();
        result = 31 * result + this.languageOptions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CompilerOptions{" +
                "\nhelp=" + this.help +
                ",\nioOptions=" + this.ioOptions +
                ",\noptimizerOptions=" + this.languageOptions +
                "\n}";
    }

    public CompilerOptions() {}

    public static CompilerOptions getDefault() {
        return new CompilerOptions();
    }

    @Override
    public boolean validate(IErrorReporter reporter) {
        return this.ioOptions.validate(reporter) &&
                this.languageOptions.validate(reporter);
    }
}
