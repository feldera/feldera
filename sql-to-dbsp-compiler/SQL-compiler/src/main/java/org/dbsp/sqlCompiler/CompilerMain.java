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

package org.dbsp.sqlCompiler;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.dbsp.sqlCompiler.compiler.backend.CircuitReplay;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.StubsWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.MultiCrates;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.MultiCratesWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDot;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitPostfix;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.sqlCompiler.compiler.visitors.VisitorProfiles;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.io.InputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/** Main entry point of the SQL compiler. */
public class CompilerMain {
    final CompilerOptions options;

    CompilerMain() {
        this.options = new CompilerOptions();
    }

    void usage(JCommander commander) {
        commander.usage();
    }

    String getVersion() {
        Properties props = new Properties();
        try (InputStream input = CompilerMain.class.getResourceAsStream("/version.properties")) {
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return props.getProperty("version", "unknown");
    }

    void showVersion() {
        System.out.println("SQL to DBSP compiler version " + this.getVersion());
    }

    public enum ParseResult {
        /** Error while parsing options */
        Error,
        /** Parsed options ok, continue compilation */
        OkContinue,
        /** Parsed options ok, stop compilation */
        OkStop
    }

    ParseResult parseOptions(String[] argv) {
        JCommander commander = JCommander.newBuilder()
                .addObject(this.options)
                .build();
        commander.setProgramName("sql-to-dbsp");
        try {
            commander.parse(argv);
        } catch (ParameterException ex) {
            if (ex.getMessage().contains("Only one main parameter allowed")) {
                if (this.options.ioOptions.outputFile.isEmpty()) {
                    System.err.println("Did you forget to specify the output file with -o?");
                }
            }
            System.err.println(ex.getMessage());
            return ParseResult.Error;
        }
        if (this.options.help) {
            this.usage(commander);
            return ParseResult.OkStop;
        }
        if (this.options.ioOptions.showVersion) {
            this.showVersion();
            return ParseResult.OkStop;
        }

        for (Map.Entry<String, String> entry: options.ioOptions.loggingLevel.entrySet()) {
            try {
                int level = Integer.parseInt(entry.getValue());
                Logger.INSTANCE.setLoggingLevel(entry.getKey(), level);
            } catch (NumberFormatException ex) {
                System.err.println("-T option must be followed by 'class=number'; could not parse " + entry);
                return ParseResult.Error;
            }
        }

        return ParseResult.OkContinue;
    }

    PrintStream getOutputStream() throws IOException {
        if (this.options.ioOptions.noRust)
            return new PrintStream(OutputStream.nullOutputStream());

        PrintStream outputStream;
        @Nullable String outputFile = this.options.ioOptions.outputFile;
        if (outputFile.isEmpty()) {
            // The caller owns its PrintStream, while stdout belongs to the JVM.
            outputStream = new PrintStream(new FilterOutputStream(System.out) {
                @Override
                public void write(byte[] bytes, int offset, int length) throws IOException {
                    this.out.write(bytes, offset, length);
                }
                @Override
                public void close() throws IOException {
                    this.flush();
                }
            });
        } else {
            outputStream = new PrintStream(new BufferedOutputStream(Files.newOutputStream(Paths.get(outputFile))));
        }
        return outputStream;
    }

    InputStream getInputFile(@Nullable String inputFile) throws IOException {
        if (inputFile == null) {
            return System.in;
        } else {
            return Files.newInputStream(Paths.get(inputFile));
        }
    }

    /** Run compiler, return exit code. */
    CompilerMessages run() {
        long jvmStart = ManagementFactory.getRuntimeMXBean().getStartTime();
        DBSPCompiler.phases.clear();
        DBSPCompiler.phases.add("JVM startup", System.currentTimeMillis() - jvmStart);
        DBSPCompiler compiler = DBSPCompiler.timed("Compiler setup", () ->
                this.options.ioOptions.readS2 || this.options.ioOptions.readS3
                        ? DBSPCompiler.forCircuitReplay(this.options) : DBSPCompiler.forSqlInput(this.options));
        this.options.validate(compiler);
        if (compiler.hasErrors())
            return compiler.messages;
        if (!this.options.ioOptions.readS2 && !this.options.ioOptions.readS3) {
            try (InputStream input = this.getInputFile(this.options.ioOptions.inputFile)) {
                compiler.setEntireInput(this.options.ioOptions.inputFile, input);
            } catch (IOException e) {
                compiler.reportError(SourcePositionRange.INVALID,
                        "Error reading file",
                        Utilities.singleQuote(this.options.ioOptions.inputFile) + " " + e.getMessage());
                return compiler.messages;
            }
        }
        if (this.options.ioOptions.verbosity >= 1)
            System.out.println(this.options);

        DBSPCircuit circuit;
        if (this.options.ioOptions.readS2 || this.options.ioOptions.readS3) {
            CircuitReplay.Stage stage = this.options.ioOptions.readS2 ? CircuitReplay.Stage.S2 : CircuitReplay.Stage.S3;
            try {
                if (this.options.ioOptions.inputFile == null)
                    throw new IllegalArgumentException("--read-" + stage.name().toLowerCase(java.util.Locale.ROOT) + " requires an input file");
                Path input = Path.of(this.options.ioOptions.inputFile);
                if (!this.options.ioOptions.writeS3.isEmpty()) {
                    Path output = Path.of(this.options.ioOptions.writeS3);
                    if (java.nio.file.Files.exists(output) && java.nio.file.Files.isSameFile(input, output))
                        throw new IllegalArgumentException("The S3 output must differ from the S2 input");
                }
                circuit = CircuitReplay.read(compiler, input, stage);
            } catch (CircuitReplay.ResourceLimit e) {
                compiler.messages.reportError(e);
                return compiler.messages;
            } catch (IOException | RuntimeException e) {
                String reason = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
                compiler.reportError(SourcePositionRange.INVALID, stage + " replay failed",
                        reason + ". Verify the input file and regenerate the " + stage + " dump with this compiler.");
                return compiler.messages;
            }
        } else {
            compiler.compileInput();
            if (compiler.hasErrors())
                return compiler.messages;
            circuit = compiler.getFinalCircuit(false);
        }
        if (options.ioOptions.anonymize || options.ioOptions.format)
            // The front-end has already emitted the output, we are done.
            return compiler.messages;
        if (compiler.hasErrors())
            return compiler.messages;
        Utilities.enforce(circuit != null);
        if (!this.options.ioOptions.writeS2.isEmpty() || !this.options.ioOptions.writeS3.isEmpty()) {
            CircuitReplay.Stage stage = this.options.ioOptions.writeS2.isEmpty() ? CircuitReplay.Stage.S3 : CircuitReplay.Stage.S2;
            String file = stage == CircuitReplay.Stage.S2 ? this.options.ioOptions.writeS2 : this.options.ioOptions.writeS3;
            try {
                CircuitReplay.write(compiler, circuit, Path.of(file), stage);
            } catch (CircuitReplay.ResourceLimit e) {
                compiler.messages.reportError(e);
                return compiler.messages;
            } catch (IOException | RuntimeException e) {
                compiler.reportError(SourcePositionRange.INVALID, stage + " export failed", e.getMessage());
                return compiler.messages;
            }
        }
        if (this.options.ioOptions.emitJsonSchema != null) {
            try {
                PrintStream outputStream = new PrintStream(
                        Files.newOutputStream(Paths.get(this.options.ioOptions.emitJsonSchema)));
                ObjectNode programMetadata = compiler.metadata.asJson();
                outputStream.println(programMetadata.toPrettyString());
                outputStream.close();
            } catch (IOException e) {
                compiler.reportError(SourcePositionRange.INVALID,
                        "Error writing to file", e.getMessage());
                return compiler.messages;
            }
        }
        if (this.options.ioOptions.emitPlan != null) {
            try {
                PrintStream outputStream = new PrintStream(
                        Files.newOutputStream(Paths.get(this.options.ioOptions.emitPlan)));
                IIndentStream stream = new IndentStream(outputStream).setIndentAmount(2);
                compiler.getPlans(stream);
                outputStream.close();
            } catch (IOException e) {
                compiler.reportError(SourcePositionRange.INVALID,
                        "Error writing to file", e.getMessage());
                return compiler.messages;
            }
        }
        if (this.options.ioOptions.emitDataflow != null) {
            try {
                PrintStream outputStream = new PrintStream(
                        Files.newOutputStream(Paths.get(this.options.ioOptions.emitDataflow)));
                compiler.getDataflow(outputStream, circuit);
                outputStream.close();
            } catch (IOException e) {
                compiler.reportError(SourcePositionRange.INVALID,
                        "Error writing to file", e.getMessage());
                return compiler.messages;
            }
        }

        if ((!this.options.ioOptions.writeS2.isEmpty() || !this.options.ioOptions.writeS3.isEmpty())
                && this.options.ioOptions.noRust)
            return compiler.messages;
        String dotFormat = null;
        if (this.options.ioOptions.emitJpeg)
            dotFormat = "jpg";
        else if (this.options.ioOptions.emitPng)
            dotFormat = "png";
        else if (this.options.ioOptions.emitSvg)
            dotFormat = "svg";
        if (dotFormat != null) {
            if (this.options.ioOptions.outputFile.isEmpty()) {
                compiler.reportError(SourcePositionRange.INVALID, "Invalid output",
                        "Must specify an output file when outputting jpeg or png");
                return compiler.messages;
            }
            ToDot.dump(compiler, this.options.ioOptions.outputFile,
                    this.options.ioOptions.verbosity, dotFormat, circuit);
            return compiler.messages;
        }
        if (this.options.ioOptions.emitCircuitIr()) {
            if (this.options.ioOptions.multiCrates()) {
                compiler.reportError(SourcePositionRange.INVALID,
                        "Incompatible options",
                        "Option --crates is not compatible with --jit or --gen2");
                return compiler.messages;
            } else {
                ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(compiler, 1);
                visitor.apply(circuit);
                String str = visitor.getJsonString();
                try (PrintStream stream = this.getOutputStream()) {
                    stream.print(str);
                } catch (IOException e) {
                    compiler.reportError(SourcePositionRange.INVALID,
                            "Error writing to output file", e.getMessage());
                }
            }
            return compiler.messages;
        }
        MultiCratesWriter multiWriter = null;
        DBSPCompiler.phases.start("Rust");
        try {
            if (!compiler.options.ioOptions.multiCrates()) {
                try (PrintStream stream = this.getOutputStream()) {
                    CircuitPostfix materializations = new CircuitPostfix(compiler);
                    materializations.apply(circuit);
                    RustFileWriter writer = new RustFileWriter(materializations);
                    writer.setOutputBuilder(new IndentStream(stream));
                    writer.add(circuit);
                    writer.write(compiler);
                    if (stream.checkError())
                        throw new IOException("Could not finish writing generated Rust to " + options.ioOptions.outputFile);
                }
            } else {
                if (options.ioOptions.emitHandles)
                    throw new CompilationError("The option '--crates' cannot be used with '--handles'");
                multiWriter = new MultiCratesWriter(options.ioOptions.outputFile, options.ioOptions.crates, true);
                multiWriter.add(circuit);
                multiWriter.write(compiler);
            }
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID,
                    "Error writing to output file", e.getMessage());
            return compiler.messages;
        } finally {
            DBSPCompiler.phases.stop("Rust");
        }

        // Generate stubs.rs file
        String outputFile = this.options.ioOptions.outputFile;
        if (!outputFile.isEmpty() && !this.options.ioOptions.noRust) {
            Path stubs;
            Path outputPath;
            if (options.ioOptions.multiCrates()) {
                // Generate globals/src/stubs.rs
                Utilities.enforce(multiWriter != null);
                String globals = multiWriter.getGlobalsName();
                outputPath = Paths.get(outputFile, MultiCrates.CRATES_DIRECTORY);
                stubs = outputPath.resolve(globals).resolve("src").resolve(DBSPCompiler.STUBS_FILE_NAME);
            } else {
                // Generate stubs.rs in the same directory
                outputPath = Paths.get(new File(outputFile).getAbsolutePath());
                stubs = outputPath.getParent().resolve(DBSPCompiler.STUBS_FILE_NAME);
            }
            StubsWriter writer = new StubsWriter(stubs);
            writer.add(circuit);
            writer.write(compiler);
        }

        Logger.INSTANCE.belowLevel(VisitorProfiles.class, 1)
                .appendSupplier(() -> DBSPCompiler.phases.toString("Phases", 0))
                .newline();
        return compiler.messages;
    }

    public static Pair<CompilerMessages, CompilerOptions> run(String... argv) {
        CompilerMain main = new CompilerMain();
        var parseResult = main.parseOptions(argv);
        CompilerOptions options = main.options;
        if (parseResult != ParseResult.OkContinue) {
            // return empty messages
            CompilerMessages result = new CompilerMessages(new DBSPCompiler(new CompilerOptions()));
            int exitCode = parseResult == ParseResult.Error ? 1 : 0;
            result.setExitCode(exitCode);
            return new Pair<>(result, options);
        }
        var messages = main.run();
        return new Pair<>(messages, options);
    }

    public static CompilerMessages execute(String... argv) throws SQLException {
        return run(argv).left;
    }

    public static CompilerMessages runAndReportErrors(String... argv) throws SQLException, IOException {
        var result = run(argv);
        PrintStream errorStream = System.err;
        FileOutputStream errorFile = null;
        if (!result.right.ioOptions.errorFile.isEmpty()) {
            errorFile = new FileOutputStream(result.right.ioOptions.errorFile);
            errorStream = new PrintStream(errorFile);
        }
        result.left.show(errorStream);
        if (errorFile != null) {
            errorFile.close();
            errorStream.close();
        }
        return result.left;
    }

    public static void main(String[] argv) throws SQLException, IOException {
        CompilerMessages messages = runAndReportErrors(argv);
        System.exit(messages.exitCode);
    }
}
