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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.StubsWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.MultiCratesWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDot;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Main entry point of the SQL compiler. */
public class CompilerMain {
    final CompilerOptions options;

    CompilerMain() {
        this.options = new CompilerOptions();
    }

    void usage(JCommander commander) {
        /*
        // JCommander mistakenly prints this as default value
        // if it manages to parse it partially.
        this.options.ioOptions.loggingLevel.clear();
         */
        commander.usage();
    }

    int parseOptions(String[] argv) {
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
            return 1;
        }
        if (this.options.help) {
            this.usage(commander);
            return 1;
        }

        /*
        for (Map.Entry<String, String> entry: options.ioOptions.loggingLevel.entrySet()) {
            try {
                int level = Integer.parseInt(entry.getValue());
                Logger.INSTANCE.setLoggingLevel(entry.getKey(), level);
            } catch (NumberFormatException ex) {
                System.err.println("-T option must be followed by 'class=number'; could not parse " + entry);
                return 1;
            }
        }
         */

        return 0;
    }

    PrintStream getOutputStream() throws IOException {
        PrintStream outputStream;
        @Nullable String outputFile = this.options.ioOptions.outputFile;
        if (outputFile.isEmpty()) {
            outputStream = System.out;
        } else {
            outputStream = new PrintStream(Files.newOutputStream(Paths.get(outputFile)));
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
    CompilerMessages run() throws SQLException {
        DBSPCompiler compiler = new DBSPCompiler(this.options);
        this.options.validate(compiler);
        String conn = this.options.ioOptions.metadataSource;
        if (!conn.isEmpty()) {
            // This requires the JDBC drivers for the respective databases to be loaded
            DataSource mockDataSource = JdbcSchema.dataSource(conn, null, null, null);
            Connection executorConnection = DriverManager.getConnection("jdbc:calcite:");
            CalciteConnection calciteConnection = executorConnection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            String schemaName = "schema";
            JdbcSchema schema = JdbcSchema.create(rootSchema, schemaName, mockDataSource, null, null);
            compiler.addSchemaSource(schemaName, schema);
        }

        try {
            InputStream input = this.getInputFile(this.options.ioOptions.inputFile);
            compiler.setEntireInput(this.options.ioOptions.inputFile, input);
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID,
                    "Error reading file",
                    Utilities.singleQuote(this.options.ioOptions.inputFile) + " " + e.getMessage());
            return compiler.messages;
        }
        if (this.options.ioOptions.verbosity >= 1)
            System.out.println(this.options);

        compiler.compileInput();
        if (compiler.hasErrors())
            return compiler.messages;
        // The following runs all compilation stages
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        if (compiler.hasErrors())
            return compiler.messages;
        assert circuit != null;
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
            }
            return compiler.messages;
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
            }
            return compiler.messages;
        }

        String dotFormat = (this.options.ioOptions.emitJpeg ? "jpg"
                            : this.options.ioOptions.emitPng ? "png"
                            : null);
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
        MultiCratesWriter multiWriter = null;
        try {
            if (!compiler.options.ioOptions.multiCrates()) {
                PrintStream stream = this.getOutputStream();
                RustFileWriter writer = new RustFileWriter();
                IIndentStream indent = new IndentStream(stream);
                writer.setOutputBuilder(indent);
                writer.add(circuit);
                writer.write(compiler);
                stream.close();
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
        }

        try {
            // Generate stubs.rs file
            String outputFile = this.options.ioOptions.outputFile;
            if (!outputFile.isEmpty()) {
                Path stubs;
                String outputPath = new File(outputFile).getAbsolutePath();
                if (options.ioOptions.multiCrates()) {
                    // Generate globals/src/stubs.rs
                    assert multiWriter != null;
                    String globals = multiWriter.getGlobalsName();
                    stubs = Paths.get(outputPath).resolve(globals).resolve("src").resolve(DBSPCompiler.STUBS_FILE_NAME);
                } else {
                    // Generate stubs.rs in the same directory
                    stubs = Paths.get(outputPath).getParent().resolve(DBSPCompiler.STUBS_FILE_NAME);
                }
                StubsWriter writer = new StubsWriter(stubs);
                writer.add(circuit);
                writer.write(compiler);
            }
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID,
                    "Error writing to proto.rs file", e.getMessage());
            return compiler.messages;
        }

        return compiler.messages;
    }

    public static CompilerMessages execute(String... argv) throws SQLException {
        CompilerMain main = new CompilerMain();
        int exitCode = main.parseOptions(argv);
        if (exitCode != 0) {
            // return empty messages
            CompilerMessages result = new CompilerMessages(new DBSPCompiler(new CompilerOptions()));
            result.setExitCode(exitCode);
            return result;
        }
        return main.run();
    }

    public static void main(String[] argv) throws SQLException {
        CompilerMessages messages = execute(argv);
        messages.show(System.err);
        System.exit(messages.exitCode);
    }
}
