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
import org.dbsp.sqlCompiler.compiler.backend.ToDotVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.Logger;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/** Main entry point of the SQL compiler. */
public class CompilerMain {
    final CompilerOptions options;

    CompilerMain() {
        this.options = new CompilerOptions();
    }

    void usage(JCommander commander) {
        // JCommander mistakenly prints this as default value
        // if it manages to parse it partially.
        this.options.ioOptions.loggingLevel.clear();
        commander.usage();
    }

    void parseOptions(String[] argv) {
        JCommander commander = JCommander.newBuilder()
                .addObject(this.options)
                .build();
        commander.setProgramName("sql-to-dbsp");
        try {
            commander.parse(argv);
        } catch (ParameterException ex) {
            System.err.println(ex.getMessage());
            System.exit(1);
        }
        if (this.options.help) {
            this.usage(commander);
            System.exit(1);
        }

        for (Map.Entry<String, String> entry: options.ioOptions.loggingLevel.entrySet()) {
            try {
                int level = Integer.parseInt(entry.getValue());
                Logger.INSTANCE.setLoggingLevel(entry.getKey(), level);
            } catch (NumberFormatException ex) {
                System.err.println("-T option must be followed by 'class=number'; could not parse " + entry);
                System.exit(1);
            }
        }
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

    static int schemaCount = 0;

    /** Run compiler, return exit code. */
    CompilerMessages run() throws SQLException {
        DBSPCompiler compiler = new DBSPCompiler(this.options);
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
                    "Error reading file", e.getMessage());
            return compiler.messages;
        }
        if (this.options.ioOptions.verbosity >= 1)
            System.out.println(this.options);

        compiler.compileInput();
        if (compiler.hasErrors())
            return compiler.messages;
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

        compiler.optimize();
        DBSPCircuit dbsp = compiler.getFinalCircuit(this.options.ioOptions.functionName);
        String dotFormat = (this.options.ioOptions.emitJpeg ? "jpg"
                            : this.options.ioOptions.emitPng ? "png"
                            : null);
        if (dotFormat != null) {
            if (this.options.ioOptions.outputFile.isEmpty()) {
                compiler.reportError(SourcePositionRange.INVALID, "Invalid output",
                        "Must specify an output file when outputting jpeg or png");
                return compiler.messages;
            }
            boolean verboseDot = this.options.ioOptions.verbosity > 1;
            ToDotVisitor.toDot(compiler, this.options.ioOptions.outputFile, verboseDot, dotFormat, dbsp);
            return compiler.messages;
        }
        try {
            PrintStream stream = this.getOutputStream();
            RustFileWriter writer = new RustFileWriter(stream);
            writer.add(dbsp);
            writer.write(compiler);
            stream.close();
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID,
                    "Error writing to file", e.getMessage());
            return compiler.messages;
        }

        try {
            if (!this.options.ioOptions.udfs.isEmpty()) {
                String outputFileName = this.options.ioOptions.outputFile;
                if (outputFileName.isEmpty()) {
                    compiler.reportError(SourcePositionRange.INVALID,
                            "No output file", "`-udf` option requires specifying an output file");
                    return compiler.messages;
                }
                File outputFile = new File(outputFileName);
                File outputDirectory = outputFile.getParentFile();
                File source = new File(this.options.ioOptions.udfs);
                File destination = new File(outputDirectory, DBSPCompiler.UDF_FILE_NAME);
                Files.copy(source.toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID,
                    "Error copying UDF file", e.getMessage());
            return compiler.messages;
        }

        return compiler.messages;
    }

    public static CompilerMessages execute(String... argv) throws SQLException {
        CompilerMain main = new CompilerMain();
        main.parseOptions(argv);
        return main.run();
    }

    public static void main(String[] argv) throws SQLException {
        CompilerMessages messages = execute(argv);
        messages.show(System.err);
        System.exit(messages.exitCode);
    }
}
