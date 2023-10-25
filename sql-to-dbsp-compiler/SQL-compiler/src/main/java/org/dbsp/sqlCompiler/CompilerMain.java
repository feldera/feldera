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
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITProgram;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.backend.*;
import org.dbsp.util.Logger;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Main entry point of the SQL compiler.
 */
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
        if (outputFile == null) {
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

    /**
     * Run compiler, return exit code.
     */
    CompilerMessages run() {
        DBSPCompiler compiler = new DBSPCompiler(this.options);
        try {
            InputStream input = this.getInputFile(this.options.ioOptions.inputFile);
            compiler.setEntireInput(this.options.ioOptions.inputFile, input);
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID, false,
                    "Error reading file", e.getMessage());
            return compiler.messages;
        }
        compiler.compileInput();
        if (compiler.hasErrors())
            return compiler.messages;
        if (this.options.ioOptions.emitJsonSchema != null) {
            try {
                PrintStream outputStream = new PrintStream(
                        Files.newOutputStream(Paths.get(this.options.ioOptions.emitJsonSchema)));
                ObjectNode ios = compiler.getIOMetadataAsJson();
                outputStream.println(ios.toPrettyString());
                outputStream.close();
            } catch (IOException e) {
                compiler.reportError(SourcePositionRange.INVALID, false,
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
            if (this.options.ioOptions.outputFile == null) {
                compiler.reportError(SourcePositionRange.INVALID, false, "Invalid output",
                        "Must specify an output file when outputting jpeg or png");
                return compiler.messages;
            }
            ToDotVisitor.toDot(compiler, this.options.ioOptions.outputFile, dotFormat, dbsp);
            return compiler.messages;
        }
        try {
            PrintStream stream = this.getOutputStream();
            if (this.options.ioOptions.jit) {
                JITProgram program = ToJitVisitor.circuitToJIT(compiler, dbsp);
                String output = program.asJson().toPrettyString();
                stream.println(output);
            } else {
                RustFileWriter writer = new RustFileWriter(compiler, stream);
                writer.emitCodeWithHandle(true);
                writer.add(dbsp);
                writer.write();
            }
            stream.close();
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID,
                    false, "Error writing to file", e.getMessage());
            return compiler.messages;
        }
        return compiler.messages;
    }

    public static CompilerMessages execute(String... argv) {
        CompilerMain main = new CompilerMain();
        main.parseOptions(argv);
        return main.run();
    }

    public static void main(String[] argv) {
        CompilerMessages messages = execute(argv);
        messages.show(System.err);
        System.exit(messages.exitCode);
    }
}
