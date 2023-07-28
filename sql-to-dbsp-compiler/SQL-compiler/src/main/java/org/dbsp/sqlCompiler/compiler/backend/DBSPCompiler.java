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

package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourceFileContents;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitOptimizer;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * This class compiles SQL statements into DBSP circuits.
 * The protocol is:
 * - create compiler
 * - repeat as much as necessary:
 *   - compile a sequence of SQL statements
 *     (CREATE TABLE, CREATE VIEW)
 *   - get the resulting circuit, starting a new one
 * This protocol allows one compiler to generate multiple independent circuits.
 * The compiler can also compile INSERT statements by simulating their
 * execution and keeping track of the contents of each table.
 * The contents after insertions can be obtained using getTableContents().
 */
public class DBSPCompiler implements IWritesLogs, ICompilerComponent, IErrorReporter {
    enum InputSource {
        /**
         * No data source set yet.
         */
        None,
        /**
         * Data received from stdin.
         */
        Stdin,
        /**
         * Data read from a file.  We read the entire file upfront, and then we compile.
         */
        File,
        /**
         * Data received through API calls (compileStatement/s).
         */
        API,
    }

    /**
     * Default implementation of the weight type.
     * Can be changed by command-line flags.
     */
    private final DBSPType weightTypeImplementation;
    /**
     * Variable that refers to the weight of the row in the z-set.
     */
    public final DBSPVariablePath weightVar;

    public final ObjectMapper mapper;
    public final CalciteCompiler frontend;
    final CalciteToDBSPCompiler midend;
    public final CompilerOptions options;
    public final CompilerMessages messages;
    public final SourceFileContents sources;
    public InputSource inputSources = InputSource.None;
    final @Nullable ArrayNode inputs;
    final @Nullable ArrayNode outputs;
    public final @Nullable ObjectNode ios;
    public final TypeCompiler typeCompiler;
    public boolean hasWarnings;

    /**
     * Circuit produced by the compiler.
     */
    public @Nullable DBSPCircuit circuit;

    public DBSPCompiler(CompilerOptions options) {
        this.options = options;
        this.mapper = new ObjectMapper();
        this.frontend = new CalciteCompiler(options);
        this.midend = new CalciteToDBSPCompiler(true, options, this);
        this.messages = new CompilerMessages(this);
        this.sources = new SourceFileContents();
        this.circuit = null;
        this.typeCompiler = new TypeCompiler(this);

        if (options.ioOptions.emitJsonSchema != null) {
            this.inputs = this.mapper.createArrayNode();
            this.outputs = this.mapper.createArrayNode();
            this.ios = this.mapper.createObjectNode();
            this.ios.set("inputs", this.inputs);
            this.ios.set("outputs", this.outputs);
        } else {
            this.inputs = null;
            this.outputs = null;
            this.ios = null;
        }
        if (options.ioOptions.jit) {
            // The JIT has hardwired I32 for the weight type.
            this.weightTypeImplementation = DBSPTypeInteger.SIGNED_32;
        } else {
            this.weightTypeImplementation = DBSPTypeInteger.SIGNED_64;
        }
        this.weightVar = DBSPTypeWeight.INSTANCE.var("w");
    }

    /**
     * Warning: in general you don't want to use this function except when you
     * generate very low-level code.  In general, you should use weightType.
     */
    public DBSPType getWeightTypeImplementation() {
        return this.weightTypeImplementation;
    }

    public boolean hasWarnings() {
        return this.hasWarnings;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this;
    }

    public TypeCompiler getTypeCompiler() {
        return this.typeCompiler;
    }

    /**
     * Report an error or warning during compilation.
     * @param range      Position in source where error is located.
     * @param warning    True if this is a warning.
     * @param errorType  A short string that categorizes the error type.
     * @param message    Error message.
     */
    public void reportError(SourcePositionRange range, boolean warning,
                            String errorType, String message) {
        if (warning)
            this.hasWarnings = true;
        this.messages.reportError(range, warning, errorType, message);
    }

    /**
     * @param generate
     * If 'false' the next "create view" statements will not generate
     * an output for the circuit
     */
    public void generateOutputForNextView(boolean generate) {
        this.midend.generateOutputForNextView(generate);
    }

   void setSource(InputSource source) {
        if (this.inputSources != InputSource.None &&
                this.inputSources != source)
            throw new UnsupportedException("Input data already received from " + this.inputSources,
                    CalciteObject.EMPTY);
        this.inputSources = source;
    }

    private void compileInternal(String statements, boolean many, @Nullable String comment) {
        if (this.inputSources != InputSource.File) {
            // If we read from file we already have read the entire data.
            // Otherwise, we append the statements to the sources.
            this.sources.append(statements);
        }

        try {
            if (many) {
                SqlNodeList nodes = this.frontend.parseStatements(statements);
                for (SqlNode node : nodes) {
                    FrontEndStatement fe = this.frontend.compile(
                            node.toString(), node, null, this.inputs, this.outputs);
                    this.midend.compile(fe);
                }
            } else {
                SqlNode node = this.frontend.parse(statements);
                Logger.INSTANCE.belowLevel(this, 2)
                        .append("Parsing result")
                        .newline()
                        .append(node.toString())
                        .newline();
                FrontEndStatement fe = this.frontend.compile(
                        statements, node, comment, this.inputs, this.outputs);
                this.midend.compile(fe);
            }
        } catch (SqlParseException e) {
            this.messages.reportError(e);
            if (this.options.optimizerOptions.throwOnError) {
                System.err.println(this.messages.toString());
                throw new RuntimeException(e);
            }
        } catch (CalciteContextException e) {
            this.messages.reportError(e);
            if (this.options.optimizerOptions.throwOnError) {
                System.err.println(this.messages.toString());
                throw new RuntimeException(e);
            }
        } catch (BaseCompilerException e) {
            this.messages.reportError(e);
            if (this.options.optimizerOptions.throwOnError) {
                System.err.println(this.messages.toString());
                throw new RuntimeException(e);
            }
        } catch (Throwable e) {
            this.messages.reportError(e);
            if (this.options.optimizerOptions.throwOnError) {
                System.err.println(this.messages.toString());
                throw new RuntimeException(e);
            }
        }
    }

    public void compileStatement(String statement, @Nullable String comment) {
        this.setSource(InputSource.API);
        this.compileInternal(statement, false, comment);
    }

    public void compileStatements(String program) {
        this.setSource(InputSource.API);
        this.compileInternal(program, true, null);
    }

    public void optimize() {
        if (this.circuit == null) {
            this.circuit = this.getFinalCircuit("tmp");
        }
        CircuitOptimizer optimizer = new CircuitOptimizer(this);
        this.circuit = optimizer.optimize(circuit);
    }

    public void compileStatement(String statement) {
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Compiling ")
                .append(statement);
        this.compileStatement(statement, null);
    }

    public void setEntireInput(@Nullable String filename, InputStream contents) throws IOException {
        if (filename != null)
            this.setSource(InputSource.File);
        else
            this.setSource(InputSource.Stdin);
        this.sources.setEntireInput(filename, contents);
    }

    public void compileInput() {
        if (this.inputSources == InputSource.None)
            throw new UnsupportedException("compileInput has been called without calling setEntireInput",
                    CalciteObject.EMPTY);
        this.compileInternal(this.sources.getWholeProgram(), true, null);
    }

    public boolean hasErrors() {
        return this.messages.exitCode != 0;
    }

    /**
     * Get the circuit generated by compiling the statements to far.
     * Start a new circuit.
     * @param name  Name to use for the produced circuit.
     */
    public DBSPCircuit getFinalCircuit(String name) {
        if (this.circuit == null) {
            DBSPPartialCircuit circuit = this.midend.getFinalCircuit();
            this.circuit = circuit.seal(name);
        }
        DBSPCircuit result = this.circuit.rename(name);
        this.circuit = null;
        return result;
    }

    /**
     * Get the contents of the tables as a result of all the INSERT statements compiled.
     */
    public TableContents getTableContents() {
        return this.midend.getTableContents();
    }

    public void showErrors(PrintStream stream) {
        this.messages.show(stream);
    }

    /**
     * Throw if any error has been encountered.
     * Displays the errors on stderr as well.
     */
    public void throwIfErrorsOccurred() {
        if (this.hasErrors()) {
            this.showErrors(System.err);
            throw new RuntimeException("Error during compilation");
        }
    }
}
