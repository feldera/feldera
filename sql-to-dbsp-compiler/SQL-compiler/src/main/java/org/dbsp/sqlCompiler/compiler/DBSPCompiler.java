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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDot;
import org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourceFileContents;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragment;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateFunctionStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.RelStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlLateness;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitOptimizer;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    /** Name of the Rust file that will contain the user-defined functions.
     * The definitions supplied by the user will be copied here. */
    public static final String UDF_FILE_NAME = "udf.rs";
    /** Name of the rust file that will contain stubs for all user-defined functions */
    public static final String STUBS_FILE_NAME = "stubs.rs";

    final GlobalTypes globalTypes = new GlobalTypes();

    /** Convert the string to the casing specified by the options */
    public ProgramIdentifier canonicalName(String name, boolean nameIsQuoted) {
        String canon = this.options.canonicalName(name, nameIsQuoted);
        return new ProgramIdentifier(canon, nameIsQuoted);
    }

    /** Where does the compiled program come from? */
    public enum InputSource {
        /** No data source set yet. */
        None,
        /** Data received from stdin. */
        Stdin,
        /** Data read from a file.  We read the entire file upfront, and then we compile. */
        File,
        /** Data received through API calls (compileStatement/s). */
        API,
    }

    /** Variable that refers to the weight of the row in the z-set. */
    public final DBSPVariablePath weightVar;

    public final SqlToRelCompiler sqlToRelCompiler;
    final CalciteToDBSPCompiler relToDBSPCompiler;
    public final CompilerOptions options;
    public final CompilerMessages messages;
    public final SourceFileContents sources;
    public InputSource inputSources = InputSource.None;
    public final ProgramMetadata metadata;

    public final TypeCompiler typeCompiler;
    public boolean hasWarnings;
    // For each view the list of columns declared with lateness
    final Map<ProgramIdentifier, Map<ProgramIdentifier, SqlLateness>> viewLateness = new HashMap<>();

    final Map<ProgramIdentifier, CreateViewStatement> views = new HashMap<>();
    /** All UDFs from the SQL program.  The ones in Rust have no bodies */
    public final List<DBSPFunction> functions = new ArrayList<>();

    public DBSPCompiler(CompilerOptions options) {
        this.options = options;
        // Setting these first allows errors to be reported
        this.messages = new CompilerMessages(this);
        this.metadata = new ProgramMetadata();
        this.sqlToRelCompiler = new SqlToRelCompiler(options, this);
        this.relToDBSPCompiler = new CalciteToDBSPCompiler(true, options,
                this, this.metadata);
        this.sources = new SourceFileContents();
        this.typeCompiler = new TypeCompiler(this);
        this.weightVar = DBSPTypeWeight.INSTANCE.var();
        this.start();
    }

    public String getPlans() {
        StringBuilder jsonPlan = new StringBuilder();
        jsonPlan.append("{");
        boolean first = true;
        List<ProgramIdentifier> sorted = Linq.list(this.views.keySet());
        sorted.sort(Comparator.comparing(ProgramIdentifier::name));
        for (ProgramIdentifier e: sorted) {
            CreateViewStatement cv = this.views.get(e);
            if (!first) {
                jsonPlan.append(",").append(System.lineSeparator());
            }
            first = false;
            jsonPlan.append("\"").append(Utilities.escapeDoubleQuotes(e.name())).append("\"");
            jsonPlan.append(":");
            String json = SqlToRelCompiler.getPlan(cv.getRelNode(), true);
            jsonPlan.append(json);
        }
        jsonPlan.append(System.lineSeparator()).append("}");
        return jsonPlan.toString();
    }

    // Will soon be overwritten
    public static ProgramIdentifier NOW_TABLE_NAME = new ProgramIdentifier("", false);
    public static ProgramIdentifier ERROR_TABLE_NAME = new ProgramIdentifier("", false);
    public static ProgramIdentifier ERROR_VIEW_NAME = new ProgramIdentifier("", false);

    // Steps executed before the actual compilation.
    void start() {
        // Compute the names based on the compiler flags.
        NOW_TABLE_NAME = this.canonicalName("now", false);
        ERROR_TABLE_NAME = this.canonicalName("ERROR_TABLE", false);
        ERROR_VIEW_NAME = this.canonicalName("ERROR_VIEW", false);
        if (this.options.ioOptions.raw)
            return;

        // Declare the system tables
        this.compileInternal(
                """
                CREATE TABLE NOW(now TIMESTAMP NOT NULL LATENESS INTERVAL 0 SECONDS);
                CREATE TABLE ERROR_TABLE(table_or_view_name VARCHAR NOT NULL, message VARCHAR NOT NULL, metadata VARIANT NOT NULL);
                CREATE VIEW ERROR_VIEW AS SELECT * FROM ERROR_TABLE;
                """,
                true, false);
    }

    public boolean hasWarnings() {
        return this.hasWarnings;
    }

    public CustomFunctions getCustomFunctions() {
        return this.sqlToRelCompiler.getCustomFunctions();
    }

    @Override
    public DBSPCompiler compiler() {
        return this;
    }

    public TypeCompiler getTypeCompiler() {
        return this.typeCompiler;
    }

    public String getSaneStructName(ProgramIdentifier name) {
        return this.globalTypes.generateSaneName(name);
    }

    public void registerStruct(DBSPTypeStruct type) {
        this.globalTypes.register(type);
    }

    @Nullable
    public DBSPTypeStruct getStructByName(ProgramIdentifier name) {
        return this.globalTypes.getStructByName(name);
    }

    public boolean isStructConstructor(ProgramIdentifier name) {
        return this.globalTypes.containsStruct(name);
    }

    /**
     * Report an error or warning during compilation.
     * @param range      Position in source where error is located.
     * @param continuation  If true, this error message is a continuation of a multi-line report.
     * @param warning    True if this is a warning.
     * @param errorType  A short string that categorizes the error type.
     * @param message    Error message.
     */
    public void reportProblem(SourcePositionRange range, boolean warning, boolean continuation,
                              String errorType, String message) {
        if (warning)
            this.hasWarnings = true;
        this.messages.reportProblem(range, warning, continuation, errorType, message);
        if (!warning && this.options.languageOptions.throwOnError) {
            System.err.println(this.messages);
            throw new CompilationError("Error during compilation");
        }
    }

    void setSource(InputSource source) {
        if (this.inputSources != InputSource.None &&
                this.inputSources != source)
            throw new UnsupportedException("Input data already received from " + this.inputSources,
                    CalciteObject.EMPTY);
        this.inputSources = source;
    }

    /** Add a new source which can provide schema information about input tables */
    public void addSchemaSource(String name, Schema schema) {
        this.sqlToRelCompiler.addSchemaSource(name, schema);
    }

    record SqlStatements(String statement, boolean many, boolean visible) {}

    final List<SqlStatements> toCompile = new ArrayList<>();

    /** Accumulate program to compile.
     * @param statements Statements to compile.
     * @param many       Is this one or many statements?
     * @param appendToSource  If true this is part of the source supplied by the user.
     *                        Otherwise, it's a "preamble" generated internally.
     */
    private void compileInternal(String statements, boolean many, boolean appendToSource) {
        if (this.inputSources != InputSource.File) {
            // If we read from file we already have read the entire data.
            // Otherwise, we append the statements to the sources.
            if (appendToSource) {
                this.sources.append(statements);
                this.sources.append("\n");
            }
        }
        this.toCompile.add(new SqlStatements(statements, many, appendToSource));
    }

    private void compileInternal(String statements, boolean many) {
        this.compileInternal(statements, many, true);
    }

    void validateForeignKeys(DBSPCircuit circuit, List<ForeignKey> foreignKeys) {
        // Invoked after all statements have been compiled and thus
        // all tables in the program are known.
        for (ForeignKey fk: foreignKeys) {
            ForeignKey.TableAndColumns self = fk.thisTable;
            ForeignKey.TableAndColumns other = fk.otherTable;
            ProgramIdentifier thisTableName = self.tableName.toIdentifier();

            if (self.columnNames.size() != other.columnNames.size()) {
                this.reportError(self.listPos, "Size mismatch",
                        "FOREIGN KEY section of table " +
                                thisTableName.singleQuote() +
                                " contains " + self.columnNames.size() + " columns," +
                                " which does not match the size the REFERENCES, which is " +
                                other.columnNames.size());
                continue;
            }

            // Check that the referred columns exist and have proper types
            ProgramIdentifier otherTableName = other.tableName.toIdentifier();
            DBSPSourceTableOperator otherTable = circuit.getInput(otherTableName);
            if (otherTable == null) {
                this.reportWarning(other.tableName.getSourcePosition(),
                        "Table not found",
                        "Table " + otherTableName.singleQuote()
                                + ", referred in FOREIGN KEY constraint of table " +
                                thisTableName.singleQuote() + ", does not exist");
                continue;
            }

            DBSPSourceTableOperator thisTable = circuit.getInput(thisTableName);
            assert thisTable != null;

            List<InputColumnMetadata> otherKeys = otherTable.metadata.getPrimaryKeys();
            if (otherKeys.size() != self.columnNames.size()) {
                this.reportError(self.listPos,
                        "PRIMARY KEY does not match",
                        "The PRIMARY KEY of table " + otherTableName.singleQuote() +
                                " does not match the FOREIGN KEY of " + thisTableName.singleQuote());
                continue;
            }

            for (int i = 0; i < self.columnNames.size(); i++) {
                SqlFragmentIdentifier selfColumn = self.columnNames.get(i);
                SqlFragmentIdentifier otherColumn = other.columnNames.get(i);
                ProgramIdentifier selfColumnName = selfColumn.toIdentifier();
                ProgramIdentifier otherColumnName = otherColumn.toIdentifier();
                InputColumnMetadata selfMeta = thisTable.metadata.getColumnMetadata(selfColumnName);
                if (selfMeta == null) {
                    this.reportError(selfColumn.getSourcePosition(),
                            "Column not found",
                            "Table " + thisTableName.singleQuote() +
                                    " does not have a column named " + selfColumnName.singleQuote());
                    continue;
                }
                InputColumnMetadata otherMeta = otherTable.metadata.getColumnMetadata(otherColumnName);
                if (otherMeta == null) {
                    this.reportError(otherColumn.getSourcePosition(),
                            "Column not found",
                            "Table " + otherTableName.singleQuote() +
                                    " does not have a column named " + otherColumnName.singleQuote());
                    continue;
                }
                if (!otherMeta.isPrimaryKey) {
                    this.reportError(selfColumn.getSourcePosition(),
                            "FOREIGN KEY points to non-key column",
                            "FOREIGN KEY column " +
                                    Utilities.singleQuote(thisTableName + "." + selfColumnName) +
                                    " refers to column " +
                                    Utilities.singleQuote(otherTableName + "." + otherColumnName) +
                                    " which is not a PRIMARY KEY");
                    continue;
                }
                if (!selfMeta.type.sameTypeIgnoringNullability(otherMeta.type)) {
                    this.reportError(selfColumn.getSourcePosition(),
                            "Mismatched FOREIGN KEY column types",
                            "FOREIGN KEY column " +
                                    Utilities.singleQuote(thisTableName + "." + selfColumnName) +
                                    " has type " + selfMeta.type.asSqlString() +
                                    " which does not match the type " + otherMeta.type.asSqlString() +
                                    " of the referenced column " +
                                    Utilities.singleQuote(otherTableName + "." + otherColumnName));
                }
            }
        }
    }

    void printMessages(@Nullable SqlParserPos pos) {
        System.err.println(this.messages);
        if (pos != null && pos != SqlParserPos.ZERO) {
            System.err.println("While compiling");
            System.err.println(this.sources.getFragment(new SourcePositionRange(pos), true));
        }
    }

    void validateViewProperty(SqlFragment key, SqlFragment value) {
        CalciteObject node = CalciteObject.create(key.getParserPosition());
        String keyString = key.getString();
        switch (keyString) {
            case CreateViewStatement.EMIT_FINAL:
                // Actual value validated elsewhere
                break;
            case "rust":
            case "connectors":
                this.validateConnectorsProperty(node, key, value);
                break;
            default:
                throw new CompilationError("Unknown property " + Utilities.singleQuote(keyString), node);
        }
    }

    void validateBooleanProperty(CalciteObject node, SqlFragment key, SqlFragment value) {
        String vs = value.getString();
        if (vs.equalsIgnoreCase("true") || vs.equalsIgnoreCase("false"))
            return;
        throw new CompilationError("Expected a boolean value for property " +
                Utilities.singleQuote(key.getString()), node);
    }

    @SuppressWarnings("unused")
    void validateConnectorsProperty(CalciteObject node, SqlFragment key, SqlFragment value) {
        // Nothing right now.
        // This is validated by the pipeline_manager, and it's relatively fast.
        // Checking that this is legal JSON may make interactive editing of the SQL program annoying.
    }

    void validateTableProperty(SqlFragment key, SqlFragment value) {
        CalciteObject node = CalciteObject.create(key.getParserPosition());
        String keyString = key.getString();
        switch (key.getString()) {
            case "materialized":
            case "append_only":
                this.validateBooleanProperty(node, key, value);
                break;
            case "connectors":
                this.validateConnectorsProperty(node, key, value);
                break;
            default:
                throw new CompilationError("Unknown property " + Utilities.singleQuote(keyString), node);
        }
    }

    List<ParsedStatement> runParser() {
        // Parse using Calcite
        List<ParsedStatement> parsed = new ArrayList<>();
        for (SqlStatements stat : this.toCompile) {
            try {
                if (stat.many) {
                    if (stat.statement.isEmpty())
                        continue;
                    parsed.addAll(this.sqlToRelCompiler.parseStatements(stat.statement, stat.visible));
                } else {
                    SqlNode node = this.sqlToRelCompiler.parse(stat.statement, stat.visible);
                    parsed.add(new ParsedStatement(node, stat.visible));
                }
                if (this.hasErrors())
                    break;
            } catch (SqlParseException e) {
                if (e.getCause() instanceof BaseCompilerException) {
                    // Exceptions we throw in parser validation code are caught
                    // by the Calcite parser and wrapped in SqlParseException.
                    // Unwrap them to retrieve source position...
                    this.messages.reportError((BaseCompilerException) e.getCause());
                } else {
                    this.messages.reportError(e);
                }
            } catch (CalciteException e) {
                this.messages.reportError(e);
            } catch (Throwable e) {
                this.messages.reportError(e);
                this.printMessages(SqlParserPos.ZERO);
                parsed.clear();
            }
        }
        this.toCompile.clear();
        return parsed;
    }

    @Nullable DBSPCircuit runAllCompilerStages() {
        List<ParsedStatement> parsed = this.runParser();
        if (this.hasErrors())
            return null;
        SqlParserPos currentViewPosition = SqlParserPos.ZERO;
        try {
            // across all tables
            List<ForeignKey> foreignKeys = new ArrayList<>();
            // All UDFs which have no bodies in SQL
            final List<SqlFunction> rustFunctions = new ArrayList<>();

            // Compile first the statements that define functions, types, and lateness
            for (ParsedStatement node: parsed) {
                Logger.INSTANCE.belowLevel(this, 4)
                        .append("Parsing result: ")
                        .appendSupplier(node::toString)
                        .newline();
                SqlKind kind = node.statement().getKind();
                if (kind == SqlKind.CREATE_TYPE) {
                    RelStatement fe = this.sqlToRelCompiler.compileCreateType(node);
                    if (fe == null)
                        // error during compilation
                        continue;
                    this.relToDBSPCompiler.compile(fe);
                    continue;
                }
                if (kind == SqlKind.CREATE_FUNCTION) {
                    CreateFunctionStatement stat = this.sqlToRelCompiler.compileCreateFunction(node, this.sources);
                    boolean exists = this.sqlToRelCompiler.functionExists(stat.function.getName());
                    if (exists) {
                        throw new CompilationError("A function named " + Utilities.singleQuote(stat.function.getName()) +
                                " is already predefined, or the name is reserved.\nPlease consider using a " +
                                "different name for the user-defined function",
                                stat.getCalciteObject());
                    }
                    SqlFunction function = stat.function;
                    if (!stat.function.isSqlFunction()) {
                        rustFunctions.add(function);
                    } else {
                        // Reload the operator table to include the newly defined SQL function.
                        // This allows the functions to be used in other function definitions.
                        // There should be a better way to do this.
                        SqlOperatorTable newFunctions = SqlOperatorTables.of(Linq.list(function));
                        this.sqlToRelCompiler.addOperatorTable(newFunctions);
                    }
                    DBSPNode func = this.relToDBSPCompiler.compile(stat);
                    this.functions.add(Objects.requireNonNull(func).to(DBSPFunction.class));
                }
                if (node.statement() instanceof SqlLateness lateness) {
                    ProgramIdentifier view = Utilities.toIdentifier(lateness.getView());
                    ProgramIdentifier column = Utilities.toIdentifier(lateness.getColumn());
                    Map<ProgramIdentifier, SqlLateness> perView = this.viewLateness.computeIfAbsent(view, (k) -> new HashMap<>());
                    if (perView.containsKey(column)) {
                        SourcePositionRange range = new SourcePositionRange(lateness.getParserPosition());
                        this.reportError(range, "Duplicate",
                                "Lateness for " + view + "." + column + " already declared");
                        this.reportError(new SourcePositionRange(perView.get(column).getLateness().getParserPosition()),
                                "Duplicate", "Location of the previous declaration", true);
                    } else {
                        Utilities.putNew(perView, column, lateness);
                    }
                    // This information will be used when compiling SqlCreateView
                }
            }

            if (!rustFunctions.isEmpty()) {
                // Reload the operator table to include the newly defined Rust function.
                // These we can load all at the end, since they can't depend on each other.
                SqlOperatorTable newFunctions = SqlOperatorTables.of(rustFunctions);
                this.sqlToRelCompiler.addOperatorTable(newFunctions);
            }

            // Compile all statements which do not define functions or types
            for (ParsedStatement node : parsed) {
                currentViewPosition = SqlParserPos.ZERO;
                SqlKind kind = node.statement().getKind();
                if (kind == SqlKind.CREATE_FUNCTION || kind == SqlKind.CREATE_TYPE)
                    continue;
                if (node.statement() instanceof SqlLateness)
                    continue;

                RelStatement fe;
                if (node.statement() instanceof SqlCreateView cv) {
                    ProgramIdentifier viewName = Utilities.toIdentifier(cv.name);
                    Map<ProgramIdentifier, SqlLateness> late = this.viewLateness.getOrDefault(viewName, new HashMap<>());
                    fe = this.sqlToRelCompiler.compileCreateView(node, late, this.sources);
                } else {
                    fe = this.sqlToRelCompiler.compile(node, this.sources);
                }
                if (fe == null)
                    // error during compilation
                    continue;

                if (fe.is(CreateViewStatement.class)) {
                    CreateViewStatement cv = fe.to(CreateViewStatement.class);
                    PropertyList properties = cv.getProperties();
                    if (properties != null)
                        properties.checkKnownProperties(this::validateViewProperty);
                    currentViewPosition = cv.createView.getParserPosition();
                    this.views.put(cv.getName(), cv);
                } else if (fe.is(CreateTableStatement.class)) {
                    CreateTableStatement ct = fe.to(CreateTableStatement.class);
                    PropertyList properties = ct.getProperties();
                    if (properties != null)
                        properties.checkKnownProperties(this::validateTableProperty);
                    foreignKeys.addAll(ct.foreignKeys);
                }
                this.relToDBSPCompiler.compile(fe);
            }

            this.sqlToRelCompiler.endCompilation(this.compiler());
            DBSPCircuit circuit = this.relToDBSPCompiler.getFinalCircuit();
            if (this.getDebugLevel() > 0)
                ToDot.dump(this, "initial.png", this.getDebugLevel(), "png", circuit);

            this.validateForeignKeys(circuit, foreignKeys);
            circuit = this.optimize(circuit);
            return circuit;
        } catch (CalciteContextException e) {
            this.messages.reportError(e);
            this.rethrow(e, currentViewPosition);
        } catch (CalciteException e) {
            this.messages.reportError(e);
            this.rethrow(e, currentViewPosition);
        } catch (BaseCompilerException e) {
            this.messages.reportError(e);
            this.rethrow(e, currentViewPosition);
        } catch (RuntimeException e) {
            Throwable current = e;
            boolean handled = false;
            while (current.getCause() != null) {
                // Try to find a nicer cause
                Throwable t = current.getCause();
                if (t instanceof CalciteException ex) {
                    this.messages.reportError(ex);
                    this.rethrow(ex, currentViewPosition);
                    handled = true;
                }
                current = t;
            }
            if (!handled) {
                this.messages.reportError(e);
                this.rethrow(e, currentViewPosition);
            }
        } catch (Throwable e) {
            this.messages.reportError(e);
            this.rethrow(new RuntimeException(e), currentViewPosition);
        }
        return null;
    }

    void rethrow(RuntimeException e, SqlParserPos pos) {
        if (this.options.languageOptions.throwOnError) {
            this.printMessages(pos);
            throw e;
        }
    }

    public ObjectNode getIOMetadataAsJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ArrayNode inputs = mapper.createArrayNode();
        for (IHasSchema input: this.metadata.inputTables.values())
            inputs.add(input.asJson());
        ArrayNode outputs = mapper.createArrayNode();
        for (IHasSchema output: this.metadata.outputViews.values())
            outputs.add(output.asJson());
        ObjectNode ios = mapper.createObjectNode();
        ios.set("inputs", inputs);
        ios.set("outputs", outputs);
        return ios;
    }

    public void submitStatementsForCompilation(String program) {
        this.setSource(InputSource.API);
        this.compileInternal(program, true);
    }

    @Nullable DBSPCircuit optimize(@Nullable DBSPCircuit circuit) {
        if (circuit == null) return circuit;
        CircuitOptimizer optimizer = new CircuitOptimizer(this);
        return optimizer.optimize(circuit);
    }

    public void removeTable(ProgramIdentifier name) {
        this.metadata.removeTable(name);
        this.relToDBSPCompiler.getTableContents().removeTable(name);
    }

    public void compileStatement(String statement) {
        this.compileInternal(statement, false);
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
        this.compileInternal(this.sources.getWholeProgram(), true);
    }

    public boolean hasErrors() {
        return this.messages.exitCode != 0;
    }

    /** Perform checks that can only be made after all the statements have been compiled */
    void postCompilationChecks() {
        // Check that all views mentioned in LATENESS statements exist
        for (ProgramIdentifier view: this.viewLateness.keySet()) {
            if (!this.views.containsKey(view)) {
                for (SqlLateness late : this.viewLateness.get(view).values()) {
                    this.compiler().reportWarning(
                            new SourcePositionRange(late.getParserPosition()), "No such view",
                            "View " + view.singleQuote() + " used in LATENESS statement not found");
                }
            }
        }
    }

    private static long compileStartTime = 0;

    public static long elapsedTimeInMs() {
        return System.currentTimeMillis() - compileStartTime;
    }

    /** Run all compilation stages.
     * Get the circuit generated by compiling the statements to far. */
    @Nullable public DBSPCircuit getFinalCircuit(boolean temporary) {
        compileStartTime = System.currentTimeMillis();
        DBSPCircuit circuit = this.runAllCompilerStages();
        this.postCompilationChecks();

        if (this.getDebugLevel() > 0 && !temporary && circuit != null) {
            ToDot.dump(this, "final.png", this.getDebugLevel(), "png", circuit);
        }
        if (this.options.ioOptions.verbosity > 1)
            System.out.println("Compilation took " + elapsedTimeInMs() + "ms");
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Compilation time ")
                .append(elapsedTimeInMs() + "ms")
                .newline();
        return circuit;
    }

    /** Get the contents of the tables as a result of all the INSERT statements compiled. */
    public TableContents getTableContents() {
        this.runAllCompilerStages();
        return this.relToDBSPCompiler.getTableContents();
    }

    /** Empty the contents of all tables that were populated by INSERT or DELETE statements */
    public void clearTables() {
        this.relToDBSPCompiler.clearTables();
    }

    public void showErrors(PrintStream stream) {
        this.messages.show(stream);
    }

    /** Throw if any error has been encountered.
     * Displays the errors on stderr as well. */
    public void throwIfErrorsOccurred() {
        if (this.hasErrors()) {
            this.showErrors(System.err);
            throw new CompilationError("Error during compilation");
        }
    }
}
