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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.dbsp.sqlCompiler.compiler.frontend.SqlComment;
import org.dbsp.sqlCompiler.compiler.frontend.SqlCommentParser;
import org.dbsp.sqlCompiler.compiler.frontend.SqlPrettyPrinter;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDot;
import org.dbsp.sqlCompiler.compiler.backend.CircuitDumps;
import org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourceFileContents;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CteToLocalViews;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RenameIdentifiers;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.WarnFloatingPointEquality;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateAggregate;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateAggregateStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateFunctionStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateIndexStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.RelStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlLateness;
import org.dbsp.sqlCompiler.compiler.visitors.VisitorProfiles;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitOptimizer;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ToJsonVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelJsonWriter;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.calcite.util.Static.RESOURCE;

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

    /** Row weight shared by front-end aggregates and their later expansion. Restored from
     * the decoded circuit when resuming at an internal file boundary. */
    public DBSPVariablePath weightVar;

    /** SQL compilation owns Calcite state; circuit replay restores its results instead. */
    private record FrontEnd(SqlToRelCompiler sql, CalciteToDBSPCompiler relational) { }
    private enum FrontEndMode {
        SQL, DEFER_UNTIL_SQL_INPUT, REPLAY
    }
    private final FrontEndMode frontEndMode;
    @Nullable private FrontEnd frontEnd;
    private FrontEnd createFrontEnd() {
        return new FrontEnd(new SqlToRelCompiler(this.options, this),
                new CalciteToDBSPCompiler(this.options, this, this.metadata));
    }

    private FrontEnd frontEnd() {
        if (this.frontEndMode == FrontEndMode.REPLAY)
            throw new IllegalStateException("SQL front end is unavailable during circuit replay");
        if (this.frontEnd == null)
            this.frontEnd = this.createFrontEnd();
        return this.frontEnd;
    }

    public SqlToRelCompiler getSqlToRelCompiler() {
        return this.frontEnd().sql();
    }

    /** Expression construction is also needed when lowering already resolved input. */
    public org.apache.calcite.rex.RexBuilder getRexBuilder() {
        return this.getSqlToRelCompiler().getRexBuilder();
    }

    private CalciteToDBSPCompiler getRelToDBSPCompiler() {
        return this.frontEnd().relational();
    }
    public final CompilerOptions options;
    public final CompilerMessages messages;
    public final SourceFileContents sources;
    public InputSource inputSources = InputSource.None;
    public final ProgramMetadata metadata;
    /** The view that each Calcite relational operator was compiled for */
    public final ViewOrigins viewOrigins = new ViewOrigins();
    /** Receives the optimizer before it runs and may insert passes into it.
     * Testing hook, for observing intermediate circuits: production code must leave it null. */
    @Nullable
    public Consumer<CircuitOptimizer> optimizerHook = null;

    /** Test-only synchronous observer of each statement at entry to relational
     * lowering. Production leaves this unset. The observer runs before mutation. */
    @Nullable public Consumer<RelStatement> relationalObservationHook = null;

    /** Test-only boundaries for separating frontend diagnostics from lowering
     * diagnostics. Observers must not modify compilation state. */
    @Nullable public Runnable relationalCompletionHook = null;
    @Nullable public Runnable bindingCompletionHook = null;

    /** Test-only streaming alternative to dump files. Receives compact observation JSON. */
    @Nullable public Consumer<String> circuitObservationHook = null;

    /** Test-only observer of exact pre-hash strings; production leaves this unset. */
    @Nullable public java.util.function.BiConsumer<String, String> hashInputHook = null;

    public boolean usesCircuitSeam() {
        CompilerOptions.IO io = this.options.ioOptions;
        return this.circuitObservationHook != null || !io.dumpCircuits.isEmpty()
                || !io.writeS2.isEmpty() || !io.writeS3.isEmpty() || io.readS2 || io.readS3
                || io.rustOptimizerPasses;
    }

    public void observeHashInput(String kind, String text) {
        if (this.hashInputHook != null)
            this.hashInputHook.accept(kind, text);
    }

    @Nullable private CircuitDumps circuitDumps = null;
    private int dumpedCompilations = 0;

    /** Test-only read-only observation of pass instances and their side data.
     * Implementations must not mutate passes, evaluate predicates, or retain
     * compiler objects beyond the compilation under observation. */
    public interface StateObserver {
        void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit);
        void after(String container, int index, int iteration, CircuitTransform pass,
                   @Nullable DBSPCircuit circuit, boolean completed);
        default void inPlace(String container, int index, String operation, DBSPCircuit circuit) { }
        /** A hybrid analysis requested an optional result from Rust. */
        default void nativeDecision(String operation, boolean answered) { }
        default void monotonicityTransfer(String operator, String rule) { }
    }

    @Nullable public StateObserver stateObserver;

    /** Run a pass with optional test observation; production calls allocate no dump state. */
    public DBSPCircuit applyObservedPass(String container, int index, int iteration,
                                        CircuitTransform pass, DBSPCircuit circuit) {
        if (this.stateObserver == null)
            return this.circuitDumps == null ? pass.apply(circuit)
                    : this.circuitDumps.apply(container, index, iteration, pass, circuit);
        this.stateObserver.before(container, index, iteration, pass, circuit);
        DBSPCircuit result = null;
        boolean completed = false;
        try {
            result = this.circuitDumps == null ? pass.apply(circuit)
                    : this.circuitDumps.apply(container, index, iteration, pass, circuit);
            completed = true;
            return result;
        } finally {
            this.stateObserver.after(container, index, iteration, pass, result, completed);
        }
    }

    /** Record an in-place circuit operation when differential-test capture is enabled. */
    public void observeCircuit(String container, int index, String operation, DBSPCircuit circuit) {
        if (this.circuitDumps != null && !this.hasErrors())
            this.circuitDumps.observe(container, index, operation, circuit);
        if (this.stateObserver != null)
            this.stateObserver.inPlace(container, index, operation, circuit);
    }

    /** Keep the capture open through the caller's final checks and clear it on every exit. */
    @Nullable private DBSPCircuit withCircuitDumps(Supplier<DBSPCircuit> compilation) {
        try {
            DBSPCircuit circuit = compilation.get();
            if (circuit != null && !this.hasErrors() && this.circuitDumps != null)
                this.circuitDumps.complete(this.options.ioOptions.inputCircuit ? "S2" : "S3");
            return circuit;
        } finally {
            this.circuitDumps = null;
        }
    }

    public final TypeCompiler typeCompiler;
    public boolean hasWarnings;
    // For each view the list of columns declared with lateness
    final Map<ProgramIdentifier, Map<ProgramIdentifier, SqlLateness>> viewLateness = new HashMap<>();

    /** A late warning needs the view spelling and source range, not a live SQL AST. */
    public record UnresolvedViewLateness(String quotedView, SourcePositionRange range) { }
    @Nullable private List<UnresolvedViewLateness> replayedViewLateness;

    public List<UnresolvedViewLateness> unresolvedViewLateness() {
        if (this.replayedViewLateness != null)
            return this.replayedViewLateness;
        List<UnresolvedViewLateness> result = new ArrayList<>();
        for (var entry: this.viewLateness.entrySet()) {
            if (!this.views.containsKey(entry.getKey())) {
                for (SqlLateness late: entry.getValue().values())
                    result.add(new UnresolvedViewLateness(entry.getKey().singleQuote(),
                            new SourcePositionRange(late.getParserPosition())));
            }
        }
        return result;
    }

    public void restoreUnresolvedViewLateness(List<UnresolvedViewLateness> pending) {
        Utilities.enforce(this.replayedViewLateness == null && this.viewLateness.isEmpty()
                && this.views.isEmpty(), () -> "Pending LATENESS checks require a fresh compiler");
        this.replayedViewLateness = List.copyOf(pending);
    }

    final Map<ProgramIdentifier, CreateViewStatement> views = new HashMap<>();
    final Map<ProgramIdentifier, CreateIndexStatement> indexes = new HashMap<>();

    public DBSPCompiler(CompilerOptions options) {
        this(options, false);
    }

    /** Construct a context for S2/S3 replay without initializing the SQL front end. */
    public static DBSPCompiler forCircuitReplay(CompilerOptions options) {
        return new DBSPCompiler(options, true);
    }

    /** Read and validate the CLI input before allocating the Calcite front end. */
    public static DBSPCompiler forSqlInput(CompilerOptions options) {
        return new DBSPCompiler(options, FrontEndMode.DEFER_UNTIL_SQL_INPUT);
    }

    protected DBSPCompiler(CompilerOptions options, boolean replay) {
        this(options, replay ? FrontEndMode.REPLAY : FrontEndMode.SQL);
    }

    private DBSPCompiler(CompilerOptions options, FrontEndMode mode) {
        this.options = options;
        // Setting these first allows errors to be reported
        this.messages = new CompilerMessages(this);
        this.metadata = new ProgramMetadata();
        this.frontEndMode = mode;
        this.frontEnd = mode == FrontEndMode.SQL ? this.createFrontEnd() : null;
        this.sources = new SourceFileContents();
        this.typeCompiler = new TypeCompiler(this);
        this.weightVar = DBSPTypeWeight.INSTANCE.var();
        this.start();
    }

    public void getDataflow(Appendable appendable, DBSPCircuit circuit) {
        IIndentStream result = new IndentStream(appendable).setIndentAmount(2);
        result.append("{").increase();
        result.appendJsonLabelAndColon("calcite_plan");
        Map<RelNode, Integer> remap = this.getPlans(result);
        result.append(",").newline();
        result.appendJsonLabelAndColon("mir");
        ToJsonVisitor toJson = new ToJsonVisitor(
                this, result, this.options.ioOptions.verbosity, remap);
        toJson.apply(circuit);
        result.append(",").newline();
        result.appendJsonLabelAndColon("sources");
        this.writeSourcesAsJson(result);
        result.newline().decrease().append("}");
    }

    @Nullable private String replayPlans;

    /** Carry the front-end plan without reconstructing executable Calcite objects. */
    public void restorePlans(String plans) {
        if (this.replayPlans != null || !this.views.isEmpty())
            throw new IllegalStateException("Plan restoration requires a fresh compiler");
        this.replayPlans = plans;
    }

    /** Write the plans to the specified appendable.  Return a Map that renumbers RelNodes */
    public Map<RelNode, Integer> getPlans(IIndentStream stream) {
        if (this.replayPlans != null) {
            stream.append(this.replayPlans);
            return Map.of();
        }
        stream.append("{").increase();
        boolean first = true;
        List<ProgramIdentifier> sorted = Linq.list(this.views.keySet());
        sorted.sort(Comparator.comparing(ProgramIdentifier::name));
        Map<RelNode, Integer> result = new HashMap<>();
        for (ProgramIdentifier e: sorted) {
            CreateViewStatement cv = this.views.get(e);
            if (!first) {
                stream.append(",").newline();
            }
            first = false;
            stream.append("\"").append(Utilities.escapeDoubleQuotes(e.name())).append("\"");
            stream.append(": ");
            RelNode rel = cv.getRel();
            RelJsonWriter planWriter = this.usesCircuitSeam()
                    ? RelJsonWriter.forReplay(result, options.ioOptions.verbosity)
                    : new RelJsonWriter(result, options.ioOptions.verbosity);
            rel.explain(planWriter);
            String json = planWriter.asString();
            stream.appendIndentedStrings(json);
        }
        stream.newline().decrease().append("}");
        return result;
    }

    public void writeSourcesAsJson(IIndentStream stream) {
        this.sources.writeAsJson(stream);
    }

    // Will be overwritten in the start() function.
    public static ProgramIdentifier NOW_TABLE_NAME = new ProgramIdentifier("", false);
    public static ProgramIdentifier ERROR_TABLE_NAME = new ProgramIdentifier("", false);
    public static ProgramIdentifier ERROR_VIEW_NAME = new ProgramIdentifier("", false);

    /** Running time of each compilation phase; printed with -TVisitorProfiles=1 */
    public static final VisitorProfiles phases = new VisitorProfiles();

    /** Run 'action' and add its running time to the named compilation phase */
    public static <T> T timed(String phase, Supplier<T> action) {
        phases.start(phase);
        try {
            return action.get();
        } finally {
            phases.stop(phase);
        }
    }

    public static void timedRun(String phase, Runnable action) {
        phases.start(phase);
        try {
            action.run();
        } finally {
            phases.stop(phase);
        }
    }

    // Steps executed before the actual compilation.
    void start() {
        InnerVisitor.profiles.clear();
        CircuitVisitor.profiles.clear();

        // Compute the names based on the compiler flags.
        NOW_TABLE_NAME = this.canonicalName("now", false);
        ERROR_TABLE_NAME = this.canonicalName("FELDERA_ERROR_TABLE", false);
        ERROR_VIEW_NAME = this.canonicalName("ERROR_VIEW", false);
        if (this.options.ioOptions.raw)
            return;

        // Declare the system tables
        this.compileInternal(
                """
                CREATE TABLE NOW(now TIMESTAMP NOT NULL); -- LATENESS INTERVAL 0 SECONDS
                -- next table will be deleted after view is hooked to proper inputs
                CREATE TABLE FELDERA_ERROR_TABLE(table_or_view_name VARCHAR NOT NULL, message VARCHAR NOT NULL, metadata VARCHAR NOT NULL);
                CREATE VIEW ERROR_VIEW AS SELECT * FROM FELDERA_ERROR_TABLE;
                """,
                true, false);
    }

    public boolean hasWarnings() {
        return this.hasWarnings;
    }

    public CustomFunctions getCustomFunctions() {
        return this.getSqlToRelCompiler().getCustomFunctions();
    }

    @Override
    public DBSPCompiler compiler() {
        return this;
    }

    public TypeCompiler getTypeCompiler() {
        return this.typeCompiler;
    }

    public void registerUDT(ProgramIdentifier name, DBSPTypeStruct type) {
        this.globalTypes.register(name, type);
    }

    @Nullable
    public DBSPTypeStruct getStructByName(ProgramIdentifier name) {
        return this.globalTypes.getStructByName(name);
    }

    public boolean isStructConstructor(ProgramIdentifier name) {
        return this.globalTypes.containsStruct(name);
    }

    @Override
    public void setErrorContext(SourcePositionRange range) {
        this.messages.setErrorContext(range);
    }

    /** Point error messages at {@code node}, unless the compiler synthesized it. */
    void setErrorContext(ParsedStatement node) {
        this.setErrorContext(node.visible()
                ? new SourcePositionRange(node.statement().getParserPosition())
                : SourcePositionRange.INVALID);
    }

    /** Compile the SET statements in {@code parsed}, ignoring all other statements. */
    void compileSqlSettings(List<ParsedStatement> parsed) {
        for (ParsedStatement node : parsed) {
            if (node.statement().getKind() != SqlKind.SET_OPTION)
                continue;
            this.setErrorContext(node);
            RelStatement set = this.getSqlToRelCompiler().compile(node, this.sources);
            if (set != null)
                this.getRelToDBSPCompiler().compile(set);
        }
        this.setErrorContext(SourcePositionRange.INVALID);
    }

    static final String WARNINGS_ARE_ERRORS = "FELDERA_WARNINGS_ARE_ERRORS";

    boolean warningsAreErrors() {
        return this.metadata.hasValue(WARNINGS_ARE_ERRORS) && !this.metadata.isFalsy(WARNINGS_ARE_ERRORS);
    }

    /** Name of the SET variable that silences all warnings with the given error type */
    public static String silencingVariable(String errorType) {
        return "FELDERA_IGNORE_WARNING_" + errorType.toUpperCase(Locale.ENGLISH).replace(" ", "_");
    }

    boolean warningIsSilenced(String warning) {
        String variable = silencingVariable(warning);
        return this.metadata.hasValue(variable) && !this.metadata.isFalsy(variable);
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
        if (warning) {
            if (this.warningIsSilenced(errorType))
                return;
        }
        if (this.warningsAreErrors())
            warning = false;
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
        this.getSqlToRelCompiler().addSchemaSource(name, schema);
    }

    record SqlStatements(String statement, boolean many, boolean visible) {}

    /** API submission boundaries affect parsing and source positions. The joined
     * source text alone cannot reproduce a sequence of single-statement calls. */
    public record SourceSubmission(String sql, boolean many, int batch) {}
    private final List<SourceSubmission> sourceSubmissions = new ArrayList<>();
    private int parsedBatches = 0;

    public List<SourceSubmission> getSourceSubmissions() {
        return List.copyOf(this.sourceSubmissions);
    }

    final List<SqlStatements> toCompile = new ArrayList<>();

    /** Accumulate program to compile.
     * @param statements Statements to compile.
     * @param many       Is this one or many statements?
     * @param appendToSource  If true this is part of the source supplied by the user.
     *                        Otherwise, it's a "preamble" generated internally.
     */
    private void compileInternal(String statements, boolean many, boolean appendToSource) {
        if (appendToSource)
            this.sourceSubmissions.add(new SourceSubmission(statements, many, this.parsedBatches));
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
            IInputOperator otherTable = circuit.getInput(otherTableName);
            if (otherTable == null) {
                this.reportWarning(other.tableName.getSourcePosition(),
                        "Table not found",
                        "Table " + otherTableName.singleQuote()
                                + ", referred in FOREIGN KEY constraint of table " +
                                thisTableName.singleQuote() + ", does not exist");
                continue;
            }

            IInputOperator thisTable = circuit.getInput(thisTableName);
            Utilities.enforce(thisTable != null);

            List<InputColumnMetadata> otherKeys = otherTable.getMetadata().getPrimaryKeys();
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
                InputColumnMetadata selfMeta = thisTable.getMetadata().getColumnMetadata(selfColumnName);
                if (selfMeta == null) {
                    this.reportError(selfColumn.getSourcePosition(),
                            "Column not found",
                            "Table " + thisTableName.singleQuote() +
                                    " does not have a column named " + selfColumnName.singleQuote());
                    continue;
                }
                InputColumnMetadata otherMeta = otherTable.getMetadata().getColumnMetadata(otherColumnName);
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

    void printMessages() {
        System.err.println(this.messages);
    }

    /** Parse using Calcite. */
    List<ParsedStatement> runParser() {
        this.parsedBatches++;
        List<ParsedStatement> parsed = new ArrayList<>();
        for (SqlStatements stat : this.toCompile) {
            try {
                if (stat.many) {
                    if (stat.statement.isEmpty()) {
                        this.getSqlToRelCompiler().emptyStatement();
                        continue;
                    }
                    parsed.addAll(this.getSqlToRelCompiler().parseStatements(stat.statement, stat.visible, false));
                } else {
                    parsed.add(this.getSqlToRelCompiler().parseStatement(stat.statement, stat.visible));
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
                    this.rethrow(new RuntimeException(e));
                }
            } catch (CalciteException e) {
                this.messages.reportError(e);
                this.rethrow(e);
            } catch (BaseCompilerException e) {
                this.messages.reportError(e);
                this.rethrow(new RuntimeException(e));
                parsed.clear();
            }
            catch (Throwable e) {
                this.messages.reportError(e);
                this.rethrow(new RuntimeException(e));
                parsed.clear();
            }
        }
        this.toCompile.clear();
        return parsed;
    }

    boolean validateCreateIndex(CreateIndexStatement statement) {
        ProgramIdentifier refersTo = statement.refersTo;
        if (this.metadata.hasTable(refersTo)) {
            this.reportWarning(
                    new SourcePositionRange(statement.namePosition),
                    "Indexed table",
                    "INDEX " + statement.indexName.singleQuote() + " refers to TABLE " +
                    statement.refersTo.singleQuote() + "; this has no effect.");
            return true;
        }
        if (!this.metadata.hasView(refersTo)) {
            this.reportError(
                    new SourcePositionRange(statement.targetPosition),
                    "Indexed object not found",
                        "Object with name " + statement.refersTo.singleQuote() +
                    " used in CREATE INDEX statement "  + statement.indexName.singleQuote() +
                    " does not exist.");
            return false;
        }
        CreateViewStatement view = Utilities.getExists(this.views, statement.refersTo);
        if (view.emitFinalColumn() >= 0) {
            this.reportError(
                    new SourcePositionRange(statement.targetPosition),
                    UnsupportedException.KIND,
                    "View " + view.relationName.singleQuote() + " has an 'emit_final' property, " +
                            "so it cannot have INDEX " + statement.indexName.singleQuote() + ".");
            return false;
        }
        int i = 0;
        for (ProgramIdentifier col: statement.columns) {
            int index = view.getColumnIndex(col);
            if (index < 0) {
                this.reportError(
                        new SourcePositionRange(statement.columnParserPosition(i)),
                        "Column not found",
                        "Column " + col.singleQuote() +
                                " used in CREATE INDEX statement "  + statement.indexName.singleQuote() +
                                " does not exist in view " + view.relationName.singleQuote());
                return false;
            }
            i++;
        }
        // Output connectors identify the rows of an indexed view by comparing their index keys
        List<RelDataTypeField> keyColumns = Linq.map(statement.columns,
                col -> view.columns.get(view.getColumnIndex(col)).field);
        WarnFloatingPointEquality.checkColumns(this,
                new SourcePositionRange(statement.getParserPosition()), "CREATE INDEX", keyColumns);
        return true;
    }

    void emitSql(List<ParsedStatement> statements) {
        final PrintStream outputStream;
        try {
            @Nullable String outputFile = this.options.ioOptions.outputFile;
            if (outputFile.isEmpty()) {
                outputStream = System.out;
            } else {
                outputStream = new PrintStream(Files.newOutputStream(Paths.get(outputFile)));
            }
        } catch (IOException e) {
            this.compiler().reportError(SourcePositionRange.INVALID,
                    "Error writing to output file", e.getMessage());
            return;
        }

        List<SqlComment> comments = this.options.ioOptions.format
                ? SqlCommentParser.parse(this.sources.getWholeProgram())
                : new ArrayList<>();
        IndentStream indentStream = new IndentStream(outputStream);
        SqlPrettyPrinter printer = new SqlPrettyPrinter(indentStream, comments);

        RenameIdentifiers renamer = new RenameIdentifiers();
        for (ParsedStatement stat : statements) {
            if (stat.visible()) {
                SqlNode node = stat.statement();
                if (this.options.ioOptions.anonymize)
                    node = Objects.requireNonNull(renamer.visitNode(node));
                Utilities.enforce(node != null);
                if (this.options.ioOptions.format) {
                    printer.print(node);
                    indentStream.append(";").newline();
                } else {
                    final SqlWriter sqlWriter = new SqlPrettyWriter(
                            SqlWriterConfig.of()
                                    .withDialect(CalciteSqlDialect.DEFAULT)
                                    .withQuoteAllIdentifiers(false)
                    );
                    node.unparse(sqlWriter, 0, 0);
                    outputStream.println(sqlWriter + ";");
                }
            }
        }
        if (this.options.ioOptions.format) {
            printer.flushRemainingComments();
        }
        outputStream.close();
    }

    /** Compile a CREATE VIEW statement.  The view's top-level common table
     * expressions may be converted to LOCAL VIEWs.  The result contains
     * one statement per converted CTE, followed by the view itself. */
    List<RelStatement> compileCreateView(ParsedStatement node, Map<ProgramIdentifier, SqlLateness> lateness) {
        @Nullable List<ParsedStatement> parts = null;
        if (this.options.languageOptions.cteViews)
            parts = this.getSqlToRelCompiler().hoistCtes(node);
        if (parts == null) {
            try {
                RelStatement fe = this.getSqlToRelCompiler().compileCreateView(
                        node, lateness, this.sources, /* retry */ true);
                return fe == null ? Linq.list() : Linq.list(fe);
            } catch (CteToLocalViews.Retry retry) {
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("Could not decorrelate query; retrying with CTEs as local views")
                        .newline();
                parts = this.getSqlToRelCompiler().hoistCtes(node);
            }
            if (parts == null) {
                // The rewrite turned out not to apply; compile again without
                // retry, so the real error surfaces.
                RelStatement fe = this.getSqlToRelCompiler().compileCreateView(
                        node, lateness, this.sources, /* retry */ false);
                return fe == null ? Linq.list() : Linq.list(fe);
            }
        }
        List<RelStatement> result = new ArrayList<>();
        ParsedStatement view = Utilities.last(parts);
        for (ParsedStatement part: parts) {
            // Lateness declarations apply to the original view only
            Map<ProgramIdentifier, SqlLateness> lat = part == view ? lateness : new HashMap<>();
            result.addAll(this.compileCreateView(part, lat));
        }
        return result;
    }

    @Nullable DBSPCircuit runAllCompilerStages() {
        return this.withCircuitDumps(this::compileStages);
    }

    @Nullable private DBSPCircuit compileStages() {
        List<ParsedStatement> parsed = timed("Parse", this::runParser);
        if (this.hasErrors())
            return null;

        try {
            // SET statements apply to the whole program irrespective of their position,
            // so they are compiled before the other statements are even validated:
            // validation emits warnings which the settings may silence or turn into errors.
            this.compileSqlSettings(parsed);
            parsed = Linq.map(parsed, this.getSqlToRelCompiler()::postParsingProcess);
            if (this.hasErrors())
                return null;

            if (this.options.ioOptions.anonymize || this.options.ioOptions.format) {
                this.emitSql(parsed);
                return null;
            }

            // across all tables
            List<ForeignKey> foreignKeys = new ArrayList<>();
            // All UDFs which have no bodies in SQL
            final List<SqlFunction> rustFunctions = new ArrayList<>();
            final List<SqlUserDefinedAggFunction> aggregateFunctions = new ArrayList<>();

            // Compile first the statements that define functions, types, and lateness
            for (ParsedStatement node: parsed) {
                if (node.visible())
                    Logger.INSTANCE.belowLevel(this, 4)
                        .append("Parsing result: ")
                        .appendSupplier(node::toString)
                        .newline();
                SqlKind kind = node.statement().getKind();
                if (kind == SqlKind.CREATE_TYPE) {
                    RelStatement fe = timed("Calcite", () -> this.getSqlToRelCompiler().compileCreateType(node));
                    if (fe == null)
                        // error during compilation
                        continue;
                    timedRun("RelToDBSP", () -> this.getRelToDBSPCompiler().compile(fe));
                    continue;
                }
                if (kind == SqlKind.OTHER && node.statement() instanceof SqlCreateAggregate) {
                    CreateAggregateStatement stat = timed("Calcite", () -> this.getSqlToRelCompiler().compileCreateAggregate(node, this.sources));
                    boolean exists = this.getSqlToRelCompiler().functionExists(stat.function.getName());
                    if (exists) {
                        throw new CompilationError("A function named " + Utilities.singleQuote(stat.function.getName()) +
                                " is already predefined, or the name is reserved.\nPlease consider using a " +
                                "different name for the user-defined aggregate",
                                stat.getCalciteObject());
                    }
                    aggregateFunctions.add(stat.function);
                    timedRun("RelToDBSP", () -> this.getRelToDBSPCompiler().compile(stat));
                }
                if (kind == SqlKind.CREATE_FUNCTION) {
                    CreateFunctionStatement stat = timed("Calcite", () -> this.getSqlToRelCompiler().compileCreateFunction(node, this.sources));
                    boolean exists = this.getSqlToRelCompiler().functionExists(stat.function.getName());
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
                        this.getSqlToRelCompiler().addOperatorTable(newFunctions);
                    }
                    timedRun("RelToDBSP", () -> this.getRelToDBSPCompiler().compile(stat));
                }
                if (node.statement() instanceof SqlLateness lateness) {
                    ProgramIdentifier view = ProgramIdentifier.fromSqlId(lateness.getView());
                    ProgramIdentifier column = ProgramIdentifier.fromSqlId(lateness.getColumn());
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

            if (!rustFunctions.isEmpty() || !aggregateFunctions.isEmpty()) {
                // Reload the operator table to include the newly defined Rust function.
                // These we can load all at the end, since they can't depend on each other.
                SqlOperatorTable newFunctions = SqlOperatorTables.of(rustFunctions);
                this.getSqlToRelCompiler().addOperatorTable(newFunctions);
                SqlOperatorTable newAggregates = SqlOperatorTables.of(aggregateFunctions);
                this.getSqlToRelCompiler().addOperatorTable(newAggregates);
            }

            // Compile all statements which do not define functions, types, or settings
            for (ParsedStatement node : parsed) {
                this.setErrorContext(node);
                SqlKind kind = node.statement().getKind();
                if (kind == SqlKind.CREATE_FUNCTION || kind == SqlKind.CREATE_TYPE || kind == SqlKind.SET_OPTION)
                    continue;
                if (node.statement() instanceof SqlLateness)
                    continue;

                List<RelStatement> compiled;
                if (node.statement() instanceof SqlCreateView cv) {
                    ProgramIdentifier viewName = ProgramIdentifier.fromSqlId(cv.name);
                    Map<ProgramIdentifier, SqlLateness> lateness = this.viewLateness.getOrDefault(viewName, new HashMap<>());
                    compiled = timed("Calcite", () -> this.compileCreateView(node, lateness));
                } else {
                    RelStatement single = timed("Calcite", () -> this.getSqlToRelCompiler().compile(node, this.sources));
                    compiled = single == null
                            // error during compilation
                            ? Linq.list()
                            : Linq.list(single);
                }

                for (RelStatement fe: compiled) {
                    if (fe.is(CreateViewStatement.class)) {
                        CreateViewStatement cv = fe.to(CreateViewStatement.class);
                        Utilities.putNew(this.views, cv.getName(), cv);
                    } else if (fe.is(CreateTableStatement.class)) {
                        CreateTableStatement ct = fe.to(CreateTableStatement.class);
                        foreignKeys.addAll(ct.foreignKeys);
                    } else if (fe.is(CreateIndexStatement.class)) {
                        CreateIndexStatement ct = fe.to(CreateIndexStatement.class);
                        boolean success = this.validateCreateIndex(ct);
                        if (!success)
                            return null;
                        Utilities.putNew(this.indexes, ct.getName(), ct);
                    }
                    timedRun("RelToDBSP", () -> this.getRelToDBSPCompiler().compile(fe));
                }
            }
            this.setErrorContext(SourcePositionRange.INVALID);

            timedRun("Calcite", () -> this.getSqlToRelCompiler().endCompilation(this.compiler()));
            if (this.bindingCompletionHook != null)
                this.bindingCompletionHook.run();
            DBSPCircuit circuit = this.getRelToDBSPCompiler().getFinalCircuit();
            if (circuit == null)
                return null;
            if (this.getDebugLevel() > 0)
                ToDot.dump(this, "initial.png", this.getDebugLevel(), "png", circuit);

            this.validateForeignKeys(circuit, foreignKeys);
            if (this.messages.exitCode == 0 && (!this.options.ioOptions.dumpCircuits.isEmpty()
                    || this.circuitObservationHook != null)) {
                this.circuitDumps = new CircuitDumps(this, this.dumpedCompilations++);
            }
            if (this.circuitDumps != null)
                this.circuitDumps.write("S2", circuit);
            if (!this.options.ioOptions.inputCircuit) {
                DBSPCircuit unoptimized = circuit;
                circuit = timed("Optimizer", () -> this.optimize(unoptimized));
            }
            if (this.circuitDumps != null && !this.hasErrors() && !this.options.ioOptions.inputCircuit)
                this.circuitDumps.write("S3", circuit);
            return circuit;
        } catch (CalciteContextException e) {
            CompilationError e0 = this.improveErrorMessage(e);
            this.messages.reportError(e0);
            this.rethrow(e0);
        } catch (CalciteException e) {
            this.messages.reportError(e);
            this.rethrow(e);
        } catch (BaseCompilerException e) {
            this.messages.reportError(e);
            this.rethrow(e);
        } catch (RuntimeException e) {
            Throwable current = e;
            boolean handled = false;
            while (current.getCause() != null) {
                // Try to find a nicer cause
                Throwable t = current.getCause();
                if (t instanceof CalciteException ex) {
                    this.messages.reportError(ex);
                    this.rethrow(ex);
                    handled = true;
                } else if (t instanceof SqlParseException ex) {
                    this.messages.reportError(ex);
                    this.rethrow(e);
                    handled = true;
                }
                current = t;
            }
            if (!handled) {
                this.messages.reportError(e);
                this.rethrow(e);
            }
        } catch (Throwable e) {
            this.messages.reportError(e);
            this.rethrow(new RuntimeException(e));
        }
        return null;
    }

    static final Pattern ITEM_ERROR = Pattern.compile(
            "Cannot apply 'ITEM' to arguments of type 'ITEM\\(([^,]+), ([^']+)\\)'(.*)", Pattern.DOTALL);

    static SourcePositionRange getRange(CalciteContextException e) {
        return new SourcePositionRange(
                new SourcePosition(e.getPosLine(), e.getPosColumn()),
                new SourcePosition(e.getEndPosLine(), e.getEndPosColumn()));
    }

    public static final Documentation.Link DISTINCT_ON_DOCUMENTATION =
            new Documentation.Link("sql/unsupported-operations", "select-distinct-on");

    /** Rewrite the error message for some Calcite errors which are confusing */
    private CompilationError improveErrorMessage(CalciteContextException e) {
        // Calcite's error message blames the SQL conformance level, which users cannot change
        Throwable cause = e.getCause();
        if (cause != null && RESOURCE.distinctOnNotAllowed().str().equals(cause.getMessage()))
            return new CompilationError(getRange(e), UnsupportedException.KIND,
                    "SELECT DISTINCT ON is not supported.\n" + DISTINCT_ON_DOCUMENTATION.citation());
        String message = e.getMessage();
        if (message != null && !message.isEmpty()) {
            var matcher = ITEM_ERROR.matcher(message);
            if (matcher.find()) {
                String source = matcher.group(1);
                String index = matcher.group(2);
                String tail = matcher.group(3);
                String newMessage = "Cannot apply indexing to arguments of type " + source + "[" + index + "]" + tail;
                return new CompilationError(newMessage, getRange(e));
            }
        }
        return new CompilationError(e);
    }

    void rethrow(RuntimeException e) {
        if (this.options.languageOptions.throwOnError) {
            if (!this.options.ioOptions.quiet)
                this.printMessages();
            throw e;
        }
    }

    public ObjectNode getIOMetadataAsJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ArrayNode inputs = mapper.createArrayNode();
        for (ProgramSchema input: this.metadata.inputTables.values())
            inputs.add(input.asJson(false));
        ArrayNode outputs = mapper.createArrayNode();
        for (ProgramSchema output: this.metadata.outputViews.values())
            outputs.add(output.asJson(false));
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
        if (circuit == null) return null;
        CircuitOptimizer optimizer = new CircuitOptimizer(this);
        if (this.optimizerHook != null)
            this.optimizerHook.accept(optimizer);
        return optimizer.optimize(circuit);
    }

    /** Run the full optimizer prefix after an experimental S2 import. Analyses
     * and their consumers share one compiler and optimizer lifetime, as in SQL
     * compilation; no isolated S4 checkpoint is accepted. */
    @Nullable public DBSPCircuit optimizeFromS2(DBSPCircuit circuit) {
        Utilities.enforce(circuit.metadata == this.metadata,
                () -> "A resumed circuit must share its compiler's metadata");
        return this.withCircuitDumps(() -> {
            if (!this.options.ioOptions.dumpCircuits.isEmpty() || this.circuitObservationHook != null)
                this.circuitDumps = new CircuitDumps(this, this.dumpedCompilations++);
            if (this.circuitDumps != null)
                this.circuitDumps.write("S2", circuit);
            DBSPCircuit result = this.optimize(circuit);
            if (result != null && !this.hasErrors())
                new org.dbsp.sqlCompiler.compiler.visitors.outer.ValidateStreamKinds(this).apply(result);
            if (this.circuitDumps != null && result != null && !this.hasErrors())
                this.circuitDumps.write("S3", result);
            if (result != null)
                this.postCompilationChecks();
            return result;
        });
    }

    /** Resume an optimized circuit at the Java tail. Recompute hashes from the
     * decoded objects: copied annotations are not evidence of hash-input parity. */
    @Nullable public DBSPCircuit finishFromS3(DBSPCircuit circuit) {
        Utilities.enforce(circuit.metadata == this.metadata,
                () -> "A resumed circuit must share its compiler's metadata");
        return this.withCircuitDumps(() -> {
            new org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.MerkleHashes(this).apply(circuit);
            if (this.hasErrors())
                return null;
            if (!this.options.ioOptions.dumpCircuits.isEmpty() || this.circuitObservationHook != null) {
                this.circuitDumps = new CircuitDumps(this, this.dumpedCompilations++);
                this.circuitDumps.write("S3", circuit);
            }
            return circuit;
        });
    }

    public void removeTable(ProgramIdentifier name) {
        this.metadata.removeTable(name);
        // Only SQL compilation accumulates INSERT/DELETE contents for runtime tests.
        if (this.frontEnd != null)
            this.frontEnd.relational().getTableContents().removeTable(name);
    }

    public void submitStatementForCompilation(String statement) {
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
        for (UnresolvedViewLateness late: this.unresolvedViewLateness())
            this.compiler().reportWarning(late.range(), "No such view",
                    "View " + late.quotedView() + " used in LATENESS statement not found");
    }

    private static long compileStartTime = 0;
    public static final DecimalFormat COMMA_FORMATTER = new DecimalFormat("#,###.##");

    public static String elapsedTimeInMs() {
        long ms = System.currentTimeMillis() - compileStartTime;
        return COMMA_FORMATTER.format(ms);
    }

    /** Run all compilation stages.
     * Get the circuit generated by compiling the statements to far. */
    @Nullable public DBSPCircuit getFinalCircuit(boolean temporary) {
        return this.withCircuitDumps(() -> this.finishCircuit(temporary));
    }

    @Nullable private DBSPCircuit finishCircuit(boolean temporary) {
        compileStartTime = System.currentTimeMillis();
        DBSPCircuit circuit = this.compileStages();
        // S2 is before the optimizer and these final checks. Replay carries the
        // pending checks and reports them after the optimizer, in the same order.
        if (circuit != null && !this.options.ioOptions.inputCircuit)
            this.postCompilationChecks();
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Compilation time ")
                .appendSupplier(() -> elapsedTimeInMs() + "ms")
                .newline();
        if (this.options.ioOptions.verbosity > 2)
            System.out.println("Compilation took " + elapsedTimeInMs() + "ms");

        if (this.getDebugLevel() > 0 && !temporary && circuit != null) {
            ToDot.dump(this, "final.png", this.getDebugLevel(), "png", circuit);
        }
        Logger.INSTANCE.belowLevel(VisitorProfiles.class, 1)
                .appendSupplier(() -> InnerVisitor.profiles.toString("Inner", 10))
                .newline()
                .appendSupplier(() -> CircuitVisitor.profiles.toString("Outer", 10))
                .newline();
        return circuit;
    }

    /** Get the contents of the tables as a result of all the INSERT statements compiled. */
    public TableContents getTableContents() {
        this.runAllCompilerStages();
        return this.getRelToDBSPCompiler().getTableContents();
    }

    /** Empty the contents of all tables that were populated by INSERT or DELETE statements */
    public void clearTables() {
        this.getRelToDBSPCompiler().clearTables();
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
