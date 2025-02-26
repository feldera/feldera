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

package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.MapEntry;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMapTypeNameSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlAttributeDefinition;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.ConvertToChecked;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.dbsp.generated.parser.DbspParserImpl;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourceFileContents;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateFunctionDeclaration;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateIndex;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlDeclareView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlExtendedColumnDeclaration;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragment;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentCharacterString;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlLateness;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlRemove;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateFunctionStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateIndexStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTypeStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DeclareViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DropTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.RelStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.TableModifyStatement;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.util.FreshName;
import org.dbsp.util.ICastable;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Properties;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The SqlToRel compiler compiles SQL into Calcite RelNode representations.
 * It is stateful.
 * A sequence of statements is supplied as SQL strings:
 * - table definition statements
 * - insert or remove statements
 * - type creation statements
 * - function creation statements
 * - view definition statements
 * - lateness statements
 * For each statement the compiler returns a RelStatement,
 * a representation roughly at the Rel level.
 * The front-end offers several APIs, invoked in this order:
 * - parse SQL (using {@link SqlToRelCompiler#parse})
 * - compile SqlNode to RelNode (using {@link SqlToRelCompiler#compile})
 * - optimize RelNode (using {@link SqlToRelCompiler#optimize(RelNode)})
 *   Optimize is called automatically from compile().
 */
public class SqlToRelCompiler implements IWritesLogs {
    private final CompilerOptions options;
    private final SqlParser.Config parserConfig;
    private final Catalog calciteCatalog;
    public final RelOptCluster cluster;
    private final SqlToRelConverter.Config converterConfig;
    /** Perform additional type validation in top of the Calcite rules. */
    @Nullable
    private SqlValidator validator;
    @Nullable
    private SqlToRelConverter converter;
    private final ExtraValidation extraValidator;
    private final CalciteConnectionConfig connectionConfig;
    private final IErrorReporter errorReporter;
    private final SchemaPlus rootSchema;
    private final CustomFunctions customFunctions;
    /** User-defined types */
    private final HashMap<ProgramIdentifier, RelStruct> udt;
    private final HashMap<ProgramIdentifier, DeclareViewStatement> declaredViews;
    /** Recursive views which have been referred */
    private final Set<ProgramIdentifier> usedViewDeclarations;
    /** Views which have been defined */
    private final Set<ProgramIdentifier> definedViews;

    public SqlToRelCompiler(CompilerOptions options, IErrorReporter errorReporter) {
        this.options = options;
        this.errorReporter = errorReporter;
        this.customFunctions = new CustomFunctions();
        this.definedViews = new HashSet<>();
        this.extraValidator = new ExtraValidation(errorReporter);

        Casing unquotedCasing = Casing.TO_UPPER;
        switch (options.languageOptions.unquotedCasing) {
            case "upper":
                //noinspection ReassignedVariable,DataFlowIssue
                unquotedCasing = Casing.TO_UPPER;
                break;
            case "lower":
                unquotedCasing = Casing.TO_LOWER;
                break;
            case "unchanged":
                unquotedCasing = Casing.UNCHANGED;
                break;
            default:
                errorReporter.reportError(SourcePositionRange.INVALID,
                        "Illegal option",
                        "Illegal value for option --unquotedCasing: " +
                                Utilities.singleQuote(options.languageOptions.unquotedCasing));
                // Continue execution.
        }

        // This influences function name lookup.
        // We want that to be case-insensitive.
        // Notice that this does NOT affect the parser, only the validator.
        java.util.Properties connConfigProp = new java.util.Properties();
        connConfigProp.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(false));
        this.udt = new HashMap<>();
        this.connectionConfig = new CalciteConnectionConfigImpl(connConfigProp);
        this.parserConfig = SqlParser.config()
                .withLex(options.languageOptions.lexicalRules)
                // Our own parser factory
                .withParserFactory(DbspParserImpl.FACTORY)
                .withUnquotedCasing(unquotedCasing)
                .withQuotedCasing(Casing.UNCHANGED)
                .withConformance(SqlConformanceEnum.LENIENT);
        this.calciteCatalog = new Catalog("schema");
        this.rootSchema = CalciteSchema.createRootSchema(false, false).plus();
        this.rootSchema.add(calciteCatalog.schemaName, this.calciteCatalog);
        // Register new types
        this.rootSchema.add("BYTEA", factory -> factory.createSqlType(SqlTypeName.VARBINARY));
        this.rootSchema.add("DATETIME", factory -> factory.createSqlType(SqlTypeName.TIMESTAMP));
        this.rootSchema.add("INT2", factory -> factory.createSqlType(SqlTypeName.SMALLINT));
        this.rootSchema.add("INT8", factory -> factory.createSqlType(SqlTypeName.BIGINT));
        this.rootSchema.add("INT4", factory -> factory.createSqlType(SqlTypeName.INTEGER));
        this.rootSchema.add("SIGNED", factory -> factory.createSqlType(SqlTypeName.INTEGER));
        this.rootSchema.add("INT64", factory -> factory.createSqlType(SqlTypeName.BIGINT));
        this.rootSchema.add("FLOAT64", factory -> factory.createSqlType(SqlTypeName.DOUBLE));
        this.rootSchema.add("FLOAT32", factory -> factory.createSqlType(SqlTypeName.REAL));
        this.rootSchema.add("FLOAT4", factory -> factory.createSqlType(SqlTypeName.REAL));
        this.rootSchema.add("FLOAT8", factory -> factory.createSqlType(SqlTypeName.DOUBLE));
        this.rootSchema.add("STRING", factory -> factory.createSqlType(SqlTypeName.VARCHAR));
        this.rootSchema.add("NUMBER", factory -> factory.createSqlType(SqlTypeName.DECIMAL));
        this.rootSchema.add("TEXT", factory -> factory.createSqlType(SqlTypeName.VARCHAR));
        this.rootSchema.add("BOOL", factory -> factory.createSqlType(SqlTypeName.BOOLEAN));

        // This planner does not do anything.
        // We use a series of planner stages later to perform the real optimizations.
        RelOptPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        planner.setExecutor(RexUtil.EXECUTOR);
        this.cluster = RelOptCluster.create(planner, new RexBuilder(TYPE_FACTORY));
        this.converterConfig = SqlToRelConverter.config()
                // Calcite recommends not using withExpand, but there are no
                // rules to decorrelate some queries that withExpand will produce,
                // e.g., AggScottTests.testAggregates4
                .withExpand(true)
                .withTrimUnusedFields(true)
                .withRelBuilderConfigTransform(t -> t.withSimplify(false))
                ;
        this.validator = null;
        this.converter = null;
        this.usedViewDeclarations = new HashSet<>();
        this.declaredViews = new HashMap<>();

        SqlOperatorTable operatorTable = this.createOperatorTable();
        this.addOperatorTable(operatorTable);
    }

    /** Create a copy of the 'source' compiler which can be used to compile
     * some generated SQL without affecting its data structures */
    public SqlToRelCompiler(SqlToRelCompiler source) {
        this.options = source.options;
        this.parserConfig = source.parserConfig;
        this.cluster = source.cluster;
        this.converterConfig = source.converterConfig;
        this.connectionConfig = source.connectionConfig;
        this.errorReporter = source.errorReporter;
        this.customFunctions = new CustomFunctions(source.customFunctions);
        this.calciteCatalog = new Catalog(source.calciteCatalog);
        this.udt = new HashMap<>(source.udt);
        this.declaredViews = new HashMap<>(source.declaredViews);
        this.rootSchema = CalciteSchema.createRootSchema(false, false).plus();
        this.copySchema(source.rootSchema);
        this.rootSchema.add(this.calciteCatalog.schemaName, this.calciteCatalog);
        this.usedViewDeclarations = new HashSet<>(source.usedViewDeclarations);
        this.definedViews = new HashSet<>(source.definedViews);
        this.extraValidator = source.extraValidator;
        this.addOperatorTable(Objects.requireNonNull(source.validator).getOperatorTable());
    }

    void copySchema(SchemaPlus source) {
        for (String name: source.getTableNames())
            this.rootSchema.add(name, Objects.requireNonNull(source.getTable(name)));
        for (String name: source.getTypeNames())
            this.rootSchema.add(name, Objects.requireNonNull(source.getType(name)));
        for (String name: source.getFunctionNames())
            for (Function function: source.getFunctions(name))
                this.rootSchema.add(name, function);
        for (String name: source.getSubSchemaNames())
            this.rootSchema.add(name, Objects.requireNonNull(source.getSubSchema(name)));
    }

    public CustomFunctions getCustomFunctions() {
        return this.customFunctions;
    }

    public static final RelDataTypeSystem TYPE_SYSTEM = new RelDataTypeSystemImpl() {
        @Override
        public int getMaxNumericPrecision() {
            return DBSPTypeDecimal.MAX_PRECISION;
        }
        @Override
        public int getMaxNumericScale() {
            return DBSPTypeDecimal.MAX_SCALE;
        }
        @Override
        public int getMaxPrecision(SqlTypeName typeName) {
            if (typeName.equals(SqlTypeName.TIME))
                return 9;
            return super.getMaxPrecision(typeName);
        }
        @Override
        public boolean shouldConvertRaggedUnionTypesToVarying() { return true; }
    };

    public static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(TYPE_SYSTEM);

    /** Invoked when front-end compilation is finished, to do additional validation */
    public void endCompilation(IErrorReporter reporter) {
        for (var declared: this.declaredViews.keySet()) {
            if (this.usedViewDeclarations.contains(declared) || this.definedViews.contains(declared))
                continue;
            DeclareViewStatement dv = this.declaredViews.get(declared);
            reporter.reportWarning(dv.getPosition(), "Unused view declaration",
                    "Declared recursive view " + Utilities.singleQuote(declared.toString()) + " not used");
        }
    }

    /** Additional validation tests on top of Calcite.
     * We need to do these before conversion to Rel, because Rel
     * does not have source position information anymore. */
    public class ExtraValidation extends SqlBasicVisitor<Void> {
        final IErrorReporter errorReporter;

        public ExtraValidation(IErrorReporter errorReporter) {
            this.errorReporter = errorReporter;
        }

        /** Calcite can build calls that have null operands.
         * Find such cases and reject them. */
        @Override
        public Void visit(SqlCall call) {
            SourcePositionRange position = new SourcePositionRange(call.getParserPosition());
            SqlOperator operator = call.getOperator();
            if (operator instanceof SqlFunction) {
                for (SqlNode node : call.getOperandList()) {
                    if (node == null)
                        this.errorReporter.reportError(position,
                                "Illegal expression",
                                "Expression is missing some required operands");
                }
            }
            if (operator instanceof SqlDatetimeSubtractionOperator) {
                assert call.operandCount() == 3;
                SqlNode interval = call.getOperandList().get(2);
                assert interval instanceof SqlIntervalQualifier;
                SqlIntervalQualifier qual = (SqlIntervalQualifier) interval;
                switch (qual.timeUnitRange) {
                    case QUARTER:
                    case WEEK:
                    case MILLISECOND:
                    case MICROSECOND:
                    case NANOSECOND:
                    case DOW:
                    case ISODOW:
                    case DOY:
                    case EPOCH:
                    case DECADE:
                    case CENTURY:
                    case MILLENNIUM:
                        this.errorReporter.reportError(position, "Unsupported interval",
                                "Interval type " + Utilities.singleQuote(qual.timeUnitRange.toString()) +
                                        " not supported for difference operator");
                        break;
                    case YEAR:
                    case YEAR_TO_MONTH:
                    case DAY_TO_HOUR:
                    case DAY_TO_MINUTE:
                    case DAY_TO_SECOND:
                    case HOUR:
                    case HOUR_TO_MINUTE:
                    case HOUR_TO_SECOND:
                    case MINUTE:
                    case MINUTE_TO_SECOND:
                    case ISOYEAR:
                    case MONTH:
                    case DAY:
                    case SECOND:
                        break;
                }
            }
            return super.visit(call);
        }

        @Override
        public Void visit(SqlDataTypeSpec type) {
            SqlTypeNameSpec typeNameSpec = type.getTypeNameSpec();
            if (typeNameSpec instanceof SqlBasicTypeNameSpec basic) {
                // I don't know how to get the SqlTypeName otherwise
                RelDataType relDataType = SqlToRelCompiler.this.specToRel(type, false);
                if (relDataType.getSqlTypeName() == SqlTypeName.DECIMAL) {
                    if (basic.getPrecision() < basic.getScale()) {
                        SourcePositionRange position = new SourcePositionRange(typeNameSpec.getParserPos());
                        this.errorReporter.reportError(position,
                                "Illegal type", "DECIMAL type must have scale <= precision");
                    }
                    if (basic.getPrecision() > DBSPTypeDecimal.MAX_PRECISION) {
                        SourcePositionRange position = new SourcePositionRange(typeNameSpec.getParserPos());
                        this.errorReporter.reportError(position,
                                "Illegal type",
                                "Maximum precision supported for DECIMAL is " + DBSPTypeDecimal.MAX_PRECISION);
                    }
                } else if (relDataType.getSqlTypeName() == SqlTypeName.FLOAT) {
                    SourcePositionRange position = new SourcePositionRange(typeNameSpec.getParserPos());
                    this.errorReporter.reportError(position,
                            "Illegal type", "Do not use the FLOAT type, please use REAL or DOUBLE");
                }
            }
            return super.visit(type);
        }
    }

    SqlOperatorTable createOperatorTable() {
        return SqlOperatorTables.chain(
                CalciteFunctions.INSTANCE.getFunctions(),
                SqlOperatorTables.of(this.customFunctions.getInitialFunctions())
        );
    }

    public void addSchemaSource(String name, Schema schema) {
        this.rootSchema.add(name, schema);
    }

    static class Conformance extends SqlDelegatingConformance {
        protected Conformance(SqlConformance delegate) {
            super(delegate);
        }

        @Override
        public boolean isGroupByOrdinal() {
            return true;
        }
    }

    /** Add a new set of operators to the operator table.  Creates a new validator, converter */
    public void addOperatorTable(SqlOperatorTable operatorTable) {
        SqlOperatorTable newOperatorTable;
        if (this.validator != null) {
            newOperatorTable = SqlOperatorTables.chain(
                    this.validator.getOperatorTable(),
                    operatorTable);
        } else {
            newOperatorTable = operatorTable;
        }
        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withIdentifierExpansion(true);
        validatorConfig = validatorConfig.withConformance(new Conformance(validatorConfig.conformance()));
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(this.rootSchema), Collections.singletonList(calciteCatalog.schemaName),
                TYPE_FACTORY, connectionConfig);
        this.validator = SqlValidatorUtil.newValidator(
                newOperatorTable,
                catalogReader,
                TYPE_FACTORY,
                validatorConfig
        );
        this.converter = new SqlToRelConverter(
                (type, query, schema, path) -> null,
                this.validator,
                catalogReader,
                this.cluster,
                StandardConvertletTable.INSTANCE,
                this.converterConfig
        );
    }

    public boolean functionExists(String identifier) {
        List<SqlOperator> operators = Objects.requireNonNull(this.validator).getOperatorTable().getOperatorList();
        for (SqlOperator op: operators) {
            if (op instanceof SqlAggFunction)
                // These names are not ambiguous
                continue;
            if (op.kind == SqlKind.ITEM ||
                    op.kind == SqlKind.DOT ||
                    op.kind == SqlKind.SEARCH)
                // These are actually not named
                continue;
            if (op.getName().equalsIgnoreCase(identifier)) {
                return true;
            }
        }
        return false;
    }

    public static String getPlan(RelNode rel, boolean json) {
        return RelOptUtil.dumpPlan("[Logical plan]", rel,
                json ? SqlExplainFormat.JSON : SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES);
    }

    public RexBuilder getRexBuilder() {
        return this.cluster.getRexBuilder();
    }

    /** Keep here a number of empty lines.  This is done to fool the SqlParser
     * below: for each invocation of parseStatements we create a new SqlParser.
     * There is no way to reuse the previous parser one, unfortunately. */
    final StringBuilder newlines = new StringBuilder();

    /** Create a new parser.
     * @param sql       Program to parse.
     * @param saveLines If true remember the number of lines used by the program. */
    SqlParser createSqlParser(String sql, boolean saveLines) {
        // This function can be invoked multiple times.
        // In order to get correct line numbers, we feed the parser extra empty lines
        // before the statements we compile in this round.
        String toParse = this.newlines + sql;
        SqlParser sqlParser = SqlParser.create(toParse, this.parserConfig);
        if (saveLines) {
            int lines = sql.split("\n").length;
            this.newlines.append("\n".repeat(lines));
        }
        return sqlParser;
    }

    ExtraValidation getExtraValidator() {
        return Objects.requireNonNull(this.extraValidator);
    }

    SqlValidator getValidator() {
        return Objects.requireNonNull(this.validator);
    }

    public SqlToRelConverter getConverter() {
        return Objects.requireNonNull(this.converter);
    }

    /** Given a SQL statement returns a SqlNode - a calcite AST
     * representation of the query.
     * @param sql  SQL query to compile */
    public SqlNode parse(String sql, boolean saveLines) throws SqlParseException {
        SqlParser sqlParser = this.createSqlParser(sql, saveLines);
        SqlNode result = sqlParser.parseStmt();
        result.accept(this.getExtraValidator());
        return result;
    }

    public SqlNode parse(String sql) throws SqlParseException {
        return this.parse(sql, true);
    }

    /** Given a list of statements separated by semicolons, parse all of them. */
    public List<ParsedStatement> parseStatements(String statements, boolean saveLines) throws SqlParseException {
        SqlParser sqlParser = this.createSqlParser(statements, saveLines);
        SqlNodeList sqlNodes = sqlParser.parseStmtList();
        for (SqlNode node: sqlNodes) {
            node.accept(this.getExtraValidator());
        }
        return Linq.map(sqlNodes, n -> new ParsedStatement(n, saveLines));
    }

    public List<ParsedStatement> parseStatements(String statements) throws SqlParseException {
        return this.parseStatements(statements, true);
    }

    RelNode optimize(RelNode rel) {
        int level = 2;
        if (rel instanceof LogicalValues)
            // Less verbose for LogicalValues
            level = 4;

        final RelNode finalRel = rel;
        Logger.INSTANCE.belowLevel(this, level)
                .append("Before optimizer")
                .increase()
                .appendSupplier(() -> getPlan(finalRel, false))
                .decrease()
                .newline();

        RelBuilder relBuilder = this.converterConfig.getRelBuilderFactory().create(
                cluster, null);
        CalciteOptimizer optimizer = new CalciteOptimizer(
                this.options.languageOptions.optimizationLevel, relBuilder);
        rel = optimizer.apply(rel);
        RelNode finalRel1 = rel;
        Logger.INSTANCE.belowLevel(this, level)
                .append("After optimizer ")
                .increase()
                .appendSupplier(() -> getPlan(finalRel1, false))
                .decrease()
                .newline();
        return rel;
    }

    private RelDataType createNullableType(RelDataType type) {
        if (type instanceof RelRecordType) {
            // This function seems to be buggy in Calcite:
            // there is sets the nullability of all record fields.
            return new RelRecordType(type.getStructKind(), type.getFieldList(), true);
        }
        return TYPE_FACTORY.createTypeWithNullability(type, true);
    }

    private RelDataType deriveType(SqlDataTypeSpec typeSpec) {
        // The implementation of SqlDataTypeSpec.deriveType does not correctly handle
        // nullability of collection elements.
        SqlTypeNameSpec typeName = typeSpec.getTypeNameSpec();
        if (typeName instanceof SqlUserDefinedTypeNameSpec udtObject) {
            SqlIdentifier identifier = udtObject.getTypeName();
            ProgramIdentifier name = Utilities.toIdentifier(identifier);
            if (this.udt.containsKey(name)) {
                RelDataType result = Utilities.getExists(this.udt, name);
                Boolean nullable = typeSpec.getNullable();
                if (nullable != null && nullable)
                    result = this.createNullableType(result);
                return result;
            }

            RelDataType result =  typeName.deriveType(this.getValidator());
            if (typeSpec.getNullable() != null && typeSpec.getNullable())
                result = this.createNullableType(result);
            return result;
        }

        if (typeName instanceof SqlBasicTypeNameSpec) {
            RelDataType result = typeSpec.deriveType(this.getValidator());
            if (typeSpec.getNullable() != null && typeSpec.getNullable())
                result = this.createNullableType(result);
            return result;
        } else if (typeName instanceof SqlCollectionTypeNameSpec collection) {
            SqlTypeNameSpec elementTypeName = collection.getElementTypeName();
            SqlDataTypeSpec newSpec = new SqlDataTypeSpec(elementTypeName, null, true, SqlParserPos.ZERO);
            RelDataType elementType = this.specToRel(newSpec, false);
            elementType = this.createNullableType(elementType);
            // Don't need 'collectionType', but there is no other way to check whether the typeName is for an ARRAY
            RelDataType collectionType = collection.deriveType(this.getValidator());
            if (collectionType.getSqlTypeName() == SqlTypeName.ARRAY) {
                RelDataType result = TYPE_FACTORY.createArrayType(elementType, -1);
                if (typeSpec.getNullable() != null && typeSpec.getNullable())
                    result = this.createNullableType(result);
                return result;
            } else {
                throw new UnimplementedException("Unsupported type", CalciteObject.create(collectionType));
            }
        } else if (typeName instanceof SqlRowTypeNameSpec row) {
            List<SqlIdentifier> fieldNames = row.getFieldNames();
            List<SqlDataTypeSpec> fieldTypes = row.getFieldTypes();
            List<RelDataType> fields = new ArrayList<>(fieldTypes.size());
            for (int i = 0; i < row.getArity(); i++) {
                SqlDataTypeSpec field = fieldTypes.get(i);
                RelDataType elementType = this.specToRel(field, false);
                // Nullability *can* be specified for ROW fields!
                fields.add(elementType);
            }
            RelDataType result = TYPE_FACTORY.createStructType(
                    fields,
                    fieldNames.stream()
                            .map(SqlIdentifier::toString)
                            .collect(Collectors.toList()));
            if (typeSpec.getNullable() != null && typeSpec.getNullable())
                result = this.createNullableType(result);
            return result;
        } else if (typeName instanceof SqlMapTypeNameSpec map) {
            SqlDataTypeSpec key = map.getKeyType();
            SqlDataTypeSpec value = map.getValType();
            RelDataType keyType = this.specToRel(key, false);
            RelDataType valueType = this.specToRel(value, false);
            // keyType = this.createNullableType(keyType);
            valueType = this.createNullableType(valueType);
            RelDataType result = TYPE_FACTORY.createMapType(keyType, valueType);
            if (typeSpec.getNullable() != null && typeSpec.getNullable())
                result = this.createNullableType(result);
            return result;
        } else {
            throw new UnimplementedException("Unsupported type: " + getSqlString(typeSpec),
                    CalciteObject.create(typeSpec.getTypeNameSpec().getParserPos()));
        }
    }

    /** Convert a type from Sql to Rel.  Insert user-defined types in the {@link SqlToRelCompiler#udt} map.
     * @param spec            Type specification in Sql representation.
     * @param neverNullable   If true never return a nullable type.  Used for primary keys. */
    public RelDataType specToRel(SqlDataTypeSpec spec, boolean neverNullable) {
        SqlTypeNameSpec typeSpec = spec.getTypeNameSpec();
        ProgramIdentifier name = new ProgramIdentifier("", false);
        RelDataType result;

        result = this.deriveType(spec);
        if (neverNullable) {
            result = TYPE_FACTORY.createTypeWithNullability(result, false);
        }
        if (typeSpec instanceof SqlUserDefinedTypeNameSpec udtObject) {
            if (result.isStruct()) {
                RelStruct retval = new RelStruct(udtObject.getTypeName(), result.getFieldList(), result.isNullable());
                if (!this.udt.containsKey(name)) {
                    Utilities.putNew(this.udt, name, retval);
                }
                return retval;
            }
        }
        return result;
    }

    /** True if the left data type can be cast to the right data type "trivially".
     * This includes:
     * - non-null to null
     * - char(n) to varchar */
    @SuppressWarnings("RedundantIfStatement")
    public static boolean canBeTriviallyCastTo(RelDataType left, RelDataType right) {
        if (left.equals(right))
            return true;
        SqlTypeName leftName = left.getSqlTypeName();
        SqlTypeName rightName = right.getSqlTypeName();
        if (left.isNullable() && !right.isNullable())
            return false;
        if (leftName != rightName) {
            if (leftName == SqlTypeName.CHAR && rightName == SqlTypeName.VARCHAR) {
                if (right.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
                    return true;
                }
            } else if (leftName == SqlTypeName.NULL && right.isNullable()) {
                return true;
            } else if (SqlTypeName.INT_TYPES.contains(leftName) && SqlTypeName.INT_TYPES.contains(rightName)) {
                if (left.getPrecision() <= right.getPrecision())
                    return true;
            }
            return false;
        }
        if (left.isStruct() != right.isStruct())
            return false;
        if (left.isStruct()) {
            if (left.getFieldCount() != right.getFieldCount())
                return false;
            List<RelDataTypeField> leftFields = left.getFieldList();
            List<RelDataTypeField> rightFields = right.getFieldList();
            for (int i = 0; i < left.getFieldCount(); i++) {
                RelDataTypeField lf = leftFields.get(i);
                RelDataTypeField rf = rightFields.get(i);
                if (!lf.getName().equals(rf.getName()))
                    return false;

                RelDataType lt = lf.getType();
                RelDataType rt = rf.getType();
                if (!canBeTriviallyCastTo(lt, rt))
                    return false;
            }
        } else if (left.getComponentType() != null) {
            assert right.getComponentType() != null;
            return canBeTriviallyCastTo(left.getComponentType(), right.getComponentType());
        } else if (left.getKeyType() != null) {
            assert right.getKeyType() != null;
            assert left.getValueType() != null;
            assert right.getValueType() != null;
            return canBeTriviallyCastTo(left.getKeyType(), right.getKeyType()) &&
                    canBeTriviallyCastTo(left.getValueType(), right.getValueType());
        } else {
            left = TYPE_FACTORY.enforceTypeWithNullability(left, true);
            return left.equals(right);
        }
        return true;
    }

    List<ForeignKey> createForeignKeys(SqlCreateTable table) {
        List<ForeignKey> result = new ArrayList<>();
        // Extract foreign key information from two places:
        // - column declaration
        // - foreign key fields
        for (SqlNode cfk: table.columnsOrForeignKeys) {
            if (cfk instanceof SqlForeignKey fk) {
                ForeignKey.TableAndColumns thisTable = new ForeignKey.TableAndColumns(
                        new SourcePositionRange(fk.columnList.getParserPosition()),
                        new SqlFragmentIdentifier(table.name),
                        Linq.map(fk.columnList, c -> new SqlFragmentIdentifier((SqlIdentifier) c)));
                ForeignKey.TableAndColumns otherTable = new ForeignKey.TableAndColumns(
                        new SourcePositionRange(fk.otherColumnList.getParserPosition()),
                        new SqlFragmentIdentifier(fk.otherTable),
                        Linq.map(fk.otherColumnList, c -> new SqlFragmentIdentifier((SqlIdentifier) c)));
                ForeignKey foreignKey = new ForeignKey(thisTable, otherTable);
                result.add(foreignKey);
            } else if (cfk instanceof SqlExtendedColumnDeclaration decl) {
                for (int i = 0; i < decl.foreignKeyColumns.size(); i++) {
                    SqlIdentifier otherColumn = decl.foreignKeyColumns.get(i);
                    SqlIdentifier otherTable = decl.foreignKeyTables.get(i);
                    ForeignKey.TableAndColumns thisTable =
                            new ForeignKey.TableAndColumns(
                                    new SourcePositionRange(decl.name.getParserPosition()),
                                    new SqlFragmentIdentifier(table.name),
                                    Linq.list(new SqlFragmentIdentifier(decl.name)));
                    ForeignKey.TableAndColumns ot =
                            new ForeignKey.TableAndColumns(
                                    new SourcePositionRange(otherColumn.getParserPosition()),
                                    new SqlFragmentIdentifier(otherTable),
                                    Linq.list(new SqlFragmentIdentifier(otherColumn)));
                    ForeignKey foreignKey = new ForeignKey(thisTable, ot);
                    result.add(foreignKey);
                }
            }
        }
        return result;
    }

    @Nullable PropertyList createProperties(@Nullable SqlNodeList list) {
        if (list == null)
            return null;
        PropertyList result = new PropertyList();
        assert list.size() % 2 == 0;
        for (int i = 0; i < list.size(); i += 2) {
            SqlNode inode = list.get(i);
            if (!(inode instanceof SqlCharStringLiteral key)) {
                this.errorReporter.reportError(new SourcePositionRange(inode.getParserPosition()),
                        "Expected a simple string", "Found " + Utilities.singleQuote(inode.toString()));
                continue;
            }
            SqlNode iinode = list.get(i+1);
            if (!(iinode instanceof SqlCharStringLiteral value)) {
                this.errorReporter.reportError(new SourcePositionRange(iinode.getParserPosition()),
                        "Expected a simple string", "Found " + Utilities.singleQuote(iinode.toString()));
                continue;
            }

            result.addProperty(new SqlFragmentCharacterString(key), new SqlFragmentCharacterString(value));
        }
        return result;
    }

    RexNode validateLatenessOrWatermark(SqlIdentifier columnName, SqlDataTypeSpec columnType,
                                        SqlNode value, SourceFileContents sources) {
        try {
            /* We generate the following SQL:
              CREATE TABLE T(... column WATERMARK expression ...);
              SELECT column - value FROM tmp;
              and validate it. */
            String sql = "CREATE TABLE TMP(\"" +
                    columnName + "\" " +
                    columnType + ");\n" +
                    "CREATE VIEW V AS SELECT \"" +
                    columnName +
                    "\" - " + value +
                    " FROM TMP;\n";
            Logger.INSTANCE.belowLevel(this, 4)
                    .newline()
                    .append(sql)
                    .newline();
            SqlToRelCompiler clone = new SqlToRelCompiler(this);
            List<ParsedStatement> list = clone.parseStatements(sql, false);
            RelStatement lastStatement = null;
            for (ParsedStatement node : list) {
                lastStatement = clone.compile(node, sources);
            }
            assert lastStatement != null;
            assert lastStatement instanceof CreateViewStatement;
            CreateViewStatement cv = (CreateViewStatement) lastStatement;
            RelNode node = cv.getRelNode();
            if (node instanceof LogicalTableScan) {
                // This means that a subtraction with 0 was reduced to a simple value
                assert this.converter != null;
                return this.converter.convertExpression(value);
            }
            if (node instanceof LogicalProject project) {
                List<RexNode> projects = project.getProjects();
                if (projects.size() == 1) {
                    RexNode subtract = projects.get(0);
                    if (subtract instanceof RexCall call) {
                        if (call.getKind() == SqlKind.MINUS || call.getKind() == SqlKind.CHECKED_MINUS) {
                            RexNode left = call.getOperands().get(0);
                            if (left instanceof RexInputRef) {
                                // This may include some casts
                                return call.getOperands().get(1);
                            }
                        }
                    }
                }
            }
        } catch (CalciteContextException e) {
            SqlParserPos pos = value.getParserPosition();
            CalciteContextException ex = new CalciteContextException(e.getMessage(), e.getCause());
            ex.setPosition(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum());
            throw ex;
        } catch (SqlParseException e) {
            // Do we need to rewrite other exceptions?
            throw new RuntimeException(e);
        }
        throw new CompilationError("Cannot subtract " + value + " from column " +
                Utilities.toIdentifier(columnName).singleQuote() + " of type " +
                columnType, CalciteObject.create(value));
    }

    RexNode validateConstantExpression(SqlExtendedColumnDeclaration column,
                                       SqlNode value, SourceFileContents sources) {
        try {
            /* We generate the following SQL:
              CREATE VIEW V AS SELECT expression;
              and validate it. */
            String sql = "CREATE VIEW V AS SELECT " + value.toSqlString(OracleSqlDialect.DEFAULT) + ";";
            Logger.INSTANCE.belowLevel(this, 4)
                    .newline()
                    .append(sql)
                    .newline();
            SqlToRelCompiler clone = new SqlToRelCompiler(this);
            List<ParsedStatement> list = clone.parseStatements(sql, false);
            RelStatement lastStatement = null;
            for (ParsedStatement node : list) {
                lastStatement = clone.compile(node, sources);
            }
            assert lastStatement != null;
            assert lastStatement instanceof CreateViewStatement;
            CreateViewStatement cv = (CreateViewStatement) lastStatement;
            RelNode node = cv.getRelNode();
            if (node instanceof LogicalValues) {
                assert this.converter != null;
                return this.converter.convertExpression(value);
            }
            if (node instanceof LogicalProject project) {
                List<RexNode> projects = project.getProjects();
                if (projects.size() == 1) {
                    return projects.get(0);
                }
            }
        } catch (CalciteContextException e) {
            SqlParserPos pos = value.getParserPosition();
            CalciteContextException ex = new CalciteContextException(e.getMessage(), e.getCause());
            ex.setPosition(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum());
            throw ex;
        } catch (SqlParseException e) {
            // Do we need to rewrite other exceptions?
            throw new RuntimeException(e);
        }
        throw new CompilationError("Cannot use " + value + " as default value for " +
                Utilities.singleQuote(column.name.getSimple()), CalciteObject.create(value));
    }

    List<RelColumnMetadata> createTableColumnsMetadata(
            SqlCreateTable ct, SqlIdentifier table, SourceFileContents sources) {
        SqlNodeList list = ct.columnsOrForeignKeys;
        List<RelColumnMetadata> result = new ArrayList<>();
        int index = 0;
        Map<String, SqlNode> columnDefinition = new HashMap<>();
        SqlKeyConstraint key = null;
        Map<String, SqlIdentifier> primaryKeys = new HashMap<>();

        // First scan for standard style PRIMARY KEY constraints.
        for (SqlNode col: Objects.requireNonNull(list)) {
            if (col instanceof SqlKeyConstraint) {
                if (key != null) {
                    this.errorReporter.reportError(new SourcePositionRange(col.getParserPosition()),
                            "Duplicate key", "PRIMARY KEY already declared");
                    this.errorReporter.reportError(new SourcePositionRange(key.getParserPosition()),
                            "Duplicate key", "Previous declaration", true);
                    break;
                }
                key = (SqlKeyConstraint) col;
                if (key.operandCount() != 2) {
                    throw new InternalCompilerError("Expected 2 operands", CalciteObject.create(key));
                }
                SqlNode operand = key.operand(1);
                if (! (operand instanceof SqlNodeList)) {
                    throw new InternalCompilerError("Expected a list of columns", CalciteObject.create(operand));
                }
                for (SqlNode keyColumn : (SqlNodeList) operand) {
                    if (!(keyColumn instanceof SqlIdentifier identifier)) {
                        throw new InternalCompilerError("Expected an identifier",
                                CalciteObject.create(keyColumn));
                    }
                    String name = identifier.getSimple();
                    if (primaryKeys.containsKey(name)) {
                        this.errorReporter.reportError(new SourcePositionRange(identifier.getParserPosition()),
                                "Duplicate key column", "Column " + Utilities.singleQuote(name) +
                                " already declared as key");
                        this.errorReporter.reportError(new SourcePositionRange(primaryKeys.get(name).getParserPosition()),
                                "Duplicate key column", "Previous declaration", true);
                    }
                    primaryKeys.put(name, identifier);
                }
            }
        }

        // Scan again the rest of the columns.
        for (SqlNode col: Objects.requireNonNull(list)) {
            SqlIdentifier name;
            SqlDataTypeSpec typeSpec;
            boolean isPrimaryKey = false;
            RexNode lateness = null;
            RexNode watermark = null;
            RexNode defaultValue = null;
            SourcePositionRange defaultValueRange = null;
            if (col instanceof SqlColumnDeclaration cd) {
                name = cd.name;
                typeSpec = cd.dataType;
            } else if (col instanceof SqlExtendedColumnDeclaration cd) {
                name = cd.name;
                typeSpec = cd.dataType;
                if (cd.primaryKey && key != null) {
                    this.errorReporter.reportError(new SourcePositionRange(col.getParserPosition()),
                            "Duplicate key",
                            "Column " + Utilities.singleQuote(name.getSimple()) +
                                    " declared PRIMARY KEY in table with another PRIMARY KEY constraint");
                    this.errorReporter.reportError(new SourcePositionRange(key.getParserPosition()),
                            "Duplicate key", "PRIMARY KEYS declared as constraint", true);
                }
                boolean declaredPrimary = primaryKeys.containsKey(name.getSimple());
                isPrimaryKey = cd.primaryKey || declaredPrimary;
                if (declaredPrimary)
                    primaryKeys.remove(name.getSimple());
                if (cd.lateness != null) {
                    lateness = this.validateLatenessOrWatermark(cd.name, cd.dataType, cd.lateness, sources);
                }
                if (cd.watermark != null) {
                    watermark = this.validateLatenessOrWatermark(cd.name, cd.dataType, cd.watermark, sources);
                }
                if (cd.defaultValue != null) {
                    defaultValueRange = new SourcePositionRange(cd.defaultValue.getParserPosition());
                    defaultValue = this.validateConstantExpression(cd, cd.defaultValue, sources);
                }
            } else if (col instanceof SqlKeyConstraint ||
                       col instanceof SqlForeignKey) {
                continue;
            } else {
                throw new UnimplementedException("Column constraint not yet implemented", 1198,
                        CalciteObject.create(col));
            }

            String colName = name.getSimple();
            SqlNode previousColumn = columnDefinition.get(colName);
            if (previousColumn != null) {
                this.errorReporter.reportError(new SourcePositionRange(name.getParserPosition()),
                        "Duplicate name", "Column with name " +
                                Utilities.singleQuote(colName) + " already defined");
                this.errorReporter.reportError(new SourcePositionRange(previousColumn.getParserPosition()),
                        "Duplicate name",
                        "Previous definition", true);
            } else {
                columnDefinition.put(colName, col);
            }
            RelDataType type = this.specToRel(typeSpec, false);
            SourcePositionRange position = new SourcePositionRange(typeSpec.getParserPosition());
            if (isPrimaryKey) {
                if (type.isNullable()) {
                    // This is either an error or a warning, depending on the value of the 'lenient' flag
                    this.errorReporter.reportProblem(position,
                            this.options.languageOptions.lenient, false,
                            "PRIMARY KEY cannot be nullable",
                            "PRIMARY KEY column " + Utilities.singleQuote(name.getSimple()) +
                                    " has type " + type + ", which is nullable");
                    // Correct the type to be not-null
                    type = this.specToRel(typeSpec, true);
                }
                SqlTypeName tn = type.getSqlTypeName();
                if (tn == SqlTypeName.ARRAY ||
                        tn == SqlTypeName.MULTISET ||
                        tn == SqlTypeName.MAP ||
                        tn == SqlTypeName.ROW ||
                        tn == SqlTypeName.VARIANT) {
                    this.errorReporter.reportError(new SourcePositionRange(typeSpec.getParserPosition()),
                            "Illegal PRIMARY KEY type",
                            "PRIMARY KEY column " + Utilities.singleQuote(name.getSimple()) +
                                    " cannot have type " + type);
                }
            }
            if (!this.options.languageOptions.unrestrictedIOTypes) {
                this.validateColumnType(false, position, type, name.getSimple(), table);
            }
            RelDataTypeField field = new RelDataTypeFieldImpl(
                    name.getSimple(), index++, type);
            RelColumnMetadata meta = new RelColumnMetadata(
                    CalciteObject.create(col), field, isPrimaryKey, Utilities.identifierIsQuoted(name),
                    lateness, watermark, defaultValue, defaultValueRange);
            result.add(meta);
        }

        if (!primaryKeys.isEmpty()) {
            for (SqlIdentifier s: primaryKeys.values()) {
                this.errorReporter.reportError(new SourcePositionRange(s.getParserPosition()),
                        "No such column", "Key field " + Utilities.singleQuote(s.toString()) +
                                " does not correspond to a column");
            }
        }

        if (this.errorReporter.hasErrors())
            throw new CompilationError("aborting.");
        return result;
    }

    private void validateColumnType(boolean view, SourcePositionRange position, RelDataType type,
                                    String columnName, SqlIdentifier tableName) {
        SqlTypeFamily family = type.getSqlTypeName().getFamily();
        boolean illegal = family == SqlTypeFamily.INTERVAL_DAY_TIME ||
                family == SqlTypeFamily.INTERVAL_YEAR_MONTH;
        if (illegal) {
            String object = view ? "view" : "table";
            this.errorReporter.reportError(position,
                    "Unsupported column type",
                    "Column " + Utilities.singleQuote(columnName) + " of " + object + " " +
                            Utilities.singleQuote(tableName.getSimple()) +
                            " has type " + Utilities.singleQuote(type.getFullTypeString()) +
                            " which is currently not allowed in a " + object + ".");
        }
    }

    @Nullable
    public List<RelColumnMetadata> createViewColumnsMetadata(
            CalciteObject node, SqlIdentifier viewName, RelRoot relRoot, @Nullable SqlNodeList columnNames,
            SqlCreateView.ViewKind kind, Map<ProgramIdentifier, SqlLateness> perColumnLateness,
            SourceFileContents sources) {
        Map<String, SqlNode> columnDefinition = new HashMap<>();
        List<RelColumnMetadata> columns = new ArrayList<>();
        RelDataType rowType = relRoot.rel.getRowType();
        SourcePositionRange position = new SourcePositionRange(viewName.getParserPosition());
        if (columnNames != null && columnNames.size() != relRoot.fields.size()) {
            this.errorReporter.reportError(position,
                    "Column count mismatch",
                    "View " + Utilities.singleQuote(viewName.getSimple()) +
                            " specifies " + columnNames.size() + " columns " +
                            " but query computes " + relRoot.fields.size() + " columns");
            return null;
        }
        int index = 0;
        boolean error = false;
        Map<String, RelDataTypeField> colByName = new HashMap<>();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        FreshName generator = new FreshName(colByName.keySet());
        for (Map.Entry<Integer, String> fieldPairs : relRoot.fields) {
            String queryFieldName = fieldPairs.getValue();
            RelDataTypeField field = fieldList.get(index);
            Objects.requireNonNull(field);
            boolean nameIsQuoted = false;
            SqlIdentifier id = null;
            if (columnNames != null) {
                id = (SqlIdentifier) columnNames.get(index);
                String columnName = id.getSimple();
                nameIsQuoted = Utilities.identifierIsQuoted(id);
                field = new RelDataTypeFieldImpl(columnName, field.getIndex(), field.getType());

                SqlNode previousColumn = columnDefinition.get(columnName);
                if (previousColumn != null) {
                    this.errorReporter.reportError(new SourcePositionRange(id.getParserPosition()),
                            "Duplicate name", "Column with name " +
                                    Utilities.singleQuote(columnName) + " already defined");
                    this.errorReporter.reportError(new SourcePositionRange(previousColumn.getParserPosition()),
                            "Duplicate name",
                            "Previous definition", true);
                    error = true;
                } else {
                    columnDefinition.put(columnName, id);
                }
            }

            String actualColumnName = field.getName();
            if (id == null)
                id = new SqlIdentifier(actualColumnName, SqlParserPos.ZERO);
            ProgramIdentifier colIdentifier = Utilities.toIdentifier(id);
            RexNode lateness = null;
            if (perColumnLateness.containsKey(colIdentifier)) {
                SqlLateness sqlLateness = Utilities.getExists(perColumnLateness, colIdentifier);
                SqlDataTypeSpec typeSpec = SqlTypeUtil.convertTypeToSpec(field.getType());
                lateness = this.validateLatenessOrWatermark(id, typeSpec, sqlLateness.getLateness(), sources);
            }

            if (colByName.containsKey(actualColumnName)) {
                if (!this.options.languageOptions.lenient) {
                    this.errorReporter.reportError(position,
                            "Duplicate column",
                            "View " + Utilities.singleQuote(viewName.getSimple()) +
                                    " contains two columns with the same name " + Utilities.singleQuote(queryFieldName) + "\n" +
                                    "You can allow this behavior using the --lenient compiler flag");
                    error = true;
                } else {
                    this.errorReporter.reportWarning(position,
                            "Duplicate column",
                            "View " + Utilities.singleQuote(viewName.getSimple()) +
                                    " contains two columns with the same name " + Utilities.singleQuote(queryFieldName) + "\n" +
                                    "Some columns will be renamed in the produced output.");
                }
                // Rename field to avoid duplicates
                actualColumnName = generator.freshName(actualColumnName, false);
                field = new RelDataTypeFieldImpl(actualColumnName, field.getIndex(), field.getType());
            }
            colByName.put(actualColumnName, field);
            RelColumnMetadata meta = new RelColumnMetadata(node,
                    field, false, nameIsQuoted, lateness, null, null, null);
            if (kind != SqlCreateView.ViewKind.LOCAL && !this.options.languageOptions.unrestrictedIOTypes)
                this.validateColumnType(true, position, field.getType(), field.getName(), viewName);
            columns.add(meta);
            index++;
        }
        if (error)
            return null;
        return columns;
    }

    /** Visitor which extracts a function from a plan of the form
     * PROJECT expression
     *   SCAN table
     * This is used by the code that generates SQL user-defined functions. */
    static class ProjectExtractor extends RelVisitor {
        @Nullable RexNode body = null;

        <T> boolean visitIfMatches(RelNode node, Class<T> clazz, Consumer<T> method) {
            T value = ICastable.as(node, clazz);
            if (value != null) {
                method.accept(value);
                return true;
            }
            return false;
        }

        @SuppressWarnings("EmptyMethod")
        void visitScan(TableScan scan) {
            // assume that the entire row has exactly 1 field
            this.body = new RexInputRef(0, scan.getRowType());
        }

        void visitProject(LogicalProject project) {
            List<RexNode> fields = project.getProjects();
            assert fields.size() == 1;
            this.body = fields.get(0);
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            // First process children
            super.visit(node, ordinal, parent);
            boolean success =
                    this.visitIfMatches(node, LogicalTableScan.class, this::visitScan) ||
                    this.visitIfMatches(node, LogicalProject.class, this::visitProject);
            if (!success)
                // Anything else is an exception.  This can happen if e.g., someone uses queries in a UDF,
                // the grammar allows that.
                throw new UnimplementedException("User-defined function too complex", CalciteObject.create(node));
        }
    }

    @Nullable
    RexNode createFunction(SqlCreateFunctionDeclaration decl, SourceFileContents sources) {
        SqlNode body = decl.getBody();
        if (body == null)
            return null;

        int newLineNumber = 0;
        SqlParserPos position = body.getParserPosition();
        try {
            /* To compile a function like
              CREATE FUNCTION fun(a type0, b type1) returning type2 as expression;
              we generate the following SQL:
              CREATE TABLE tmp(a type0, b type1);
              SELECT expression FROM tmp;
              The generated code for the query select expression
              is used to obtain the body of the function.
            */
            StringBuilder builder = new StringBuilder();
            SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config(), builder);
            builder.append("CREATE TABLE TMP(");
            if (decl.getParameters().isEmpty())
                // Tables need to have at least one column, so create an unused one if needed
                builder.append("__unused__ INT");
            else
                decl.getParameters().unparse(writer, 0, 0);
            builder.append(");\n");
            builder.append("CREATE VIEW TMP0 AS SELECT\n");
            newLineNumber = builder.toString().split("\n").length + 1;

            SourcePositionRange range = new SourcePositionRange(body.getParserPosition());
            String bodyExpression = sources.getFragment(range, false);
            builder.append(bodyExpression);
            builder.append("\nFROM TMP;");

            String sql = builder.toString();
            Logger.INSTANCE.belowLevel(this, 4)
                    .newline()
                    .append(sql)
                    .newline();
            SqlToRelCompiler clone = new SqlToRelCompiler(this);
            List<ParsedStatement> list = clone.parseStatements(sql, true);
            RelStatement statement = null;
            for (ParsedStatement node: list) {
                statement = clone.compile(node, sources);
            }

            CreateViewStatement view = Objects.requireNonNull(statement).as(CreateViewStatement.class);
            assert view != null;
            RelNode node = view.getRelNode();
            ProjectExtractor extractor = new ProjectExtractor();
            extractor.go(node);
            return Objects.requireNonNull(extractor.body);
        } catch (CalciteContextException e) {
            throw this.rewriteException(e, newLineNumber, position);
        } catch (SqlParseException e) {
            // Do we need to rewrite other exceptions?
            throw new RuntimeException(e);
        }
    }

    /** Replaces references to a recursive view's name by references to another view */
    static class ReplaceRecursiveViews extends SqlShuttle {
        final Set<ProgramIdentifier> usedViews = new HashSet<>();

        static class CallAndChild {
            final SqlCall call;
            int childIndex;

            public CallAndChild(SqlCall call) {
                this.call = call;
                this.childIndex = 0;
            }

            void nextChild() {
                ++this.childIndex;
            }

            boolean hasNextChild() {
                return this.childIndex < call.operandCount();
            }

            SqlNode getChild() {
                return call.operand(this.childIndex);
            }

            SqlKind getKind() {
                return this.call.getKind();
            }
        }

        final Map<ProgramIdentifier, DeclareViewStatement> declaredViews;
        final java.util.function.Function<ProgramIdentifier, ProgramIdentifier> getInputName;
        final List<CallAndChild> stack = new ArrayList<>();

        ReplaceRecursiveViews(HashMap<ProgramIdentifier, DeclareViewStatement> declaredViews,
                              java.util.function.Function<ProgramIdentifier, ProgramIdentifier> getInputName) {
            this.declaredViews = declaredViews;
            this.getInputName = getInputName;
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable SqlNode visit(SqlCall call) {
            CallAndChild cc = new CallAndChild(call);
            this.stack.add(cc);
            CallCopyingArgHandler argHandler = new CallCopyingArgHandler(call, false);
            for (; cc.hasNextChild(); cc.nextChild()) {
                argHandler.visitChild(this, call, cc.childIndex, cc.getChild());
            }
            Utilities.removeLast(this.stack);
            return argHandler.result();
        }

        boolean inSelectFrom() {
            for (int i = 0; i < this.stack.size(); i++) {
                int index = this.stack.size() - i - 1;
                CallAndChild call = this.stack.get(index);
                if (call.call instanceof SqlBasicCall)
                    continue;
                if (call.getKind() == SqlKind.SELECT) {
                    return call.childIndex == SqlSelect.FROM_OPERAND;
                }
            }
            return false;
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable SqlNode visit(SqlIdentifier id) {
            boolean inFrom = inSelectFrom();
            if (id.isSimple()) {
                ProgramIdentifier simple = Utilities.toIdentifier(id);
                if (inFrom && this.declaredViews.containsKey(simple)) {
                    this.usedViews.add(simple);
                    id = id.setName(0, this.getInputName.apply(simple).name());
                }
            } else {
                SqlIdentifier component = id.getComponent(0);
                ProgramIdentifier simple = Utilities.toIdentifier(component);
                if (this.declaredViews.containsKey(simple)) {
                    this.usedViews.add(simple);
                    id = id.setName(0, this.getInputName.apply(simple).name());
                }
            }
            return id;
        }
    }

    public static String getSqlString(SqlDataTypeSpec type) {
        StringBuilder builder = new StringBuilder();
        SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config(), builder);
        type.unparse(writer, 0, 0);
        return builder.toString();
    }

    static void appendPrecision(RelDataType type, StringBuilder builder) {
        if (type.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED)
            builder.append("(")
                    .append(type.getPrecision())
                    .append(")");
    }

    public void toSql(RelDataType type, StringBuilder builder, boolean topLevel) {
        switch (type.getSqlTypeName()) {
            case BOOLEAN: builder.append("BOOLEAN"); break;
            case TINYINT: builder.append("TINYINT"); break;
            case SMALLINT: builder.append("SMALLINT"); break;
            case INTEGER: builder.append("INTEGER"); break;
            case BIGINT: builder.append("BIGINT"); break;
            case DECIMAL:
                builder.append("DECIMAL(")
                        .append(type.getPrecision())
                        .append(", ")
                        .append(type.getScale());
                break;
            case REAL: builder.append("REAL"); break;
            case DOUBLE: builder.append("DOUBLE"); break;
            case DATE: builder.append("DATE"); break;
            case TIME:
                builder.append("TIME");
                appendPrecision(type, builder);
                break;
            case TIMESTAMP:
                builder.append("TIMESTAMP");
                appendPrecision(type, builder);
                break;
            case INTERVAL_YEAR: builder.append("INTERVAL YEAR"); break;
            case INTERVAL_YEAR_MONTH: builder.append("INTERVAL YEAR TO MONTH"); break;
            case INTERVAL_MONTH: builder.append("INTERVAL MONTH"); break;
            case INTERVAL_DAY: builder.append("INTERVAL DAY"); break;
            case INTERVAL_DAY_HOUR: builder.append("INTERVAL DAY TO HOUR"); break;
            case INTERVAL_DAY_MINUTE: builder.append("INTERVAL DAY TO MINUTE"); break;
            case INTERVAL_DAY_SECOND: builder.append("INTERVAL DAY TO SECOND"); break;
            case INTERVAL_HOUR: builder.append("INTERVAL HOUR"); break;
            case INTERVAL_HOUR_MINUTE: builder.append("INTERVAL HOUR TO MINUTE"); break;
            case INTERVAL_HOUR_SECOND: builder.append("INTERVAL HOUR TO SECOND"); break;
            case INTERVAL_MINUTE: builder.append("INTERVAL MINUTE"); break;
            case INTERVAL_MINUTE_SECOND: builder.append("INTERVAL MINUTE TO SECOND"); break;
            case INTERVAL_SECOND: builder.append("INTERVAL SECOND"); break;
            case CHAR:
                builder.append("CHAR(")
                        .append(type.getPrecision())
                        .append(")");
                break;
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                builder.append("VARCHAR");
                appendPrecision(type, builder);
                break;
            case NULL:
                builder.append("NULL");
                break;
            case ARRAY:
                toSql(Objects.requireNonNull(type.getComponentType()), builder, false);
                builder.append(" ARRAY");
                break;
            case MAP:
                builder.append("MAP<");
                toSql(Objects.requireNonNull(type.getKeyType()), builder, false);
                builder.append(", ");
                toSql(Objects.requireNonNull(type.getValueType()), builder, false);
                builder.append(">");
                break;
            case ROW:
                boolean first = true;
                if (topLevel) {
                    for (var field: type.getFieldList()) {
                        if (!first)
                            builder.append(", ");
                        first = false;
                        builder.append(field.getName());
                        builder.append(" ");
                        toSql(field.getType(), builder, false);
                        if (!field.getType().isNullable())
                            builder.append(" NOT NULL");
                    }
                } else {
                    builder.append("ROW(");
                    for (var field : type.getFieldList()) {
                        if (!first)
                            builder.append(", ");
                        first = false;
                        builder.append(field.getName());
                        builder.append(" ");
                        toSql(field.getType(), builder, false);
                    }
                    builder.append(")");
                }
                break;
            case GEOMETRY:
                builder.append("GEOMETRY");
                break;
            case VARIANT:
                builder.append("VARIANT");
                break;
            default:
                throw new UnimplementedException(type.getFullTypeString(), CalciteObject.create(type));
        }
    }

    /** Replace references to recursive views that are not defined yet with
     *  references to some fictitious input tables. */
    SqlNode replaceRecursiveViews(SqlNode query) {
        HashMap<ProgramIdentifier, DeclareViewStatement> toReplace = new HashMap<>();
        for (var e : this.declaredViews.entrySet()) {
            if (this.definedViews.contains(e.getKey()))
                continue;
            Utilities.putNew(toReplace, e.getKey(), e.getValue());
        }
        if (toReplace.isEmpty())
            return query;
        ReplaceRecursiveViews rr = new ReplaceRecursiveViews(
                toReplace, DeclareViewStatement::inputViewName);
        this.usedViewDeclarations.addAll(rr.usedViews);
        query = rr.visitNode(query);
        return Objects.requireNonNull(query);
    }

    // Adjust the source position in the exception to match the original position
    CalciteContextException rewriteException(
            CalciteContextException e,
            int startLineNumberInGeneratedCode, SqlParserPos original) {
        int line = original.getLineNum() - e.getPosLine() + startLineNumberInGeneratedCode;
        int endLine = original.getEndLineNum() - e.getEndPosLine() + startLineNumberInGeneratedCode;
        // If the error is on the first line, we need to adjust the column, otherwise we don't.
        // The temporary generated code always starts after a newline.
        int col = e.getPosColumn();
        int endCol = e.getEndPosColumn();
        if (e.getPosLine() == startLineNumberInGeneratedCode)
            col += original.getColumnNum() - 1;
        if (e.getEndPosLine() == startLineNumberInGeneratedCode)
            endCol += original.getColumnNum() - 1;
        return new CalciteContextException(
                e.getMessage() == null ? "" : e.getMessage(), e.getCause(),
                line, col, endLine, endCol);
    }

    private DropTableStatement compileDropTable(ParsedStatement node) {
        SqlDropTable dt = (SqlDropTable) node.statement();
        ProgramIdentifier tableName = Utilities.toIdentifier(dt.name);
        this.calciteCatalog.dropTable(tableName);
        return new DropTableStatement(node, tableName);
    }

    @Nullable
    private CreateIndexStatement compileCreateIndex(ParsedStatement node) {
        CalciteObject object = CalciteObject.create(node);
        SqlCreateIndex ci = (SqlCreateIndex) node.statement();
        if (ci.ifNotExists)
            throw new UnsupportedException("IF NOT EXISTS not supported for INDEX", object);
        Map<ProgramIdentifier, SqlIdentifier> columns = new HashMap<>();
        if (ci.columns.isEmpty()) {
            this.errorReporter.reportError(new SourcePositionRange(ci.getParserPosition()),
                    "Invalid statement",
                    "Empty column set in CREATE INDEX statement ");
            return null;
        }
        boolean success = true;
        for (SqlNode col: ci.columns) {
            assert col instanceof SqlIdentifier;
            SqlIdentifier id = (SqlIdentifier) col;
            ProgramIdentifier pid = Utilities.toIdentifier(id);
            if (columns.containsKey(pid)) {
                SqlIdentifier previous = columns.get(pid);
                this.errorReporter.reportError(new SourcePositionRange(col.getParserPosition()),
                        "Duplicated name",
                        "Column " + pid.singleQuote() + " duplicated in index");
                this.errorReporter.reportError(new SourcePositionRange(previous.getParserPosition()),
                        "Previous definition", "", true);
                success = false;
            } else {
                Utilities.putNew(columns, pid, id);
            }
        }
        if (!success)
            return null;
        var result = new CreateIndexStatement(node, Utilities.toIdentifier(ci.name), ci);
        success = this.calciteCatalog.addDefinition(result.indexName, this.errorReporter, result);
        if (!success)
            return null;
        return result;
    }

    @Nullable
    private CreateTableStatement compileCreateTable(ParsedStatement node, SourceFileContents sources) {
        CalciteObject object = CalciteObject.create(node);
        SqlCreateTable ct = (SqlCreateTable) node.statement();
        if (ct.ifNotExists)
            throw new UnsupportedException("IF NOT EXISTS not supported for TABLE", object);
        ProgramIdentifier tableName = Utilities.toIdentifier(ct.name);
        if (node.visible() && this.functionExists(tableName.name())) {
            this.errorReporter.reportError(new SourcePositionRange(ct.name.getParserPosition()),
                    "Reserved name",
                    "Table " + tableName.singleQuote() +
                            " has the same name as a predefined function");
            return null;
        }
        List<RelColumnMetadata> cols = this.createTableColumnsMetadata(ct, ct.name, sources);
        @Nullable PropertyList properties = this.createProperties(ct.tableProperties);
        Properties props = null;
        if (properties != null) {
            properties.checkDuplicates(this.errorReporter);
            properties.checkKnownProperties(this::validateTableProperty);
            props = new Properties(properties);
        }
        List<ForeignKey> fk = this.createForeignKeys(ct);
        CreateTableStatement table = new CreateTableStatement(node, tableName, cols, fk, props);
        boolean success = this.calciteCatalog.addTable(table, this.errorReporter);
        if (!success)
            return null;
        return table;
    }

    public CreateFunctionStatement compileCreateFunction(ParsedStatement node, SourceFileContents sources) {
        SqlCreateFunctionDeclaration decl = (SqlCreateFunctionDeclaration) node.statement();
        List<Map.Entry<String, RelDataType>> parameters = Linq.map(
                decl.getParameters(), param -> {
                    SqlAttributeDefinition attr = (SqlAttributeDefinition) param;
                    String name = attr.name.getSimple();
                    RelDataType type = this.specToRel(attr.dataType, false);
                    return new MapEntry<>(name, type);
                });
        RelDataType structType = TYPE_FACTORY.createStructType(parameters);
        SqlDataTypeSpec retType = decl.getReturnType();
        RelDataType returnType = this.specToRel(retType, false);
        Boolean nullableResult = retType.getNullable();
        if (nullableResult != null && nullableResult)
            returnType = this.createNullableType(returnType);
        RexNode bodyExp = this.createFunction(decl, sources);
        ExternalFunction function = this.customFunctions.createUDF(
                CalciteObject.create(node), decl.getName(), structType, returnType, bodyExp);
        return new CreateFunctionStatement(node, function);
    }

    RelRoot sqlToRel(SqlNode node) {
        SqlToRelConverter converter = this.getConverter();
        return converter.convertQuery(node, true, true);
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

    @Nullable
    public CreateViewStatement compileCreateView(
            ParsedStatement node, Map<ProgramIdentifier, SqlLateness> lateness,
            SourceFileContents sources) {
        CalciteObject object = CalciteObject.create(node);
        SqlCreateView cv = (SqlCreateView) node.statement();
        SqlNode query = cv.query;
        if (cv.getReplace())
            throw new UnsupportedException("OR REPLACE not supported", object);
        Logger.INSTANCE.belowLevel(this, 2)
                .appendSupplier(node.statement()::toString)
                .newline();
        query = this.replaceRecursiveViews(query);
        RelRoot relRoot = this.sqlToRel(query);
        List<RelColumnMetadata> columns = this.createViewColumnsMetadata(CalciteObject.create(node),
                cv.name, relRoot, cv.columnList, cv.viewKind, lateness, sources);
        ProgramIdentifier viewName = Utilities.toIdentifier(cv.name);
        if (columns == null)
            // error
            return null;
        @Nullable PropertyList viewProperties = this.createProperties(cv.viewProperties);
        int emitFinal = -1;
        Properties props = null;
        if (viewProperties != null) {
            viewProperties.checkDuplicates(this.errorReporter);
            SqlFragment materialized = viewProperties.getPropertyValue("materialized");
            if (materialized != null) {
                this.errorReporter.reportWarning(materialized.getSourcePosition(),
                        "Materialized property not used",
                        "The 'materialized' property for views is not used, " +
                                "please use 'CREATE MATERIALIZED VIEW' instead");
            }

            SqlFragment val = viewProperties.getPropertyValue(CreateViewStatement.EMIT_FINAL);
            if (val != null) {
                try {
                    int index = Integer.parseInt(val.getString());
                    if (index < 0 || index >= columns.size())
                        this.errorReporter.reportError(
                                new SourcePositionRange(val.getParserPosition()),
                                "Illegal column number",
                                "View " + viewName.singleQuote() +
                            " does not have a column with number " + index);
                } catch (NumberFormatException ignored) {
                    ProgramIdentifier canonical =
                            new ProgramIdentifier(options.canonicalName(
                                    val.getString(), false), false);
                    for (int i = 0; i < columns.size(); i++) {
                        if (columns.get(i).getName().equals(canonical)) {
                            emitFinal = i;
                            break;
                        }
                    }
                    if (emitFinal < 0) {
                        this.errorReporter.reportError(
                                new SourcePositionRange(val.getParserPosition()),
                                "Illegal column name",
                                "Column " + canonical.singleQuote() +
                                        " not found in " + viewName.singleQuote());
                    }
                }
            }

            viewProperties.checkKnownProperties(this::validateViewProperty);
            props = new Properties(viewProperties);
        }

        // Convert plan to use checked arithmetic
        // Must be done before optimizations.
        ConvertToChecked checkedConverter = new ConvertToChecked(this.getRexBuilder());
        RelNode checked = checkedConverter.visit(relRoot.rel);
        relRoot = relRoot.withRel(checked);

        RelNode optimized = this.optimize(relRoot.rel);
        relRoot = relRoot.withRel(optimized);
        CreateViewStatement view = new CreateViewStatement(node,
                viewName, columns, cv, relRoot, emitFinal, props);
        // From Calcite's point of view we treat this view just as another table.
        boolean success = this.calciteCatalog.addTable(view, this.errorReporter);
        if (!success)
            return null;

        // If there is a corresponding DeclareViewStatement, validate the types
        if (this.declaredViews.containsKey(view.relationName)) {
            DeclareViewStatement dv = this.declaredViews.get(view.relationName);
            RelDataType viewType = view.getRowType();
            RelDataType declaredType = dv.getRowType();
            if (!canBeTriviallyCastTo(viewType, declaredType)) {
               this.errorReporter.reportError(view.getPosition(), "Type mismatch",
                        "Type inferred for view " + view.relationName.singleQuote() +
                        " is " + this.typeToColumns(view.relationName, viewType));
                this.errorReporter.reportError(dv.getPosition(), "Type mismatch",
                        "does not match the declared type " +
                                this.typeToColumns(view.relationName, declaredType) + ":",
                        true);
            }
        }

        this.definedViews.add(view.relationName);
        return view;
    }

    String typeToColumns(ProgramIdentifier view, RelDataType type) {
        StringBuilder builder = new StringBuilder();
        builder.append(view).append("(");
        toSql(type, builder, true);
        builder.append(")");
        return builder.toString();
    }

    /** Compile a SQL statement.
     * @param node         Compiled version of the SQL statement.
     * @param sources      Contents of the source files, for error reporting. */
    @Nullable
    public RelStatement compile(ParsedStatement node, SourceFileContents sources) {
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Compiling ")
                .appendSupplier(node::toString)
                .newline();
        SqlKind kind = node.statement().getKind();
        switch (kind) {
            case CREATE_INDEX:
                return this.compileCreateIndex(node);
            case CREATE_VIEW:
                // Invoked recursively when synthesizing code, e.g., for validating lateness
                return this.compileCreateView(node, new HashMap<>(), sources);
            case DROP_TABLE:
                return this.compileDropTable(node);
            case CREATE_TABLE:
                return this.compileCreateTable(node, sources);
            case INSERT:
                return this.compileInsert(node);
            case DELETE:
                // We expect this to be a REMOVE statement
                if (node.statement() instanceof SqlRemove)
                    return this.compileRemove(node);
                break;
            case SELECT:
                throw new UnsupportedException(
                        "Raw 'SELECT' statements are not supported; did you forget to CREATE VIEW?",
                        CalciteObject.create(node));
            case OTHER:
                if (node.statement() instanceof SqlDeclareView)
                    return this.compileDeclareView(node);
                break;
            default:
                break;
        }
        throw new UnimplementedException("SQL statement not yet implemented", CalciteObject.create(node));
    }

    @Nullable
    private DeclareViewStatement compileDeclareView(ParsedStatement node) {
        SqlDeclareView cv = (SqlDeclareView) node.statement();
        List<RelColumnMetadata> columns = new ArrayList<>();
        int index = 0;
        for (SqlNode n: cv.columns) {
            SqlColumnDeclaration cd = (SqlColumnDeclaration) n;
            String name = cd.name.getSimple();
            RelDataType type = this.specToRel(cd.dataType, false);
            RelDataTypeField field = new RelDataTypeFieldImpl(name, index++, type);
            var meta = new RelColumnMetadata(CalciteObject.create(n), field, false,
                    Utilities.identifierIsQuoted(cd.name), null, null, null, null);
            columns.add(meta);
        }
        var result = new DeclareViewStatement(node, Utilities.toIdentifier(cv.name), columns);
        Utilities.putNew(this.declaredViews, Utilities.toIdentifier(cv.name), result);
        boolean success = this.calciteCatalog.addTable(result, this.errorReporter);
        if (!success)
            return null;
        return result;
    }

    private TableModifyStatement compileRemove(ParsedStatement node) {
        SqlRemove remove = (SqlRemove) node.statement();
        SqlNode table = remove.getTargetTable();
        if (!(table instanceof SqlIdentifier id))
            throw new UnimplementedException("REMOVE not supported for " + table, CalciteObject.create(table));
        TableModifyStatement stat = new TableModifyStatement(
                node, false, Utilities.toIdentifier(id), remove.getSource());
        RelRoot values = this.sqlToRel(stat.data);
        values = values.withRel(this.optimize(values.rel));
        stat.setTranslation(values.rel);
        return stat;
    }

    private TableModifyStatement compileInsert(ParsedStatement node) {
        SqlInsert insert = (SqlInsert) node.statement();
        SqlNode table = insert.getTargetTable();
        if (!(table instanceof SqlIdentifier id))
            throw new UnimplementedException("INSERT NOT SUPPORTED FOR " + table, CalciteObject.create(table));
        TableModifyStatement stat = new TableModifyStatement(node, true, Utilities.toIdentifier(id), insert.getSource());
        RelRoot values = this.sqlToRel(stat.data);
        values = values.withRel(this.optimize(values.rel));
        stat.setTranslation(values.rel);
        return stat;
    }

    @Nullable
    public CreateTypeStatement compileCreateType(ParsedStatement node) {
        SqlCreateType ct = (SqlCreateType) node.statement();
        RelProtoDataType proto = typeFactory -> {
            if (ct.dataType != null) {
                return this.specToRel(ct.dataType, false);
            } else {
                ProgramIdentifier name = Utilities.toIdentifier(ct.name);
                if (SqlToRelCompiler.this.udt.containsKey(name))
                    return SqlToRelCompiler.this.udt.get(name);
                final RelDataTypeFactory.Builder builder = typeFactory.builder();
                for (SqlNode def : Objects.requireNonNull(ct.attributeDefs)) {
                    final SqlAttributeDefinition attributeDef =
                            (SqlAttributeDefinition) def;
                    final SqlDataTypeSpec typeSpec = attributeDef.dataType;
                    RelDataType type = this.specToRel(typeSpec, false);
                    if (typeSpec.getNullable() != null && typeSpec.getNullable()) {
                        // This is tricky, because it is not using the typeFactory that is
                        // the lambda argument above, but hopefully it should be the same
                        assert typeFactory == TYPE_FACTORY;
                        type = this.createNullableType(type);
                    }
                    builder.add(attributeDef.name.getSimple(), type);
                }
                RelDataType result = builder.build();
                RelStruct retval = new RelStruct(ct.name, result.getFieldList(), result.isNullable());
                Utilities.putNew(SqlToRelCompiler.this.udt, name, retval);
                return retval;
            }
        };
        ProgramIdentifier typeName = Utilities.toIdentifier(ct.name);
        this.rootSchema.add(typeName.name(), proto);
        RelDataType relDataType = proto.apply(TYPE_FACTORY);
        CreateTypeStatement result = new CreateTypeStatement(node, ct, typeName, relDataType);
        boolean success = this.calciteCatalog.addType(typeName, this.errorReporter, result);
        if (!success)
            return null;
        return result;
    }
}
