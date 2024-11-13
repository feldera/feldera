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
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlAttributeDefinition;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
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
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlExtendedColumnDeclaration;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragment;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentCharacterString;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlLateness;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlRemove;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateFunctionStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTypeStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DropTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.LatenessStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.TableModifyStatement;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.util.ICastable;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * The calcite compiler compiles SQL into Calcite RelNode representations.
 * It is stateful.
 * The protocol is:
 * - compiler is initialized (startCompilation)
 * - a sequence of statements is supplied as SQL strings:
 *   - table definition statements
 *   - insert or delete statements
 *   - type creation statements
 *   - function creation statements
 *   - view definition statements
 * For each statement the compiler returns a representation suitable for passing
 * to the mid-end.
 * The front-end is itself composed of several stages:
 * - compile SQL to SqlNode
 * - compile SqlNode to RelNode
 * - optimize RelNode
 */
public class CalciteCompiler implements IWritesLogs {
    private final CompilerOptions options;
    private final SqlParser.Config parserConfig;
    private final Catalog calciteCatalog;
    public final RelOptCluster cluster;
    public final RelDataTypeFactory typeFactory;
    private final SqlToRelConverter.Config converterConfig;
    /** Perform additional type validation in top of the Calcite rules. */
    @Nullable
    private SqlValidator validator;
    @Nullable
    private SqlToRelConverter converter;
    @Nullable
    private ValidateTypes validateTypes;
    private final CalciteConnectionConfig connectionConfig;
    private final IErrorReporter errorReporter;
    private final SchemaPlus rootSchema;
    private final CustomFunctions customFunctions;
    /** User-defined types */
    private final HashMap<String, RelStruct> udt;

    /** Create a copy of the 'source' compiler which can be used to compile
     * some generated SQL without affecting its data structures */
    public CalciteCompiler(CalciteCompiler source) {
        this.options = source.options;
        this.parserConfig = source.parserConfig;
        this.cluster = source.cluster;
        this.typeFactory = source.typeFactory;
        this.converterConfig = source.converterConfig;
        this.connectionConfig = source.connectionConfig;
        this.errorReporter = source.errorReporter;
        this.customFunctions = new CustomFunctions(source.customFunctions);
        this.calciteCatalog = new Catalog(source.calciteCatalog);
        this.udt = new HashMap<>(source.udt);
        this.rootSchema = CalciteSchema.createRootSchema(false, false).plus();
        this.copySchema(source.rootSchema);
        this.rootSchema.add(this.calciteCatalog.schemaName, this.calciteCatalog);
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

    /** Additional validation tests on top of Calcite.
     * We need to do these before conversion to Rel, because Rel
     * does not have source position information anymore. */
    public class ValidateTypes extends SqlShuttle {
        final IErrorReporter reporter;

        public ValidateTypes(IErrorReporter reporter) {
            this.reporter = reporter;
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable SqlNode visit(SqlDataTypeSpec type) {
            SqlTypeNameSpec typeNameSpec = type.getTypeNameSpec();
            if (typeNameSpec instanceof SqlBasicTypeNameSpec basic) {
                // I don't know how to get the SqlTypeName otherwise
                RelDataType relDataType = CalciteCompiler.this.specToRel(type, false);
                if (relDataType.getSqlTypeName() == SqlTypeName.DECIMAL) {
                    if (basic.getPrecision() < basic.getScale()) {
                        SourcePositionRange position = new SourcePositionRange(typeNameSpec.getParserPos());
                        this.reporter.reportError(position,
                                "Illegal type", "DECIMAL type must have scale <= precision");
                    }
                } else if (relDataType.getSqlTypeName() == SqlTypeName.FLOAT) {
                    SourcePositionRange position = new SourcePositionRange(typeNameSpec.getParserPos());
                    this.reporter.reportError(position,
                            "Illegal type", "Do not use the FLOAT type, please use REAL or DOUBLE");
                }
            }
            return super.visit(type);
        }
    }

    public CalciteCompiler(CompilerOptions options, IErrorReporter errorReporter) {
        this.options = options;
        this.errorReporter = errorReporter;
        this.customFunctions = new CustomFunctions();

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
        Properties connConfigProp = new Properties();
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
        this.typeFactory = new SqlTypeFactoryImpl(TYPE_SYSTEM);
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
        this.cluster = RelOptCluster.create(planner, new RexBuilder(this.typeFactory));
        this.converterConfig = SqlToRelConverter.config()
                // Calcite recommends not using withExpand, but there are no
                // rules to decorrelate some queries that withExpand will produce,
                // e.g., AggScottTests.testAggregates4
                .withExpand(true);
        this.validator = null;
        this.validateTypes = null;
        this.converter = null;

        SqlOperatorTable operatorTable = this.createOperatorTable();
        this.addOperatorTable(operatorTable);
    }

    SqlOperatorTable createOperatorTable() {
        return SqlOperatorTables.chain(
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                        // Libraries of functions supported.
                        EnumSet.of(SqlLibrary.STANDARD,
                                SqlLibrary.MYSQL,
                                SqlLibrary.POSTGRESQL,
                                SqlLibrary.BIG_QUERY,
                                SqlLibrary.SPARK,
                                SqlLibrary.SPATIAL)),
                SqlOperatorTables.of(this.customFunctions.getInitialFunctions())
        );
    }

    public void addSchemaSource(String name, Schema schema) {
        this.rootSchema.add(name, schema);
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
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(this.rootSchema), Collections.singletonList(calciteCatalog.schemaName),
                this.typeFactory, connectionConfig);
        this.validator = SqlValidatorUtil.newValidator(
                newOperatorTable,
                catalogReader,
                this.typeFactory,
                validatorConfig
        );
        this.validateTypes = new ValidateTypes(errorReporter);
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

    ValidateTypes getTypeValidator() {
        return Objects.requireNonNull(this.validateTypes);
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
        result.accept(this.getTypeValidator());
        return result;
    }

    public SqlNode parse(String sql) throws SqlParseException {
        return this.parse(sql, true);
    }

    public record ParsedStatement(SqlNode statement, boolean visible) {
        @Override
        public String toString() {
            return this.statement.toString();
        }
    };

    /** Given a list of statements separated by semicolons, parse all of them. */
    public List<ParsedStatement> parseStatements(String statements, boolean saveLines) throws SqlParseException {
        SqlParser sqlParser = this.createSqlParser(statements, saveLines);
        SqlNodeList sqlNodes = sqlParser.parseStmtList();
        for (SqlNode node: sqlNodes) {
            node.accept(this.getTypeValidator());
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

        Logger.INSTANCE.belowLevel(this, level)
                .append("Before optimizer")
                .increase()
                .append(getPlan(rel, false))
                .decrease()
                .newline();

        RelBuilder relBuilder = this.converterConfig.getRelBuilderFactory().create(
                cluster, null);
        CalciteOptimizer optimizer = new CalciteOptimizer(
                this.options.languageOptions.optimizationLevel, relBuilder);
        rel = optimizer.apply(rel);
        Logger.INSTANCE.belowLevel(this, level)
                .append("After optimizer ")
                .increase()
                .append(getPlan(rel, false))
                .decrease()
                .newline();
        return rel;
    }

    RelDataType createNullableType(RelDataType type) {
        if (type instanceof RelRecordType) {
            // This function seems to be buggy in Calcite:
            // there is sets the nullability of all record fields.
            return new RelRecordType(type.getStructKind(), type.getFieldList(), true);
        }
        return this.typeFactory.createTypeWithNullability(type, true);
    }

    RelDataType nullableElements(RelDataType dataType) {
        if (dataType instanceof ArraySqlType array) {
            RelDataType elementType = array.getComponentType();
            elementType = this.nullableElements(elementType);
            elementType = this.createNullableType(elementType);
            return new ArraySqlType(elementType, dataType.isNullable());
        } else if (dataType instanceof MapSqlType map) {
            RelDataType valueType = map.getValueType();
            valueType = this.nullableElements(valueType);
            valueType = this.createNullableType(valueType);
            return new MapSqlType(map.getKeyType(), valueType, map.isNullable());
        } else {
            return dataType;
        }
    }

    /** Convert a type from Sql to Rel.
     * @param spec            Type specification in Sql representation.
     * @param ignoreNullable  If true never return a nullable type. */
    public RelDataType specToRel(SqlDataTypeSpec spec, boolean ignoreNullable) {
        SqlTypeNameSpec typeName = spec.getTypeNameSpec();
        String name = "";
        RelDataType result;
        if (typeName instanceof SqlUserDefinedTypeNameSpec udtObject) {
            SqlIdentifier identifier = udtObject.getTypeName();
            name = identifier.getSimple();
            if (this.udt.containsKey(name)) {
                result = Utilities.getExists(this.udt, name);
                Boolean nullable = spec.getNullable();
                if (nullable != null && nullable && !ignoreNullable) {
                    result = this.createNullableType(result);
                }
                return result;
            }
        }
        result = typeName.deriveType(this.getValidator());
        result = this.nullableElements(result);

        Boolean nullable = spec.getNullable();
        if (nullable != null && nullable && !ignoreNullable) {
            result = this.createNullableType(result);
        }
        if (typeName instanceof SqlUserDefinedTypeNameSpec udtObject) {
            if (result.isStruct()) {
                RelStruct retval = new RelStruct(udtObject.getTypeName(), result.getFieldList(), result.isNullable());
                Utilities.putNew(this.udt, name, retval);
                return retval;
            }
        }
        return result;
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

    RexNode validateLatenessOrWatermark(SqlExtendedColumnDeclaration column,
                                        SqlNode value, SourceFileContents sources) {
        try {
            /* We generate the following SQL:
              CREATE TABLE T(... column WATERMARK expression ...);
              SELECT column - expression FROM tmp;
              and validate it. */
            String sql = "CREATE TABLE TMP(" +
                    column.name + " " +
                    column.dataType + ");\n" +
                    "CREATE VIEW V AS SELECT " +
                    column.name +
                    " - " + value +
                    " FROM TMP;\n";
            Logger.INSTANCE.belowLevel(this, 2)
                    .newline()
                    .append(sql)
                    .newline();
            CalciteCompiler clone = new CalciteCompiler(this);
            List<ParsedStatement> list = clone.parseStatements(sql, false);
            FrontEndStatement lastStatement = null;
            for (ParsedStatement node : list) {
                lastStatement = clone.compile(node, sources);
            }
            assert lastStatement != null;
            assert lastStatement instanceof CreateViewStatement;
            CreateViewStatement cv = (CreateViewStatement) lastStatement;
            RelNode node = cv.getRelNode();
            if (node instanceof LogicalTableScan) {
                // This means that a subtraction with 0 was reduced to nothing
                assert this.converter != null;
                return this.converter.convertExpression(value);
            }
            if (node instanceof LogicalProject project) {
                List<RexNode> projects = project.getProjects();
                if (projects.size() == 1) {
                    RexNode subtract = projects.get(0);
                    if (subtract instanceof RexCall call) {
                        if (call.getKind() == SqlKind.MINUS) {
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
                Utilities.singleQuote(column.name.getSimple()) + " of type " +
                column.dataType, CalciteObject.create(value));
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
                            "Duplicate key", "Previous declaration");
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
                                "Duplicate key column", "Previous declaration");
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
                            "Duplicate key", "PRIMARY KEYS declared as constraint");
                }
                boolean declaredPrimary = primaryKeys.containsKey(name.getSimple());
                isPrimaryKey = cd.primaryKey || declaredPrimary;
                if (declaredPrimary)
                    primaryKeys.remove(name.getSimple());
                SqlToRelConverter converter = this.getConverter();
                if (cd.lateness != null) {
                    lateness = this.validateLatenessOrWatermark(cd, cd.lateness, sources);
                }
                if (cd.watermark != null) {
                    watermark = this.validateLatenessOrWatermark(cd, cd.watermark, sources);
                }
                if (cd.defaultValue != null) {
                    // workaround for https://issues.apache.org/jira/browse/CALCITE-6129
                    if (cd.defaultValue instanceof SqlLiteral literal) {
                        if (literal.getTypeName() == SqlTypeName.NULL) {
                            RelDataType type = literal.createSqlType(converter.getCluster().getTypeFactory());
                            defaultValue = converter.getRexBuilder().makeLiteral(null, type);
                        }
                    }
                    if (defaultValue == null)
                        defaultValue = converter.convertExpression(cd.defaultValue);
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
                        "Previous definition");
            } else {
                columnDefinition.put(colName, col);
            }
            RelDataType type = this.specToRel(typeSpec, false);
            SourcePositionRange position = new SourcePositionRange(typeSpec.getParserPosition());
            if (isPrimaryKey) {
                if (type.isNullable()) {
                    // This is either an error or a warning, depending on the value of the 'lenient' flag
                    this.errorReporter.reportProblem(position,
                            this.options.languageOptions.lenient,
                            "PRIMARY KEY cannot be nullable",
                            "PRIMARY KEY column " + Utilities.singleQuote(name.getSimple()) +
                                    " has type " + type + ", which is nullable");
                    // Correct the type to be not-null
                    type = this.specToRel(typeSpec, true);
                }
                SqlTypeName tn = type.getSqlTypeName();
                if (tn == SqlTypeName.ARRAY || tn == SqlTypeName.MULTISET || tn == SqlTypeName.MAP) {
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
                    lateness, watermark, defaultValue);
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
            SqlCreateView.ViewKind kind) {
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
        for (Map.Entry<Integer, String> fieldPairs : relRoot.fields) {
            String queryFieldName = fieldPairs.getValue();
            RelDataTypeField field = fieldList.get(index);
            Objects.requireNonNull(field);
            boolean nameIsQuoted = false;
            if (columnNames != null) {
                SqlIdentifier id = (SqlIdentifier) columnNames.get(index);
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
                            "Previous definition");
                    error = true;
                } else {
                    columnDefinition.put(columnName, id);
                }
            }

            String actualColumnName = field.getName();
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
            }
            colByName.put(queryFieldName, field);
            RelColumnMetadata meta = new RelColumnMetadata(node,
                    field, false, nameIsQuoted, null, null, null);
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
            SqlWriter writer = new SqlPrettyWriter(
                    SqlPrettyWriter.config(), builder);
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
            Logger.INSTANCE.belowLevel(this, 2)
                    .newline()
                    .append(sql)
                    .newline();
            CalciteCompiler clone = new CalciteCompiler(this);
            List<ParsedStatement> list = clone.parseStatements(sql, true);
            FrontEndStatement statement = null;
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

    @Nullable
    public FrontEndStatement compile(ParsedStatement node, SourceFileContents sources) {
        String sqlStatement = node.toString();
        CalciteObject object = CalciteObject.create(node.statement);
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Compiling ")
                .append(sqlStatement)
                .newline();
        SqlKind kind = node.statement().getKind();
        switch (kind) {
            case DROP_TABLE: {
                SqlDropTable dt = (SqlDropTable) node.statement;
                String tableName = dt.name.getSimple();
                this.calciteCatalog.dropTable(tableName);
                return new DropTableStatement(node, sqlStatement, tableName);
            }
            case CREATE_TABLE: {
                SqlCreateTable ct = (SqlCreateTable) node.statement;
                if (ct.ifNotExists)
                    throw new UnsupportedException("IF NOT EXISTS not supported", object);
                String tableName = ct.name.getSimple();
                List<RelColumnMetadata> cols = this.createTableColumnsMetadata(ct, ct.name, sources);
                @Nullable PropertyList properties = this.createProperties(ct.tableProperties);
                if (properties != null)
                    properties.checkDuplicates(this.errorReporter);
                List<ForeignKey> fk = this.createForeignKeys(ct);
                CreateTableStatement table = new CreateTableStatement(
                        node, sqlStatement, tableName, Utilities.identifierIsQuoted(ct.name), cols, fk, properties);
                boolean success = this.calciteCatalog.addTable(
                        tableName, table.getEmulatedTable(), this.errorReporter, table);
                if (!success)
                    return null;
                return table;
            }
            case CREATE_FUNCTION: {
                SqlCreateFunctionDeclaration decl = (SqlCreateFunctionDeclaration) node.statement;
                List<Map.Entry<String, RelDataType>> parameters = Linq.map(
                        decl.getParameters(), param -> {
                            SqlAttributeDefinition attr = (SqlAttributeDefinition) param;
                            String name = attr.name.getSimple();
                            RelDataType type = this.specToRel(attr.dataType, false);
                            return new MapEntry<>(name, type);
                        });
                RelDataType structType = this.typeFactory.createStructType(parameters);
                SqlDataTypeSpec retType = decl.getReturnType();
                RelDataType returnType = this.specToRel(retType, false);
                Boolean nullableResult = retType.getNullable();
                if (nullableResult != null && nullableResult)
                    returnType = this.createNullableType(returnType);
                RexNode bodyExp = this.createFunction(decl, sources);
                ExternalFunction function = this.customFunctions.createUDF(
                        CalciteObject.create(node), decl.getName(), structType, returnType, bodyExp);
                return new CreateFunctionStatement(node, sqlStatement, function);
            }
            case CREATE_VIEW: {
                SqlToRelConverter converter = this.getConverter();
                SqlCreateView cv = (SqlCreateView) node.statement;
                SqlNode query = cv.query;
                if (cv.getReplace())
                    throw new UnsupportedException("OR REPLACE not supported", object);
                Logger.INSTANCE.belowLevel(this, 2)
                        .append(query.toString())
                        .newline();
                RelRoot relRoot = converter.convertQuery(query, true, true);
                List<RelColumnMetadata> columns = this.createViewColumnsMetadata(CalciteObject.create(node),
                        cv.name, relRoot, cv.columnList, cv.viewKind);
                if (columns == null)
                    return null;
                @Nullable PropertyList viewProperties = this.createProperties(cv.viewProperties);
                if (viewProperties != null) {
                    viewProperties.checkDuplicates(this.errorReporter);
                    SqlFragment materialized = viewProperties.getPropertyValue("materialized");
                    if (materialized != null) {
                        this.errorReporter.reportWarning(materialized.getSourcePosition(),
                                "Materialized property not used",
                                "The 'materialized' property for views is not used, " +
                                        "please use 'CREATE MATERIALIZED VIEW' instead");
                    }
                }
                RelNode optimized = this.optimize(relRoot.rel);
                relRoot = relRoot.withRel(optimized);
                String viewName = cv.name.getSimple();
                CreateViewStatement view = new CreateViewStatement(node, sqlStatement,
                        cv.name.getSimple(), Utilities.identifierIsQuoted(cv.name),
                        columns, cv, relRoot, viewProperties);
                // From Calcite's point of view we treat this view just as another table.
                boolean success = this.calciteCatalog.addTable(viewName, view.getEmulatedTable(), this.errorReporter, view);
                if (!success)
                    return null;
                return view;
            }
            case CREATE_TYPE: {
                SqlCreateType ct = (SqlCreateType) node.statement;
                RelProtoDataType proto = typeFactory -> {
                    if (ct.dataType != null) {
                        return this.specToRel(ct.dataType, false);
                    } else {
                        String name = ct.name.getSimple();
                        if (CalciteCompiler.this.udt.containsKey(name))
                            return CalciteCompiler.this.udt.get(name);
                        final RelDataTypeFactory.Builder builder = typeFactory.builder();
                        for (SqlNode def : Objects.requireNonNull(ct.attributeDefs)) {
                            final SqlAttributeDefinition attributeDef =
                                    (SqlAttributeDefinition) def;
                            final SqlDataTypeSpec typeSpec = attributeDef.dataType;
                            RelDataType type = this.specToRel(typeSpec, false);
                            if (typeSpec.getNullable() != null && typeSpec.getNullable()) {
                                // This is tricky, because it is not using the typeFactory that is
                                // the lambda argument above, but hopefully it should be the same
                                assert typeFactory == this.typeFactory;
                                type = this.createNullableType(type);
                            }
                            builder.add(attributeDef.name.getSimple(), type);
                        }
                        RelDataType result = builder.build();
                        RelStruct retval = new RelStruct(ct.name, result.getFieldList(), result.isNullable());
                        Utilities.putNew(CalciteCompiler.this.udt, name, retval);
                        return retval;
                    }
                };

                String typeName = ct.name.getSimple();
                this.rootSchema.add(typeName, proto);
                RelDataType relDataType = proto.apply(this.typeFactory);
                FrontEndStatement result = new CreateTypeStatement(node, sqlStatement, ct, typeName, relDataType);
                boolean success = this.calciteCatalog.addType(typeName, this.errorReporter, result);
                if (!success)
                    return null;
                return result;
            }
            case INSERT: {
                SqlToRelConverter converter = this.getConverter();
                SqlInsert insert = (SqlInsert) node.statement;
                SqlNode table = insert.getTargetTable();
                if (!(table instanceof SqlIdentifier id))
                    throw new CompilationError("INSERT statement expected a table name", CalciteObject.create(table));
                TableModifyStatement stat = new TableModifyStatement(node, true, sqlStatement, id.toString(), insert.getSource());
                RelRoot values = converter.convertQuery(stat.data, true, true);
                values = values.withRel(this.optimize(values.rel));
                stat.setTranslation(values.rel);
                return stat;
            }
            case DELETE: {
                // We expect this to be a REMOVE statement
                SqlToRelConverter converter = this.getConverter();
                if (node.statement instanceof SqlRemove insert) {
                    SqlNode table = insert.getTargetTable();
                    if (!(table instanceof SqlIdentifier id))
                        throw new CompilationError("REMOVE statement expected a table name", CalciteObject.create(table));
                    TableModifyStatement stat = new TableModifyStatement(node, false, sqlStatement, id.toString(), insert.getSource());
                    RelRoot values = converter.convertQuery(stat.data, true, true);
                    values = values.withRel(this.optimize(values.rel));
                    stat.setTranslation(values.rel);
                    return stat;
                }
                break;
            }
            case SELECT: {
                throw new UnsupportedException(
                        "Raw 'SELECT' statements are not supported; did you forget to CREATE VIEW?",
                        CalciteObject.create(node));
            }
            case OTHER: {
                if (node.statement instanceof SqlLateness lateness) {
                    RexNode expr = this.getConverter().convertExpression(lateness.getLateness());
                    return new LatenessStatement(node, sqlStatement,
                            lateness.getView(), lateness.getColumn(), expr);
                }
                break;
            }
        }
        throw new UnimplementedException("SQL statement not yet implemented", CalciteObject.create(node));
    }
}
