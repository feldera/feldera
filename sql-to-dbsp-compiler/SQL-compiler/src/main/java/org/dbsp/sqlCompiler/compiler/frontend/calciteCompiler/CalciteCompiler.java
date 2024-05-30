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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.MapEntry;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlAttributeDefinition;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.dbsp.generated.parser.DbspParserImpl;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateFunctionStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTypeStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DropTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.SqlLateness;
import org.dbsp.sqlCompiler.compiler.frontend.statements.SqlLatenessStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.SqlRemove;
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
    /** If true the next view will be an output, otherwise it's just an intermediate result */
    boolean generateOutputForNextView = true;
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
        this.generateOutputForNextView = false;
        this.addOperatorTable(this.createOperatorTable());
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

    public void generateOutputForNextView(boolean generate) {
        this.generateOutputForNextView = generate;
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
            if (typeNameSpec instanceof SqlBasicTypeNameSpec) {
                SqlBasicTypeNameSpec basic = (SqlBasicTypeNameSpec) typeNameSpec;
                // I don't know how to get the SqlTypeName otherwise
                RelDataType relDataType = CalciteCompiler.this.specToRel(type);
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

    public static String getPlan(RelNode rel) {
        return RelOptUtil.dumpPlan("[Logical plan]", rel,
                SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES);
    }

    public RexBuilder getRexBuilder() {
        return this.cluster.getRexBuilder();
    }

    /** Keep here a number of empty lines.  This is done to fool the SqlParser
     * below: for each invocation of parseStatements we create a new SqlParser.
     * There is no way to reuse the previous parser one, unfortunately. */
    final StringBuilder newlines = new StringBuilder();

    SqlParser createSqlParser(String sql) {
        // This function can be invoked multiple times.
        // In order to get correct line numbers, we feed the parser extra empty lines
        // before the statements we compile in this round.
        String toParse = newlines + sql;
        SqlParser sqlParser = SqlParser.create(toParse, this.parserConfig);
        int lines = sql.split("\n").length;
        this.newlines.append("\n".repeat(lines));
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
    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser sqlParser = this.createSqlParser(sql);
        SqlNode result = sqlParser.parseStmt();
        result.accept(this.getTypeValidator());
        return result;
    }

    /** Given a list of statements separated by semicolons, parse all of them. */
    public SqlNodeList parseStatements(String statements) throws SqlParseException {
        SqlParser sqlParser = this.createSqlParser(statements);
        SqlNodeList sqlNodes = sqlParser.parseStmtList();
        for (SqlNode node: sqlNodes) {
            node.accept(this.getTypeValidator());
        }
        return sqlNodes;
    }

    RelNode optimize(RelNode rel) {
        int level = 2;
        if (rel instanceof LogicalValues)
            // Less verbose for LogicalValues
            level = 4;

        Logger.INSTANCE.belowLevel(this, level)
                .append("Before optimizer")
                .increase()
                .append(getPlan(rel))
                .decrease()
                .newline();

        RelBuilder relBuilder = this.converterConfig.getRelBuilderFactory().create(
                cluster, null);
        // This converts (some) correlated sub-queries into standard joins.
        rel = RelDecorrelator.decorrelateQuery(rel, relBuilder);
        Logger.INSTANCE.belowLevel(this, level)
                .append("After decorrelator")
                .increase()
                .append(getPlan(rel))
                .decrease()
                .newline();

        CalciteOptimizer optimizer = new CalciteOptimizer(this.options.languageOptions.optimizationLevel);
        rel = optimizer.apply(rel);
        Logger.INSTANCE.belowLevel(this, level)
                .append("After optimizer ")
                .increase()
                .append(getPlan(rel))
                .decrease()
                .newline();
        return rel;
    }

    public RelDataType specToRel(SqlDataTypeSpec spec) {
        SqlTypeNameSpec typeName = spec.getTypeNameSpec();
        String name = "";
        if (typeName instanceof SqlUserDefinedTypeNameSpec) {
            SqlUserDefinedTypeNameSpec udt = (SqlUserDefinedTypeNameSpec) typeName;
            SqlIdentifier identifier = udt.getTypeName();
            name = identifier.getSimple();
            if (this.udt.containsKey(name))
                return Utilities.getExists(this.udt, name);
        }
        RelDataType result = typeName.deriveType(this.getValidator());
        Boolean nullable = spec.getNullable();
        if (nullable != null && nullable)
            result = this.typeFactory.createTypeWithNullability(result, true);
        if (typeName instanceof SqlUserDefinedTypeNameSpec) {
            SqlUserDefinedTypeNameSpec udt = (SqlUserDefinedTypeNameSpec) typeName;
            if (result.isStruct()) {
                RelStruct retval = new RelStruct(udt.getTypeName(), result.getFieldList(), result.isNullable());
                Utilities.putNew(this.udt, name, retval);
                return retval;
            }
        }
        return result;
    }

    List<RelColumnMetadata> createTableColumnsMetadata(SqlNodeList list) {
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
                    if (!(keyColumn instanceof SqlIdentifier)) {
                        throw new InternalCompilerError("Expected an identifier",
                                CalciteObject.create(keyColumn));
                    }
                    SqlIdentifier identifier = (SqlIdentifier) keyColumn;
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
            if (col instanceof SqlColumnDeclaration) {
                SqlColumnDeclaration cd = (SqlColumnDeclaration) col;
                name = cd.name;
                typeSpec = cd.dataType;
            } else if (col instanceof SqlExtendedColumnDeclaration) {
                SqlExtendedColumnDeclaration cd = (SqlExtendedColumnDeclaration) col;
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
                if (cd.lateness != null)
                    lateness = converter.convertExpression(cd.lateness);
                if (cd.watermark != null)
                    watermark = converter.convertExpression(cd.watermark);
                if (cd.defaultValue != null) {
                    // workaround for https://issues.apache.org/jira/browse/CALCITE-6129
                    if (cd.defaultValue instanceof SqlLiteral) {
                        SqlLiteral literal = (SqlLiteral) cd.defaultValue;
                        if (literal.getTypeName() == SqlTypeName.NULL) {
                            RelDataType type = literal.createSqlType(converter.getCluster().getTypeFactory());
                            defaultValue = converter.getRexBuilder().makeLiteral(null, type);
                        }
                    }
                    if (defaultValue == null)
                        defaultValue = converter.convertExpression(cd.defaultValue);
                }
            } else if (col instanceof SqlKeyConstraint) {
                continue;
            } else {
                throw new UnimplementedException(CalciteObject.create(col));
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
            RelDataType type = this.specToRel(typeSpec);
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

    public List<RelColumnMetadata> createColumnsMetadata(CalciteObject node,
            SqlIdentifier objectName, boolean view, RelRoot relRoot, @Nullable SqlNodeList columnNames) {
        List<RelColumnMetadata> columns = new ArrayList<>();
        RelDataType rowType = relRoot.rel.getRowType();
        if (columnNames != null && columnNames.size() != relRoot.fields.size()) {
            this.errorReporter.reportError(
                    new SourcePositionRange(objectName.getParserPosition()),
                    "Column count mismatch",
                    (view ? "View " : " Table ") + objectName.getSimple() +
                            " specifies " + columnNames.size() + " columns " +
                            " but query computes " + relRoot.fields.size() + " columns");
            return columns;
        }
        int index = 0;
        Map<String, RelDataTypeField> colByName = new HashMap<>();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        for (Map.Entry<Integer, String> fieldPairs : relRoot.fields) {
            String specifiedName = fieldPairs.getValue();
            RelDataTypeField field = fieldList.get(index);
            Objects.requireNonNull(field);
            boolean nameIsQuoted = false;
            if (columnNames != null) {
                SqlIdentifier id = (SqlIdentifier)columnNames.get(index);
                String columnName = id.getSimple();
                nameIsQuoted = Utilities.identifierIsQuoted(id);
                field = new RelDataTypeFieldImpl(columnName, field.getIndex(), field.getType());
            }

            if (specifiedName != null) {
                if (colByName.containsKey(specifiedName)) {
                    if (!this.options.languageOptions.lenient) {
                        this.errorReporter.reportError(
                                new SourcePositionRange(objectName.getParserPosition()),
                                "Duplicate column",
                                (view ? "View " : "Table ") + objectName.getSimple() +
                                        " contains two columns with the same name " + Utilities.singleQuote(specifiedName) + "\n" +
                                        "You can allow this behavior using the --lenient compiler flag");
                    } else {
                        this.errorReporter.reportWarning(
                                new SourcePositionRange(objectName.getParserPosition()),
                                "Duplicate column",
                                (view ? "View " : "Table ") + objectName.getSimple() +
                                        " contains two columns with the same name " + Utilities.singleQuote(specifiedName) + "\n" +
                                        "Some columns will be renamed in the produced output.");
                    }
                }
                colByName.put(specifiedName, field);
            }
            RelColumnMetadata meta = new RelColumnMetadata(node,
                    field, false, nameIsQuoted, null, null, null);
            columns.add(meta);
            index++;
        }
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

        void visitScan(TableScan scan) {
            // nothing
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
                // Anything else is an exception
                throw new UnimplementedException("Function too complex", CalciteObject.create(node));
        }
    }

    @Nullable
    RexNode createFunction(SqlCreateFunctionDeclaration decl) {
        SqlNode body = decl.getBody();
        if (body == null)
            return null;

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
            decl.getParameters().unparse(writer, 0, 0);
            builder.append(");\n");
            builder.append("CREATE VIEW TMP0 AS SELECT ");
            body.unparse(writer, 0, 0);
            builder.append(" FROM TMP;");

            String sql = builder.toString();
            CalciteCompiler clone = new CalciteCompiler(this);
            SqlNodeList list = clone.parseStatements(sql);
            FrontEndStatement statement = null;
            for (SqlNode node: list) {
                statement = clone.compile(node.toString(), node, null);
            }

            CreateViewStatement view = Objects.requireNonNull(statement).as(CreateViewStatement.class);
            assert view != null;
            RelNode node = view.getRelNode();
            ProjectExtractor extractor = new ProjectExtractor();
            extractor.go(node);
            return Objects.requireNonNull(extractor.body);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    /** Compile a SQL statement.
     * @param node         Compiled version of the SQL statement.
     * @param sqlStatement SQL statement as a string to compile.
     * @param comment      Additional information about the compiled statement. */
    @Nullable
    public FrontEndStatement compile(
            String sqlStatement,
            SqlNode node,
            @Nullable String comment) {
        CalciteObject object = CalciteObject.create(node);
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Compiling ")
                .append(sqlStatement)
                .newline();
        SqlKind kind = node.getKind();
        switch (kind) {
            case DROP_TABLE: {
                SqlDropTable dt = (SqlDropTable) node;
                String tableName = dt.name.getSimple();
                this.calciteCatalog.dropTable(tableName);
                return new DropTableStatement(node, sqlStatement, tableName, comment);
            }
            case CREATE_TABLE: {
                SqlCreateTable ct = (SqlCreateTable) node;
                if (ct.ifNotExists)
                    throw new UnsupportedException("IF NOT EXISTS not supported", object);
                String tableName = ct.name.getSimple();
                List<RelColumnMetadata> cols;
                if (ct.columnList != null) {
                    cols = this.createTableColumnsMetadata(Objects.requireNonNull(ct.columnList));
                } else {
                    throw new UnimplementedException();
                    /*
                    if (ct.query == null)
                        throw new UnsupportedException("CREATE TABLE cannot contain a query",
                                CalciteObject.create(node));
                    Logger.INSTANCE.belowLevel(this, 1)
                            .append(ct.query.toString())
                            .newline();
                    RelRoot relRoot = converter.convertQuery(ct.query, true, true);
                    cols = this.createColumnsMetadata(
                            ct.name, false, relRoot, null);
                     */
                }
                CreateTableStatement table = new CreateTableStatement(
                        node, sqlStatement, tableName, Utilities.identifierIsQuoted(ct.name), comment, cols);
                boolean success = this.calciteCatalog.addTable(
                        tableName, table.getEmulatedTable(), this.errorReporter, table);
                if (!success)
                    return null;
                return table;
            }
            case CREATE_FUNCTION: {
                SqlCreateFunctionDeclaration decl = (SqlCreateFunctionDeclaration) node;
                List<Map.Entry<String, RelDataType>> parameters = Linq.map(
                        decl.getParameters(), param -> {
                            SqlAttributeDefinition attr = (SqlAttributeDefinition) param;
                            String name = attr.name.getSimple();
                            RelDataType type = this.specToRel(attr.dataType);
                            return new MapEntry<>(name, type);
                        });
                RelDataType structType = this.typeFactory.createStructType(parameters);
                SqlDataTypeSpec retType = decl.getReturnType();
                RelDataType returnType = this.specToRel(retType);
                Boolean nullableResult = retType.getNullable();
                if (nullableResult != null)
                    returnType = this.typeFactory.createTypeWithNullability(returnType,  nullableResult);
                RexNode bodyExp = this.createFunction(decl);
                ExternalFunction function = this.customFunctions.createUDF(
                        CalciteObject.create(node), decl.getName(), structType, returnType, bodyExp);
                return new CreateFunctionStatement(node, sqlStatement, function);
            }
            case CREATE_VIEW: {
                SqlToRelConverter converter = this.getConverter();
                SqlCreateView cv = (SqlCreateView) node;
                SqlNode query = cv.query;
                if (cv.getReplace())
                    throw new UnsupportedException("OR REPLACE not supported", object);
                Logger.INSTANCE.belowLevel(this, 2)
                        .append(query.toString())
                        .newline();
                RelRoot relRoot = converter.convertQuery(query, true, true);
                List<RelColumnMetadata> columns = this.createColumnsMetadata(CalciteObject.create(node),
                        cv.name, true, relRoot, cv.columnList);
                RelNode optimized = this.optimize(relRoot.rel);
                relRoot = relRoot.withRel(optimized);
                String viewName = cv.name.getSimple();
                CreateViewStatement view = new CreateViewStatement(
                        node, sqlStatement,
                        cv.name.getSimple(), Utilities.identifierIsQuoted(cv.name),
                        comment, columns, cv.query, relRoot);
                // From Calcite's point of view we treat this view just as another table.
                boolean success = this.calciteCatalog.addTable(viewName, view.getEmulatedTable(), this.errorReporter, view);
                if (!success)
                    return null;
                return view;
            }
            case CREATE_TYPE: {
                SqlCreateType ct = (SqlCreateType) node;
                RelProtoDataType proto = typeFactory -> {
                    if (ct.dataType != null) {
                        return this.specToRel(ct.dataType);
                    } else {
                        String name = ct.name.getSimple();
                        if (CalciteCompiler.this.udt.containsKey(name))
                            return CalciteCompiler.this.udt.get(name);
                        final RelDataTypeFactory.Builder builder = typeFactory.builder();
                        for (SqlNode def : Objects.requireNonNull(ct.attributeDefs)) {
                            final SqlAttributeDefinition attributeDef =
                                    (SqlAttributeDefinition) def;
                            final SqlDataTypeSpec typeSpec = attributeDef.dataType;
                            final RelDataType type = this.specToRel(typeSpec);
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
                SqlInsert insert = (SqlInsert) node;
                SqlNode table = insert.getTargetTable();
                if (!(table instanceof SqlIdentifier))
                    throw new UnimplementedException(CalciteObject.create(table));
                SqlIdentifier id = (SqlIdentifier) table;
                TableModifyStatement stat = new TableModifyStatement(node, true, sqlStatement, id.toString(), insert.getSource(), comment);
                RelRoot values = converter.convertQuery(stat.data, true, true);
                values = values.withRel(this.optimize(values.rel));
                stat.setTranslation(values.rel);
                return stat;
            }
            case DELETE: {
                // We expect this to be a REMOVE statement
                SqlToRelConverter converter = this.getConverter();
                if (node instanceof SqlRemove) {
                    SqlRemove insert = (SqlRemove) node;
                    SqlNode table = insert.getTargetTable();
                    if (!(table instanceof SqlIdentifier))
                        throw new UnimplementedException(CalciteObject.create(table));
                    SqlIdentifier id = (SqlIdentifier) table;
                    TableModifyStatement stat = new TableModifyStatement(node, false, sqlStatement, id.toString(), insert.getSource(), comment);
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
                if (node instanceof SqlLateness) {
                    SqlLateness lateness = (SqlLateness) node;
                    RexNode expr = this.getConverter().convertExpression(lateness.getLateness());
                    return new SqlLatenessStatement(lateness, sqlStatement,
                            lateness.getView(), lateness.getColumn(), expr);
                }
                break;
            }
        }
        throw new UnimplementedException(CalciteObject.create(node));
    }
}
