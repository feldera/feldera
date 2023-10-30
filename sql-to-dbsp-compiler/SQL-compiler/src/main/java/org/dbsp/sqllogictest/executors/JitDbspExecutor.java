package org.dbsp.sqllogictest.executors;

import net.hydromatic.sqllogictest.*;
import net.hydromatic.sqllogictest.executors.JdbcExecutor;
import net.hydromatic.sqllogictest.executors.SqlSltTestExecutor;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqllogictest.SqlTestPrepareInput;
import org.dbsp.sqllogictest.SqlTestPrepareTables;
import org.dbsp.sqllogictest.SqlTestPrepareViews;
import org.dbsp.util.TableValue;

import javax.annotation.Nullable;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * An executor that uses the DBSP JIT compiler/service to run tests.
 */
public class JitDbspExecutor extends SqlSltTestExecutor {
    protected final JdbcExecutor statementExecutor;
    protected final List<String> tablesCreated;
    final SqlTestPrepareInput inputPreparation;
    final SqlTestPrepareTables tablePreparation;
    final SqlTestPrepareViews viewPreparation;
    protected final CompilerOptions compilerOptions;
    static final String rustDirectory = "../temp/src/";
    static final String jitExecutableDirectory = "..";

    public JitDbspExecutor(JdbcExecutor executor,
                           OptionsParser.SuppliedOptions options,
                           CompilerOptions compilerOptions) {
        super(options);
        this.statementExecutor = executor;
        this.tablesCreated = new ArrayList<>();
        this.compilerOptions = compilerOptions;
        this.inputPreparation = new SqlTestPrepareInput();
        this.tablePreparation = new SqlTestPrepareTables();
        this.viewPreparation = new SqlTestPrepareViews();
    }

    Connection getStatementExecutorConnection() {
        return this.statementExecutor.getConnection();
    }

    DBSPZSetLiteral getTableContents(String table) throws SQLException {
        return DbspJdbcExecutor.getTableContents(this.getStatementExecutorConnection(), table);
    }

    @Nullable
    String rewriteCreateTable(String command) throws SQLException {
        Matcher m = DbspJdbcExecutor.PAT_CREATE.matcher(command);
        if (!m.find())
            return null;
        String tableName = m.group(1);
        this.tablesCreated.add(tableName);
        return DbspJdbcExecutor.generateCreateStatement(this.getStatementExecutorConnection(), tableName);
    }

    public boolean statement(SltSqlStatement statement) throws SQLException {
        this.statementExecutor.statement(statement);
        String command = statement.statement.toLowerCase();
        this.options.message("Executing " + command + "\n", 2);

        if (command.startsWith("create index"))
            return true;
        if (command.startsWith("create distinct index"))
            return false;
        if (command.contains("create table") || command.contains("drop table")) {
            String create = this.rewriteCreateTable(command);
            if (create != null) {
                statement = new SltSqlStatement(create, statement.shouldPass);
                this.tablePreparation.add(statement);
            } else {
                Matcher m = DbspJdbcExecutor.PAT_DROP.matcher(command);
                String tableName = m.group(1);
                this.tablesCreated.remove(tableName);
            }
        } else if (command.contains("create view")) {
            this.viewPreparation.add(statement);
        } else if (command.contains("drop view")) {
            this.viewPreparation.remove(statement);
        } else {
            this.inputPreparation.add(statement);
        }
        return true;
    }

    public TableValue[] getInputSets() throws SQLException {
        TableValue[] result = new TableValue[this.tablesCreated.size()];
        int i = 0;
        for (String table: this.tablesCreated) {
            DBSPZSetLiteral lit = this.getTableContents(table);
            result[i++] = new TableValue(table, lit);
        }
        return result;
    }

    // These should be reused from JdbcExecutor, but are not public there.

    /**
     * Run a batch of queries accumulated
     * @return null if we have to stop immediately, or a new batch otherwise.
     */
    @Nullable
    JitTestBatch runBatch(JitTestBatch batch, TestStatistics result) throws SQLException, NoSuchAlgorithmException {
        System.out.println("Running a batch of " + batch.size() + " queries");
        batch.prepareInputs(this.tablePreparation.statements);
        batch.prepareInputs(this.viewPreparation.definitions());
        batch.setInputContents(this.getInputSets());
        boolean failed = batch.run(result);
        if (failed)
            return null;
        return batch.nextBatch();
    }

    @Override
    public TestStatistics execute(SltTestFile testFile, OptionsParser.SuppliedOptions options) throws SQLException, NoSuchAlgorithmException {
        this.statementExecutor.establishConnection();
        this.statementExecutor.dropAllViews();
        this.statementExecutor.dropAllTables();
        this.startTest();
        TestStatistics result = new TestStatistics(
                options.stopAtFirstError, options.verbosity);
        result.incFiles();
        int queryNo = 0;
        int batchSize = 1;
        int skip = 0; // 710;  // used only for debugging

        int remainingInBatch = batchSize;
        JitTestBatch batch = new JitTestBatch(
                this.compilerOptions, this.options, rustDirectory, jitExecutableDirectory, queryNo);
        for (ISqlTestOperation operation: testFile.fileContents) {
            SltSqlStatement stat = operation.as(SltSqlStatement.class);
            if (stat != null) {
                if (batch.hasQueries()) {
                    batch = this.runBatch(batch, result);
                    if (batch == null)
                        return result;
                    remainingInBatch = batchSize;
                }
                try {
                    if (this.buggyOperations.contains(stat.statement)) {
                        this.options.message("Skipping buggy test " + stat.statement + "\n", 1);
                    } else {
                        this.statement(stat);
                        if (!stat.shouldPass) {
                            options.err.println("Statement should have failed: " + operation);
                        }
                    }
                } catch (SQLException ex) {
                    if (stat.shouldPass)
                        this.options.error(ex);
                }
                this.statementsExecuted++;
            } else {
                SqlTestQuery query = operation.to(options.err, SqlTestQuery.class);
                if (this.buggyOperations.contains(query.getQuery())) {
                    options.message("Skipping " + query.getQuery(), 2);
                    result.incIgnored();
                    continue;
                }
                if (skip > 0) {
                    skip--;
                    continue;
                }
                // Debugging code commented-out.
                // if (!"label-510".equals(query.getName())) continue;
                batch.addQuery(query);
                remainingInBatch--;
                queryNo++;
                if (remainingInBatch == 0) {
                    batch = this.runBatch(batch, result);
                    if (batch == null)
                        return result;
                    remainingInBatch = batchSize;
                }
            }
        }
        if (remainingInBatch != batchSize) {
            this.runBatch(batch, result);
        }
        this.statementExecutor.dropAllViews();
        this.statementExecutor.dropAllTables();
        this.getStatementExecutorConnection().close();
        options.message(this.elapsedTime(queryNo), 1);
        return result;
    }

    public static void register(OptionsParser parser) {
        parser.registerExecutor("jit", () -> {
            OptionsParser.SuppliedOptions options = parser.getOptions();
            try {
                JdbcExecutor inner = Objects.requireNonNull(options.getExecutorByName("hsql"))
                        .as(JdbcExecutor.class);
                CompilerOptions compilerOptions = new CompilerOptions();
                compilerOptions.ioOptions.jit = true;
                compilerOptions.languageOptions.throwOnError = options.stopAtFirstError;
                compilerOptions.languageOptions.lenient = true;
                compilerOptions.ioOptions.quiet = true;
                JitDbspExecutor result = new JitDbspExecutor(
                        Objects.requireNonNull(inner), options, compilerOptions);
                Set<String> bugs = options.readBugsFile();
                result.avoid(bugs);
                return result;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
