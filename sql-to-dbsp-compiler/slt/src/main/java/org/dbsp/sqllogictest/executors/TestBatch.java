package org.dbsp.sqllogictest.executors;

import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.SltSqlStatement;
import net.hydromatic.sqllogictest.SqlTestQuery;
import net.hydromatic.sqllogictest.TestStatistics;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * A test batch is a description of multiple SQL Logic Test tests that share common
 * input tables and execute each one query.
 */
public abstract class TestBatch {
    final OptionsParser.SuppliedOptions options;
    final List<SqlTestQuery> queries;
    final int firstQueryNo;
    final String filesDirectory;
    @Nullable
    InputGenerator inputGenerator = null;

    protected TestBatch(OptionsParser.SuppliedOptions options, String filesDirectory, int firstQueryNo) {
        this.options = options;
        this.filesDirectory = filesDirectory;
        this.queries = new ArrayList<>();
        this.firstQueryNo = firstQueryNo;
    }

    File fileFromName(String name) {
        return new File(Paths.get(this.filesDirectory, name).toUri());
    }

    public abstract <T extends SltSqlStatement> void prepareInputs(Iterable<T> inputAndViewPreparation);

    public void setInputGenerator(InputGenerator inputGenerator) {
        this.inputGenerator = inputGenerator;
    }

    void addQuery(SqlTestQuery query) {
        this.queries.add(query);
    }

    boolean hasQueries() {
        return !this.queries.isEmpty();
    }

    public int size() {
        return this.queries.size();
    }

    /** Run the batch.  Return 'true' if we have to stop execution immediately. */
    abstract boolean run(TestStatistics statistics);
}
