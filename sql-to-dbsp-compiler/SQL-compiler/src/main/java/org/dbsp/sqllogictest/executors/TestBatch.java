package org.dbsp.sqllogictest.executors;

import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.SltSqlStatement;
import net.hydromatic.sqllogictest.SqlTestQuery;
import net.hydromatic.sqllogictest.TestStatistics;
import org.dbsp.util.TableValue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
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
    TableValue[] inputContents = new TableValue[0];

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

    public void setInputContents(TableValue[] inputContents) {
        this.inputContents = inputContents;
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
    abstract boolean run(TestStatistics statistics) throws IOException, NoSuchAlgorithmException;
}
