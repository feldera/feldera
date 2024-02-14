package org.dbsp.sqllogictest;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.dbsp.util.StringPrintStream;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;

public class DBTests {
    @SuppressWarnings("SqlDialectInspection")
    @Test
    public void HSQLDBTest() throws SQLException, ClassNotFoundException {
        Class.forName("org.hsqldb.jdbcDriver");
        String jdbcUrl = "jdbc:hsqldb:mem:db";
        Connection connection = DriverManager.getConnection(jdbcUrl, "", "");
        try (Statement s = connection.createStatement()) {
            s.execute("""
                    create table mytable(
                    id integer not null primary key,
                    strcol varchar(25))
                    """);

            s.execute("insert into mytable values(0, 'str0')");
            s.execute("insert into mytable values(1, 'str1')");
        }

        StringPrintStream direct = new StringPrintStream();
        try (Statement s = connection.createStatement()) {
            ResultSet resultSet = s.executeQuery("SELECT * FROM mytable WHERE id > 0");
            Utilities.showResultSet(resultSet, direct.getPrintStream());
        }

        DataSource mockDataSource = JdbcSchema.dataSource(jdbcUrl, "org.hsqldb.jdbcDriver", "", "");
        Connection executorConnection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = executorConnection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("schema", JdbcSchema.create(rootSchema, "schema", mockDataSource, null, null));

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();
        RelBuilder r = RelBuilder.create(config);
        RelNode node = r
                .scan("schema", "MYTABLE")
                .filter(r.equals(r.field("ID"), r.literal(1)))
                .project(
                        r.field("ID"),
                        r.field("STRCOL")
                )
                .build();
        RelRunner runner = calciteConnection.unwrap(RelRunner.class);
        try (PreparedStatement ps = runner.prepareStatement(node)) {
            ps.execute();
            ResultSet resultSet = ps.getResultSet();
            StringPrintStream throughCalcite = new StringPrintStream();
            Utilities.showResultSet(resultSet, throughCalcite.getPrintStream());
            Assert.assertEquals(direct.toString(), throughCalcite.toString());
        }
    }

    @Test @Ignore("Fails due to a bug in HSQLDB")
    public void HSQLDBDoubleNegTest() throws SQLException {
        // Reproduction for https://sourceforge.net/p/hsqldb/bugs/1680/
        // and https://sourceforge.net/p/hsqldb/bugs/1681/
        String jdbcUrl = "jdbc:hsqldb:mem:db";
        Connection connection = DriverManager.getConnection(jdbcUrl, "", "");
        try (Statement s = connection.createStatement()) {
            s.execute("SELECT +2;");
            s.execute("SELECT - -2;");
        }
    }
}
