package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPVecExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.Linq;
import org.junit.Test;

public class StructTests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        return super.testOptions(incremental, optimize);
    }

    @Test
    public void issue3262() {
        // Duplicates a test from CatalogTests, since programs with handles cannot be used to generate Rust tests.
        var ccs = this.getCCS(
                "CREATE TABLE fails (named_pairs MAP<VARCHAR, ROW(k VARCHAR, v VARCHAR)>);" +
                        "CREATE VIEW V AS SELECT named_pairs['a'].k FROM fails;");
        ccs.step("INSERT INTO fails VALUES(MAP['a', ROW('2', '3'), 'b', NULL])",
                """
                         result | weight
                        -----------------
                         2| 1""");
        this.addRustTestCase(ccs);
    }

    @Test
    public void rowTest() {
        String sql = """
                CREATE TABLE t(
                    group_id bigint,
                    x bigint,
                    y bigint,
                    z bigint
                );
                
                CREATE VIEW v2 AS
                    SELECT group_id,
                        ARG_MAX(z, (x,y))
                    FROM t
                    group by group_id;""";
        var ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES(0, 1, 2, 3), (0, 1, 1, 1), (4, 5, 6, 7);",
                """
                 group_id | z | weight
                -----------------------------
                 0        | 3 | 1
                 4        | 7 | 1""");
        this.addRustTestCase(ccs);
    }

    @Test
    public void nestedStructTest() {
        String sql = """
            CREATE TYPE address_typ AS (
               street          VARCHAR(30),
               city            VARCHAR(30),
               state           CHAR(2),
               postal_code     VARCHAR(6));
            CREATE TYPE person_typ AS (
               firstname       VARCHAR(30),
               lastname        VARCHAR(30),
               address         ADDRESS_TYP);
            CREATE TABLE PERS(p0 PERSON_TYP, p1 PERSON_TYP);
            CREATE VIEW V AS
            SELECT PERS.p0.address FROM PERS
            WHERE PERS.p0.firstname = 'Mike'""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        DBSPExpression address0 = new DBSPTupleExpression(true,
                new DBSPStringLiteral("Broadway", true),
                new DBSPStringLiteral("New York", true),
                new DBSPStringLiteral("NY", true),
                new DBSPStringLiteral("10000", true)
        );
        DBSPExpression person0 = new DBSPTupleExpression(true,
                new DBSPStringLiteral("Mike", true),
                new DBSPStringLiteral("John", true),
                address0
        );
        DBSPExpression pair = new DBSPTupleExpression(person0, person0);
        DBSPZSetExpression input = new DBSPZSetExpression(pair);
        DBSPZSetExpression output = new DBSPZSetExpression(new DBSPTupleExpression(address0));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }

    @Test
    public void structConstructorTest() {
        String ddl = """
            CREATE TYPE address_typ AS (
               street          VARCHAR(30),
               city            VARCHAR(30),
               state           CHAR(2),
               postal_code     VARCHAR(6));
            CREATE TYPE person_typ AS (
               firstname       VARCHAR(30),
               lastname        VARCHAR(30),
               address         ADDRESS_TYP);
            CREATE TABLE PERS(p0 PERSON_TYP, p1 PERSON_TYP);
            CREATE VIEW V0 AS
            SELECT address_typ(PERS.p0.address.street, PERS.p1.address.city, 'CA', '90000') FROM PERS;
            """;
        CompilerCircuitStream ccs = this.getCCS(ddl);
        DBSPExpression address0 = new DBSPTupleExpression(true,
                new DBSPStringLiteral("Broadway", true),
                new DBSPStringLiteral("New York", true),
                new DBSPStringLiteral("NY", true),
                new DBSPStringLiteral("10000", true)
        );
        DBSPExpression person0 = new DBSPTupleExpression( true,
                new DBSPStringLiteral("Mike", true),
                new DBSPStringLiteral("John", true),
                address0
        );
        DBSPExpression pair = new DBSPTupleExpression(person0, person0);
        DBSPZSetExpression input = new DBSPZSetExpression(pair);
        DBSPZSetExpression output = new DBSPZSetExpression(new DBSPTupleExpression(
                new DBSPTupleExpression(
                        new DBSPStringLiteral("Broadway", true),
                        new DBSPStringLiteral("New York", true),
                        new DBSPStringLiteral("CA", true),
                        new DBSPStringLiteral("90000", true))));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }

    @Test
    public void structArrayTest() {
        String ddl = """
            CREATE TYPE address_typ AS (
               street          VARCHAR ARRAY);
            CREATE TABLE PERS(p0 address_typ);
            CREATE VIEW V AS SELECT PERS.p0.street[1] FROM PERS;
            """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPExpression pers = new DBSPTupleExpression(true,
                new DBSPVecExpression(true,
                    new DBSPStringLiteral("Broadway", true),
                    new DBSPStringLiteral("5th Avenue", true),
                    new DBSPStringLiteral("1st Street", true)));
        DBSPZSetExpression input = new DBSPZSetExpression(new DBSPTupleExpression(pers));
        DBSPZSetExpression output = new DBSPZSetExpression(new DBSPTupleExpression(
                        new DBSPStringLiteral("Broadway", true)));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }

    @Test
    public void unnestStructTest() {
        String ddl = """
            CREATE TYPE address_typ AS (
               street          VARCHAR ARRAY);
            CREATE TABLE PERS(p0 address_typ);
            CREATE VIEW V AS SELECT st FROM PERS, UNNEST(PERS.p0.street) AS st;""";
        CompilerCircuitStream ccs = this.getCCS(ddl);
        DBSPExpression pers = new DBSPTupleExpression(true,
                new DBSPVecExpression(true,
                        new DBSPStringLiteral("Broadway", true),
                        new DBSPStringLiteral("5th Avenue", true),
                        new DBSPStringLiteral("1st Street", true)));
        DBSPZSetExpression input = new DBSPZSetExpression(new DBSPTupleExpression(pers));
        DBSPZSetExpression output = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPStringLiteral("Broadway", true)),
                new DBSPTupleExpression(
                        new DBSPStringLiteral("5th Avenue", true)),
                new DBSPTupleExpression(
                        new DBSPStringLiteral("1st Street", true)));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }

    @Test
    public void unnestStructVecStructTest() {
        String ddl = """
            CREATE TYPE simple AS (s INT, t BOOLEAN);
            CREATE TYPE vec AS (fields SIMPLE ARRAY);
            CREATE TABLE T(col vec);
            CREATE VIEW V AS SELECT A.* FROM (T CROSS JOIN UNNEST(T.col.fields) A)""";
        CompilerCircuitStream ccs = this.getCCS(ddl);
        DBSPBoolLiteral t = new DBSPBoolLiteral(true, true);
        DBSPExpression t0 = new DBSPTupleExpression(true,
                new DBSPVecExpression(true,
                        new DBSPTupleExpression(
                                true, new DBSPI32Literal(0, true), t),
                        new DBSPTupleExpression(
                                true, new DBSPI32Literal(1, true), t),
                        new DBSPTupleExpression(
                                true, new DBSPI32Literal(2, true), t)));
        DBSPExpression t1 = new DBSPTupleExpression(true,
                new DBSPVecExpression(true,
                        new DBSPTupleExpression(
                                true, new DBSPI32Literal(3, true), t),
                        new DBSPTupleExpression(
                                true, new DBSPI32Literal(4, true), t),
                        new DBSPTupleExpression(
                                true, new DBSPI32Literal(5, true), t)));
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(t0),
                new DBSPTupleExpression(t1));
        DBSPZSetExpression output = new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(0, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(1, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(2, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(3, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(4, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(5, true), t));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }

    @Test
    public void selectiveUnnestStructVecStructTest() {
        String ddl = """
            CREATE TYPE simple AS (s INT, t BOOLEAN);
            CREATE TYPE vec AS (fields SIMPLE ARRAY);
            CREATE TABLE T(col vec);
            CREATE VIEW V AS SELECT A.s + 1 FROM (T CROSS JOIN UNNEST(T.col.fields) A)""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPBoolLiteral t = new DBSPBoolLiteral(true, true);
        DBSPExpression t0 = new DBSPTupleExpression(true,
                new DBSPVecExpression(true,
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(0, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(1, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(2, true), t), true)));
        DBSPExpression t1 = new DBSPTupleExpression(true,
                new DBSPVecExpression(true,
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(3, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(4, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(5, true), t), true)));
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(t0),
                new DBSPTupleExpression(t1));
        DBSPZSetExpression output = new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(1, true)),
                new DBSPTupleExpression(new DBSPI32Literal(2, true)),
                new DBSPTupleExpression(new DBSPI32Literal(3, true)),
                new DBSPTupleExpression(new DBSPI32Literal(4, true)),
                new DBSPTupleExpression(new DBSPI32Literal(5, true)),
                new DBSPTupleExpression(new DBSPI32Literal(6, true)));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }

    @Test
    public void selectiveUnnestStructVecStructTestOrdinality() {
        String ddl = """
            CREATE TYPE simple AS (s INT, t BOOLEAN);
            CREATE TYPE vec AS (fields SIMPLE ARRAY);
            CREATE TABLE T(col vec);
            CREATE VIEW V AS SELECT A.s + 1 FROM (T CROSS JOIN UNNEST(T.col.fields) WITH ORDINALITY A)""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPBoolLiteral t = new DBSPBoolLiteral(true, true);
        DBSPExpression t0 = new DBSPTupleExpression(true,
                new DBSPVecExpression(true,
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(0, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(1, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(2, true), t), true)));
        DBSPExpression t1 = new DBSPTupleExpression(true,
                new DBSPVecExpression(true,
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(3, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(4, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(5, true), t), true)));
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(t0),
                new DBSPTupleExpression(t1));
        DBSPZSetExpression output = new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(1, true)),
                new DBSPTupleExpression(new DBSPI32Literal(2, true)),
                new DBSPTupleExpression(new DBSPI32Literal(3, true)),
                new DBSPTupleExpression(new DBSPI32Literal(4, true)),
                new DBSPTupleExpression(new DBSPI32Literal(5, true)),
                new DBSPTupleExpression(new DBSPI32Literal(6, true)));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }

    @Test
    public void structArrayStructTest() {
        String ddl = """
            CREATE TYPE address_typ AS (
               street          VARCHAR(30),
               city            VARCHAR(30),
               state           CHAR(2),
               postal_code     VARCHAR(6));
            CREATE TYPE person_typ AS (
               firstname       VARCHAR(30),
               lastname        VARCHAR(30),
               address         ADDRESS_TYP ARRAY);
            CREATE TABLE PERS(p0 PERSON_TYP);
            CREATE VIEW V AS SELECT PERS.p0.address[1] FROM PERS WHERE PERS.p0.firstname = 'Mike';
            """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPExpression address0 = new DBSPTupleExpression(true,
                new DBSPStringLiteral("Broadway", true),
                new DBSPStringLiteral("New York", true),
                new DBSPStringLiteral("NY", true),
                new DBSPStringLiteral("10000", true)
        );
        DBSPExpression person0 = new DBSPTupleExpression(true,
                new DBSPStringLiteral("Mike", true),
                new DBSPStringLiteral("John", true),
                new DBSPVecExpression(true, address0, address0)
        );
        DBSPExpression data = new DBSPTupleExpression(person0);
        DBSPZSetExpression input = new DBSPZSetExpression(data);
        DBSPZSetExpression output = new DBSPZSetExpression(new DBSPTupleExpression(address0));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }

    @Test
    public void nullableStruct() {
        String ddl = """
            CREATE TYPE address_typ AS (
               id       INTEGER,
               code     VARCHAR);
            CREATE TABLE Address(a ADDRESS_TYP);
            CREATE VIEW V AS SELECT Address.a.code FROM Address WHERE Address.a.id = 1;
            """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPType tuple = new DBSPTypeTuple(
                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true),
                DBSPTypeString.varchar(true)
        ).withMayBeNull(true);
        DBSPExpression address0 = tuple.none();
        DBSPZSetExpression input = new DBSPZSetExpression(new DBSPTupleExpression(address0));
        ccs.addPair(new Change(input), new Change());
        this.addRustTestCase(ccs);
    }
}
