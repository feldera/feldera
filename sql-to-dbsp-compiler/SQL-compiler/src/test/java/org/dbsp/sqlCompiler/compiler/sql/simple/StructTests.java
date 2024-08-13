package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Linq;
import org.junit.Test;

public class StructTests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        return super.testOptions(incremental, optimize);
    }

    @Test
    public void nestedStructTest() {
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
            CREATE VIEW V AS
            SELECT PERS.p0.address FROM PERS
            WHERE PERS.p0.firstname = 'Mike'""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPExpression address0 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPStringLiteral("Broadway", true),
                new DBSPStringLiteral("New York", true),
                new DBSPStringLiteral("NY", true),
                new DBSPStringLiteral("10000", true)
        );
        DBSPExpression person0 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPStringLiteral("Mike", true),
                new DBSPStringLiteral("John", true),
                address0
        );
        DBSPExpression pair = new DBSPTupleExpression(person0, person0);
        DBSPZSetLiteral input = new DBSPZSetLiteral(pair);
        DBSPZSetLiteral output = new DBSPZSetLiteral(new DBSPTupleExpression(address0));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase("nestedStructTest", ccs);
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
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPExpression address0 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPStringLiteral("Broadway", true),
                new DBSPStringLiteral("New York", true),
                new DBSPStringLiteral("NY", true),
                new DBSPStringLiteral("10000", true)
        );
        DBSPExpression person0 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPStringLiteral("Mike", true),
                new DBSPStringLiteral("John", true),
                address0
        );
        DBSPExpression pair = new DBSPTupleExpression(person0, person0);
        DBSPZSetLiteral input = new DBSPZSetLiteral(pair);
        DBSPZSetLiteral output = new DBSPZSetLiteral(new DBSPTupleExpression(
                new DBSPTupleExpression(
                        new DBSPStringLiteral("Broadway", true),
                        new DBSPStringLiteral("New York", true),
                        new DBSPStringLiteral("CA", true),
                        new DBSPStringLiteral("90000", true))));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase("structConstructorTest", ccs);
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
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPExpression pers = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPVecLiteral(true,
                    new DBSPStringLiteral("Broadway", true),
                    new DBSPStringLiteral("5th Avenue", true),
                    new DBSPStringLiteral("1st Street", true)));
        DBSPZSetLiteral input = new DBSPZSetLiteral(new DBSPTupleExpression(pers));
        DBSPZSetLiteral output = new DBSPZSetLiteral(new DBSPTupleExpression(
                        new DBSPStringLiteral("Broadway", true)));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase("structArrayTest", ccs);
    }

    @Test
    public void unnestStructTest() {
        String ddl = """
            CREATE TYPE address_typ AS (
               street          VARCHAR ARRAY);
            CREATE TABLE PERS(p0 address_typ);""";
        String v = "CREATE VIEW V AS SELECT st FROM PERS, UNNEST(PERS.p0.street) AS st";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
        compiler.compileStatements(v);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPExpression pers = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPVecLiteral(true,
                        new DBSPStringLiteral("Broadway", true),
                        new DBSPStringLiteral("5th Avenue", true),
                        new DBSPStringLiteral("1st Street", true)));
        DBSPZSetLiteral input = new DBSPZSetLiteral(new DBSPTupleExpression(pers));
        DBSPZSetLiteral output = new DBSPZSetLiteral(
                new DBSPTupleExpression(
                        new DBSPStringLiteral("Broadway", true)),
                new DBSPTupleExpression(
                        new DBSPStringLiteral("5th Avenue", true)),
                new DBSPTupleExpression(
                        new DBSPStringLiteral("1st Street", true)));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase("unnestStructTest", ccs);
    }

    @Test
    public void unnestStructVecStructTest() {
        String ddl = """
            CREATE TYPE simple AS (s INT, t BOOLEAN);
            CREATE TYPE vec AS (fields SIMPLE ARRAY);
            CREATE TABLE T(col vec);
            CREATE VIEW V AS SELECT A.* FROM (T CROSS JOIN UNNEST(T.col.fields) A)""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPBoolLiteral t = new DBSPBoolLiteral(true, true);
        DBSPExpression t0 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPVecLiteral(true,
                        new DBSPTupleExpression(
                                CalciteObject.EMPTY, true, new DBSPI32Literal(0, true), t),
                        new DBSPTupleExpression(
                                CalciteObject.EMPTY, true, new DBSPI32Literal(1, true), t),
                        new DBSPTupleExpression(
                                CalciteObject.EMPTY, true, new DBSPI32Literal(2, true), t)));
        DBSPExpression t1 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPVecLiteral(true,
                        new DBSPTupleExpression(
                                CalciteObject.EMPTY, true, new DBSPI32Literal(3, true), t),
                        new DBSPTupleExpression(
                                CalciteObject.EMPTY, true, new DBSPI32Literal(4, true), t),
                        new DBSPTupleExpression(
                                CalciteObject.EMPTY, true, new DBSPI32Literal(5, true), t)));
        DBSPZSetLiteral input = new DBSPZSetLiteral(
                new DBSPTupleExpression(t0),
                new DBSPTupleExpression(t1));
        DBSPZSetLiteral output = new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(0, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(1, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(2, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(3, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(4, true), t),
                new DBSPTupleExpression(new DBSPI32Literal(5, true), t));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase("unnestStructVecStructTest", ccs);
    }

    @Test
    public void selectiveUnnestStructVecStructTest() {
        String ddl = """
            CREATE TYPE simple AS (s INT, t BOOLEAN);
            CREATE TYPE vec AS (fields SIMPLE ARRAY);
            CREATE TABLE T(col vec);
            CREATE VIEW V AS SELECT A.s + 1 FROM (T CROSS JOIN UNNEST(T.col.fields) A)""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPBoolLiteral t = new DBSPBoolLiteral(true, true);
        DBSPExpression t0 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPVecLiteral(true,
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(0, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(1, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(2, true), t), true)));
        DBSPExpression t1 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPVecLiteral(true,
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(3, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(4, true), t), true),
                        new DBSPTupleExpression(Linq.list(new DBSPI32Literal(5, true), t), true)));
        DBSPZSetLiteral input = new DBSPZSetLiteral(
                new DBSPTupleExpression(t0),
                new DBSPTupleExpression(t1));
        DBSPZSetLiteral output = new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(1, true)),
                new DBSPTupleExpression(new DBSPI32Literal(2, true)),
                new DBSPTupleExpression(new DBSPI32Literal(3, true)),
                new DBSPTupleExpression(new DBSPI32Literal(4, true)),
                new DBSPTupleExpression(new DBSPI32Literal(5, true)),
                new DBSPTupleExpression(new DBSPI32Literal(6, true)));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase("selectiveUnnestStructVecStructTest", ccs);
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
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPExpression address0 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPStringLiteral("Broadway", true),
                new DBSPStringLiteral("New York", true),
                new DBSPStringLiteral("NY", true),
                new DBSPStringLiteral("10000", true)
        );
        DBSPExpression person0 = new DBSPTupleExpression(CalciteObject.EMPTY, true,
                new DBSPStringLiteral("Mike", true),
                new DBSPStringLiteral("John", true),
                new DBSPVecLiteral(true, address0, address0)
        );
        DBSPExpression data = new DBSPTupleExpression(person0);
        DBSPZSetLiteral input = new DBSPZSetLiteral(data);
        DBSPZSetLiteral output = new DBSPZSetLiteral(new DBSPTupleExpression(address0));
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase("structArrayStructTest", ccs);
    }
}
