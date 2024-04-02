package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.junit.Ignore;
import org.junit.Test;

public class StructTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions options = super.testOptions(incremental, optimize);
        options.ioOptions.emitHandles = false;
        return options;
    }

    @Test
    public void structTest() {
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
            CREATE VIEW V AS SELECT PERS.p0.address FROM PERS WHERE PERS.p0.firstname = 'Mike';
            CREATE VIEW V0 AS SELECT address_typ(PERS.p0.address.street, PERS.p1.address.city, 'CA', '90000') FROM PERS;
            """;

        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase("structArray", ccs);
    }

    @Test @Ignore("Calcite seems to have a bug in the inference of the result types")
    public void structArrayTest2() {
        String ddl = """
            CREATE TYPE address_typ AS (
               street          VARCHAR(30),
               city            VARCHAR(30),
               state           CHAR(2),
               postal_code     VARCHAR(6));
            CREATE TABLE PERS(p0 address_typ ARRAY);
            CREATE VIEW V AS SELECT PERS.p0[1] FROM PERS;
            """;

        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase("structArrayTest", ccs);
    }

    @Test @Ignore
    public void structArrayTest() {
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
        this.addRustTestCase("structArrayTest", ccs);
    }
}
