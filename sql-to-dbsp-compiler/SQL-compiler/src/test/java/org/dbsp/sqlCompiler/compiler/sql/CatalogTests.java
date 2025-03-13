package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.junit.Assert;
import org.junit.Test;

/** Tests that emit Rust code using the catalog. */
public class CatalogTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions result = super.testOptions(incremental, optimize);
        // result.ioOptions.sqlNames = true;
        result.ioOptions.emitHandles = false;
        result.languageOptions.unrestrictedIOTypes = false;
        return result;
    }

    @Test
    public void issue3615() {
        this.statementsFailingInCompilation("""
                CREATE TABLE example (
                    inserted_xid BIGINT not null,
                    deleted_xid BIGINT not null default null -- '9223372036854775807'
                );""", "Nullable default value assigned to non-null column 'deleted_xid'");
    }

    @Test
    public void issue3262() {
        this.getCCS("""
                  CREATE TABLE T(p MAP<VARCHAR, ROW(k VARCHAR, v VARCHAR)>);
                  CREATE VIEW V AS SELECT p['a'].k FROM T;""");
    }

    @Test
    public void testArray() {
        this.getCCS("""
                  CREATE TYPE X AS (x INT);
                  CREATE FUNCTION R() RETURNS X NOT NULL AS X(1);
                  CREATE TABLE T(p int array, m MAP<INT, X ARRAY>);
                  CREATE VIEW V AS SELECT p[1], m[2][3] FROM T;""");
    }

    @Test
    public void nullableRow() {
        this.getCCS("""
                  CREATE TABLE T(p ROW(k VARCHAR, v VARCHAR));
                  CREATE VIEW V AS SELECT t.p.k FROM T;""");
    }

    @Test
    public void issue3263() {
        this.statementsFailingInCompilation(
                "CREATE TYPE BOXED_VALUE AS ROW(Value VARCHAR);",
                "User-defined types cannot be defined to be ROW types");
    }

    @Test
    public void issue3635() {
        this.compileRustTestCase("""
                CREATE TABLE t (
                    id VARCHAR,
                    state VARCHAR,
                    r ROW(
                        a ROW(
                            d VARCHAR
                        ),
                        b ROW(
                            e ROW(
                                f ROW(
                                    h VARCHAR,
                                    i VARCHAR
                                ),
                                g VARCHAR
                            )
                        ),
                        c MAP<VARCHAR, VARCHAR>
                    )
                );
                
                CREATE MATERIALIZED VIEW v
                AS
                SELECT t.r.b -- or t.r.b.e, or t.r.b.e.g
                FROM t
                WHERE state = 'ACTIVE' -- this is necessary
                ;""");
    }

    @Test
    public void issue3262a() {
        this.compileRustTestCase("""
                CREATE TABLE T (
                        h VARCHAR,
                        i MAP<VARCHAR, VARCHAR>,
                        j ROW(
                            k ROW(
                                l VARCHAR
                            ),
                            m ROW(
                                n ROW(
                                    o ROW(
                                        p VARCHAR,
                                        q VARCHAR
                                    ),
                                    r VARCHAR
                                ) NULL
                            ),
                            s MAP<VARCHAR, ROW(
                                t VARCHAR
                            )>
                        ),
                        x BIGINT
                    );
                CREATE VIEW V AS SELECT h, t.i['b'], t.j.k.l, t.j.m, t.j.m.n FROM T;""");
    }

    @Test
    public void issue3219() {
        this.statementsFailingInCompilation("""
                CREATE TABLE CUSTOMER (
                    "cc_NUM" BIGINT NOT NULL PRIMARY KEY
                );
                CREATE TABLE TRANSACTION (
                    "cc_NUM" BIGINT NOT NULL,
                    FOREIGN KEY ("cc_NUM") REFERENCES CUSTOMER(cc_NUM)
                )""", """
                    6|    FOREIGN KEY ("cc_NUM") REFERENCES CUSTOMER(cc_NUM)
                                                                     ^^^^^^
                """);
    }

    @Test
    public void issue3056() {
        this.statementsFailingInCompilation("""
                CREATE FUNCTION ZERO() RETURNS INT AS 0;
                CREATE FUNCTION PLUSONE(x integer) RETURNS INT AS x + 1;
                CREATE FUNCTION NONCONSTANT() RETURNS TIMESTAMP AS NOW();
                CREATE TABLE t (
                        r INT DEFAULT ZERO(),
                        r1 TIMESTAMP DEFAULT NONCONSTANT(),
                        r2 INT DEFAULT PLUSONE(ZERO()),
                        insert_time TIMESTAMP DEFAULT NOW()
                    )""",
                "Default value for column 'r1' must be a compile-time constant.*" +
                "Default value for column 'insert_time' must be a compile-time constant", true);
        this.compileRustTestCase("""
                CREATE FUNCTION ZERO() RETURNS INT AS 0;
                CREATE FUNCTION PLUSONE(x integer) RETURNS INT AS x + 1;
                CREATE FUNCTION NONCONSTANT() RETURNS TIMESTAMP AS NOW();
                CREATE TABLE t (
                        r INT DEFAULT ZERO(),
                        r2 INT DEFAULT PLUSONE(ZERO())
                    )""");
    }

    @Test
    public void issue2946() {
        this.compileRustTestCase("CREATE VIEW v(c0) AS (SELECT NULL);");
    }

    @Test
    public void issue2939() {
        String sql = """
                CREATE TABLE T(id int);
                CREATE VIEW V(x, y) AS SELECT id, id FROM T;""";
        var ccs = this.getCCS(sql);
        assert ccs.compiler.messages.messages.isEmpty();
    }

    @Test
    public void duplicateViewExplicitColumnName() {
        String sql = """
                CREATE TABLE T(id int);
                CREATE VIEW V AS SELECT id as col0, cast(id AS BIGINT) as col0 FROM T;""";
        var ccs = this.getCCS(sql);
        assert ccs.compiler.messages.messages.isEmpty();
    }

    @Test
    public void issue2949() {
        String sql = """
                CREATE TABLE t4(c0 BOOLEAN, c1 DOUBLE, c2 VARCHAR);
                CREATE MATERIALIZED VIEW v1(c0, c1, c2) AS (SELECT t4.c2, t4.c0, t4.c0 FROM t4 WHERE t4.c0);""";
        var cc = this.getCC(sql);
        // TestUtil.assertMessagesContain does not work for warnings is 'quiet' = true.
        assert cc.compiler.messages.messages.toString()
                .contains("Column 'c1' of table 't4' is unused");
    }

    @Test
    public void duplicatedViewColumnName() {
        String sql = """
                CREATE TABLE T(id int);
                CREATE VIEW V(x, x) AS SELECT id, id+1 FROM T;""";
        this.statementsFailingInCompilation(sql, "Column with name 'x' already defined");
    }

    @Test
    public void negativeIndexTests() {
        String sql = """
                CREATE TABLE T(id int, v VARCHAR, z INT ARRAY);
                CREATE VIEW V AS SELECT * FROM T;
                CREATE INDEX IX ON V(id, id);""";
        this.statementsFailingInCompilation(sql,
                "Column 'id' duplicated in index");
        sql = """
                CREATE TABLE T(id int, v VARCHAR, z INT ARRAY);
                CREATE VIEW V AS SELECT * FROM T;
                CREATE INDEX IX ON V(unknown);""";
        this.statementsFailingInCompilation(sql,
                "Column 'unknown' used in CREATE INDEX statement 'ix' does not exist in view 'v'");
        sql = """
                CREATE TABLE T(id int, v VARCHAR, z INT ARRAY);
                CREATE VIEW V AS SELECT * FROM T;
                CREATE INDEX IX ON Z(id);""";
        this.statementsFailingInCompilation(sql,
                "Object with name 'z' used in CREATE INDEX statement 'ix' does not exist.");
        sql = """
                CREATE TABLE T(id int, v VARCHAR, z INT ARRAY);
                CREATE VIEW V AS SELECT * FROM T;
                CREATE INDEX IX ON V(z);""";
        this.statementsFailingInCompilation(sql,
                "Cannot index on column 'z' because it has type ARRAY");
        sql = """
                CREATE TABLE T(id int, v VARCHAR, z INT ARRAY);
                CREATE INDEX IX ON T(id);""";
        this.shouldWarn(sql, "INDEX 'ix' refers to TABLE 't'; this has no effect.");
    }

    @Test
    public void indexTest() {
        String sql = """
                CREATE TABLE T(id int, v VARCHAR, z INT ARRAY);
                CREATE VIEW V AS SELECT * FROM T;
                CREATE INDEX IX ON V(id, v);""";
        this.getCCS(sql);
    }

    @Test
    public void issue2028() {
        String sql = """
                CREATE TABLE varchar_pk (
                    pk VARCHAR NOT NULL PRIMARY KEY
                );

                CREATE VIEW V WITH ('rust' = '//code emitted')
                AS SELECT * FROM varchar_pk;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testNulls() {
        // Test for nullability of array elements and structure fields
        String sql = """
                CREATE TYPE s AS (
                    xN INT,
                    x INT NOT NULL,
                    aN INT ARRAY,
                    -- aNN INT NOT NULL ARRAY NOT NULL, -- not allowed
                    a INT ARRAY NOT NULL
                );

                CREATE TYPE n AS (
                    svN s,
                    sv s NOT NULL,
                    svAN s ARRAY -- vec of nullable s
                );

                CREATE TABLE T (
                   f s,
                   g n,
                   h s NOT NULL,
                   i n NOT NULL,
                   r ROW(le s, ri INT)
                );

                CREATE VIEW V AS SELECT * FROM T;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        compiler.showErrors(System.err);
        Assert.assertNotNull(circuit);
        DBSPType type = circuit.getSingleOutputType().to(DBSPTypeZSet.class).elementType;
        Assert.assertTrue(type.is(DBSPTypeTuple.class));
        DBSPTypeTuple t = type.to(DBSPTypeTuple.class);
        Assert.assertEquals(5, t.size());
        DBSPType t0 = t.getFieldType(0);
        Assert.assertTrue(t0.is(DBSPTypeTuple.class));

        DBSPTypeTuple sN = t0.to(DBSPTypeTuple.class);
        Assert.assertEquals(4, sN.size());
        DBSPType i32 = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false);
        DBSPType i32N = i32.withMayBeNull(true);
        Assert.assertTrue(sN.mayBeNull);
        Assert.assertTrue(sN.getFieldType(0).sameType(i32N));
        Assert.assertTrue(sN.getFieldType(1).sameType(i32));
        Assert.assertTrue(sN.getFieldType(2).sameType(new DBSPTypeArray(i32N, true)));
        Assert.assertTrue(sN.getFieldType(3).sameType(new DBSPTypeArray(i32N, false)));

        DBSPType t1 = t.getFieldType(1);
        Assert.assertTrue(t1.is(DBSPTypeTuple.class));
        DBSPTypeTuple nN = t1.to(DBSPTypeTuple.class);
        Assert.assertEquals(3, nN.size());
        DBSPType vecS = new DBSPTypeArray(sN.withMayBeNull(true), true);
        Assert.assertTrue(nN.mayBeNull);
        Assert.assertTrue(nN.getFieldType(0).sameType(sN));
        Assert.assertTrue(nN.getFieldType(1).sameType(sN.withMayBeNull(false)));
        Assert.assertTrue(nN.getFieldType(2).sameType(vecS));

        DBSPType t2 = t.getFieldType(2);
        Assert.assertTrue(t2.sameType(sN.withMayBeNull(false)));

        DBSPType t3 = t.getFieldType(3);
        Assert.assertTrue(t3.sameType(nN.withMayBeNull(false)));

        DBSPType t4 = t.getFieldType(4);
        Assert.assertTrue(t4.is(DBSPTypeTuple.class));
    }

    @Test
    public void issue1894() {
        String sql = """
                CREATE TYPE trans AS (
                    category STRING
                );
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1755() {
        String sql = """
                CREATE TYPE CustomType AS (
                    version TINYINT not null
                );

                CREATE TABLE Data (
                    id BIGINT not null primary key,
                    msg CustomType
                );
                
                CREATE VIEW V AS SELECT * FROM Data;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1755_2() {
        String sql = """
                CREATE TYPE ADDRESS AS (
                   address_type    VARCHAR,
                   address         VARCHAR
                );

                CREATE TYPE CustomType AS (
                    version TINYINT not null,
                    address ADDRESS
                );

                CREATE TABLE Data (
                    id BIGINT not null primary key,
                    msg CustomType
                );""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1941() {
        String sql = """
                CREATE TABLE Data (
                    map MAP<INT, INT>
                );""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testSanitizeNames() {
        String sql = """
                create table t1(
                c1 integer,
                "col" boolean,
                "SPACES INSIDE" CHAR,
                "CC" CHAR,
                "quoted "" with quote" CHAR,
                U&"d\\0061t\\0061" CHAR, -- 'data' spelled in Unicode
                José CHAR,
                "Gosé" CHAR,
                "\uD83D\uDE00❤" varchar not null,
                "αβγ" boolean not null,
                ΔΘ boolean not null);
                create view v1 as select * from t1;""";
        this.getCCS(sql);
    }

    @Test
    public void testComplex() {
        String statements = """
                -- Git repository.
                create table repository (
                    repository_id bigint not null primary key,
                    type varchar not null,
                    url varchar not null,
                    name varchar not null
                );

                -- Commit inside a Git repo.
                create table git_commit (
                    git_commit_id bigint not null,
                    repository_id bigint not null,
                    commit_id varchar not null,
                    commit_date timestamp not null,
                    commit_owner varchar not null
                );

                -- CI pipeline.
                create table pipeline (
                    pipeline_id bigint not null,
                    name varchar not null,
                    create_date timestamp not null,
                    createdby_user_id bigint not null,
                    update_date timestamp,
                    updatedby_user_id bigint
                );


                -- Git commits used by each pipeline.
                create table pipeline_sources (
                    git_commit_id bigint not null,
                    pipeline_id bigint not null
                );

                -- Binary artifact created by a CI pipeline.
                create table artifact (
                    artifact_id bigint not null,
                    artifact_uri varchar not null,
                    path varchar not null,
                    create_date timestamp not null,
                    createdby_user_id bigint not null,
                    update_date timestamp,
                    updatedby_user_id bigint,
                    checksum varchar not null,
                    checksum_type varchar not null,
                    artifact_size_in_bytes bigint not null,
                    artifact_type varchar not null,
                    builtby_pipeline_id bigint not null,
                    parent_artifact_id bigint
                );

                -- Vulnerabilities discovered in source code.
                create table vulnerability (
                    vulnerability_id bigint not null,
                    discovery_date timestamp not null,
                    discovered_by varchar not null,
                    discovered_in bigint not null /*git_commit_id*/,
                    update_date timestamp,
                    updatedby_user_id bigint,
                    checksum varchar not null,
                    checksum_type varchar not null,
                    vulnerability_reference_id varchar not null,
                    severity varchar,
                    priority varchar
                );

                -- Deployed k8s objects.
                create table k8sobject (
                    k8sobject_id bigint not null,
                    create_date timestamp not null,
                    createdby_user_id bigint not null,
                    update_date timestamp,
                    updatedby_user_id bigint,
                    checksum varchar not null,
                    checksum_type varchar not null,
                    deployed_id bigint not null /*k8scluster_id*/,
                    deployment_type varchar not null,
                    k8snamespace varchar not null
                );

                -- Binary artifacts used to construct k8s objects.
                create table k8sartifact (
                    artifact_id bigint not null,
                    k8sobject_id bigint not null
                );

                -- K8s clusters.
                create table k8scluster (
                    k8scluster_id bigint not null,
                    k8s_uri varchar not null,
                    path varchar not null,
                    name varchar not null,
                    k8s_serivce_provider varchar not null
                );

                -- Vulnerabilities that affect each pipeline.
                create view pipeline_vulnerability (
                    pipeline_id,
                    vulnerability_id
                ) as
                    SELECT pipeline_sources.pipeline_id as pipeline_id, vulnerability.vulnerability_id as vulnerability_id FROM
                    pipeline_sources
                    INNER JOIN
                    vulnerability
                    ON pipeline_sources.git_commit_id = vulnerability.discovered_in;

                -- Vulnerabilities that could propagate to each artifact.
                create view artifact_vulnerability (
                    artifact_id,
                    vulnerability_id
                ) as
                    SELECT artifact.artifact_id as artifact_id, pipeline_vulnerability.vulnerability_id as vulnerability_id FROM
                    artifact
                    INNER JOIN
                    pipeline_vulnerability
                    ON artifact.builtby_pipeline_id = pipeline_vulnerability.pipeline_id;

                -- Vulnerabilities in the artifact or any of its children.
                create view transitive_vulnerability(
                    artifact_id,
                    via_artifact_id,
                    vulnerability_id
                ) as
                    SELECT artifact_id, artifact_id as via_artifact_id, vulnerability_id from artifact_vulnerability
                    UNION
                    (
                        SELECT
                            artifact.parent_artifact_id as artifact_id,
                            artifact.artifact_id as via_artifact_id,
                            artifact_vulnerability.vulnerability_id as vulnerability_id FROM
                        artifact
                        INNER JOIN
                        artifact_vulnerability
                        ON artifact.artifact_id = artifact_vulnerability.artifact_id
                        WHERE artifact.parent_artifact_id IS NOT NULL
                    );

                -- create view k8sobject_vulnerability ();

                -- create view k8scluster_vulnerability ();

                -- Number of vulnerabilities.
                -- Most severe vulnerability.
                -- create view k8scluster_vulnerability_stats ();""";
        this.getCCS(statements);
    }

    // Test for https://github.com/feldera/feldera/issues/1151
    @Test
    public void primaryKeyTest() {
        // This is identical to ComplexQueriesTest.primaryKeyTest, but here
        // we generate code in a different way.
        String sql = "CREATE TABLE event_t ( id BIGINT NOT NULL PRIMARY KEY, local_event_dt DATE )";
        this.getCCS(sql);
    }

    @Test
    public void primaryKeyTest2() {
        String sql = """
                create table t1(
                   id1 bigint not null,
                   id2 bigint not null,
                   str1 varchar not null,
                   str2 varchar,
                   int1 bigint not null,
                   int2 bigint,
                   primary key(id1, id2)
                )""";
        this.getCCS(sql);
    }

    @Test
    public void issue3637() {
        var ccs = this.getCCS("""
                CREATE TABLE t (id VARCHAR);
                
                DECLARE RECURSIVE VIEW v(
                    id VARCHAR,
                    parent_id VARCHAR
                );
                
                CREATE MATERIALIZED VIEW v
                AS SELECT id,
                    -- Delta lake output connector using field type Null instead of using the explicit VARCHAR
                    NULL AS parent_id
                FROM t;""");
        var circuit = ccs.getCircuit();
        DBSPSinkOperator v = circuit.getSink(new ProgramIdentifier("v", false));
        Assert.assertNotNull(v);
        DBSPType rowType = v.getOutputZSetElementType();
        Assert.assertTrue(rowType.is(DBSPTypeTuple.class));
        DBSPTypeTuple tuple = rowType.to(DBSPTypeTuple.class);
        Assert.assertEquals(2, tuple.size());
        DBSPType second = tuple.getFieldType(1);
        Assert.assertEquals(second, DBSPTypeString.varchar(true));
    }

    @Test
    public void materializedTest2() {
        String sql = """
                create table T(
                   I int not null
                ) with (
                   'materialized' = 'true'
                );
                create materialized view V as SELECT * FROM T;""";
        this.getCCS(sql);
    }
}
