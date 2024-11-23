package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.junit.Assert;
import org.junit.Test;

/** Tests that emit Rust code using the catalog. */
public class CatalogTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions result = super.testOptions(incremental, optimize);
        result.ioOptions.emitHandles = false;
        result.languageOptions.unrestrictedIOTypes = false;
        return result;
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
        var ccs = this.getCCS(sql);
        assert ccs.compiler.messages.messages.isEmpty();
    }

    @Test
    public void duplicatedViewColumnName() {
        String sql = """
                CREATE TABLE T(id int);
                CREATE VIEW V(x, x) AS SELECT id, id+1 FROM T;""";
        this.statementsFailingInCompilation(sql, "Column with name 'x' already defined");
    }

    @Test
    public void issue2028() {
        String sql = """
                CREATE TABLE varchar_pk (
                    pk VARCHAR NOT NULL PRIMARY KEY
                );

                CREATE VIEW V AS SELECT * FROM varchar_pk;""";
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
                   i n NOT NULL
                );

                CREATE VIEW V AS SELECT * FROM T;
                """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        DBSPType type = circuit.getSingleOutputType().to(DBSPTypeZSet.class).elementType;
        Assert.assertTrue(type.is(DBSPTypeTuple.class));
        DBSPTypeTuple t = type.to(DBSPTypeTuple.class);
        Assert.assertEquals(4, t.size());
        DBSPType t0 = t.getFieldType(0);
        Assert.assertTrue(t0.is(DBSPTypeTuple.class));

        DBSPTypeTuple sN = t0.to(DBSPTypeTuple.class);
        Assert.assertEquals(4, sN.size());
        DBSPType i32 = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false);
        DBSPType i32N = i32.withMayBeNull(true);
        Assert.assertTrue(sN.mayBeNull);
        Assert.assertTrue(sN.getFieldType(0).sameType(i32N));
        Assert.assertTrue(sN.getFieldType(1).sameType(i32));
        Assert.assertTrue(sN.getFieldType(2).sameType(new DBSPTypeVec(i32N, true)));
        Assert.assertTrue(sN.getFieldType(3).sameType(new DBSPTypeVec(i32N, false)));

        DBSPType t1 = t.getFieldType(1);
        Assert.assertTrue(t1.is(DBSPTypeTuple.class));
        DBSPTypeTuple nN = t1.to(DBSPTypeTuple.class);
        Assert.assertEquals(3, nN.size());
        DBSPType vecS = new DBSPTypeVec(sN.withMayBeNull(true), true);
        Assert.assertTrue(nN.mayBeNull);
        Assert.assertTrue(nN.getFieldType(0).sameType(sN));
        Assert.assertTrue(nN.getFieldType(1).sameType(sN.withMayBeNull(false)));
        Assert.assertTrue(nN.getFieldType(2).sameType(vecS));

        DBSPType t2 = t.getFieldType(2);
        Assert.assertTrue(t2.sameType(sN.withMayBeNull(false)));

        DBSPType t3 = t.getFieldType(3);
        Assert.assertTrue(t3.sameType(nN.withMayBeNull(false)));
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
        CompilerCircuitStream ccs = this.getCCS(sql);
        this.addRustTestCase(ccs);
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
        CompilerCircuitStream ccs = this.getCCS(statements);
        this.addRustTestCase(ccs);
    }

    // Test for https://github.com/feldera/feldera/issues/1151
    @Test
    public void primaryKeyTest() {
        // This is identical to ComplexQueriesTest.primaryKeyTest, but here
        // we generate code in a different way.
        String sql = "CREATE TABLE event_t ( id BIGINT NOT NULL PRIMARY KEY, local_event_dt DATE )";
        CompilerCircuitStream ccs = this.getCCS(sql);
        this.addRustTestCase(ccs);
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
        CompilerCircuitStream ccs = this.getCCS(sql);
        this.addRustTestCase(ccs);
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
        CompilerCircuitStream ccs = this.getCCS(sql);
        this.addRustTestCase(ccs);
    }
}
