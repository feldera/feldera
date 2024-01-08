package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.util.Logger;
import org.junit.Test;

/** Tests that emit Rust code using the catalog. */
public class CatalogTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions result = super.testOptions(incremental, optimize);
        result.ioOptions.emitCatalog = true;
        return result;
    }

    @Test
    public void testSanitizeNames() {
        String statements = "create table t1(\n" +
                "c1 integer,\n" +
                "\"col\" boolean,\n" +
                "\"SPACES INSIDE\" CHAR,\n" +
                "\"CC\" CHAR,\n" +
                "\"quoted \"\" with quote\" CHAR,\n" +
                "U&\"d\\0061t\\0061\" CHAR,\n" + // 'data' spelled in Unicode
                "José CHAR,\n"  +
                "\"Gosé\" CHAR,\n" +
                "\"\uD83D\uDE00❤\" varchar not null,\n" +
                "\"αβγ\" boolean not null,\n" +
                "ΔΘ boolean not null" +
                ");\n" +
                "create view v1 as select * from t1;";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(statements);
        this.addRustTestCase("docTest", compiler, getCircuit(compiler));
    }

    @Test
    public void docTest() {
        // The example given in the documentation
        String statements = """
                -- define Person table
                CREATE TABLE Person
                (
                    name    VARCHAR,
                    age     INT,
                    present BOOLEAN
                );
                CREATE VIEW Adult AS SELECT Person.name FROM Person WHERE Person.age > 18;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(statements);
        this.addRustTestCase("docTest", compiler, getCircuit(compiler));
    }

    @Test
    public void testComplex() {
        Logger.INSTANCE.setLoggingLevel(CircuitCloneVisitor.class, 2);
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
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(statements);
        this.addRustTestCase("docTest", compiler, getCircuit(compiler));
    }

    // Test for https://github.com/feldera/feldera/issues/1151
    @Test
    public void primaryKeyTest() {
        // This is identical to ComplexQueriesTest.primaryKeyTest, but here
        // we generate code in a different way.
        String sql = "CREATE TABLE event_t ( id BIGINT NOT NULL PRIMARY KEY, local_event_dt DATE )";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(sql, compiler, circuit);
    }
}
