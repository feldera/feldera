package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.util.Logger;
import org.junit.Test;

import java.io.IOException;

/** Tests that emit Rust code for input and outputs with handles. */
public class HandlesTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions result = super.testOptions(incremental, optimize);
        result.ioOptions.emitHandles = true;
        return result;
    }

    @Test
    public void testSanitizeNames() throws IOException, InterruptedException {
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
    public void docTest() throws IOException, InterruptedException {
        // The example given in the documentation
        String statements = "-- define Person table\n" +
                "CREATE TABLE Person\n" +
                "(\n" +
                "    name    VARCHAR,\n" +
                "    age     INT,\n" +
                "    present BOOLEAN\n" +
                ");\n" +
                "CREATE VIEW Adult AS SELECT Person.name FROM Person WHERE Person.age > 18;";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(statements);
        this.addRustTestCase("docTest", compiler, getCircuit(compiler));
    }

    @Test
    public void testComplex() throws IOException, InterruptedException {
        Logger.INSTANCE.setLoggingLevel(CircuitCloneVisitor.class, 2);
        String statements = "-- Git repository.\n" +
                "create table repository (\n" +
                "    repository_id bigint not null primary key,\n" +
                "    type varchar not null,\n" +
                "    url varchar not null,\n" +
                "    name varchar not null\n" +
                ");\n" +
                "\n" +
                "-- Commit inside a Git repo.\n" +
                "create table git_commit (\n" +
                "    git_commit_id bigint not null,\n" +
                "    repository_id bigint not null,\n" +
                "    commit_id varchar not null,\n" +
                "    commit_date timestamp not null,\n" +
                "    commit_owner varchar not null\n" +
                ");\n" +
                "\n" +
                "-- CI pipeline.\n" +
                "create table pipeline (\n" +
                "    pipeline_id bigint not null,\n" +
                "    name varchar not null,\n" +
                "    create_date timestamp not null,\n" +
                "    createdby_user_id bigint not null,\n" +
                "    update_date timestamp,\n" +
                "    updatedby_user_id bigint\n" +
                ");\n" +
                "\n" +
                "\n" +
                "-- Git commits used by each pipeline.\n" +
                "create table pipeline_sources (\n" +
                "    git_commit_id bigint not null,\n" +
                "    pipeline_id bigint not null\n" +
                ");\n" +
                "\n" +
                "-- Binary artifact created by a CI pipeline.\n" +
                "create table artifact (\n" +
                "    artifact_id bigint not null,\n" +
                "    artifact_uri varchar not null,\n" +
                "    path varchar not null,\n" +
                "    create_date timestamp not null,\n" +
                "    createdby_user_id bigint not null,\n" +
                "    update_date timestamp,\n" +
                "    updatedby_user_id bigint,\n" +
                "    checksum varchar not null,\n" +
                "    checksum_type varchar not null,\n" +
                "    artifact_size_in_bytes bigint not null,\n" +
                "    artifact_type varchar not null,\n" +
                "    builtby_pipeline_id bigint not null,\n" +
                "    parent_artifact_id bigint\n" +
                ");\n" +
                "\n" +
                "-- Vulnerabilities discovered in source code.\n" +
                "create table vulnerability (\n" +
                "    vulnerability_id bigint not null,\n" +
                "    discovery_date timestamp not null,\n" +
                "    discovered_by varchar not null,\n" +
                "    discovered_in bigint not null /*git_commit_id*/,\n" +
                "    update_date timestamp,\n" +
                "    updatedby_user_id bigint,\n" +
                "    checksum varchar not null,\n" +
                "    checksum_type varchar not null,\n" +
                "    vulnerability_reference_id varchar not null,\n" +
                "    severity varchar,\n" +
                "    priority varchar\n" +
                ");\n" +
                "\n" +
                "-- Deployed k8s objects.\n" +
                "create table k8sobject (\n" +
                "    k8sobject_id bigint not null,\n" +
                "    create_date timestamp not null,\n" +
                "    createdby_user_id bigint not null,\n" +
                "    update_date timestamp,\n" +
                "    updatedby_user_id bigint,\n" +
                "    checksum varchar not null,\n" +
                "    checksum_type varchar not null,\n" +
                "    deployed_id bigint not null /*k8scluster_id*/,\n" +
                "    deployment_type varchar not null,\n" +
                "    k8snamespace varchar not null\n" +
                ");\n" +
                "\n" +
                "-- Binary artifacts used to construct k8s objects.\n" +
                "create table k8sartifact (\n" +
                "    artifact_id bigint not null,\n" +
                "    k8sobject_id bigint not null\n" +
                ");\n" +
                "\n" +
                "-- K8s clusters.\n" +
                "create table k8scluster (\n" +
                "    k8scluster_id bigint not null,\n" +
                "    k8s_uri varchar not null,\n" +
                "    path varchar not null,\n" +
                "    name varchar not null,\n" +
                "    k8s_serivce_provider varchar not null\n" +
                ");\n" +
                "\n" +
                "-- Vulnerabilities that affect each pipeline.\n" +
                "create view pipeline_vulnerability (\n" +
                "    pipeline_id,\n" +
                "    vulnerability_id\n" +
                ") as\n" +
                "    SELECT pipeline_sources.pipeline_id as pipeline_id, " +
                "vulnerability.vulnerability_id as vulnerability_id FROM\n" +
                "    pipeline_sources\n" +
                "    INNER JOIN\n" +
                "    vulnerability\n" +
                "    ON pipeline_sources.git_commit_id = vulnerability.discovered_in;\n" +
                "\n" +
                "-- Vulnerabilities that could propagate to each artifact.\n" +
                "create view artifact_vulnerability (\n" +
                "    artifact_id,\n" +
                "    vulnerability_id\n" +
                ") as\n" +
                "    SELECT artifact.artifact_id as artifact_id, " +
                "pipeline_vulnerability.vulnerability_id as vulnerability_id FROM\n" +
                "    artifact\n" +
                "    INNER JOIN\n" +
                "    pipeline_vulnerability\n" +
                "    ON artifact.builtby_pipeline_id = pipeline_vulnerability.pipeline_id;\n" +
                "\n" +
                "-- Vulnerabilities in the artifact or any of its children.\n" +
                "create view transitive_vulnerability(\n" +
                "    artifact_id,\n" +
                "    via_artifact_id,\n" +
                "    vulnerability_id\n" +
                ") as\n" +
                "    SELECT artifact_id, artifact_id as via_artifact_id, " +
                "vulnerability_id from artifact_vulnerability\n" +
                "    UNION\n" +
                "    (\n" +
                "        SELECT\n" +
                "            artifact.parent_artifact_id as artifact_id,\n" +
                "            artifact.artifact_id as via_artifact_id,\n" +
                "            artifact_vulnerability.vulnerability_id as vulnerability_id FROM\n" +
                "        artifact\n" +
                "        INNER JOIN\n" +
                "        artifact_vulnerability\n" +
                "        ON artifact.artifact_id = artifact_vulnerability.artifact_id\n" +
                "        WHERE artifact.parent_artifact_id IS NOT NULL\n" +
                "    );\n" +
                "\n" +
                "-- create view k8sobject_vulnerability ();\n" +
                "\n" +
                "-- create view k8scluster_vulnerability ();\n" +
                "\n" +
                "-- Number of vulnerabilities.\n" +
                "-- Most severe vulnerability.\n" +
                "-- create view k8scluster_vulnerability_stats ();";
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
