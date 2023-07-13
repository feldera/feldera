-- Git repository.
create table repository (
    repository_id bigint not null,
    type varchar not null,
    url varchar not null,
    name varchar not null
);

-- Commit inside a Git repo.
create table git_commit (
    git_commit_id bigint not null,
    repository_id bigint not null /* foreign key references repository(repository_id)*/,
    commit_id varchar not null,
    commit_date timestamp not null,
    commit_owner varchar not null
);

-- CI/CD pipeline.
create table pipeline (
    pipeline_id bigint not null,
    create_date timestamp not null,
    createdby_user_id bigint not null,
    update_date timestamp,
    updatedby_user_id bigint
);

-- Git commits used by each pipeline.
create table pipeline_sources (
    git_commit_id bigint not null /* foreign key references git_commit(git_commit_id) */,
    pipeline_id bigint not null /* foreign key references pipeline(pipeline_id) */
);

-- Binary artifact created by a CI pipeline.
create table artifact (
    artifact_id bigint not null,
    artifact_uri varchar not null,
    create_date timestamp not null,
    createdby_user_id bigint not null,
    checksum varchar not null,
    checksum_type varchar not null,
    artifact_size_in_bytes bigint not null,
    artifact_type varchar not null,
    builtby_pipeline_id bigint not null /* foreign key references pipeline(pipeline_id) */,
    parent_artifact_id bigint foreign key references artifact(artifact_id)
);

-- Vulnerabilities discovered in source code.
create table vulnerability (
    vulnerability_id bigint not null,
    discovery_date timestamp not null,
    discovered_by_user_id bigint not null,
    discovered_in bigint not null /* foreign key references git_commit(git_commit_id) */,
    update_date timestamp,
    updated_by_user_id bigint,
    checksum varchar not null,
    checksum_type varchar not null,
    vulnerability_reference_id varchar not null,
    severity int,
    priority varchar
);

-- K8s clusters.
create table k8scluster (
    k8scluster_id bigint not null,
    k8s_uri varchar not null,
    name varchar not null,
    k8s_service_provider varchar not null
);


-- Deployed k8s objects.
create table k8sobject (
    k8sobject_id bigint not null,
    artifact_id bigint not null foreign key references artifact(artifact_id),
    create_date timestamp not null,
    createdby_user_id bigint not null,
    update_date timestamp,
    updatedby_user_id bigint,
    checksum varchar not null,
    checksum_type varchar not null,
    deployed_id bigint not null foreign key references k8scluster(k8scluster_id),
    deployment_type varchar not null,
    k8snamespace varchar not null
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

-- Vulnerabilities that affect each artifact.
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
create view transitive_artifact_vulnerability(
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

-- Vulnerabilities that affect each k8s object.
create view k8sobject_vulnerability (
    k8sobject_id,
    vulnerability_id
) as
    SELECT k8sobject.k8sobject_id, transitive_artifact_vulnerability.vulnerability_id FROM
    k8sobject
    INNER JOIN
    transitive_artifact_vulnerability
    ON k8sobject.artifact_id = transitive_artifact_vulnerability.artifact_id;

-- Vulnerabilities that affect each k8s cluster.
create view k8scluster_vulnerability (
    k8scluster_id,
    vulnerability_id
) as
    SELECT
        k8sobject.deployed_id as k8scluster_id,
        k8sobject_vulnerability.vulnerability_id FROM
    k8sobject_vulnerability
    INNER JOIN
    k8sobject
    ON k8sobject_vulnerability.k8sobject_id = k8sobject.k8sobject_id;

-- Per-cluster statistics:
-- * Number of vulnerabilities.
-- * Most severe vulnerability.
create view k8scluster_vulnerability_stats (
    k8scluster_id,
    k8scluster_name,
    total_vulnerabilities,
    most_severe_vulnerability
) as
    SELECT
        cluster_id,
        k8scluster.name as k8scluster_name,
        total_vulnerabilities,
        most_severe_vulnerability
    FROM
    (
        SELECT
            cluster_id,
            COUNT(*) as total_vulnerabilities,
            MAX(severity) as most_severe_vulnerability
        FROM
        (
            SELECT k8scluster_vulnerability.k8scluster_id as cluster_id, vulnerability.vulnerability_id, vulnerability.severity FROM
            k8scluster_vulnerability
            INNER JOIN
            vulnerability
            ON k8scluster_vulnerability.vulnerability_id = vulnerability.vulnerability_id
        )
        GROUP BY cluster_id
    )
    INNER JOIN
    k8scluster
    ON k8scluster.k8scluster_id = cluster_id;
