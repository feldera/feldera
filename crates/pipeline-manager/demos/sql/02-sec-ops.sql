-- Use Case: Incremental Analytics for Security Operations (sec-ops)
--
-- Detect security vulnerabilities in Kubernetes deployments in real-time.
--
-- This example illustrates the use of incremental analytics to detect security
-- vulnerabilities affecting production Kubernetes deployments in real-time.
-- It tracks security vulnerabilities as they propagate through the software supply
-- chain, instantly updating the list of affected Kubernetes objects in response
-- to input changes.
--
-- ## How to run
--
-- * Paste this SQL code in the Feldera WebConsole and hit Play ▶️.
-- * In the Change Stream tab, select the `k8scluster_vulnerability_stats` view.
-- * You should see vulnerability counts change in real-time as the
--   data generator pushes input changes to the pipeline.
--
-- ## Detailed Description
--
-- We analyze how security vulnerabilities propagate through the software supply
-- chain starting from source code into production Kubernetes cluster:
--
-- 1. A vulnerability is introduced into source code in a particular Git commit.
--    We assume that this vulnerability is detected and reported by a security scanner.
-- 2. The affected commit is picked up by a CI/CD pipeline, which uses it to
--    generate one or more binary artifacts.
-- 3. These binary artifacts are deployed in a Kubernetes cluster.
--
-- Traditional batch analytics tools implement this analysis by periodically
-- re-running all queries from scratch, which can take hours, ultimately
-- delaying the discovery of vulnerabilities.
--
-- In contrast, Feldera evaluates the queries incrementally, instantly updating
-- SQL views whenever new or modified data is added to the input tables.
--
-- We configure Feldera's builtin data generator to produce a continuous stream
-- of updates to input tables, which model Kubernetes clusters, CI/CD pipelines, Git
-- commits, etc.
--
-- ## Takeaways
--
-- This example illustrates how Feldera can run complex analytical workloads in real-time:
--
-- * It enables users to convert slow batch jobs into real-time pipelines,
--   reducing time to results from hours to seconds.
--
-- * Feldera allows constructing complex analytical pipelines in a modular way
--   by breaking them up into multiple views, with more complex views being
--   defined on top of simpler views.


-- CI/CD pipelines.
create table pipeline (
    pipeline_id bigint not null primary key,
    create_date timestamp not null,
    createdby_user_id bigint not null,
    update_date timestamp,
    updatedby_user_id bigint
) WITH ('connectors' = '[{
    "name": "pipeline",
    "transport": {
        "name": "datagen",
        "config": {
            "plan": [{
                "rate": 100,
                "fields": {
                    "pipeline_id": { "strategy": "uniform", "range": [1, 1001] },
                    "create_date": { "strategy": "uniform" },
                    "createdby_user_id": { "strategy": "uniform", "range": [1, 101] },
                    "update_date": { "strategy": "uniform" },
                    "updatedby_user_id": { "strategy": "uniform", "range": [1,101] }
                }
            }]
        }
    }
}]');

-- Git commits used by each pipeline.
create table pipeline_sources (
    pipeline_source_id bigint not null primary key,
    git_commit_id bigint not null,
    pipeline_id bigint not null foreign key references pipeline(pipeline_id)
) WITH (
    'connectors' = '[{
        "name": "pipelie_sources",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "rate": 100,
                    "fields": {
                        "pipeline_source_id": { "strategy": "uniform", "range": [1, 10001] },
                        "git_commit_id": { "strategy": "uniform", "range": [1, 5001] },
                        "pipeline_id": { "strategy": "uniform", "range": [1, 1001] }
                    }
                }]
            }

        }
    }]'
);

-- Binary artifacts created by CI pipelines.
create table artifact (
    artifact_id bigint not null primary key,
    -- artifact_uri varchar not null,
    checksum varchar not null,
    artifact_size_in_bytes bigint not null,
    artifact_type int not null,
    builtby_pipeline_id bigint not null foreign key references pipeline(pipeline_id),
    parent_artifact_id bigint foreign key references artifact(artifact_id)
) WITH (
    'connectors' = '[{
        "name": "artifact",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "rate": 100,
                    "fields": {
                        "artifact_id": { "strategy": "uniform", "range": [1,10001] },
                        "checksum": { "strategy": "uniform", "range": [64,65] },
                        "artifact_size_in_bytes": { "strategy": "uniform", "range": [0, 1000000001] },
                        "artifact_type": { "strategy": "uniform", "range": [1,11] },
                        "builtby_pipeline_id": { "strategy": "uniform", "range": [1,1001] },
                        "parent_artifact_id": { "strategy": "uniform", "range": [1, 10001] }
                    }
                }]
            }
        }
    }]'
);

-- Vulnerabilities discovered in source code.
create table vulnerability (
    vulnerability_id bigint not null primary key,
    discovered_in bigint not null,
    discovery_date timestamp not null,
    checksum varchar not null,
    vulnerability_reference_id varchar,
    severity int,
    priority varchar
)
WITH (
    -- Instruct Feldera to store the snapshot of the table, allowing the
    -- user to browse it via the UI or API.
    'materialized' = 'true',
    'connectors' = '[{
        "name": "vulnerability",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "rate": 100,
                    "fields": {
                        "vulnerability_id": { "strategy": "uniform", "range": [1, 1001] },
                        "discovered_in": { "strategy": "uniform", "range": [1, 5001] },
                        "discovery_date": { "strategy": "uniform" },
                        "checksum": { "strategy": "uniform", "range": [64,65] },
                        "vulnerability_reference_id": { "strategy": "word" },
                        "severity": { "strategy": "uniform", "range": [1,11] },
                        "priority": { "values": ["LOW", "MEDIUM", "HIGH"], "strategy": "uniform" }
                    }
                }]
            }
        }
    }]'
);

-- Kubernetes clusters.
create table k8scluster (
    k8scluster_id bigint not null primary key,
    name varchar not null)
WITH  (
    'connectors' = '[{
        "name": "k8scluster",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 10,
                    "fields": {
                        "name": { "values": ["cluster1", "cluster2", "cluster3", "cluster4", "cluster5", "cluster6", "cluster7", "cluster8", "cluster9", "cluster10"] }
                    }
                }]
            }
        }
    }]'
);


-- Deployed Kubernetes objects.
create table k8sobject (
    k8sobject_id bigint not null primary key,
    artifact_id bigint not null foreign key references artifact(artifact_id),
    checksum varchar not null,
    deployed_id bigint not null foreign key references k8scluster(k8scluster_id),
    k8snamespace varchar not null
)WITH  (
    'connectors' = '[{
        "name": "k8sobject",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "rate": 100,
                    "fields": {
                        "k8sobject_id": { "strategy": "uniform", "range": [1, 10001] },
                        "artifact_id": { "strategy": "uniform", "range": [1, 10001] },
                        "checksum": { "strategy": "uniform", "range": [64,65] },
                        "deployed_id": { "strategy": "uniform", "range": [0,10] },
                        "k8snamespace": { "strategy": "word" }
                    }
                }]
            }
        }
    }]'
);

-- Vulnerabilities that affect each CI/CD pipeline.
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
--
-- Note that this view uses the `pipeline_vulnerability` view defined above as
-- input. This pattern continues below, where we continue building more complex
-- views on top. This illustrates how Feldera enables the construction of complex
-- analytical pipelines in a modular way.
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
create materialized view k8scluster_vulnerability_stats (
    k8scluster_id,
    k8scluster_name,
    total_vulnerabilities,
    most_severe_vulnerability
) AS
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
