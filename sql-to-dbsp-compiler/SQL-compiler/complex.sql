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
                
                create view pipeline_vulnerability (
                    pipeline_id,
                    vulnerability_id
                ) as
                    SELECT pipeline_sources.pipeline_id as pipeline_id, vulnerability.vulnerability_id as vulnerability_id FROM
                    pipeline_sources
                    INNER JOIN
                    vulnerability
                    ON pipeline_sources.git_commit_id = vulnerability.discovered_in;
                
                create view artifact_vulnerability (
                    artifact_id,
                    vulnerability_id
                ) as
                    SELECT artifact.artifact_id as artifact_id, pipeline_vulnerability.vulnerability_id as vulnerability_id FROM
                    artifact
                    INNER JOIN
                    pipeline_vulnerability
                    ON artifact.builtby_pipeline_id = pipeline_vulnerability.pipeline_id;
                
                -- Vulnerabilities that could propagate to each artifact.
                -- create view artifact_vulnerability ();
                
                -- Vulnerabilities in the artifact or any of its children.
                -- create view transitive_vulnerability ();
                
                -- create view k8sobject_vulnerability ();
                
                -- create view k8scluster_vulnerability ();
                
                -- Number of vulnerabilities.
                -- Most severe vulnerability.
                -- create view k8scluster_vulnerability_stats ();
