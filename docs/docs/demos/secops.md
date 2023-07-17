# Developer Security Operations

This demo walks through a developer security operations (DevSecOps)
scenario.  This scenario uses DBSP to analyze streams of data about
software as it is transformed from source code to binary artifacts
through CI build pipelines and deployed using Kubernetes.

The data streams also report vulnerabilities discovered in software
source code.  Our goal is to link source code vulnerabilities to the
binaries built from them directly or indirectly, to Kubernetes images
built using those vulnerable binaries, and to Kubernetes clusters
running those vulnerable images.

## Schema

Suppose each record in `pipeline` represents a run of the CI build
pipeline, using sources associated with the pipeline via
`pipeline_sources`, each record in which is associated with a Git
commit in `git_commit`, which in turn is drawn from a Git repository
represented as `repository`.  When a vulnerability is discovered, a
`vulnerability` record associates it with `pipeline_sources`.

A pipeline produces `artifact`s, which may in turn have further
`artifact`s derived from them.  A Kubernete image, represented by
`k8sobject` draws on a particular artifact, and can be deployed as
any number of Kubernetes clusters, represented by `k8scluster`.

The following diagram illustrates these relationships:

```mermaid
erDiagram
    repository {
        bigint repository_id PK
        varchar type
        varchar url
        varchar name
    }
    git_commit {
        bigint git_commit_id PK
        bigint repository_id FK
        timestamp commit_date
        varchar commit_owner
    }
    pipeline {
        bigint pipeline_id PK
        timestamp create_date
        timestamp update_date
    }
    pipeline_sources {
        bigint pipeline_id FK
        bigint git_commit_id FK
    }
    artifact {
       string artifact_id PK
       varchar artifact_uri
       timestamp create_date
       bigint builtby_pipeline_id FK
       bigint parent_artifact_id FK
       varchar checksum
    }
    vulnerability {
        bigint vulnerability_id PK
        timestamp discovery_date
        bigint discovered_in FK
        int severity
        varchar priority
    }
    k8scluster {
        bigint k8scluster_id PK
        varchar k8s_uri
        varchar name
    }
    k8sobject {
        bigint k8sobject_id PK
        bigint artifact_id FK
        timestamp create_date
        timestamp update_date
        varchar checksum
        bigint deployed_id FK
    }
    pipeline ||--|| pipeline_sources : "has sources"
    pipeline_sources ||--o{ git_commit : "from Git commit"
    git_commit ||--|| repository : "within Git repository"
    k8sobject ||--|| artifact : "built from"
    k8sobject ||--o{ k8scluster : "deployed as"
    vulnerability ||--|| pipeline_sources : "discovered in"
    artifact ||--o| artifact : "parent artifact"
    artifact ||--|| pipeline : "built by pipeline"
```
