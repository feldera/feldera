## Installation

Install nightly Roc build locally:
`source cli-roc/install_nightly_roc.sh`

Build cli:
`cd cli-roc`
`make`

## Feldera CLI
Feldera CLI enables user to work with multiple Feldera deployments within a single directory.
Each deployment has a corresponding directory whose name matches the deployment URI
`feldera.json` file marks a working directory of Feldera CLI, where known deployment's directories are created.

### List of commands:
feldera --help
feldera health
feldera version
feldera http://localhost:8080 [{remote_name}]
feldera {remote_name}
feldera local
feldera local stop
feldera (ps|pipelines)
feldera (cs|connectors) [kafka|debezium|http|snowflake]
feldera (pm|programs)
feldera pull (all | {pipeline_name})
feldera push (all | {pipeline_name})
feldera start {pipeline_name} # Push local pipeline and start it
feldera stop [{pipeline_name}]
feldera bench [{pipeline_name}]
feldera ui

### feldera.json
```
{
  default: 'https://...',
  known: [
    ['deploymentA', { url: 'https://aaa...'}],
    ['deploymentB', { url: 'https://bbb...'}],
    ['deploymentC', { url: 'https://aaa...'}]
  ]
}
```