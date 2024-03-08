# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- SQL: support for SORT_ARRAY function
  ([#1490](https://github.com/feldera/feldera/pull/1490))
- SQL: support for ARRAY_REVERSE function
  ([#1491](https://github.com/feldera/feldera/pull/1491))
- SQL: support for ARRAY_MAX, ARRAY_MIN functions
  ([#1492](https://github.com/feldera/feldera/pull/1492))
- SQL: support for ARRAY_COMPACT function
  ([#1493](https://github.com/feldera/feldera/pull/1493))
- SQL: support for ARRAY_PREPEND function
  ([#1496](https://github.com/feldera/feldera/pull/1496))
- adapters: add an input connector for Amazon S3 (#1485)
- SQL: support for ARRAY_CONTAINS function
  ([#1499](https://github.com/feldera/feldera/pull/1499))
- SQL: support for ARRAY_REPEAT function
  ([#1497](https://github.com/feldera/feldera/pull/1497))
- SQL: support for ROUND(DOUBLE, digit), TRUNCATE(DOUBLE, digit)
  ([#1512](https://github.com/feldera/feldera/pull/1512))
  
## [0.11.0] - 2024-03-05

### Added

- SQL: support for ARRAY_SIZE, ARRAY_LENGTH functions
  ([#1489](https://github.com/feldera/feldera/pull/1489))
- SQL: support for ARRAY_POSITION function
  ([#1480](https://github.com/feldera/feldera/pull/1480))

## [0.10.0] - 2024-02-22

### Fixed

- WebConsole: Fix metrics values depend on polling period
- WebConsole: Improve WebConsole throughput and memory graphs
  ([#491](https://github.com/feldera/feldera/pull/491))

### Added

- JDBC sink support via Debezium JDBC sink connector
  ([#1384](https://github.com/feldera/feldera/pull/1384))
- SQL: Allow tables definitions to be retrieved from JDBC data sources
  ([#1416](https://github.com/feldera/feldera/pull/1416))
- SQL: Support for EXP function
  ([#1406](https://github.com/feldera/feldera/pull/1406))
- SQL: Support for ARRAY_APPEND function
  ([#1419](https://github.com/feldera/feldera/pull/1419))
- SQL: support for TUMBLE streaming windows
  ([#1404](https://github.com/feldera/feldera/pull/1404))
- API: endpoint to get a list of demo URLs
  ([#1385](https://github.com/feldera/feldera/pull/1385))
- WebConsole: Add functionality that would enable setting up demos in the sandbox
  ([#1321](https://github.com/feldera/feldera/pull/1321))
- WebConsole: Add Swagger link to navbar, make navbar more compact

### Changed

- SQL: Illegal date literals and parsing illegal dates cause runtime
  errors instead of producing `NULL` ([#1398](https://github.com/feldera/feldera/pull/1398))
- WebConsole: Various design adjustments
- WebConsole: Show the entire generated API key
- WebConsole: Improve login UX with AWS Cognito (redirect to desired page after successful login)
  ([#1364](https://github.com/feldera/feldera/pull/1364))

## [0.9.0] - 2024-02-06

### Added

- Compiler option for handling unquoted identifiers
  ([#1360](https://github.com/feldera/feldera/pull/1360))
- SQL: Support for `is_inf`, `is_nan` and `log` methods for
  double ([#1344](https://github.com/feldera/feldera/pull/1344))

### Changed

- API now handles SQL table and column names in a case-insensitive manner, consistent
  with the semantics of SQL (with the exception of case-sensitive relations
  declared using quotes).
  ([#1369](https://github.com/feldera/feldera/pull/1369))
- API: name and description are optional for connector PATCH endpoint.
  OpenAPI documentation for program, connector and service is more consistent.
  ([#1315](https://github.com/feldera/feldera/pull/1315))
- SQL: check decimal precision while casting between decimal
  types ([#1300](https://github.com/feldera/feldera/pull/1300))
- pipeline-manager: automatically queue programs for compilation (#1325)

### Removed

- Remove the Python dbsp and feldera-api-client modules.
  In lieu of Python modules, direct requests to the REST API are now used.
  ([#1338](https://github.com/feldera/feldera/pull/1338))

## [0.8.0] - 2024-01-23

### Added

- API: Generalized upsert operation that allows partial modifications to table
  records ([#1296](https://github.com/feldera/feldera/pull/1296))
- SQL: Functions on binary string (octet_length, position, overlay,
  substring) ([#1264](https://github.com/feldera/feldera/pull/1264))
- pipeline-manager: add PUT endpoints for Programs, Pipelines, and Connectors (#1248)
- Documentation: Adding a markdown page for videos (#1126)
- SQL: Add hyperbolic functions ([#1280](https://github.com/feldera/feldera/pull/1280))

### Fixed

- pipeline-manager: fix a resource usage problem with http streaming under high load
  ([#1257](https://github.com/feldera/feldera/pull/1257))
- SQL: Changed the semantics of integer arithmetic to match SQL
  standard ([#1247](https://github.com/feldera/feldera/pull/1247))
- WebConsole: Connector name change doesn't reflect in the pipeline builder (#1238)
- WebConsole: UI sends HTTP request in an infinite loop (#1085)
- pipeline-manager: allow docs website to CORS allowed origins (#1285)

### Changed

- Python demo and test scripts are standalone as they no longer depend on dbsp python library
  ([#1230](https://github.com/feldera/feldera/pull/1230))
- pipeline-manager: update service endpoints to use names instead of IDs in URLs,
  and add PUT endpoint ([#1263](https://github.com/feldera/feldera/pull/1263))
- SQL: Changed the semantics of integer arithmetic to match SQL
  standard ([#1247](https://github.com/feldera/feldera/pull/1247))
- pipeline-manager: use names instead of IDs in API endpoints (#1214)
- WebConsole: use names instead of IDs as an API entity identifier (#1214)
- WebConsole: Bearer token expiration now triggers a background token refresh or redirects to a login page (#1100)
- Minimal rust version required to build feldera increased to 1.75 (was 1.73).

## [0.7.0] - 2024-01-09

### Added

- WebConsole: Add ability to edit connector configuration as JSON
- SQL: Preliminary support for computations with bounded memory on unbounded
  streams ([#1197](https://github.com/feldera/feldera/pull/1197))

### Fixed

- SQL: Changed semantics of division to match SQL standard
  ([#1201](https://github.com/feldera/feldera/pull/1201))
- WebConsole: display AWS Cognito username in user profile dropdown (#1077)

## [0.6.0] - 2023-12-19

### Fixed

- WebConsole: Vendor logos now change color when in dark mode

### Changed

- WebConsole: group.id Kafka connector configuration field is now optional
- pipeline-manager: reference programs and connectors by name when creating pipelines (#1143)
- pipeline-manager: update-program should allow any field to be updated (#1191)

### Added

- SQL: support for trigonometric functions `sin` and `cos` ([#1118](https://github.com/feldera/feldera/pull/1118))
- SQL: support for mathematical constant `PI` ([#1123](https://github.com/feldera/feldera/pull/1123))
- WebConsole: 'Inspect connector' button in the connector list in Pipeline Builder that opens a non-editable popup
- SQL: Support for user-defined functions, declared in SQL and implemented in
  Rust ([#1129](https://github.com/feldera/feldera/pull/1129))
- SQL: support for other trigonometric functions supported by
  Calcite ([#1127](https://github.com/feldera/feldera/pull/1127))
- WebConsole: Add Settings page, add a view to manage API keys (#1136)

## [0.5.0] - 2023-12-05

### Fixed

- WebConsole: Add "Queued" status for pipelines whose programs are enqueued to be compiled (#1032)
- WebConsole: Random input generator doesn't work for a decimal column (#1006)
- pipeline-manager: do not allow pipelines to be started mid-compilation (#1081)

### Added

- WebConsole: Support big numeric SQL types in Data Browser, Data Import and Data Generator (#851)
- WebConsole: Display SQL types in Data inspect and insert tables
- SQL: parser support for 'DEFAULT' column values in DDL (#1061)
- pipeline-manager: add Service to database and API
  ([#1074](https://github.com/feldera/feldera/pull/1074))
- SQL: support for trigonometric functions `sin` and `cos` ([#1118](https://github.com/feldera/feldera/pull/1118))
- pipeline-manager: create and manage API keys via the REST API ([#1126](https://github.com/feldera/feldera/pull/1126))
- pipeline-manager: expose authorization and security scheme through
  OpenAPI ([#1126](https://github.com/feldera/feldera/pull/1126))

## [0.4.0] - 2023-11-21

### Fixed

- [SQL] Fix bugs in parsing of KEY and FOREIGN KEY constraints
- Use better defaults for running the pipeline-manager (#994)
  ([#1011](https://github.com/feldera/feldera/pull/1011))
- WebConsole: Fix unable to delete orphaned output connectors,
  SQL views in Pipeline Builder do not get removed when removed from the program
  ([#854](https://github.com/feldera/feldera/issues/854))
- WebConsole: Replace youtube link on Webconsole UI home page
  ([#935](https://github.com/feldera/feldera/issues/935))
- WebConsole: Kafka authentication protocol security.protocol field
  ([#963](https://github.com/feldera/feldera/issues/963))
- WebConsole: Differentiate compiling and pending in web-console editor
  ([#695](https://github.com/feldera/feldera/issues/695))
- WebConsole: Report number of parsing errors per connector
  ([#776](https://github.com/feldera/feldera/issues/776))
- WebConsole: Table browser doesn't always correctly apply updates - no longer reproduces
  ([#635](https://github.com/feldera/feldera/issues/635))
- WebConsole: MUI DataGridPro fails to load data on Next.js > 13.4.8-canary.9 - no longer reproduces
  ([#494](https://github.com/feldera/feldera/issues/494))
- WebConsole: Spacing for SQL icon is off in pipeline view
  ([#932](https://github.com/feldera/feldera/issues/932))
- WebConsole: Pipeline failure state in Pipeline Management isn't cleared immediately - no longer reproduces
  ([#1012](https://github.com/feldera/feldera/issues/1012))
- Refactor list of tables and views in breadcrumbs to an autocomplete combobox
  ([997](https://github.com/feldera/feldera/issues/997))
- REST API: Fold ResourceConfig into RuntimeConfig to allow users to configure resources
  ([#1035](https://github.com/feldera/feldera/pull/1035))

### Added

- [SQL] New aggregation functions: `BIT_AND`, `BIT_OR`, `BIT_XOR`.
  Concatenation for `BINARY values`.  `TO_HEX` function.
  ([#996](https://github.com/feldera/feldera/pull/996))
- pipeline-manager now exposes a scrape endpoint for metrics, starting with the compiler service
  ([#1031](https://github.com/feldera/feldera/pull/1031))

## [0.3.2] - 2023-11-10

### Fixed

- Fix a bug in Pipeline Management table where sometimes pipeline action buttons do not send
  requests ([#1008](https://github.com/feldera/feldera/pull/1008))
- Fix pipeline with no program displays COMPILING status in Pipeline Management table

## [0.3.1] - 2023-11-09

### Fixed

- Display package version below pipeline manager banner ([#988](https://github.com/feldera/feldera/pull/988))
- Fix Data Browser regression where data rows become invalid after switching between relations
  ([#993](https://github.com/feldera/feldera/issues/993))
  ([#999](https://github.com/feldera/feldera/issues/999))

## [0.3.0] - 2023-11-07

### Removed

- Removed support for `FLOAT` SQL data type, since it is ambiguous.
  `REAL` is recommended instead.
  ([#980](https://github.com/feldera/feldera/pull/980))

### Added

- Support various forms of TopK computations in SQL
  ([#968](https://github.com/feldera/feldera/pull/968))
- Support for ORDER BY with LIMIT
  ([#954](https://github.com/feldera/feldera/pull/954))
- Allow configuring resource requirements per pipeline
  ([940](https://github.com/feldera/feldera/pull/940))
- Restructure and expansion of cloud documentation
  ([#957](https://github.com/feldera/feldera/pull/957))
- Secrets can be referenced using a string pattern
  in the Kafka connector input and output configuration
  ([#949](https://github.com/feldera/feldera/pull/949),
  [#970](https://github.com/feldera/feldera/pull/970))
- Number of records in the pipeline table in the web
  console is no longer rounded
  ([#967](https://github.com/feldera/feldera/pull/967))
- Ability to pause/resume row updates in Data Inspection tables
  ([#603](https://github.com/feldera/feldera/issues/603))

### Fixed

- Reduce Docker logging noise from Kafka connect and Redpanda.
- Regression in pipeline shutdown logic
  ([#961](https://github.com/feldera/feldera/pull/961))

## [0.2.0] - 2023-10-24

### Fixed

- Avoid shutting down pipelines when they encounter
  errors during lifecycle state changes.
  ([#869](https://github.com/feldera/feldera/pull/869))
- The compiler flag `-js` generates primary key information
  ([#772](https://github.com/feldera/feldera/issues/772))
- Fixes a regression caused by using the --auth-provider
  argument for the pipeline-manager in docker-compose, which
  was backward incompatible
  ([#900](https://github.com/feldera/feldera/pull/900))

### Added

- Preliminary support for BINARY and VARBINARY SQL
  data types
  ([#917](https://github.com/feldera/feldera/pull/917))
- Experimental Snowflake sink
  ([#774](https://github.com/feldera/feldera/issues/774))
- WebConsole: Snowflake output connector dialog and node
  ([#859](https://github.com/feldera/feldera/issues/859))
- Source and sink connector documentation
  ([#882](https://github.com/feldera/feldera/pull/882))
- Enforce distinct outputs. This is equivalent to applying
  `SELECT DISTINCT` to each output view.
  ([#871](https://github.com/feldera/feldera/issues/871))
- Ignore outermost `ORDER BY` clauses, which don't make
  sense for incremental queries.
  ([#883](https://github.com/feldera/feldera/pull/8830))

## [0.1.7] - 2023-10-10

### Added

- Add the ability to authorize access to Pipeline Manager via Web Console
  through AWS Cognito and Google Identity Platform as authentication providers
  ([#787](https://github.com/feldera/feldera/issues/787))
- Added a `--lenient` SQL compiler flag to allow views with multiple
  columns with the same name.
- Added a demo based on the "Feldera: The Basics" tutorial. People who don't
  want to manually complete all steps in the tutorial can instead play with the
  pre-built pipeline.
  ([#822](https://github.com/feldera/feldera/pull/822))
- Support input tables with primary keys
  ([#826](https://github.com/feldera/feldera/issues/826))
- Add health check endpoints to pipeline-manager components
  ([#855](https://github.com/feldera/feldera/pull/855))
- Added documentation for deploying Feldera Cloud on AWS EKS.
  ([#850](https://github.com/feldera/feldera/pull/850))
- DB migration until now was performed during DB connection setup. Now, users
  running the standalone services must invoke the new migrations binary to
  explicitly perform database upgrades. The pipeline-manager binary retains the
  old behavior for convenience.
  ([#856](https://github.com/feldera/feldera/pull/856))
- WebConsole: data tables' column configuration is preserved between page refreshes
  ([#696](https://github.com/feldera/feldera/issues/696))

### Fixed

- Busy-wait loop in Kafka producer.
  ([#842](https://github.com/feldera/feldera/issues/842))

## [0.1.6] - 2023-09-28

### Fixed

- Fixes URL endpoints to access program editor ([#809](https://github.com/feldera/feldera/pull/809))

## [0.1.5] - 2023-09-27

### Added

- Add Debezium MySQL input connector
  ([#813](https://github.com/feldera/feldera/issues/813))
- WebConsole: Add confirmation dialog for delete actions
  ([#766](https://github.com/feldera/feldera/issues/766))

### Added

- WebConsole: Add AWS Cognito authentication to authorize requests to Pipeline Manager
  ([#787](https://github.com/feldera/feldera/issues/787))

### Fixed

- Fixes a regression in the CSV parser where it rejected the last row as an invalid row if `\r\n` was used for
  line-endings
  ([#801](https://github.com/feldera/feldera/pull/801))
- Clarify some label names for output connectors
  ([#802](https://github.com/feldera/feldera/issues/802))

### Enhanced

- Made connector type icons look nicer in Pipeline Builder
- Forbid empty group.id in Kafka form
  ([#840](https://github.com/feldera/feldera/issues/840))
- Enable multiline text input for .pem SSL certificates
  ([#841](https://github.com/feldera/feldera/issues/841))
- Prefer last lines of output when reporting an error
  ([#784](https://github.com/feldera/feldera/issues/784))

## [0.1.4] - 2023-09-26

Milestone [v0.1.4](https://github.com/feldera/feldera/milestone/1)

### Added

- WebConsole: Add Kafka Authentication options for connectors
  ([#614](https://github.com/feldera/feldera/issues/614))
- WebConsole: Add breadcrumbs for all pages
  ([#622](https://github.com/feldera/feldera/issues/622))
- WebConsole: Add ability to edit connectors from Pipeline Builder
  ([#664](https://github.com/feldera/feldera/issues/664))

## [0.1.3] - 2023-09-11

### Fixed

- SecOps demo: Fixes a regression in the SecOps demo related to auto-commit behavior
  ([#667](https://github.com/feldera/feldera/pull/667)).

### Added

- WebConsole: The pipeline view now also shows a graph of memory utilization over time
  ([#610](https://github.com/feldera/feldera/pull/610))
- WebConsole: Added a way to delete rows when browsing the tables in a pipeline
  ([#612](https://github.com/feldera/feldera/issues/612)).
- WebConsole: Improved error reporting: added the ability to open new github issues
  in the Health dashboard on the Home screen
  ([#531](https://github.com/feldera/feldera/issues/531)).

## [0.1.2] - 2023-09-07

### Fixed

- Manager: Reduce compile time for pipeline by removing unnecessary dependencies
  ([#593](https://github.com/feldera/feldera/issues/593)).
- WebConsole: Fixes the configuration dialog for kafka inputs where the topic.
  name would not be preserved on errors ([#594](https://github.com/feldera/feldera/issues/594)).
- WebConsole: Fixes the links to documentation
  ([#596](https://github.com/feldera/feldera/issues/596)).
- Docker: Logs now print localhost:8080 instead 0.0.0.0:8080
  ([#597](https://github.com/feldera/feldera/issues/597)).
- Docker: Fixes documentation link that appears in logs feldera.com/docs
  ([#598](https://github.com/feldera/feldera/issues/598)).
- SQL Compiler: Better error reporting for tables with duplicate column names
  ([#624](https://github.com/feldera/feldera/issues/624)).
- SQL Compiler: Fixed a bug where 'c' and 'g' are not allowed column names
  ([#633](https://github.com/feldera/feldera/issues/633)).
- SQL Compiler: Fixed a bug where it was not possible to add lower-case column
  names in tables ([#636](https://github.com/feldera/feldera/issues/636)).

### Added

- REST API: A Pipeline can now receive deletes for rows in the paused state
  ([#612](https://github.com/feldera/feldera/issues/612)).

### Removed

- WebConsole: Removed the Auto Offset Reset option from the Kafka output
  connector configuration dialog (it only applies to Kafka inputs)
  ([#602](https://github.com/feldera/feldera/issues/602)).

## [0.1.1] - 2023-08-25

- Docker: Reduce logging when running demo via docker-compose
  ([#575](https://github.com/feldera/feldera/issues/575)).
- Docker: compose script now exposes the pipeline-manager on port 8080.

## [0.1.0] - 2023-08-25

- Initial release of Feldera CAP.
