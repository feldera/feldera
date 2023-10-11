# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.7] - 2023-10-10

### Added

- Added a `--lenient` SQL compiler flag to allow views with multiple
  columns with the same name.
- Added a demo based on the "Feldera: The Basics" tutorial.  People who don't
  want to manually complete all steps in the tutorial can instead play with the
  pre-built pipeline.
  ([#822](https://github.com/feldera/feldera/pull/822))
- Support input tables with primary keys
  ([#826](https://github.com/feldera/feldera/issues/826))
- Add health check endpoints to pipeline-manager components
  ([#855](https://github.com/feldera/feldera/pull/855))
- Web Console: Add confirmation dialog for delete actions
  ([#766](https://github.com/feldera/feldera/issues/766))
- Added documentation for deploying Feldera Cloud on AWS EKS.
  ([#850](https://github.com/feldera/feldera/pull/850))
- DB migration until now was performed during DB connection setup. Now, users
  running the standalone services must invoke the new migrations binary to
  explicitly perform database upgrades. The pipeline-manager binary retains the
  old behavior for convenience.
  ([#856](https://github.com/feldera/feldera/pull/856))

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

### Fixed

- Fixes a regression in the CSV parser where it rejected the last row as an invalid row if `\r\n` was used for line-endings
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
