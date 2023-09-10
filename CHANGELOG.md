# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  names in tablesq ([#636](https://github.com/feldera/feldera/issues/636)).

## Added

- REST API: A Pipeline can now receive deletes for rows in the paused state
  ([#612](https://github.com/feldera/feldera/issues/612)).


## Removed

- WebConsole: Removed the Auto Offset Reset option from the Kafka output
  connector configuration dialog (it only applies to Kafka inputs)
  ([#602](https://github.com/feldera/feldera/issues/602)).


## [0.1.1] - 2023-08-25

- Docker: Reduce logging when running demo via docker-compose
  ([#575](https://github.com/feldera/feldera/issues/575)).
- Docker: compose script now exposes the pipeline-manager on port 8080.


## [0.1.0] - 2023-08-25

- Initial release of Feldera CAP.
