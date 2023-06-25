# What is DBSP?

DBSP is an acronym stating for "DataBase Stream Processor."
DBSP is two things:

* a new theory that unifies databases, stream computations, and
  incremental view maintenance.
* an open-source implementation of this theory in a practical
  streaming query engine

This site contains materials related to both aspects of DBSP.

A short version of the theoretical foundations is described in the following paper:
[DBSP: Automatic Incremental View Maintenance for Rich Query Languages](../static/vldb23.pdf)
*Mihai Budiu, Tej Chajed, Frank McSherry, Leonid Ryzhyk, and Val Tannen*,
Proceedings of the VLDB Endowment (VLDB), Vancouver, Canada, August, 2023, pages 1601-1614

The DBSP code is available on
[github](https://github.com/feldera/dbsp) using an MIT open-source
license.  It consists of a Rust runtime and a SQL compiler.
