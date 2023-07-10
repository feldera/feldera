# What is DBSP?

DBSP, which stands for "DataBase Stream Processor," is an open source
streaming query engine based on a new theory that unifies databases,
streaming computation, and incremental view maintenance.  This site
explains how to use DBSP.

For a short introduction to theoretical foundations of DBSP, refer to
the following paper: [DBSP: Automatic Incremental View Maintenance for
Rich Query Languages](../static/vldb23.pdf) *Mihai Budiu, Tej Chajed,
Frank McSherry, Leonid Ryzhyk, and Val Tannen*, Proceedings of the
VLDB Endowment (VLDB), Vancouver, Canada, August, 2023, pages
1601-1614

The DBSP code is available on
[github](https://github.com/feldera/dbsp) using an MIT open-source
license.  It consists of a Rust runtime and a SQL compiler.
