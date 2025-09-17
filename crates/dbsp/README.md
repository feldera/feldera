# DBSP

Database Stream Processor (DBSP) is a computational engine for
continuous analysis of changing data. With DBSP, a programmer writes
code in terms of computations on a complete data set, but DBSP
implements it incrementally, meaning that changes to the data set run
in time proportional to the size of the change rather than the size of
the data set. This is a major advantage for applications that work
with large data sets that change frequently in small ways.

The DBSP computational engine is a component of the [Feldera
Continuous Analytics Platform](https://www.feldera.com).

# Resources

## Learning Materials

- A few simple examples:

  * [Degrees in a graph].

  * [Orgchart].

- A [tutorial] with explanations for a series of simple examples.

- The [`circuit` module documentation] provides a catalog of
  computational elements.

- More sophisticated uses can be found as [benchmarks].

[Degrees in a graph]: https://github.com/feldera/feldera/blob/main/crates/dbsp/examples/degrees.rs
[Orgchart]: https://github.com/feldera/feldera/blob/main/crates/dbsp/examples/orgchart.rs
[tutorial]: https://docs.rs/dbsp/latest/dbsp/tutorial
[`circuit` module documentation]: https://docs.rs/dbsp/latest/dbsp/circuit
[benchmarks]: https://github.com/feldera/feldera/tree/main/crates/dbsp/benches

## Documentation

- [Rustdocs].

- [Feldera Continuous Analytics Platform documentation][1].

- The formal theory behind DBSP, as a [paper] or as [video] of a talk.

[Rustdocs]: https://docs.rs/dbsp

[1]: https://docs.feldera.com/

[paper]: https://docs.feldera.com/vldb23.pdf

[video]: https://www.youtube.com/watch?v=iT4k5DCnvPU

## Feedback

Some ways that you can give us your feedback:

- Join our [community Slack].

- [File an issue].

- [Submit a pull request]. DBSP uses the [Developer Certificate of
  Origin] (DCO) (see the [contribution guidelines]).

[community Slack]: https://www.feldera.com/slack/

[file an issue]: https://github.com/feldera/feldera/issues

[submit a pull request]: https://github.com/feldera/feldera/pulls

[Developer Certificate of Origin]: https://developercertificate.org/

[contribution guidelines]: https://github.com/feldera/feldera/blob/main/CONTRIBUTING.md
