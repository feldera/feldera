//! The harness of the connector suites: each case that `#[engine_test]` registers becomes one
//! test, which an engine's crate runs with [`run_suites`].
//!
//! A suite is a function of an engine's fixture, so one copy of it checks every engine. The
//! harness takes the command line of the standard test harness (name filters, `--exact`,
//! `--skip`, `--ignored`, `--list`, `--test-threads`, and `RUST_TEST_THREADS`), so
//! `cargo test` and `cargo nextest` run the suites like any other tests.
//!
//! The harness ignores a case that requires a capability that the engine does not have, and,
//! on an engine that asks for it, a case whose service does not answer. Before the tests run,
//! it names on stderr what the run lacks for each such case.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use feldera_adapterlib::test::{Capability, ConnectorTestEngine};
use libtest_mimic::{Arguments, Failed, Trial};

use crate::test::pipeline::PipelineTestEngine;
use crate::test::service::Service;

/// A suite case, by the fixture that it needs.
pub enum CaseFn {
    /// A case of formats, transports, or connectors.
    Connector(fn(&dyn ConnectorTestEngine)),
    /// A case that runs pipelines.
    Pipeline(fn(&dyn PipelineTestEngine)),
}

/// The lock that a case holds while it runs, by key, with the meaning of `serial_test`.
pub enum CaseLock {
    /// The case runs alone among the cases of its key; the plain `serial` has the empty key.
    Serial(&'static str),
    /// The case runs beside the other parallel cases of its key, but never beside a serial
    /// case of that key.
    Parallel(&'static str),
}

/// A suite case, as `#[engine_test]` registers it.
pub struct EngineCase {
    /// The module of the case, as `module_path!` names it.
    pub module: &'static str,
    /// The name of the case's function.
    pub name: &'static str,
    /// The case.
    pub run: CaseFn,
    /// The lock that the case holds while it runs, if any.
    pub lock: Option<CaseLock>,
    /// Whether the harness skips the case unless ignored tests are asked for.
    pub ignore: bool,
    /// Why the case is ignored, when the attribute says.
    pub ignore_reason: Option<&'static str>,
    /// The text that the case's panic message must contain, for a case that must panic.
    pub should_panic: Option<&'static str>,
    /// The capabilities that the case needs from the engine.
    pub requires: &'static [Capability],
    /// The services outside the test binary that the case uses.
    pub services: &'static [Service],
}

inventory::collect!(EngineCase);

/// The environment variable that names the one case that a child process of
/// [`run_suites`] runs.
const CASE_VARIABLE: &str = "FELDERA_ENGINE_TEST_CASE";

/// The locks of the cases, by key: a serial case holds its key's lock to write, a parallel
/// case to read.
static LOCKS: Mutex<BTreeMap<&'static str, Arc<RwLock<()>>>> = Mutex::new(BTreeMap::new());

/// Returns the lock of `key`.
fn lock_of(key: &'static str) -> Arc<RwLock<()>> {
    LOCKS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(key)
        .or_default()
        .clone()
}

impl EngineCase {
    /// The name of the case's test: its module path in `dbsp_adapters`, then its name.
    pub fn test_name(&self) -> String {
        let module = self
            .module
            .strip_prefix("dbsp_adapters::")
            .unwrap_or(self.module);
        format!("{module}::{}", self.name)
    }

    /// Runs the case against `engine` in this process, and checks the panic that it must
    /// raise, if any.
    fn run_here(&self, engine: &dyn PipelineTestEngine) -> Result<(), Failed> {
        check_panic(self.should_panic, || match self.run {
            CaseFn::Connector(case) => case(engine),
            CaseFn::Pipeline(case) => case(engine),
        })
    }
}

/// What a run lacks for a case: the capabilities that the engine does not have, and the
/// services that do not answer.
#[derive(Debug, Default, PartialEq, Eq)]
struct Gate {
    missing: Vec<Capability>,
    absent: Vec<Service>,
}

impl Gate {
    /// Returns what a run on `engine` lacks for `case`. A service counts as absent only on an
    /// engine that skips absent services, and only if `present` says that it does not answer.
    fn of(
        case: &EngineCase,
        engine: &dyn PipelineTestEngine,
        present: impl Fn(Service) -> bool,
    ) -> Self {
        let missing = case
            .requires
            .iter()
            .copied()
            .filter(|capability| !engine.has(*capability))
            .collect();
        let absent = if engine.skip_absent_services() {
            case.services
                .iter()
                .copied()
                .filter(|service| !present(*service))
                .collect()
        } else {
            Vec::new()
        };
        Gate { missing, absent }
    }

    /// Returns `true` if the run lacks nothing that the case needs.
    fn is_open(&self) -> bool {
        self.missing.is_empty() && self.absent.is_empty()
    }
}

impl Display for Gate {
    /// Writes what the run lacks, for example
    /// `needs Storage, Checkpoints; Kafka does not answer at localhost:9092`.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if !self.missing.is_empty() {
            let names: Vec<String> = self
                .missing
                .iter()
                .map(|capability| format!("{capability:?}"))
                .collect();
            parts.push(format!("needs {}", names.join(", ")));
        }
        for service in &self.absent {
            parts.push(format!(
                "{service:?} does not answer at {}",
                service.address()
            ));
        }
        write!(f, "{}", parts.join("; "))
    }
}

/// Returns the report of the cases in `gated`, which a run on the engine named `engine`
/// ignores because it lacks what they need: a line for each case, then the number of cases
/// of each missing capability and of each absent service. Returns `None` if `gated` is
/// empty.
fn gate_report(engine: &str, gated: &[(String, Gate)]) -> Option<String> {
    /// Returns `counts` as a list such as `Storage 2, Checkpoints 1`.
    fn list<K: Debug>(counts: &BTreeMap<K, usize>) -> String {
        let items: Vec<String> = counts
            .iter()
            .map(|(key, count)| format!("{key:?} {count}"))
            .collect();
        items.join(", ")
    }

    if gated.is_empty() {
        return None;
    }
    let mut report = format!(
        "Engine '{engine}' ignores {} cases that need what this run lacks:\n",
        gated.len()
    );
    let mut capabilities = BTreeMap::new();
    let mut services = BTreeMap::new();
    for (name, gate) in gated {
        report += &format!("    {name}: {gate}\n");
        for capability in &gate.missing {
            *capabilities.entry(*capability).or_insert(0) += 1;
        }
        for service in &gate.absent {
            *services.entry(*service).or_insert(0) += 1;
        }
    }
    if !capabilities.is_empty() {
        report += &format!("Cases by missing capability: {}\n", list(&capabilities));
    }
    if !services.is_empty() {
        report += &format!("Cases by absent service: {}\n", list(&services));
    }
    Some(report)
}

/// Runs `run` and, if `expected` is set, requires it to panic with a message that contains
/// `expected`. Without `expected`, a panic propagates.
fn check_panic(expected: Option<&str>, run: impl FnOnce()) -> Result<(), Failed> {
    let Some(expected) = expected else {
        run();
        return Ok(());
    };
    match catch_unwind(AssertUnwindSafe(run)) {
        Ok(()) => Err("the case did not panic".into()),
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or_default();
            if message.contains(expected) {
                Ok(())
            } else {
                Err(format!("the panic {message:?} does not contain {expected:?}").into())
            }
        }
    }
}

/// Runs the case named `name` in a child process of this binary, with its output in a file,
/// and returns that output as the failure if the case fails.
///
/// A file rather than a pipe, so that a process that the case leaves running cannot keep
/// the harness waiting for the end of the output.
fn run_in_child(name: &str) -> Result<(), Failed> {
    let fail = |what: &str, error: std::io::Error| Failed::from(format!("{what}: {error}"));
    let mut output: File = tempfile::tempfile().map_err(|e| fail("cannot create a file", e))?;
    let stderr = output
        .try_clone()
        .map_err(|e| fail("cannot share the output file", e))?;
    let executable = std::env::current_exe().map_err(|e| fail("cannot find this binary", e))?;
    let status = Command::new(executable)
        .env(CASE_VARIABLE, name)
        .stdin(Stdio::null())
        .stdout(
            output
                .try_clone()
                .map_err(|e| fail("cannot share the output file", e))?,
        )
        .stderr(stderr)
        .status()
        .map_err(|e| fail("cannot run the case", e))?;
    if status.success() {
        return Ok(());
    }
    let mut text = Vec::new();
    output
        .seek(SeekFrom::Start(0))
        .and_then(|_| output.read_to_end(&mut text))
        .map_err(|e| fail("cannot read the output of the case", e))?;
    Err(format!("{}\n{status}", String::from_utf8_lossy(&text).trim_end()).into())
}

/// Splits the name filters, the positional arguments, from the options: the standard harness
/// takes several filters, and `libtest-mimic` takes one.
fn split_filters(arguments: impl IntoIterator<Item = String>) -> (Vec<String>, Vec<String>) {
    const WITH_VALUE: [&str; 6] = [
        "--test-threads",
        "--logfile",
        "--skip",
        "--color",
        "--format",
        "-Z",
    ];
    let mut filters = Vec::new();
    let mut options = Vec::new();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        if WITH_VALUE.contains(&argument.as_str()) {
            options.push(argument);
            options.extend(arguments.next());
        } else if argument.starts_with('-') {
            options.push(argument);
        } else {
            filters.push(argument);
        }
    }
    (filters, options)
}

/// Returns the test of `case` on `engine`, and what the run lacks for the case. The test is
/// ignored if the case says so or if the run lacks something that the case needs; `present`
/// tells whether a service answers. With `capture`, the test runs the case in a child
/// process.
fn trial(
    case: &'static EngineCase,
    engine: &Arc<dyn PipelineTestEngine>,
    capture: bool,
    present: impl Fn(Service) -> bool,
) -> (Trial, Gate) {
    let gate = Gate::of(case, engine.as_ref(), present);
    let ignored = case.ignore || !gate.is_open();
    let name = case.test_name();
    let engine = engine.clone();
    let trial = Trial::test(name.clone(), move || {
        let run = || {
            if capture {
                run_in_child(&name)
            } else {
                case.run_here(engine.as_ref())
            }
        };
        match case.lock {
            Some(CaseLock::Serial(key)) => {
                let lock = lock_of(key);
                let _guard = lock.write().unwrap_or_else(PoisonError::into_inner);
                run()
            }
            Some(CaseLock::Parallel(key)) => {
                let lock = lock_of(key);
                let _guard = lock.read().unwrap_or_else(PoisonError::into_inner);
                run()
            }
            None => run(),
        }
    })
    .with_ignored_flag(ignored);
    (trial, gate)
}

/// Runs the registered suite cases against `engine` and exits, as the test harness of an
/// engine's test target (`harness = false`).
///
/// Each case runs in a child process of this binary, which captures its output and shows it
/// only if the case fails, as the standard harness does. With `--nocapture`, which nextest
/// passes as it runs each test in a process of its own, the cases run in this process.
pub fn run_suites<E>(engine: E) -> !
where
    E: PipelineTestEngine + 'static,
{
    let mut cases: Vec<&'static EngineCase> = inventory::iter::<EngineCase>.into_iter().collect();
    cases.sort_by_key(|case| case.test_name());

    if let Ok(name) = std::env::var(CASE_VARIABLE) {
        let case = cases
            .iter()
            .find(|case| case.test_name() == name)
            .unwrap_or_else(|| panic!("no suite case is named {name}"));
        let passed = match catch_unwind(AssertUnwindSafe(|| case.run_here(&engine))) {
            Ok(Ok(())) => true,
            Ok(Err(failed)) => {
                eprintln!("{}", failed.message().unwrap_or_default());
                false
            }
            // The panic hook has printed the message.
            Err(_) => false,
        };
        std::process::exit(if passed { 0 } else { 101 });
    }

    let mut command_line = std::env::args();
    let program = command_line.next().unwrap_or_default();
    let (filters, options) = split_filters(command_line);
    let mut arguments = Arguments::from_iter(std::iter::once(program).chain(options));
    // The standard harness also takes the number of threads from `RUST_TEST_THREADS`.
    if arguments.test_threads.is_none() {
        arguments.test_threads = std::env::var("RUST_TEST_THREADS")
            .ok()
            .and_then(|threads| threads.parse().ok());
    }
    let capture = !arguments.nocapture;
    let exact = arguments.exact;
    let engine: Arc<dyn PipelineTestEngine> = Arc::new(engine);
    let mut trials = Vec::new();
    // The cases that the run selects and ignores for what it lacks.
    let mut gated = Vec::new();
    for case in cases {
        let name = case.test_name();
        let selected = filters.is_empty()
            || filters.iter().any(|filter| {
                if exact {
                    name == *filter
                } else {
                    name.contains(filter.as_str())
                }
            });
        if !selected {
            continue;
        }
        let (test, gate) = trial(case, &engine, capture, Service::is_present);
        if !gate.is_open() && !arguments.is_filtered_out(&test) {
            gated.push((name, gate));
        }
        trials.push(test);
    }
    // `--list` prints only the tests, as nextest expects.
    if !arguments.list
        && let Some(report) = gate_report(engine.name(), &gated)
    {
        eprint!("{report}");
    }
    libtest_mimic::run(&arguments, trials).exit()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use anyhow::{Result as AnyResult, bail};
    use arrow::array::RecordBatch;
    use feldera_adapterlib::catalog::SerBatchReader;
    use feldera_adapterlib::test::{Capability, ConnectorTestEngine, InputOptions, TestCollection};
    use feldera_types::program_schema::Relation;
    use uuid::Uuid;

    use super::{
        CaseFn, EngineCase, Gate, check_panic, gate_report, lock_of, split_filters, trial,
    };
    use crate::ControllerError;
    use crate::engine::{CircuitFactory, OpenStorageFn};
    use crate::test::pipeline::{CatalogSetup, PipelineTestEngine, TestCircuit};
    use crate::test::service::Service;

    /// An engine that lacks the capabilities in `lacks`, and that skips absent services if
    /// `skip_absent_services` says so. The tests build its trials but do not run them.
    struct ToyEngine {
        lacks: Vec<Capability>,
        skip_absent_services: bool,
    }

    /// Returns a [`ToyEngine`] that lacks `lacks`.
    fn toy_engine(lacks: &[Capability], skip_absent_services: bool) -> Arc<dyn PipelineTestEngine> {
        Arc::new(ToyEngine {
            lacks: lacks.to_vec(),
            skip_absent_services,
        })
    }

    impl ConnectorTestEngine for ToyEngine {
        fn name(&self) -> &'static str {
            "toy"
        }

        fn has(&self, capability: Capability) -> bool {
            !self.lacks.contains(&capability)
        }

        fn skip_absent_services(&self) -> bool {
            self.skip_absent_services
        }

        fn input_collection(
            &self,
            _relation: &Relation,
            _options: &InputOptions,
        ) -> AnyResult<Box<dyn TestCollection>> {
            bail!("the toy engine has no collections")
        }

        fn output_batch(
            &self,
            _relation: &Relation,
            _rows: &RecordBatch,
            _weights: &[i64],
        ) -> AnyResult<Arc<dyn SerBatchReader>> {
            bail!("the toy engine has no batches")
        }

        fn indexed_output_batch(
            &self,
            _key: &Relation,
            _relation: &Relation,
            _rows: &RecordBatch,
            _weights: &[i64],
        ) -> AnyResult<Arc<dyn SerBatchReader>> {
            bail!("the toy engine has no batches")
        }
    }

    impl PipelineTestEngine for ToyEngine {
        fn circuit_factory(
            &self,
            _circuit: &TestCircuit,
            _setup: Option<CatalogSetup>,
        ) -> Result<CircuitFactory, ControllerError> {
            Err(ControllerError::not_supported(
                "the toy engine has no circuits",
            ))
        }

        fn open_storage(&self) -> OpenStorageFn {
            Arc::new(|_config| Ok(None))
        }

        fn rewind_resume_state(&self, _path: &Path, _checkpoint: Uuid) {}

        fn storage_released(&self, _path: &Path) -> bool {
            true
        }
    }

    fn no_case(_engine: &dyn ConnectorTestEngine) {}

    /// A case that needs storage and checkpoints, and a Kafka broker and a Postgres server.
    static GATED_CASE: EngineCase = EngineCase {
        module: "dbsp_adapters::transport::kafka::tests",
        name: "gated",
        run: CaseFn::Connector(no_case),
        lock: None,
        ignore: false,
        ignore_reason: None,
        should_panic: None,
        requires: &[Capability::Storage, Capability::Checkpoints],
        services: &[Service::Kafka, Service::Postgres],
    };

    /// A case that the attribute ignores, and that needs nothing.
    static IGNORED_CASE: EngineCase = EngineCase {
        module: "dbsp_adapters::format::json::tests",
        name: "ignored",
        run: CaseFn::Connector(no_case),
        lock: None,
        ignore: true,
        ignore_reason: None,
        should_panic: None,
        requires: &[],
        services: &[],
    };

    /// A run lacks the capabilities that the engine does not have, in the order of the case,
    /// and the absent services only on an engine that skips them.
    #[test]
    fn a_gate_names_what_the_run_lacks() {
        let all_but_kafka_answer = |service| service != Service::Kafka;
        let gate = |engine: Arc<dyn PipelineTestEngine>| {
            Gate::of(&GATED_CASE, engine.as_ref(), all_but_kafka_answer)
        };
        assert_eq!(gate(toy_engine(&[], false)), Gate::default());
        assert!(gate(toy_engine(&[], false)).is_open());
        assert_eq!(
            gate(toy_engine(
                &[
                    Capability::Checkpoints,
                    Capability::Bootstrap,
                    Capability::Storage
                ],
                false
            )),
            Gate {
                missing: vec![Capability::Storage, Capability::Checkpoints],
                absent: Vec::new(),
            }
        );
        let gate = gate(toy_engine(&[], true));
        assert_eq!(
            gate,
            Gate {
                missing: Vec::new(),
                absent: vec![Service::Kafka],
            }
        );
        assert!(!gate.is_open());
    }

    /// A case's test is ignored when the attribute says so or when the run lacks what the
    /// case needs, and runs otherwise.
    #[test]
    fn a_case_is_ignored_when_the_run_lacks_what_it_needs() {
        let ignored = |case: &'static EngineCase, engine, present: bool| {
            trial(case, &engine, true, |_| present).0.has_ignored_flag()
        };
        // Like the static engine: every capability, and absent services are not skipped.
        assert!(!ignored(&GATED_CASE, toy_engine(&[], false), false));
        assert!(ignored(&IGNORED_CASE, toy_engine(&[], false), true));
        assert!(ignored(
            &GATED_CASE,
            toy_engine(&[Capability::Storage], false),
            true
        ));
        assert!(!ignored(
            &GATED_CASE,
            toy_engine(&[Capability::Profiles], false),
            true
        ));
        assert!(ignored(&GATED_CASE, toy_engine(&[], true), false));
        assert!(!ignored(&GATED_CASE, toy_engine(&[], true), true));
        assert_eq!(
            trial(&GATED_CASE, &toy_engine(&[], false), true, |_| true)
                .0
                .name(),
            "transport::kafka::tests::gated"
        );
    }

    /// The report names what the run lacks for each case, and counts the cases of each
    /// capability and service; a run that lacks nothing reports nothing.
    #[test]
    fn the_report_names_what_each_case_lacks() {
        assert_eq!(gate_report("toy", &[]), None);
        let gated = [
            (
                "a".to_string(),
                Gate {
                    missing: vec![Capability::Storage, Capability::Checkpoints],
                    absent: vec![Service::Kafka],
                },
            ),
            (
                "b".to_string(),
                Gate {
                    missing: vec![Capability::Storage],
                    absent: Vec::new(),
                },
            ),
        ];
        let kafka = Service::Kafka.address();
        assert_eq!(
            gate_report("toy", &gated).unwrap(),
            format!(
                "Engine 'toy' ignores 2 cases that need what this run lacks:\n    \
                 a: needs Storage, Checkpoints; Kafka does not answer at {kafka}\n    \
                 b: needs Storage\n\
                 Cases by missing capability: Storage 2, Checkpoints 1\n\
                 Cases by absent service: Kafka 1\n"
            )
        );
    }

    /// Returns the Rust files under `dir`.
    fn source_files(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(source_files(&path));
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
        files
    }

    /// Returns the number of suite functions in `source`, the public functions whose only
    /// argument is an engine, and the names of those without `#[engine_test]` among the
    /// attributes above them.
    fn suite_functions(source: &str) -> (usize, Vec<String>) {
        let lines: Vec<&str> = source.lines().collect();
        let mut count = 0;
        let mut missing = Vec::new();
        for (at, line) in lines.iter().enumerate() {
            let Some(rest) = line.trim_start().strip_prefix("pub fn ") else {
                continue;
            };
            let signature: String = lines[at..]
                .iter()
                .take_while(|line| !line.contains('{'))
                .chain(lines[at..].iter().find(|line| line.contains('{')))
                .copied()
                .collect();
            let Some(arguments) = signature
                .split_once('(')
                .and_then(|(_, rest)| rest.split_once(')'))
                .map(|(arguments, _)| arguments)
            else {
                continue;
            };
            let arguments: Vec<&str> = arguments
                .split(',')
                .map(str::trim)
                .filter(|argument| !argument.is_empty())
                .collect();
            let is_case = matches!(arguments.as_slice(), [argument]
                if argument.ends_with("&dyn ConnectorTestEngine")
                    || argument.ends_with("&dyn PipelineTestEngine"));
            if !is_case {
                continue;
            }
            count += 1;
            // The attributes and doc comments above the function. rustfmt splits a long
            // attribute over several lines, which are indented further or close the attribute.
            let indent = line.len() - line.trim_start().len();
            let attributed = lines[..at]
                .iter()
                .rev()
                .take_while(|above| {
                    let text = above.trim_start();
                    let deeper = above.len() - text.len() > indent;
                    text.starts_with("#[")
                        || text.starts_with("///")
                        || text.starts_with(")]")
                        || (deeper && !text.is_empty())
                })
                .any(|above| above.contains("engine_test"));
            if !attributed {
                missing.push(rest[..rest.find('(').unwrap_or(rest.len())].to_string());
            }
        }
        (count, missing)
    }

    /// Every suite function carries `#[engine_test]`, so that no case is left out of the
    /// engines' test runs.
    #[test]
    fn every_suite_function_is_an_engine_test() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut cases = 0;
        let mut missing = Vec::new();
        for path in source_files(&root) {
            let (count, lacking) = suite_functions(&fs::read_to_string(&path).unwrap());
            cases += count;
            missing.extend(
                lacking
                    .into_iter()
                    .map(|name| format!("{}: {name}", path.display())),
            );
        }
        assert!(cases > 0);
        assert!(
            missing.is_empty(),
            "suite functions without #[engine_test]: {missing:#?}"
        );
    }

    /// The check finds a suite function without the attribute, also inside an inline module,
    /// and passes over a helper that takes more than the engine.
    #[test]
    fn a_suite_function_without_engine_test_is_found() {
        let source = "/// A case.\n#[engine_test(serial)]\npub fn listed(engine: &dyn \
                      PipelineTestEngine) {\n}\n\n/// Another.\npub fn unlisted(\n    engine: &dyn \
                      ConnectorTestEngine,\n) {\n}\n\npub fn helper(rows: usize) {}\n\npub fn \
                      check(engine: &dyn ConnectorTestEngine, rows: usize) {}\n\n/// A case \
                      with a long attribute.\n#[engine_test(\n    serial,\n    \
                      requires(Storage)\n)]\npub fn wrapped(engine: &dyn PipelineTestEngine) \
                      {\n}\n\nmod inline {\n    #[engine_test]\n    pub fn nested(engine: &dyn \
                      ConnectorTestEngine) {}\n\n    pub fn forgotten(engine: &dyn \
                      PipelineTestEngine) {}\n}\n";
        assert_eq!(
            suite_functions(source),
            (5, vec!["unlisted".to_string(), "forgotten".to_string()])
        );
    }

    /// The registered cases have distinct test names, without the crate's name.
    #[test]
    fn registered_cases_have_distinct_names() {
        let mut names = BTreeSet::new();
        for case in inventory::iter::<EngineCase> {
            let name = case.test_name();
            assert!(!name.starts_with("dbsp_adapters::"), "{name}");
            assert!(names.insert(name.clone()), "two cases are named {name}");
        }
        assert!(!names.is_empty());
    }

    #[test]
    fn a_test_name_drops_the_crate_name() {
        fn case(_engine: &dyn ConnectorTestEngine) {}
        let case = EngineCase {
            module: "dbsp_adapters::transport::kafka::tests",
            name: "metadata_json",
            run: CaseFn::Connector(case),
            lock: None,
            ignore: false,
            ignore_reason: None,
            should_panic: None,
            requires: &[],
            services: &[],
        };
        assert_eq!(case.test_name(), "transport::kafka::tests::metadata_json");
    }

    #[test]
    fn a_case_that_must_panic_passes_only_on_its_panic() {
        assert!(check_panic(Some("offset '3'"), || panic!("provided offset '3' gone")).is_ok());
        assert!(check_panic(Some("offset '3'"), || {}).is_err());
        assert!(check_panic(Some("offset '3'"), || panic!("another error")).is_err());
        assert!(check_panic(None, || {}).is_ok());
    }

    #[test]
    fn filters_are_the_positional_arguments() {
        let arguments = [
            "kafka::",
            "--skip",
            "pubsub",
            "--exact",
            "clock",
            "--test-threads",
            "4",
            "--skip=tls",
            "--nocapture",
        ];
        let (filters, options) = split_filters(arguments.map(String::from));
        assert_eq!(filters, ["kafka::", "clock"]);
        assert_eq!(
            options,
            [
                "--skip",
                "pubsub",
                "--exact",
                "--test-threads",
                "4",
                "--skip=tls",
                "--nocapture"
            ]
        );
    }

    #[test]
    fn a_key_has_one_lock() {
        assert!(Arc::ptr_eq(&lock_of("delta_s3"), &lock_of("delta_s3")));
        assert!(!Arc::ptr_eq(&lock_of("delta_s3"), &lock_of("")));
    }
}
