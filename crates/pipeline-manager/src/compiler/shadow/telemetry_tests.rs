//! Run the normal launcher with local stand-ins for the compiler executables.
use super::*;
use crate::compiler::sql_compiler::perform_sql_compilation;
use crate::config::{DatabaseConfig, ShadowCompiler};
use crate::db::storage_postgres::StoragePostgres;
use clap::Parser;
use feldera_observability::json_logging::{JsonPipelineFormat, LogIdentity, ServiceName};
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc};

async fn check_launch_scope(common: &CommonConfig, config: &CompilerConfig) {
    let pg = tokio_postgres::Config::new();
    let pool = deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(
        pg.clone(),
        tokio_postgres::NoTls,
    ))
    .build()
    .unwrap();
    let db = Arc::new(AsyncMutex::new(StoragePostgres {
        db_config: DatabaseConfig::new(String::new(), None),
        config: pg,
        pool,
        #[cfg(feature = "postgresql_embedded")]
        pg_inst: None,
    }));
    for (stored, output) in [
        (false, SqlCompilationOutput::IrOnly),
        (false, SqlCompilationOutput::Full),
        (true, SqlCompilationOutput::IrOnly),
        (true, SqlCompilationOutput::Full),
    ] {
        let skipped = COUNTERS.skipped.load(Ordering::Relaxed);
        // A platform mismatch returns before any database access.
        let primary = perform_sql_compilation(
            common,
            config,
            stored.then(|| db.clone()),
            TenantId(Uuid::nil()),
            PipelineId(Uuid::nil()),
            None,
            "other platform",
            Version(1),
            &json!({}),
            "SELECT 1",
            output,
        )
        .await;
        assert!(matches!(primary, Err(SqlCompilationError::SystemError(_))));
        let eligible = stored && output == SqlCompilationOutput::Full;
        assert_eq!(
            COUNTERS.skipped.load(Ordering::Relaxed) - skipped,
            u64::from(eligible)
        );
    }
}

#[tokio::test]
async fn shadow_admission_counters_and_diagnostics() {
    const ROOT: &str = "FELDERA_SHADOW_TEST_ROOT";
    let Ok(root) = std::env::var(ROOT) else {
        // Isolate PATH, counters and the log subscriber from parallel tests.
        let dir = tempfile::tempdir().unwrap();
        crate::run_test_in_new_process(
            "compiler::shadow::telemetry_tests::shadow_admission_counters_and_diagnostics",
            [(ROOT, dir.path()), ("PATH", dir.path())],
        );
        return;
    };
    let root = PathBuf::from(root);
    let log = root.join("shadow.log");
    let subscriber = tracing_subscriber::fmt()
        .event_format(JsonPipelineFormat::new(LogIdentity::Service {
            service_name: Some(ServiceName::CompilerServer),
        }))
        .with_writer(fs::File::create(&log).unwrap())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let mut config = CompilerConfig::parse_from(["test", "--shadow-sql-compiler", "hybrid"]);
    config.compiler_working_directory = root.display().to_string();
    config.sql_compiler_path = root.join("compiler.jar").display().to_string();
    fs::write(&config.sql_compiler_path, []).unwrap();
    let common = CommonConfig::test_config();
    let id = PipelineId(Uuid::now_v7());
    let program_config = json!({"compiler": "java"});
    let sql = "SELECT 'private SQL literal'";
    let primary = Err(SqlCompilationError::SqlError(SqlCompilationInfo {
        exit_code: 1,
        messages: vec![],
    }));
    let start = |config: &CompilerConfig, primary: &SqlCompilationResult| {
        launch(&common, config, &program_config, id, sql, primary).unwrap()
    };

    // A new process starts with no unstable feature.
    assert!(launch(&common, &config, &program_config, id, sql, &primary).is_none());
    crate::enable_test_unstable_features();

    check_launch_scope(&common, &config).await;

    let busy = SLOT.try_lock().unwrap();
    assert!(launch(&common, &config, &program_config, id, sql, &primary).is_none());
    drop(busy);
    let long_sql = "x".repeat(MAX_SQL_BYTES + 1);
    assert!(launch(&common, &config, &program_config, id, &long_sql, &primary).is_none());
    let large = Ok((
        json!({"schema": "x".repeat(MAX_OBSERVATION_BYTES)}),
        Duration::ZERO,
        SqlCompilationInfo::success(),
    ));
    assert!(launch(&common, &config, &program_config, id, sql, &large).is_none());

    let direct_id = PipelineId(Uuid::now_v7());
    fs::create_dir_all(ephemeral_compilation_dir(&config).join(direct_id.to_string())).unwrap();
    let error = perform_sql_compilation_inner(SqlCompilationRequest {
        common_config: &common,
        config: &config,
        db: None,
        tenant_id: TenantId(Uuid::nil()),
        pipeline_id: direct_id,
        pipeline_name: None,
        platform_version: &common.platform_version,
        program_version: Version(1),
        program_config: &program_config,
        program_code: sql,
        output: SqlCompilationOutput::Shadow,
        shadow: &mut Attempt::default(),
    })
    .await
    .unwrap_err();
    assert!(matches!(error, SqlCompilationError::SystemError(text)
        if text.contains("Could not start SQL compiler 'sql2dbsp'") && text.contains("PATH")));

    // The hybrid executable is absent from the private PATH.
    for _ in 0..2 {
        assert_eq!(
            start(&config, &primary).await.unwrap(),
            Outcome::Failed(Failure::SystemError)
        );
    }
    let hybrid = root.join("sql2dbsp");
    let result = json!({
        "schema_version": 1, "compiler": "hybrid", "producer_commit": env!("VERGEN_GIT_SHA"),
        "runtime_abi": env!("VERGEN_GIT_SHA"), "engine": "classic", "status": "resource_limit",
        "reason": "compilation deadline expired", "messages": [], "artifacts": {}
    });
    fs::write(&hybrid, format!(
        "#!/bin/sh\n/bin/mkdir -p \"$3/result\"\necho '{result}' > \"$3/result/result.json\"\nexit 1\n"
    )).unwrap();
    fs::set_permissions(&hybrid, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(start(&config, &primary).await.unwrap(), Outcome::TimedOut);
    let java = root.join("java");
    let executable = |body: &str| {
        fs::write(&java, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(&java, fs::Permissions::from_mode(0o700)).unwrap();
    };
    config.shadow_sql_compiler = Some(ShadowCompiler::Java);
    executable("exit 1");
    assert_eq!(start(&config, &primary).await.unwrap(), Outcome::Equal);
    let accepted = Ok((
        json!({"schema": {}}),
        Duration::ZERO,
        SqlCompilationInfo::success(),
    ));
    assert_eq!(start(&config, &accepted).await.unwrap(), Outcome::Different);
    executable("echo 'private schema name' >&2\nexit 7");
    assert_eq!(
        start(&config, &primary).await.unwrap(),
        Outcome::Failed(Failure::SystemError)
    );
    executable("kill -TERM $$");
    assert_eq!(
        start(&config, &primary).await.unwrap(),
        Outcome::Failed(Failure::TerminatedBySignal)
    );

    let message = json!({
        "start_line_number": 1, "start_column": 1, "end_line_number": 1, "end_column": 1,
        "warning": false, "error_type": "", "message": "x".repeat(MAX_OBSERVATION_BYTES)
    });
    let diagnostics = root.join("diagnostics.json");
    fs::write(&diagnostics, json!([message]).to_string()).unwrap();
    executable("/bin/cat \"${0%/*}/diagnostics.json\" >&2\nexit 1");
    assert_eq!(
        start(&config, &primary).await.unwrap(),
        Outcome::Failed(Failure::ObservationTooLarge)
    );

    let mut started =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1()).unwrap();
    executable(
        r#"echo $$ > "${0%/*}/started.pid"
kill -USR1 "$PPID"
kill -STOP $$"#,
    );
    let running = start(&config, &primary);
    assert!(started.recv().await.is_some());
    let pid = fs::read_to_string(root.join("started.pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(config.shadow_sql_compiler_timeout_secs)).await;
    tokio::time::resume();
    assert_eq!(running.await.unwrap(), Outcome::TimedOut);
    assert_eq!(
        nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pid), None),
        Err(nix::errno::Errno::ESRCH)
    );

    let contents = fs::read_to_string(&log).unwrap();
    assert!(!contents.contains(sql) && !contents.contains("private schema name"));
    let events: Vec<Value> = contents
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    let fields = |message: &str| {
        events
            .iter()
            .map(|e| &e["fields"])
            .filter(|f| f["message"] == message)
            .collect::<Vec<_>>()
    };
    let check_counters = |event, expected: &[(&str, u64)]| {
        let events = fields(event);
        let last = events.last().unwrap();
        for &(name, value) in expected {
            assert_eq!(last[name], value, "{name}");
        }
    };
    check_counters(
        "SQL shadow skipped",
        &[
            ("skipped", 5),
            ("dropped_busy", 1),
            ("skipped_sql_too_large", 1),
            ("skipped_observation_too_large", 1),
        ],
    );
    check_counters(
        "SQL shadow completed",
        &[
            ("completed", 9),
            ("compared", 2),
            ("equal", 1),
            ("different", 1),
            ("timed_out", 2),
            ("failed_to_start", 2),
            ("skipped_observation_too_large", 2),
        ],
    );
    let mismatches = fields("SQL shadow mismatch");
    let warnings: Vec<_> = events
        .iter()
        .filter(|e| e["level"] == "WARN")
        .map(|e| &e["fields"])
        .collect();
    assert_eq!(
        (mismatches.len(), warnings.len()),
        (1, 3),
        "one difference and one warning per distinct cause"
    );
    assert_eq!(mismatches[0]["difference_class"], "Verdict");
    assert_eq!(warnings[0]["program"], "sql2dbsp");
    assert_eq!(warnings[0]["io_error"], "NotFound");
    assert_eq!(warnings[1]["exit_code"], 7);
    assert_eq!(warnings[2]["signal"], libc::SIGTERM);

    let stale_dir = root.join("stale");
    fs::write(&stale_dir, []).unwrap();
    let cleanup = Cleanup {
        directory: stale_dir.clone(),
        groups: [None, None],
    };
    let mut slot = SLOT.try_lock().unwrap();
    assert_eq!(
        cleanup.finish(Outcome::Different, &mut slot).await,
        Outcome::Different
    );
    drop(slot);
    executable("exit 1");
    assert_eq!(
        start(&config, &primary).await.unwrap(),
        Outcome::Failed(Failure::Cleanup)
    );
    fs::remove_file(&stale_dir).unwrap();
    fs::create_dir(&stale_dir).unwrap();
    assert_eq!(start(&config, &primary).await.unwrap(), Outcome::Equal);
    assert!(!stale_dir.exists());
    let recovery = fs::read_to_string(log).unwrap();
    assert_eq!(recovery.matches("SQL shadow cleanup failed.").count(), 1);
    assert_eq!(recovery.matches("SQL shadow cleanup recovered.").count(), 1);
}
