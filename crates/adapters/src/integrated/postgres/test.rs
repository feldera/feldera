// Postgres CDC input connector tests that reach no engine. The `postgres_cdc`
// connector suite holds the pipeline tests.

#[cfg(feature = "with-postgres-cdc")]
mod cdc_tests {
    use serial_test::serial;

    use crate::integrated::postgres::tests::{pg_connect, postgres_url};
    use crate::test::wait;

    /// Shutting the connector's runtime down closes the PostgreSQL connections
    /// it hosts. That is what releases a replication slot when an etl worker
    /// outlives the stop grace, so a backend held by a hosted connection must
    /// be gone once the runtime is.
    #[test]
    #[serial]
    fn test_cdc_runtime_shutdown_closes_hosted_connections() {
        let url = postgres_url();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let backend_pid: i32 = runtime.block_on(async {
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .expect("failed to connect");
            tokio::spawn(connection);
            let pid = client
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0);
            // A query that does not return stands in for an etl worker that
            // does not stop.
            tokio::spawn(async move {
                let _ = client.execute("SELECT pg_sleep(600)", &[]).await;
            });
            // Let the hosted task send its query before the runtime goes.
            tokio::task::yield_now().await;
            pid
        });
        runtime.shutdown_timeout(crate::integrated::postgres::cdc_input::RUNTIME_SHUTDOWN_TIMEOUT);

        let mut observer = pg_connect(&url, &None);
        wait(
            || {
                observer
                    .query(
                        "SELECT 1 FROM pg_stat_activity WHERE pid = $1",
                        &[&backend_pid],
                    )
                    .unwrap()
                    .is_empty()
            },
            5_000,
        )
        .expect("the backend of a hosted connection outlived the runtime");
    }
}
