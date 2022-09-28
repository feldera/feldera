macro_rules! run_queries {
    // Runs a single query
    (@single $query:ident, $nexmark_config:expr) => {{
        let circuit_closure = run_queries!(@circuit $query);

        let num_cores = $nexmark_config.cpu_cores;
        let expected_num_events = $nexmark_config.max_events;
        let (dbsp, input_handle) = Runtime::init_circuit(num_cores, circuit_closure).unwrap();

        // Create a channel for the coordinating thread to determine whether the
        // producer or consumer step is completed first.
        let (step_done_tx, step_done_rx) = mpsc::sync_channel(2);

        // Start the DBSP runtime processing steps only when it receives a message to do
        // so. The DBSP processing happens in its own thread where the resource usage
        // calculation can also happen.
        let (dbsp_step_tx, dbsp_step_rx) = mpsc::sync_channel(1);
        let dbsp_join_handle = spawn_dbsp_consumer(dbsp, dbsp_step_rx, step_done_tx.clone());

        // Start the generator inputting the specified number of batches to the circuit
        // whenever it receives a message.
        let (source_step_tx, source_step_rx): (mpsc::SyncSender<()>, mpsc::Receiver<()>) =
            mpsc::sync_channel(1);
        let (source_exhausted_tx, source_exhausted_rx) = mpsc::sync_channel(1);
        spawn_source_producer(
            $nexmark_config,
            input_handle,
            source_step_rx,
            step_done_tx,
            source_exhausted_tx,
        );

        let input_stats = coordinate_input_and_steps(
            expected_num_events,
            dbsp_step_tx,
            source_step_tx,
            step_done_rx,
            source_exhausted_rx,
            dbsp_join_handle,
        )
        .unwrap();

        // Return the user/system CPU overhead from the generator/input thread.
        NexmarkResult {
            num_events: input_stats.num_events,
            ..NexmarkResult::default()
        }
    }};

    // Returns a closure for a circuit with the nexmark source that returns
    // the input handle.
    (@circuit q13) => {
        |circuit: &mut Circuit<()>| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();
            let (side_stream, mut side_input_handle) =
                circuit.add_input_zset::<(usize, String, u64), isize>();

            let output = q13(stream, side_stream);

            output.inspect(move |_zs| ());

            // Ensure the side-input is loaded here so we can return a single input
            // handle like the other queries.
            side_input_handle.append(&mut q13_side_input());

            input_handle
        }
    };
    (@circuit $query:ident) => {
        |circuit: &mut Circuit<()>| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = $query(stream);

            output.inspect(move |_zs| ());

            input_handle
        }
    };

    ($nexmark_config:expr, $max_events:expr, $queries_to_run:expr, queries => { $($query:ident),+ $(,)? } $(,)?) => {{
        let mut results: Vec<NexmarkResult> = Vec::new();

        $(
            if $queries_to_run.len() == 0 || $queries_to_run.contains(&paste::paste!(NexmarkQuery::[<$query:upper>])) {
                println!("Starting {} bench of {} events...", stringify!($query), $max_events);

                let before_stats = ALLOC.stats();
                let start = Instant::now();

                let thread_nexmark_config = $nexmark_config.clone();
                let result = run_queries!(@single $query, thread_nexmark_config);
                let after_stats = ALLOC.stats();

                results.push(NexmarkResult {
                    name: stringify!($query).to_owned(),
                    before_stats,
                    after_stats,
                    elapsed: start.elapsed(),
                    ..result
                });
            }
        )+

        results
    }};
}
