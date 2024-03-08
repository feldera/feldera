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
        let dbsp_join_handle = spawn_dbsp_consumer(stringify!($query), $nexmark_config.profile_path.as_deref(), dbsp, dbsp_step_rx, step_done_tx.clone());

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

        ALLOC.reset_stats();
        let before_stats = ALLOC.stats();
        let start = Instant::now();

        let input_stats = coordinate_input_and_steps(
            expected_num_events,
            dbsp_step_tx,
            source_step_tx,
            step_done_rx,
            source_exhausted_rx,
            dbsp_join_handle,
        )
        .unwrap();

        let elapsed = start.elapsed();
        let after_stats = ALLOC.stats();

        // Return the user/system CPU overhead from the generator/input thread.
        NexmarkResult {
            name: stringify!($query).to_owned(),
            num_cores,
            before_stats,
            after_stats,
            elapsed,
            num_events: input_stats.num_events,
        }
    }};

    // Returns a closure for a circuit with the nexmark source that returns
    // the input handle.
    (@circuit q13) => {
        |circuit: &mut RootCircuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();
            let (side_stream, side_input_handle) =
                circuit.add_input_zset::<Tup3<u64, String, u64>>();

            let output = q13(stream, side_stream);

            output.inspect(move |_zs| ());

            // Ensure the side-input is loaded here so we can return a single input
            // handle like the other queries.
            side_input_handle.append(&mut q13_side_input());

            Ok(input_handle)
        }
    };
    (@circuit $query:ident) => {
        |circuit: &mut RootCircuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            let output = $query(stream);

            output.inspect(move |_zs| ());

            Ok(input_handle)
        }
    };

    ($nexmark_config:expr, $max_events:expr, $queries_to_run:expr, queries => { $($query:ident),+ $(,)? } $(,)?) => {{
        let mut results: Vec<NexmarkResult> = Vec::new();

        $(
            if $queries_to_run.len() == 0 || $queries_to_run.contains(&paste::paste!(NexmarkQuery::[<$query:upper>])) {
                println!("Starting {} bench of {} events...", stringify!($query), $max_events);

                let thread_nexmark_config = $nexmark_config.clone();
                results.push(run_queries!(@single $query, thread_nexmark_config));
            }
        )+

        results
    }};
}
