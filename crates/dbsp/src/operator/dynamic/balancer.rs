enum Policy {
    Shard,
    Broadcast,
}

#[derive(Default, Clone)]
struct WorkerStats {
    size: usize,
}

struct Balancer {
    stats: Vec<Vec<WorkerStats>>,
}

impl Balancer {
    fn new(num_streams: usize, num_workers: usize) -> Self {
        Self {
            stats: vec![vec![WorkerStats::default(); num_workers]; num_streams],
        }
    }

    fn update_worker_stats(
        &mut self,
        stream_index: usize,
        worker_index: usize,
        stats: WorkerStats,
    ) {
        self.stats[stream_index][worker_index] = stats;
    }

    fn get_policy(&self, stream_index: usize, current_policy: &Policy) -> Policy {
        todo!()
    }
}

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    fn dyn_accumulate_trace_with_balancer(
        &self,
        balancer: &Balancer,
        stream_index: usize,
    ) -> Stream<C, B> {
        // Exchange sender

        // Extra stream for the accumulator

        // Accumulator

        // Connect the stream (and register it with the circuit)

        // Integral with feedback connector

        // Connect the loop.
    }
}
