use anyhow::Result as AnyResult;
use chrono::Datelike;
use clap::Parser;
use dbsp::circuit::CircuitConfig;
use dbsp::utils::Tup2;
use dbsp::{
    circuit::Layout, utils::Tup3, DBSPHandle, OrdIndexedZSet, OutputHandle, RootCircuit, Runtime,
    ZSetHandle,
};
use futures::{
    future::{self, Ready},
    prelude::*,
};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};
use tarpc::{
    context,
    serde_transport::tcp::listen,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Bincode,
};

mod service;
use service::*;

#[derive(Debug, Clone, Parser)]
struct Args {
    /// IP address and TCP port to listen, in the form `<ip>:<port>`.  If
    /// `<port>` is `0`, the kernel will pick a free port.
    #[clap(long, default_value = "127.0.0.1:0")]
    address: SocketAddr,
}

fn build_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    ZSetHandle<Record>,
    OutputHandle<OrdIndexedZSet<String, VaxMonthly>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_zset::<Record>();
    let subset = input_stream.filter(|r| {
        r.location == "England"
            || r.location == "Northern Ireland"
            || r.location == "Scotland"
            || r.location == "Wales"
    });
    let monthly_totals = subset
        .map_index(|r| {
            (
                Tup3(r.location.clone(), r.date.year(), r.date.month() as u8),
                r.daily_vaccinations.unwrap_or(0),
            )
        })
        .aggregate_linear(|v| *v as i64);
    let most_vax = monthly_totals
        .map_index(|(Tup3(l, y, m), sum)| {
            (
                l.clone(),
                VaxMonthly {
                    count: *sum as u64,
                    year: *y,
                    month: *m,
                },
            )
        })
        .topk_desc(3);
    Ok((input_handle, most_vax.output()))
}

struct Inner {
    circuit: DBSPHandle,
    input_handle: ZSetHandle<Record>,
    output_handle: OutputHandle<OrdIndexedZSet<String, VaxMonthly>>,
}

impl Inner {
    fn new(layout: impl Into<CircuitConfig>) -> AnyResult<Inner> {
        let (circuit, (input_handle, output_handle)) =
            Runtime::init_circuit(layout, build_circuit)?;
        Ok(Inner {
            circuit,
            input_handle,
            output_handle,
        })
    }
}

#[derive(Clone)]
struct Server(Arc<Mutex<Option<Inner>>>);

impl Server {
    fn new() -> Server {
        Server(Arc::new(Mutex::new(None)))
    }
    fn inner(&self) -> MutexGuard<'_, Option<Inner>> {
        self.0.lock().unwrap()
    }
    fn replace(&self, layout: impl Into<CircuitConfig>) {
        let mut inner = self.inner();

        // First clear the old server, if any, and clean up.  It's important to
        // do this first in case the old server is using resources that the new
        // server will also need (e.g. listening on ports).
        drop(inner.take());

        *inner = Some(Inner::new(layout).unwrap());
    }
}
impl Circuit for Server {
    type InitFut = Ready<()>;
    fn init(self, _: context::Context, layout: Layout) -> Self::InitFut {
        self.replace(layout);
        future::ready(())
    }
    type RunFut = Ready<Vec<(String, VaxMonthly, i64)>>;
    fn run(self, _: context::Context, mut records: Vec<Tup2<Record, i64>>) -> Self::RunFut {
        self.inner()
            .as_ref()
            .unwrap()
            .input_handle
            .append(&mut records);
        self.inner().as_mut().unwrap().circuit.step().unwrap();
        future::ready(
            self.inner()
                .as_ref()
                .unwrap()
                .output_handle
                .consolidate()
                .iter()
                .collect(),
        )
    }
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let Args { address } = Args::parse();

    let mut listener = listen(address, Bincode::default).await?;
    println!("Listening on port {}", listener.local_addr().port());
    listener.config_mut().max_frame_length(usize::MAX);
    let server = Server::new();
    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        .map(|channel| channel.execute(server.clone().serve()))
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    Ok(())
}
