use anyhow::Result;
use clap::Parser;
use f5a::gateway::RestGateway;
use f5a::options::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let (options, settings) = Cli::parse().into_settings()?;
    let gateway = RestGateway::connect(&options)?;
    f5a::runner::run(gateway, settings).await
}
