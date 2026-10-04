use anyhow::Result;
use clap::Parser;
use f5a::gateway::RestGateway;
use f5a::options::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let (options, settings) = Cli::parse().into_settings()?;
    // Printed before the console takes the screen, so they stay in the
    // scrollback once it quits.
    for warning in options.warnings() {
        eprintln!("warning: {warning}");
    }
    let gateway = RestGateway::connect(&options)?;
    f5a::runner::run(gateway, settings).await
}
