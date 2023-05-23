use anyhow::Result;
use csv::Reader;
use serde::Deserialize;
use time::Date;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct Record {
    location: String,
    date: Date,
    daily_vaccinations: Option<u64>,
}

fn main() -> Result<()> {
    let path = format!(
        "{}/examples/tutorial/vaccinations.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    for result in Reader::from_path(path)?.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
    }
    Ok(())
}
