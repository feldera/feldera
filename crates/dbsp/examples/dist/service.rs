use chrono::NaiveDate;
use dbsp::circuit::Layout;
use rkyv::Archive;
use size_of::SizeOf;

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    SizeOf,
    serde::Deserialize,
    serde::Serialize,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Record {
    pub location: String,
    pub date: NaiveDate,
    pub daily_vaccinations: Option<u64>,
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    SizeOf,
    serde::Deserialize,
    serde::Serialize,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct VaxMonthly {
    pub count: u64,
    pub year: i32,
    pub month: u8,
}

#[tarpc::service]
pub trait Circuit {
    async fn init(layout: Layout);
    async fn run(records: Vec<(Record, i64)>) -> Vec<(String, VaxMonthly, i64)>;
}
