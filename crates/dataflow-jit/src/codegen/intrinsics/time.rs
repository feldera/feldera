use crate::utils::TimeExt;
use chrono::{NaiveTime, Timelike};

pub(super) unsafe extern "C" fn time_hour(time: u64) -> u32 {
    if let Some(time) = NaiveTime::from_nanoseconds(time) {
        time.hour()
    } else {
        tracing::error!("failed to get hour from {time}");
        0
    }
}

pub(super) unsafe extern "C" fn time_minute(time: u64) -> u32 {
    if let Some(time) = NaiveTime::from_nanoseconds(time) {
        time.minute()
    } else {
        tracing::error!("failed to get minute from {time}");
        0
    }
}

pub(super) unsafe extern "C" fn time_second(time: u64) -> u32 {
    if let Some(time) = NaiveTime::from_nanoseconds(time) {
        time.second()
    } else {
        tracing::error!("failed to get second from {time}");
        0
    }
}

pub(super) unsafe extern "C" fn time_millisecond(time: u64) -> u32 {
    if let Some(time) = NaiveTime::from_nanoseconds(time) {
        (time.second() * 1000) + (time.nanosecond() / 1_000_000)
    } else {
        tracing::error!("failed to get millisecond from {time}");
        0
    }
}

pub(super) unsafe extern "C" fn time_microsecond(time: u64) -> u32 {
    if let Some(time) = NaiveTime::from_nanoseconds(time) {
        (time.second() * 1_000_000) + (time.nanosecond() / 1000)
    } else {
        tracing::error!("failed to get microsecond from {time}");
        0
    }
}
