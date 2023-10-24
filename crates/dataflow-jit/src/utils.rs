use chrono::{NaiveTime, Timelike};
use rust_decimal::Decimal;
use xxhash_rust::xxh3::Xxh3Builder;

pub(crate) type HashMap<K, V, S = Xxh3Builder> = std::collections::HashMap<K, V, S>;

#[cfg(test)]
pub(crate) fn test_logger() {
    use is_terminal::IsTerminal;
    use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

    let filter = EnvFilter::try_from_env("DATAFLOW_JIT_LOG")
        .or_else(|_| EnvFilter::try_new("info,cranelift_codegen=off,cranelift_jit=off"))
        .unwrap();
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_test_writer()
                .with_ansi(std::io::stdout().is_terminal()),
        )
        .try_init();
}

pub trait NativeRepr: Sized {
    type Repr;

    fn to_repr(self) -> Self::Repr;

    fn from_repr(repr: Self::Repr) -> Self;
}

impl NativeRepr for Decimal {
    type Repr = u128;

    #[inline]
    fn to_repr(self) -> Self::Repr {
        u128::from_le_bytes(self.serialize())
    }

    #[inline]
    fn from_repr(repr: Self::Repr) -> Self {
        Self::deserialize(repr.to_le_bytes())
    }
}

#[macro_export]
macro_rules! row {
    (@column null) => { $crate::ir::literal::NullableConstant::null() };
    (@column ? null) => { $crate::ir::literal::NullableConstant::null() };

    (@column ? $expr:expr) => {
        $crate::ir::literal::NullableConstant::Nullable(
            ::std::option::Option::Some(
                $crate::ir::Constant::from($expr),
            ),
        )
    };

    (@column $expr:expr) => {
        $crate::ir::literal::NullableConstant::NonNull(
            $crate::ir::Constant::from($expr),
        )
    };

    [$($($(@ $__capture:tt)? ?)? $expr:expr),* $(,)?] => {
        $crate::ir::literal::RowLiteral::new(
            ::std::vec![$(
                ::defile::defile!(row!(
                    // @ escaped as @@
                    @@ column
                    $($($__capture)??)?
                    @ $expr // <- tell defile to handle this
                )),
            )*],
        )
    };
}

pub const SECONDS_TO_NANOS: u64 = 1_000_000_000;
pub const MILLIS_TO_NANOS: u64 = 1_000_000;
pub const MICROS_TO_NANOS: u64 = 1000;

pub trait TimeExt: Sized {
    const MAX: Self;

    fn to_nanoseconds(self) -> u64;

    fn from_nanoseconds(nanos: u64) -> Option<Self>;

    fn from_milliseconds(millis: u64) -> Option<Self>;

    fn from_microseconds(micros: u64) -> Option<Self>;
}

impl TimeExt for NaiveTime {
    const MAX: Self = match NaiveTime::from_num_seconds_from_midnight_opt(
        23 * 3600 + 59 * 60 + 59,
        999_999_999,
    ) {
        Some(max) => max,
        None => panic!(),
    };

    #[inline]
    fn to_nanoseconds(self) -> u64 {
        (self.num_seconds_from_midnight() as u64 * SECONDS_TO_NANOS) + self.nanosecond() as u64
    }

    #[inline]
    fn from_nanoseconds(nanos: u64) -> Option<Self> {
        let (secs, nanos) = (nanos / SECONDS_TO_NANOS, nanos % SECONDS_TO_NANOS);
        NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nanos as u32)
    }

    #[inline]
    fn from_milliseconds(millis: u64) -> Option<Self> {
        Self::from_nanoseconds(millis * MILLIS_TO_NANOS)
    }

    #[inline]
    fn from_microseconds(micros: u64) -> Option<Self> {
        Self::from_nanoseconds(micros * MICROS_TO_NANOS)
    }
}
