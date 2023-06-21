use dbsp::algebra::Decimal;

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
