//! Explicit experimental evaluation choices. These are not a runtime compatibility
//! handshake: the target's supported registry and platform envelope still need
//! independent validation before production use.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SemanticsProfile {
    #[default]
    Runtime,
    /// Preserve the measured Java EXP and two-argument LOG kernels using a
    /// pinned pure-Rust implementation. Other operations keep runtime behavior.
    LegacyExpLog,
    /// The measured Janino envelope: legacy EXP/LOG kernels, LOG domain checks,
    /// case-sensitive nonfinite float spellings and padded timestamp fields.
    /// This is not a complete implementation of Java's parsers or SQL runtime.
    LegacyJanino,
}

impl SemanticsProfile {
    pub const fn identity(self) -> &'static str {
        match self {
            Self::Runtime => "runtime-v1",
            Self::LegacyExpLog => "legacy-exp-log-libm-0.2.15-v1",
            Self::LegacyJanino => "legacy-janino-v1",
        }
    }
}

impl std::str::FromStr for SemanticsProfile {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "runtime-v1" => Ok(Self::Runtime),
            "legacy-exp-log-libm-0.2.15-v1" => Ok(Self::LegacyExpLog),
            "legacy-janino-v1" => Ok(Self::LegacyJanino),
            _ => Err("unknown fold semantics profile"),
        }
    }
}
