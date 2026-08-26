//! The pure application core: state, messages, and the reducer.

pub mod bundle_settings;
pub mod capture;
pub mod commands;
pub mod msg;
pub mod state;
pub mod update;

pub use msg::{Cmd, KeyPress, Msg};
pub use state::App;
pub use update::update;
