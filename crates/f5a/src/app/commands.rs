//! Parsing and completion for the `:` command bar.

use std::fmt;

/// A parsed command-bar line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Start {
        pipeline: Option<String>,
    },
    Pause {
        pipeline: Option<String>,
    },
    Resume {
        pipeline: Option<String>,
    },
    Stop {
        pipeline: Option<String>,
        force: bool,
    },
    Restart {
        pipeline: Option<String>,
    },
    Clear {
        pipeline: Option<String>,
    },
    Delete {
        pipeline: Option<String>,
    },
    Profile,
    Sql,
    /// Record a Samply CPU profile and save it once it is ready.
    Samply {
        duration_secs: u64,
    },
    /// Download the support bundle and save it.
    Bundle,
    /// Open the dialog that shapes the support bundle.
    BundleSettings,
    ConnectorPause,
    ConnectorResume,
    Tenant {
        name: Option<String>,
    },
    RefreshEvery {
        seconds: u64,
    },
    Filter {
        text: String,
    },
    Sort {
        column: String,
    },
    Help,
    Quit,
}

/// A concise parse error for the command bar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandError(pub String);

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CommandError {}

const COMMAND_NAMES: [&str; 21] = [
    "start",
    "pause",
    "resume",
    "stop",
    "force-stop",
    "restart",
    "clear",
    "delete",
    "profile",
    "sql",
    "samply",
    "bundle",
    "connector-pause",
    "connector-resume",
    "tenant",
    "refresh-every",
    "filter",
    "sort",
    "help",
    "quit",
    "q",
];

/// Complete a partial command name; returns the unique completion, if any.
pub fn complete(input: &str) -> Option<String> {
    let prefix = input.trim_start_matches(':').trim();
    if prefix.is_empty() || prefix.contains(' ') {
        return None;
    }
    let mut matches = COMMAND_NAMES
        .iter()
        .filter(|name| name.starts_with(prefix) && **name != "q");
    let first = matches.next()?;
    match matches.next() {
        None => Some((*first).to_string()),
        Some(_) => None,
    }
}

/// Parse a command-bar line. A leading colon is optional.
pub fn parse(input: &str) -> Result<Command, CommandError> {
    let trimmed = input.trim().trim_start_matches(':').trim();
    let mut words = trimmed.split_whitespace();
    let name = words.next().unwrap_or_default().to_ascii_lowercase();
    let first = words.next().map(str::to_string);
    if words.next().is_some() {
        return Err(CommandError("too many arguments".to_string()));
    }

    match name.as_str() {
        "start" => Ok(Command::Start { pipeline: first }),
        "pause" => Ok(Command::Pause { pipeline: first }),
        "resume" => Ok(Command::Resume { pipeline: first }),
        "stop" => Ok(Command::Stop {
            pipeline: first,
            force: false,
        }),
        "force-stop" | "fstop" => Ok(Command::Stop {
            pipeline: first,
            force: true,
        }),
        "restart" => Ok(Command::Restart { pipeline: first }),
        "clear" => Ok(Command::Clear { pipeline: first }),
        "delete" => Ok(Command::Delete { pipeline: first }),
        "profile" | "hotspots" => no_argument(first, Command::Profile),
        "sql" => no_argument(first, Command::Sql),
        "samply" => Ok(Command::Samply {
            duration_secs: seconds(first.as_deref(), 30, "samply")?,
        }),
        "bundle" | "support-bundle" => match first.as_deref() {
            None => Ok(Command::Bundle),
            Some("settings") => Ok(Command::BundleSettings),
            Some(_) => Err(CommandError("usage: bundle [settings]".to_string())),
        },
        "connector-pause" => no_argument(first, Command::ConnectorPause),
        "connector-resume" => no_argument(first, Command::ConnectorResume),
        "tenant" => Ok(Command::Tenant { name: first }),
        "refresh-every" => Ok(Command::RefreshEvery {
            seconds: seconds(first.as_deref(), 2, "refresh-every")?,
        }),
        "filter" => Ok(Command::Filter {
            text: first.unwrap_or_default(),
        }),
        "sort" => Ok(Command::Sort {
            column: first.ok_or_else(|| {
                CommandError("usage: sort <name|status|rps|records|memory|storage|age>".to_string())
            })?,
        }),
        "help" | "?" => no_argument(first, Command::Help),
        "quit" | "exit" | "q" => no_argument(first, Command::Quit),
        "" => Err(CommandError("type `help` for commands".to_string())),
        other => Err(CommandError(format!("unknown command `{other}`"))),
    }
}

fn no_argument(argument: Option<String>, command: Command) -> Result<Command, CommandError> {
    if argument.is_some() {
        return Err(CommandError("this command takes no arguments".to_string()));
    }
    Ok(command)
}

fn seconds(argument: Option<&str>, default_secs: u64, name: &str) -> Result<u64, CommandError> {
    let parsed = argument
        .unwrap_or(&default_secs.to_string())
        .parse::<u64>()
        .map_err(|_| CommandError(format!("{name} takes a duration in seconds")))?;
    if parsed == 0 {
        return Err(CommandError(format!("{name} needs a positive duration")));
    }
    // A day-long window is almost certainly a typo.
    if parsed > 86_400 {
        return Err(CommandError(format!("{name} is capped at 86400 seconds")));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::{Command, complete, parse};

    #[test]
    fn lifecycle_commands_accept_an_optional_pipeline() {
        assert_eq!(parse(":start"), Ok(Command::Start { pipeline: None }));
        assert_eq!(
            parse("start orders"),
            Ok(Command::Start {
                pipeline: Some("orders".to_string())
            })
        );
        assert_eq!(
            parse("force-stop orders"),
            Ok(Command::Stop {
                pipeline: Some("orders".to_string()),
                force: true
            })
        );
        assert_eq!(parse("restart"), Ok(Command::Restart { pipeline: None }));
        assert_eq!(
            parse("restart orders"),
            Ok(Command::Restart {
                pipeline: Some("orders".to_string())
            })
        );
        assert_eq!(parse("delete"), Ok(Command::Delete { pipeline: None }));
        assert_eq!(
            parse("delete old"),
            Ok(Command::Delete {
                pipeline: Some("old".to_string())
            })
        );
        assert_eq!(parse("pause"), Ok(Command::Pause { pipeline: None }));
        assert_eq!(parse("resume"), Ok(Command::Resume { pipeline: None }));
        assert_eq!(parse("clear"), Ok(Command::Clear { pipeline: None }));
    }

    #[test]
    fn durations_default_and_validate() {
        assert_eq!(parse("samply"), Ok(Command::Samply { duration_secs: 30 }));
        assert_eq!(parse("samply 5"), Ok(Command::Samply { duration_secs: 5 }));
        assert!(parse("samply 0").is_err());
        assert!(parse("samply forever").is_err());
        assert!(parse("samply 90000").is_err());
        assert!(parse("bench 10").is_err(), "the benchmark tab is gone");
    }

    #[test]
    fn view_and_admin_commands_parse() {
        assert_eq!(parse("profile"), Ok(Command::Profile));
        assert_eq!(parse("sql"), Ok(Command::Sql));
        assert_eq!(parse("bundle"), Ok(Command::Bundle));
        assert_eq!(parse("support-bundle"), Ok(Command::Bundle));
        assert_eq!(parse("bundle settings"), Ok(Command::BundleSettings));
        assert_eq!(
            parse("bundle now").unwrap_err().to_string(),
            "usage: bundle [settings]"
        );
        assert_eq!(parse("connector-pause"), Ok(Command::ConnectorPause));
        assert_eq!(parse("connector-resume"), Ok(Command::ConnectorResume));
        assert_eq!(
            parse("tenant acme"),
            Ok(Command::Tenant {
                name: Some("acme".to_string())
            })
        );
        assert_eq!(
            parse("refresh-every 5"),
            Ok(Command::RefreshEvery { seconds: 5 })
        );
        assert_eq!(
            parse("filter ord"),
            Ok(Command::Filter {
                text: "ord".to_string()
            })
        );
        assert_eq!(
            parse("sort rps"),
            Ok(Command::Sort {
                column: "rps".to_string()
            })
        );
        assert_eq!(parse("help"), Ok(Command::Help));
        assert_eq!(parse("q"), Ok(Command::Quit));
    }

    #[test]
    fn errors_stay_short_and_actionable() {
        assert_eq!(
            parse("").unwrap_err().to_string(),
            "type `help` for commands"
        );
        assert_eq!(
            parse("warp 9").unwrap_err().to_string(),
            "unknown command `warp`"
        );
        assert_eq!(
            parse("sql now").unwrap_err().to_string(),
            "this command takes no arguments"
        );
        assert_eq!(
            parse("start a b").unwrap_err().to_string(),
            "too many arguments"
        );
        assert!(parse("sort").is_err());
    }

    #[test]
    fn completion_requires_a_unique_prefix() {
        assert_eq!(complete("prof"), Some("profile".to_string()));
        assert_eq!(complete("rest"), Some("restart".to_string()));
        assert_eq!(complete("res"), None, "restart and resume share it");
        assert_eq!(complete("sam"), Some("samply".to_string()));
        assert_eq!(complete("bu"), Some("bundle".to_string()));
        assert_eq!(complete(":te"), Some("tenant".to_string()));
        assert_eq!(complete("s"), None);
        assert_eq!(complete(""), None);
        assert_eq!(complete("start ord"), None);
        assert_eq!(complete("qu"), Some("quit".to_string()));
    }
}
