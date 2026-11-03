use feldera_types::secret_redaction::secret_redactor;
use std::{
    any::Any,
    backtrace::{Backtrace, BacktraceStatus},
    borrow::Cow,
    panic::{PanicHookInfo, set_hook, take_hook},
    sync::{
        Once,
        atomic::{AtomicU64, Ordering},
    },
};

/// Number of times the process has panicked.
pub static N_PANICS: AtomicU64 = AtomicU64::new(0);

pub fn enable_counting_panics() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let prev = take_hook();
        set_hook(Box::new(move |info| {
            N_PANICS.fetch_add(1, Ordering::Relaxed);
            // A payload that copied a resolved secret is printed as its
            // reference; any other payload keeps the default printer.
            match redacted_panic_message(info) {
                Some(message) => eprintln!("{message}\n{}", backtrace_note()),
                None => prev(info),
            }
        }));
    });
}

/// The panic message with resolved secrets replaced, or `None` when the
/// payload holds none.
fn redacted_panic_message(info: &PanicHookInfo<'_>) -> Option<String> {
    let payload = redacted_payload(info.payload())?;
    let location = info
        .location()
        .map(|l| format!(" at {}:{}:{}", l.file(), l.line(), l.column()))
        .unwrap_or_default();
    let thread = std::thread::current();
    Some(format!(
        "thread '{}' panicked{location}:\n{payload}",
        thread.name().unwrap_or("<unnamed>")
    ))
}

/// What the default hook prints after the message: the backtrace when
/// `RUST_BACKTRACE` asks for one, else the note that says how to get it.
/// The frames hold no payload, so they are safe to print.
fn backtrace_note() -> String {
    let backtrace = Backtrace::capture();
    match backtrace.status() {
        BacktraceStatus::Captured => format!("stack backtrace:\n{backtrace}"),
        _ => "note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"
            .to_string(),
    }
}

/// The text of `payload` with resolved secrets replaced, or `None` when it
/// is no text or holds none.
fn redacted_payload(payload: &(dyn Any + Send)) -> Option<String> {
    let text = payload
        .downcast_ref::<&str>()
        .map(|text| text.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())?;
    match secret_redactor().redact_for_panic(&text) {
        Cow::Owned(redacted) => Some(redacted),
        Cow::Borrowed(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::panic::{N_PANICS, backtrace_note, enable_counting_panics, redacted_payload};
    use feldera_types::secret_redaction::secret_redactor;
    use std::any::Any;

    /// Panic redaction can legitimately omit a whole payload when another
    /// thread registers a secret. Isolate tests that require an idle registry;
    /// this also keeps hook installation separate from proptest's hooks.
    fn child_output(test_name: &str) -> Option<std::process::Output> {
        const CHILD: &str = "FELDERA_TEST_PANIC_CHILD";
        if std::env::var(CHILD).as_deref() == Ok(test_name) {
            return None;
        }
        Some(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", test_name, "--nocapture"])
                .env(CHILD, test_name)
                .env("RUST_BACKTRACE", "0")
                .output()
                .unwrap(),
        )
    }

    /// A panic payload that copied a resolved secret shows its reference.
    #[test]
    fn panic_payload_shows_the_secret_reference() {
        if let Some(output) = child_output("panic::tests::panic_payload_shows_the_secret_reference")
        {
            assert!(output.status.success(), "{output:?}");
            return;
        }
        secret_redactor().register("${secret:kubernetes:pg/uri}", "panic-test-secret-value");
        let payload: Box<dyn Any + Send> = Box::new("bad panic-test-secret-value".to_string());
        assert_eq!(
            redacted_payload(&*payload).as_deref(),
            Some("bad ${secret:kubernetes:pg/uri}")
        );
        let payload: Box<dyn Any + Send> = Box::new("no secret here");
        assert_eq!(redacted_payload(&*payload), None);
    }

    /// A redacted panic keeps what the default hook prints after the
    /// message: the backtrace, or the note on how to enable it.
    #[test]
    fn redacted_panic_keeps_the_backtrace_or_its_note() {
        let note = backtrace_note();
        assert!(
            note.starts_with("stack backtrace:\n") || note.contains("RUST_BACKTRACE=1"),
            "{note}"
        );
    }

    #[test]
    fn counting_hook_redacts_the_payload() {
        if let Some(output) = child_output("panic::tests::counting_hook_redacts_the_payload") {
            assert!(output.status.success(), "{output:?}");
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(
                stderr.contains("${secret:kubernetes:panic/key}"),
                "{stderr}"
            );
            assert!(!stderr.contains("hook-secret-value-2136"), "{stderr}");
            assert!(stderr.contains("RUST_BACKTRACE=1"), "{stderr}");
            return;
        }
        enable_counting_panics();
        let before = N_PANICS.load(Ordering::Relaxed);
        secret_redactor().register("${secret:kubernetes:panic/key}", "hook-secret-value-2136");
        let result = std::panic::catch_unwind(|| panic!("failure hook-secret-value-2136"));
        assert!(result.is_err());
        assert_eq!(N_PANICS.load(Ordering::Relaxed), before + 1);
    }
}
