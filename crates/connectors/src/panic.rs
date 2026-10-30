use feldera_types::secret_redaction::secret_redactor;
use std::{
    any::Any,
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
                Some(message) => eprintln!("{message}"),
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

/// The text of `payload` with resolved secrets replaced, or `None` when it
/// is no text or holds none.
fn redacted_payload(payload: &(dyn Any + Send)) -> Option<String> {
    let text = payload
        .downcast_ref::<&str>()
        .map(|text| text.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())?;
    match secret_redactor().redact(&text) {
        Cow::Owned(redacted) => Some(redacted),
        Cow::Borrowed(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::panic::{N_PANICS, enable_counting_panics, redacted_payload};
    use feldera_types::secret_redaction::secret_redactor;
    use std::any::Any;

    /// A panic payload that copied a resolved secret shows its reference.
    #[test]
    fn panic_payload_shows_the_secret_reference() {
        secret_redactor().register("${secret:kubernetes:pg/uri}", "panic-test-secret-value");
        let payload: Box<dyn Any + Send> = Box::new("bad panic-test-secret-value".to_string());
        assert_eq!(
            redacted_payload(&*payload).as_deref(),
            Some("bad ${secret:kubernetes:pg/uri}")
        );
        let payload: Box<dyn Any + Send> = Box::new("no secret here");
        assert_eq!(redacted_payload(&*payload), None);
    }

    /// Test that [enable_counting_panics] actually increments [N_PANICS].
    ///
    /// This is set to be ignored because it races with proptest initialization:
    /// both [enable_counting_panics] and proptests set a panic hook, which is
    /// unavoidably racy.  See https://github.com/rust-lang/rust/issues/92649
    /// for an explanation of the race and a proposed way to avoid it (currently
    /// available only in nightly).
    #[test]
    #[ignore = "races with proptests"]
    fn test_counting_panics() {
        enable_counting_panics();
        let panics_before = N_PANICS.load(Ordering::Relaxed);
        std::thread::spawn(|| panic!()).join().unwrap_err();
        let panics_after = N_PANICS.load(Ordering::Relaxed);
        assert!(
            panics_after > panics_before,
            "{panics_after} should be greater than {panics_before}"
        );
    }
}
