//! Which browser shows a Samply profile.
//!
//! The Firefox Profiler web app runs in Firefox and in Chromium-based
//! browsers but not in Safari, so on a Mac whose default browser is Safari
//! the profile must be steered elsewhere. `BROWSER` in the environment wins,
//! then an installed Firefox, then a Chromium-based browser, then the
//! platform's default opener.

use std::path::{Path, PathBuf};

/// A command that opens a URL: the program and its leading arguments; the
/// URL goes last.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Browser {
    /// Short name for toasts, e.g. "Firefox" or "the default browser".
    pub label: String,
    pub command: Vec<String>,
}

impl Browser {
    /// The full argument vector that opens `url`.
    ///
    /// ```
    /// use f5a::browser::Browser;
    ///
    /// let browser = Browser {
    ///     label: "Firefox".to_string(),
    ///     command: vec!["open".to_string(), "-a".to_string(), "Firefox".to_string()],
    /// };
    /// assert_eq!(browser.argv("http://x"), vec!["open", "-a", "Firefox", "http://x"]);
    /// ```
    pub fn argv(&self, url: &str) -> Vec<String> {
        let mut argv = self.command.clone();
        argv.push(url.to_string());
        argv
    }
}

/// macOS application bundles, in order of preference.
const MAC_APPS: [&str; 7] = [
    "Firefox",
    "Firefox Developer Edition",
    "Firefox Nightly",
    "Google Chrome",
    "Chromium",
    "Brave Browser",
    "Microsoft Edge",
];

/// Executables on other Unixes, in order of preference.
const UNIX_PROGRAMS: [&str; 9] = [
    "firefox",
    "firefox-esr",
    "firefox-developer-edition",
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
    "brave-browser",
    "microsoft-edge",
];

/// Pick the browser from what the environment and the machine offer.
/// `has_mac_app` answers whether `/Applications/<name>.app` (or the user's
/// copy) exists; `on_path` whether an executable of that name is on `PATH`.
///
/// ```
/// use f5a::browser::pick_browser;
///
/// let firefox = pick_browser(None, true, |name| name == "Firefox", |_| false);
/// assert_eq!(firefox.command, vec!["open", "-a", "Firefox"]);
/// let chrome = pick_browser(None, false, |_| false, |name| name == "google-chrome");
/// assert_eq!(chrome.command, vec!["google-chrome"]);
/// let forced = pick_browser(Some("my-browser --new-window"), true, |_| true, |_| true);
/// assert_eq!(forced.command, vec!["my-browser", "--new-window"]);
/// let fallback = pick_browser(None, true, |_| false, |_| false);
/// assert_eq!(fallback.command, vec!["open"]);
/// ```
pub fn pick_browser(
    env_browser: Option<&str>,
    is_macos: bool,
    has_mac_app: impl Fn(&str) -> bool,
    on_path: impl Fn(&str) -> bool,
) -> Browser {
    if let Some(command) = env_browser.map(str::trim).filter(|value| !value.is_empty()) {
        let words: Vec<String> = command.split_whitespace().map(str::to_string).collect();
        return Browser {
            label: format!("$BROWSER ({})", words[0]),
            command: words,
        };
    }
    if is_macos {
        if let Some(app) = MAC_APPS.iter().find(|app| has_mac_app(app)) {
            return Browser {
                label: (*app).to_string(),
                command: vec!["open".to_string(), "-a".to_string(), (*app).to_string()],
            };
        }
        return Browser {
            label: "the default browser".to_string(),
            command: vec!["open".to_string()],
        };
    }
    if let Some(program) = UNIX_PROGRAMS.iter().find(|program| on_path(program)) {
        return Browser {
            label: (*program).to_string(),
            command: vec![(*program).to_string()],
        };
    }
    Browser {
        label: "the default browser".to_string(),
        command: vec!["xdg-open".to_string()],
    }
}

/// Whether a macOS application bundle is installed system-wide or for the user.
fn mac_app_installed(name: &str) -> bool {
    let bundle = format!("{name}.app");
    let system = Path::new("/Applications").join(&bundle);
    let user =
        std::env::var_os("HOME").map(|home| PathBuf::from(home).join("Applications").join(&bundle));
    system.is_dir() || user.is_some_and(|path| path.is_dir())
}

/// Whether an executable of this name sits in one of the `PATH` directories.
fn on_path(program: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|directory| directory.join(program).is_file())
    })
}

/// The browser this machine should open profiles with.
///
/// ```
/// use f5a::browser::detect_browser;
///
/// let browser = detect_browser();
/// assert!(!browser.command.is_empty());
/// ```
pub fn detect_browser() -> Browser {
    let env_browser = std::env::var("BROWSER").ok();
    pick_browser(
        env_browser.as_deref(),
        cfg!(target_os = "macos"),
        mac_app_installed,
        on_path,
    )
}

/// A command that shows a saved file in the desktop's file manager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Revealer {
    /// Short name for toasts, e.g. "Finder".
    pub label: String,
    /// The program and its leading arguments; the target goes last.
    pub command: Vec<String>,
    /// Whether the command wants the file's directory rather than the file:
    /// `open -R` selects a file, `xdg-open` can only open a folder.
    pub takes_directory: bool,
}

impl Revealer {
    /// The full argument vector that shows `path`.
    ///
    /// ```
    /// use f5a::browser::Revealer;
    ///
    /// let finder = Revealer {
    ///     label: "Finder".to_string(),
    ///     command: vec!["open".to_string(), "-R".to_string()],
    ///     takes_directory: false,
    /// };
    /// assert_eq!(finder.argv("/tmp/a.zip"), vec!["open", "-R", "/tmp/a.zip"]);
    /// let folder = Revealer {
    ///     label: "the file manager".to_string(),
    ///     command: vec!["xdg-open".to_string()],
    ///     takes_directory: true,
    /// };
    /// assert_eq!(folder.argv("/tmp/a.zip"), vec!["xdg-open", "/tmp"]);
    /// assert_eq!(folder.argv("a.zip"), vec!["xdg-open", "."]);
    /// ```
    pub fn argv(&self, path: &str) -> Vec<String> {
        let mut argv = self.command.clone();
        let target = if self.takes_directory {
            let parent = Path::new(path).parent().map(Path::to_path_buf);
            match parent {
                Some(directory) if !directory.as_os_str().is_empty() => {
                    directory.display().to_string()
                }
                _ => ".".to_string(),
            }
        } else {
            path.to_string()
        };
        argv.push(target);
        argv
    }
}

/// The file manager command for this platform: Finder's reveal on macOS,
/// the folder through `xdg-open` elsewhere.
///
/// ```
/// use f5a::browser::detect_revealer;
///
/// assert!(!detect_revealer().command.is_empty());
/// ```
pub fn detect_revealer() -> Revealer {
    if cfg!(target_os = "macos") {
        Revealer {
            label: "Finder".to_string(),
            command: vec!["open".to_string(), "-R".to_string()],
            takes_directory: false,
        }
    } else {
        Revealer {
            label: "the file manager".to_string(),
            command: vec!["xdg-open".to_string()],
            takes_directory: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Browser, pick_browser};

    #[test]
    fn firefox_beats_chrome_beats_the_default_opener_on_macos() {
        let both = pick_browser(None, true, |_| true, |_| false);
        assert_eq!(both.command, vec!["open", "-a", "Firefox"]);
        let chrome_only = pick_browser(None, true, |name| name == "Google Chrome", |_| false);
        assert_eq!(chrome_only.label, "Google Chrome");
        assert_eq!(chrome_only.command, vec!["open", "-a", "Google Chrome"]);
        let neither = pick_browser(None, true, |_| false, |_| true);
        assert_eq!(
            neither.command,
            vec!["open"],
            "PATH programs do not count on macOS"
        );
        assert_eq!(neither.label, "the default browser");
    }

    #[test]
    fn linux_prefers_path_programs_then_xdg_open() {
        let firefox = pick_browser(None, false, |_| true, |name| name == "firefox-esr");
        assert_eq!(firefox.command, vec!["firefox-esr"]);
        let brave = pick_browser(None, false, |_| false, |name| name == "brave-browser");
        assert_eq!(brave.command, vec!["brave-browser"]);
        let nothing = pick_browser(None, false, |_| false, |_| false);
        assert_eq!(nothing.command, vec!["xdg-open"]);
    }

    #[test]
    fn the_browser_variable_wins_and_blank_values_are_ignored() {
        let forced = pick_browser(Some("  chromium --incognito "), true, |_| true, |_| true);
        assert_eq!(forced.command, vec!["chromium", "--incognito"]);
        assert_eq!(forced.label, "$BROWSER (chromium)");
        let blank = pick_browser(Some("   "), true, |name| name == "Firefox", |_| false);
        assert_eq!(blank.command, vec!["open", "-a", "Firefox"]);
    }

    #[test]
    fn the_url_goes_last() {
        let browser = Browser {
            label: "x".to_string(),
            command: vec!["xdg-open".to_string()],
        };
        assert_eq!(
            browser.argv("http://127.0.0.1:3000/"),
            vec!["xdg-open", "http://127.0.0.1:3000/"]
        );
    }
}
