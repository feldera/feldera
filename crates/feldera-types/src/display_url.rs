//! A URL for logs and errors: shown without its credentials.

use url::Url;

/// Stands in for a value that can hold a credential.
pub const REDACTED: &str = "[REDACTED]";

/// Debug output of named values as `{name: "[REDACTED]"}`: it shows that each
/// value exists, and hides it.
pub struct HiddenValues<'a>(pub Vec<&'a str>);

impl std::fmt::Debug for HiddenValues<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.0.iter().map(|name| (name, REDACTED)))
            .finish()
    }
}

/// Schemes whose username is a container name, not a credential.
const SCHEMES_WITH_CONTAINER_USERNAME: [&str; 4] = ["abfs", "abfss", "wasb", "wasbs"];

/// `url` without userinfo, query string and fragment. All three can hold
/// credentials: a password, a token used as the username, or the token of
/// a signed URL.
///
/// A local path, or any text that holds none of them, is returned as is.
/// Text that holds one of them but does not parse as a URL with a host,
/// such as `user:pw@host/db`, is replaced by [`REDACTED`].
pub fn url_without_credentials(url: &str) -> String {
    match Url::parse(url) {
        Ok(parsed) if parsed.cannot_be_a_base() && url.contains('@') => REDACTED.to_string(),
        Ok(mut parsed) => {
            // An Azure URL names the container in the username.
            let keep_username = parsed.password().is_none()
                && SCHEMES_WITH_CONTAINER_USERNAME.contains(&parsed.scheme());
            if !keep_username {
                let _ = parsed.set_username("");
            }
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) if !url.contains('@') && !url.contains('?') && !url.contains('#') => url.to_string(),
        Err(_) => REDACTED.to_string(),
    }
}

/// `text` with every copy of `url` replaced by [`url_without_credentials`],
/// for the message of a library error that repeats the URL it was given.
/// A library can also print the URL after parsing it, with the path
/// normalized and the credentials kept, so the canonical form, the userinfo
/// and the query string go on their own as well.
pub fn text_without_url(text: &str, url: &str) -> String {
    if url.is_empty() {
        return text.to_string();
    }
    let safe = url_without_credentials(url);
    let mut text = text.replace(url, &safe);
    let Ok(parsed) = Url::parse(url) else {
        return text;
    };
    text = text.replace(parsed.as_str(), &safe);
    let mut parts = Vec::new();
    if let Some(query) = parsed.query() {
        parts.push(format!("?{query}"));
    }
    if let Some(fragment) = parsed.fragment() {
        parts.push(format!("#{fragment}"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        parts.push(match parsed.password() {
            Some(password) => format!("{}:{password}@", parsed.username()),
            None => format!("{}@", parsed.username()),
        });
    }
    // The text as written can differ from the parsed form in its encoding.
    if let Some((_, rest)) = url.split_once("://") {
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        if let Some(at) = rest[..authority_end].rfind('@') {
            parts.push(rest[..=at].to_string());
        }
        if let Some(query) = rest.find('?') {
            let end = rest[query..].find('#').map_or(rest.len(), |i| query + i);
            parts.push(rest[query..end].to_string());
        }
    }
    for part in parts {
        if part.len() > 1 {
            text = text.replace(&part, "");
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::{REDACTED, text_without_url, url_without_credentials};

    /// A library can print the URL after normalizing its path; the
    /// credentials and the query string still go.
    #[test]
    fn a_normalized_copy_of_the_url_is_covered() {
        let url = "s3://ak:secret-key@bucket/table?X-Amz-Signature=sig";
        assert_eq!(
            text_without_url(
                "open s3://ak:secret-key@bucket/table/?X-Amz-Signature=sig failed",
                url
            ),
            "open s3://bucket/table/ failed"
        );
    }

    #[test]
    fn credentials_and_query_string_are_left_out() {
        assert_eq!(
            url_without_credentials("s3://ak:secret-key@bucket/table?X-Amz-Signature=sig"),
            "s3://bucket/table"
        );
        assert_eq!(
            url_without_credentials("postgres://u:pw@db.internal:5432/app"),
            "postgres://db.internal:5432/app"
        );
        assert_eq!(url_without_credentials("/tmp/table"), "/tmp/table");
        assert_eq!(url_without_credentials("not a url?token=secret"), REDACTED);
    }

    /// A string without a scheme parses as an opaque URL whose "scheme" is
    /// the username, so its password cannot be removed; a fragment can
    /// carry a token; a token can be the username of a Git or HTTP URL.
    #[test]
    fn opaque_urls_fragments_and_username_tokens_are_left_out() {
        assert_eq!(url_without_credentials("user:pw@host/db"), REDACTED);
        assert_eq!(url_without_credentials("user:pw@host:5432"), REDACTED);
        assert_eq!(
            url_without_credentials("https://h/p#token=abc"),
            "https://h/p"
        );
        assert_eq!(
            url_without_credentials("https://ghp_token@github.com/org/repo.git"),
            "https://github.com/org/repo.git"
        );
    }

    /// Azure URLs name the container in the username; it is no credential.
    #[test]
    fn azure_container_names_stay() {
        assert_eq!(
            url_without_credentials("abfss://container@acct.dfs.core.windows.net/t"),
            "abfss://container@acct.dfs.core.windows.net/t"
        );
        assert_eq!(
            url_without_credentials("wasbs://container:key@acct.blob.core.windows.net/t"),
            "wasbs://acct.blob.core.windows.net/t"
        );
    }

    #[test]
    fn a_library_error_that_repeats_the_url_loses_the_credentials() {
        let url = "s3://ak:secret-key@bucket/table?X-Amz-Signature=sig";
        assert_eq!(
            text_without_url(&format!("Could not parse {url} as a URL: {url}"), url),
            "Could not parse s3://bucket/table as a URL: s3://bucket/table"
        );
        assert_eq!(text_without_url("no url here", url), "no url here");
    }
}
