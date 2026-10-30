//! A URL for logs and errors: shown without its credentials.

use url::Url;

/// Stands in for a value that can hold a credential.
pub const REDACTED: &str = "[REDACTED]";

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
pub fn text_without_url(text: &str, url: &str) -> String {
    if url.is_empty() || !text.contains(url) {
        return text.to_string();
    }
    text.replace(url, &url_without_credentials(url))
}

#[cfg(test)]
mod tests {
    use super::{REDACTED, text_without_url, url_without_credentials};

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
