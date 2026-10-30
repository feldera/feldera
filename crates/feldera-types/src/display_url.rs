//! A URL for logs and errors: shown without its credentials.

use url::Url;

/// Stands in for a value that can hold a credential.
pub const REDACTED: &str = "[REDACTED]";

/// `url` without userinfo and query string. Both can hold credentials: a
/// password, or the token of a signed URL.
///
/// A local path, or any text that holds neither, is returned as is. Text
/// that does not parse as a URL but holds one of them is replaced by
/// [`REDACTED`].
pub fn url_without_credentials(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.to_string()
        }
        Err(_) if !url.contains('@') && !url.contains('?') => url.to_string(),
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
