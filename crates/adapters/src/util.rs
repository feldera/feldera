use std::borrow::Cow;

/// Returns the index of the first character following the last newline
/// in `data`.
pub(crate) fn split_on_newline(data: &[u8]) -> usize {
    let data_len = data.len();
    let index = data
        .iter()
        .rev()
        .position(|&x| x == b'\n')
        .unwrap_or(data_len);

    data_len - index
}

pub(crate) fn truncate_ellipse<'a>(s: &'a str, len: usize, ellipse: &str) -> Cow<'a, str> {
    if s.len() <= len {
        return Cow::Borrowed(s);
    } else if len == 0 {
        return Cow::Borrowed("");
    }

    let result = s.chars().take(len).chain(ellipse.chars()).collect();
    Cow::Owned(result)
}
