use std::borrow::Cow;

pub(crate) fn truncate_ellipse<'a>(s: &'a str, len: usize, ellipse: &str) -> Cow<'a, str> {
    if s.len() <= len {
        return Cow::Borrowed(s);
    } else if len == 0 {
        return Cow::Borrowed("");
    }

    let result = s.chars().take(len).chain(ellipse.chars()).collect();
    Cow::Owned(result)
}
