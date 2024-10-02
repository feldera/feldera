use dashmap::DashMap;
use regex::Regex;
use std::sync::LazyLock;

static REGEXS: LazyLock<DashMap<String, Regex>> = LazyLock::new(|| DashMap::new());

pub fn re_extract(
    s: Option<String>,
    p: Option<String>,
    group: Option<i32>,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    Ok(do_re_extract(s, p, group))
}

fn do_re_extract(s: Option<String>, p: Option<String>, group: Option<i32>) -> Option<String> {
    let s = s?;
    let p = p?;
    let group = group?;

    let re = REGEXS
        .entry(p.clone())
        .or_try_insert_with(|| Regex::new(&p))
        .ok()?;

    Some(re.captures(&s)?.get(group as usize)?.as_str().to_string())
}
