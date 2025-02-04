use dashmap::DashMap;
use regex::Regex;
use std::sync::LazyLock;
use feldera_sqllib::*;

static REGEXS: LazyLock<DashMap<SqlString, Regex>> = LazyLock::new(|| DashMap::new());

pub fn re_extract(
    s: Option<SqlString>,
    p: Option<SqlString>,
    group: Option<i32>,
) -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
    Ok(do_re_extract(s, p, group))
}

fn do_re_extract(s: Option<SqlString>, p: Option<SqlString>, group: Option<i32>) -> Option<SqlString> {
    let s = s?;
    let p = p?;
    let group = group?;

    let re = REGEXS
        .entry(p.clone())
        .or_try_insert_with(|| Regex::new(p.str()))
        .ok()?;

    Some(SqlString::from_ref(re.captures(s.str())?.get(group as usize)?.as_str()))
}
