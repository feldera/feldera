//! This RFC 3339 parser is adapted from the `chrono` crate. The only change is that we make the
//! timezone component optional, allowing, e.g., `2025-11-15T16:39:57` and `2025-11-15 16:39:57`
//! timestamps. Unfortunately, this simple change required copying a bunch of `chrono`-internal code.
//!
//! Since the chrono error type cannot be instantiated outside of the `chrono` crate, we changed
//! the implementation to return a `String` error message instead. This gave us the ability to
//! return more detailed error messages.

use chrono::{format::Parsed, DateTime, FixedOffset};

/// Parses an RFC 3339 date-and-time string into a `DateTime<FixedOffset>` value.
///
/// Parses all valid RFC 3339 values (as well as the subset of valid ISO 8601 values that are
/// also valid RFC 3339 date-and-time values) and returns a new [`DateTime`] with a
/// [`FixedOffset`] corresponding to the parsed timezone. While RFC 3339 values come in a wide
/// variety of shapes and sizes, `1996-12-19T16:39:57-08:00` is an example of the most commonly
/// encountered variety of RFC 3339 formats.
///
/// Why isn't this named `parse_from_iso8601`? That's because ISO 8601 allows representing
/// values in a wide range of formats, only some of which represent actual date-and-time
/// instances (rather than periods, ranges, dates, or times). Some valid ISO 8601 values are
/// also simultaneously valid RFC 3339 values, but not all RFC 3339 values are valid ISO 8601
/// values (or the other way around).
pub(crate) fn parse_timestamp_from_rfc3339(s: &str) -> Result<DateTime<FixedOffset>, String> {
    let mut parsed = Parsed::new();
    let (s, _) = parse_rfc3339(&mut parsed, s)?;
    if !s.is_empty() {
        return Err(format!(
            "RFC3339 string is too long; trailing characters: {s}"
        ));
    }
    parsed.to_datetime().map_err(|e| e.to_string())
}

pub(crate) fn parse_rfc3339<'a>(
    parsed: &mut Parsed,
    mut s: &'a str,
) -> Result<(&'a str, ()), String> {
    macro_rules! try_consume {
        ($e:expr) => {{
            let (s_, v) = $e.map_err(|e| e.to_string())?;
            s = s_;
            v
        }};
    }

    // an adapted RFC 3339 syntax from Section 5.6:
    //
    // date-fullyear  = 4DIGIT
    // date-month     = 2DIGIT ; 01-12
    // date-mday      = 2DIGIT ; 01-28, 01-29, 01-30, 01-31 based on month/year
    // time-hour      = 2DIGIT ; 00-23
    // time-minute    = 2DIGIT ; 00-59
    // time-second    = 2DIGIT ; 00-58, 00-59, 00-60 based on leap second rules
    // time-secfrac   = "." 1*DIGIT
    // time-numoffset = ("+" / "-") time-hour ":" time-minute
    // time-offset    = "Z" / time-numoffset
    // partial-time   = time-hour ":" time-minute ":" time-second [time-secfrac]
    // full-date      = date-fullyear "-" date-month "-" date-mday
    // full-time      = partial-time time-offset
    // date-time      = full-date "T" full-time
    //
    // some notes:
    //
    // - quoted characters can be in any mixture of lower and upper cases.
    //
    // - it may accept any number of fractional digits for seconds.
    //   for Chrono, this means that we should skip digits past first 9 digits.
    //
    // - unlike RFC 2822, the valid offset ranges from -23:59 to +23:59.
    //   note that this restriction is unique to RFC 3339 and not ISO 8601.
    //   since this is not a typical Chrono behavior, we check it earlier.
    //
    // - For readability a full-date and a full-time may be separated by a space character.

    parsed
        .set_year(try_consume!(scan::number(s, 4, 4)))
        .map_err(|e| e.to_string())?;
    s = scan::char(s, b'-')?;
    parsed
        .set_month(try_consume!(scan::number(s, 2, 2)))
        .map_err(|e| e.to_string())?;
    s = scan::char(s, b'-')?;
    parsed
        .set_day(try_consume!(scan::number(s, 2, 2)))
        .map_err(|e| e.to_string())?;

    s = match s.as_bytes().first() {
        Some(&b't' | &b'T' | &b' ') => &s[1..],
        Some(c) => return Err(format!("invalid time designator '{}' in RFC3339 string; supported designators are 't', 'T', and ' '", *c as char)),
        None => return Err("Expected time designator 't', 'T', or ' ', but found end of string".to_string()),
    };

    parsed
        .set_hour(try_consume!(scan::number(s, 2, 2)))
        .map_err(|e| e.to_string())?;
    s = scan::char(s, b':')?;
    parsed
        .set_minute(try_consume!(scan::number(s, 2, 2)))
        .map_err(|e| e.to_string())?;
    s = scan::char(s, b':')?;
    parsed
        .set_second(try_consume!(scan::number(s, 2, 2)))
        .map_err(|e| e.to_string())?;
    if s.starts_with('.') {
        let nanosecond = try_consume!(scan::nanosecond(&s[1..]));
        parsed
            .set_nanosecond(nanosecond)
            .map_err(|e| e.to_string())?;
    }

    let offset = try_consume!(scan::timezone_offset(s, |s| scan::char(s, b':'),));
    // This range check is similar to the one in `FixedOffset::east_opt`, so it would be redundant.
    // But it is possible to read the offset directly from `Parsed`. We want to only successfully
    // populate `Parsed` if the input is fully valid RFC 3339.
    // Max for the hours field is `23`, and for the minutes field `59`.
    const MAX_RFC3339_OFFSET: i32 = (23 * 60 + 59) * 60;
    if !(-MAX_RFC3339_OFFSET..=MAX_RFC3339_OFFSET).contains(&offset) {
        return Err(format!(
            "offset {offset} is out of range; expected range is -{MAX_RFC3339_OFFSET}..={MAX_RFC3339_OFFSET}"
        ));
    }
    parsed
        .set_offset(i64::from(offset))
        .map_err(|e| e.to_string())?;

    Ok((s, ()))
}

mod scan {
    /// Tries to parse the non-negative number from `min` to `max` digits.
    ///
    /// The absence of digits at all is an unconditional error.
    /// More than `max` digits are consumed up to the first `max` digits.
    /// Any number that does not fit in `i64` is an error.
    #[inline]
    pub(super) fn number(s: &str, min: usize, max: usize) -> Result<(&str, i64), String> {
        assert!(min <= max);

        // We are only interested in ascii numbers, so we can work with the `str` as bytes. We stop on
        // the first non-numeric byte, which may be another ascii character or beginning of multi-byte
        // UTF-8 character.
        let bytes = s.as_bytes();
        if bytes.len() < min {
            return Err(format!(
                "expected at least {min} digits, but found {}",
                bytes.len()
            ));
        }

        let mut n = 0i64;
        for (i, c) in bytes.iter().take(max).cloned().enumerate() {
            // cloned() = copied()
            if !c.is_ascii_digit() {
                if i < min {
                    return Err(format!(
                        "invalid character '{}' in timestamp string",
                        c as char
                    ));
                } else {
                    return Ok((&s[i..], n));
                }
            }

            n = match n
                .checked_mul(10)
                .and_then(|n| n.checked_add((c - b'0') as i64))
            {
                Some(n) => n,
                None => return Err("invalid timestamp string".to_string()),
            };
        }

        Ok((&s[core::cmp::min(max, bytes.len())..], n))
    }

    /// Tries to consume exactly one given character.
    pub(super) fn char(s: &str, c1: u8) -> Result<&str, String> {
        match s.as_bytes().first() {
            Some(&c) if c == c1 => Ok(&s[1..]),
            Some(c) => Err(format!(
                "expected character '{}', but found '{}'",
                c1 as char, *c as char
            )),
            None => Err(format!(
                "expected character '{}', but found end of string",
                c1 as char
            )),
        }
    }

    /// Tries to consume at least one digits as a fractional second.
    /// Returns the number of whole nanoseconds (0--999,999,999).
    pub(super) fn nanosecond(s: &str) -> Result<(&str, i64), String> {
        // record the number of digits consumed for later scaling.
        let origlen = s.len();
        let (s, v) = number(s, 1, 9)?;
        let consumed = origlen - s.len();

        // scale the number accordingly.
        static SCALE: [i64; 10] = [
            0,
            100_000_000,
            10_000_000,
            1_000_000,
            100_000,
            10_000,
            1_000,
            100,
            10,
            1,
        ];
        let v = v
            .checked_mul(SCALE[consumed])
            .ok_or(format!("fractional second value {v} is out of range"))?;

        // if there are more than 9 digits, skip next digits.
        let s = s.trim_start_matches(|c: char| c.is_ascii_digit());

        Ok((s, v))
    }

    /// Parse a timezone from `s` and return the offset in seconds.
    ///
    /// The `consume_colon` function is used to parse a mandatory or optional `:`
    /// separator between hours offset and minutes offset.
    ///
    /// The `allow_missing_minutes` flag allows the timezone minutes offset to be
    /// missing from `s`.
    ///
    /// The `allow_tz_minus_sign` flag allows the timezone offset negative character
    /// to also be `−` MINUS SIGN (U+2212) in addition to the typical
    /// ASCII-compatible `-` HYPHEN-MINUS (U+2D).
    /// This is part of [RFC 3339 & ISO 8601].
    ///
    /// [RFC 3339 & ISO 8601]: https://en.wikipedia.org/w/index.php?title=ISO_8601&oldid=1114309368#Time_offsets_from_UTC
    pub(crate) fn timezone_offset<F>(
        mut s: &str,
        mut consume_colon: F,
    ) -> Result<(&str, i32), String>
    where
        F: FnMut(&str) -> Result<&str, String>,
    {
        match s.as_bytes().first() {
            Some(&b'Z' | &b'z') => return Ok((&s[1..], 0)),
            None => return Ok((s, 0)),
            _ => (),
        }

        fn digits(s: &str, descr: &str) -> Result<(u8, u8), String> {
            let b = s.as_bytes();
            if b.len() < 2 {
                Err(format!(
                    "invalid timezone offset spec: expected at least 2 digits for {descr}, but found {}",
                    b.len()
                ))
            } else {
                Ok((b[0], b[1]))
            }
        }
        let negative = match s.chars().next() {
            Some('+') => {
                // PLUS SIGN (U+2B)
                s = &s['+'.len_utf8()..];

                false
            }
            Some('-') => {
                // HYPHEN-MINUS (U+2D)
                s = &s['-'.len_utf8()..];

                true
            }
            Some('−') => {
                s = &s['−'.len_utf8()..];

                true
            }
            Some(c) => {
                return Err(format!(
                    "expected '+' or '-', but found '{c}' in timezone offset string"
                ))
            }
            None => return Err("expected '+' or '-', but found end of string".to_string()),
        };

        // hours (00--99)
        let hours = match digits(s, "hours")? {
            (h1 @ b'0'..=b'9', h2 @ b'0'..=b'9') => i32::from((h1 - b'0') * 10 + (h2 - b'0')),
            (h1, h2) => {
                return Err(format!(
                    "invalid timezone offset hours: '{}{}'",
                    h1 as char, h2 as char
                ))
            }
        };
        s = &s[2..];

        // colons (and possibly other separators)
        s = consume_colon(s)?;

        // minutes (00--59)
        // if the next two items are digits then we have to add minutes
        let minutes = if let Ok(ds) = digits(s, "minutes") {
            match ds {
                (m1 @ b'0'..=b'5', m2 @ b'0'..=b'9') => i32::from((m1 - b'0') * 10 + (m2 - b'0')),
                (m1 @ b'6'..=b'9', m2 @ b'0'..=b'9') => {
                    return Err(format!(
                        "invalid timezone offset minutes: '{}{}' (expected 00-59)",
                        m1 as char, m2 as char
                    ))
                }
                (m1, m2) => {
                    return Err(format!(
                        "invalid timezone offset minutes: '{}{}'",
                        m1 as char, m2 as char
                    ))
                }
            }
        } else {
            0
        };
        s = match s.len() {
            len if len >= 2 => &s[2..],
            0 => s,
            _ => {
                return Err(format!(
                    "invalid timezone offset seconds: expected at least 2 digits for seconds, but found {}",
                    s.len()
                ))
            }
        };

        let seconds = hours * 3600 + minutes * 60;
        Ok((s, if negative { -seconds } else { seconds }))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_timestamp_from_rfc3339() {
        let timestamp = parse_timestamp_from_rfc3339("2025-11-15T16:39:57+00:00").unwrap();
        assert_eq!(
            timestamp,
            DateTime::parse_from_rfc3339("2025-11-15T16:39:57+00:00").unwrap()
        );

        let timestamp = parse_timestamp_from_rfc3339("2025-11-15 16:39:57+00:00").unwrap();
        assert_eq!(
            timestamp,
            DateTime::parse_from_rfc3339("2025-11-15T16:39:57+00:00").unwrap()
        );

        let timestamp = parse_timestamp_from_rfc3339("2025-11-15 16:39:57").unwrap();
        assert_eq!(
            timestamp,
            DateTime::parse_from_rfc3339("2025-11-15T16:39:57+00:00").unwrap()
        );

        let timestamp = parse_timestamp_from_rfc3339("2025-11-15 16:39:57+02:00").unwrap();
        assert_eq!(
            timestamp,
            DateTime::parse_from_rfc3339("2025-11-15T16:39:57+02:00").unwrap()
        );

        let result = parse_timestamp_from_rfc3339("2025-11-15 16:39");
        assert_eq!(
            result,
            Err("expected character ':', but found end of string".to_string())
        );

        let result = parse_timestamp_from_rfc3339("2025-11-15 16:39:9");
        assert_eq!(
            result,
            Err("expected at least 2 digits, but found 1".to_string())
        );

        let result = parse_timestamp_from_rfc3339("2025-11-15 16:39:99");
        assert_eq!(result, Err("input is out of range".to_string()));

        let result = parse_timestamp_from_rfc3339("2025-11-15q16:39:39");
        assert_eq!(result, Err("invalid time designator 'q' in RFC3339 string; supported designators are 't', 'T', and ' '".to_string()));
    }
}
