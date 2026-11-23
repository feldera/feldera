//! Convert Calcite timestamp-with-zone literals to runtime microseconds.
use chrono::{Datelike, Days, FixedOffset, NaiveDate, NaiveDateTime, Offset, TimeZone, Weekday};

/// Calcite stores local time and a zone name. Java resolves an overlap with
/// the earlier offset and moves a time in a spring gap forward by the gap.
pub(super) fn zoned_micros(value: &str) -> Option<i64> {
    let (time, zone) = value.rsplit_once(' ')?;
    let local = NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S%.f").ok()?;
    // java.util.TimeZone.toZoneId resolves the Java short IDs before consulting
    // the IANA database. Several names also exist in tzdata with different rules.
    let zone = match zone {
        "ACT" => "Australia/Darwin",
        "AET" => "Australia/Sydney",
        "AGT" => "America/Argentina/Buenos_Aires",
        "ART" => "Africa/Cairo",
        "AST" => "America/Anchorage",
        "BET" => "America/Sao_Paulo",
        "BST" => "Asia/Dhaka",
        "CAT" => "Africa/Harare",
        "CNT" => "America/St_Johns",
        "CST" => "America/Chicago",
        "CTT" => "Asia/Shanghai",
        "EAT" => "Africa/Addis_Ababa",
        "ECT" => "Europe/Paris",
        "EST" => "-05:00",
        "HST" => "-10:00",
        "IET" => "America/Indiana/Indianapolis",
        "IST" => "Asia/Kolkata",
        "JST" => "Asia/Tokyo",
        "MIT" => "Pacific/Apia",
        "MST" => "-07:00",
        "NET" => "Asia/Yerevan",
        "NST" => "Pacific/Auckland",
        "PLT" => "Asia/Karachi",
        "PNT" => "America/Phoenix",
        "PRT" => "America/Puerto_Rico",
        "PST" => "America/Los_Angeles",
        "SST" => "Pacific/Guadalcanal",
        "VST" => "Asia/Ho_Chi_Minh",
        name => name,
    };
    if let Ok(tz) = zone.parse::<chrono_tz::Tz>() {
        if let Some(instant) = tz.from_local_datetime(&local).earliest() {
            return Some(instant.timestamp_micros());
        }
        let (_, before) = chrono_tz::GapInfo::new(&local, &tz)?.begin?;
        return before
            .fix()
            .from_local_datetime(&local)
            .single()
            .map(|t| t.timestamp_micros());
    }
    let offset = if let Some(id) = zone.strip_prefix("SystemV/") {
        system_v_offset(id, local)?
    } else if let Some(value) = zone.strip_prefix("GMT") {
        custom_gmt_offset(value)?
    } else {
        zone.parse::<FixedOffset>().ok()?
    };
    Some(
        offset
            .from_local_datetime(&local)
            .single()?
            .timestamp_micros(),
    )
}

/// java.util.TimeZone accepts H, HH, HMM, HHMM and H:MM after the sign.
fn custom_gmt_offset(value: &str) -> Option<FixedOffset> {
    let (sign, digits) = value.split_at_checked(1)?;
    let (hours, minutes) = if let Some((h, m)) = digits.split_once(':') {
        if h.len() > 2 || m.len() != 2 {
            return None;
        }
        (h, m)
    } else if digits.len() <= 2 {
        (digits, "00")
    } else {
        digits.split_at_checked(digits.len() - 2)?
    };
    if !hours.bytes().all(|c| c.is_ascii_digit()) || !minutes.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let hours: u32 = hours.parse().ok()?;
    let minutes: u32 = minutes.parse().ok()?;
    // java.time.ZoneOffset, used by getMicros, accepts at most 18 hours.
    if minutes >= 60 || hours > 18 || (hours == 18 && minutes != 0) {
        return None;
    }
    let seconds = (hours * 3600 + minutes * 60) as i32;
    match sign {
        "+" => FixedOffset::east_opt(seconds),
        "-" => FixedOffset::west_opt(seconds),
        _ => None,
    }
}

/// The JDK retains the System V names that were removed from IANA tzdata.
/// Their daylight rules differ from the contemporary US and Canadian zones.
fn system_v_offset(id: &str, local: NaiveDateTime) -> Option<FixedOffset> {
    let west = match id {
        "AST4" | "AST4ADT" => 4,
        "EST5" | "EST5EDT" => 5,
        "CST6" | "CST6CDT" => 6,
        "MST7" | "MST7MDT" => 7,
        "PST8" | "PST8PDT" => 8,
        "YST9" | "YST9YDT" => 9,
        "HST10" => 10,
        _ => return None,
    };
    let year = local.year();
    if !id.ends_with("DT") || year < 1900 {
        return FixedOffset::west_opt(west * 3600);
    }
    let sunday_on_or_after = |month, day, hour| {
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        let days = Weekday::Sun.days_since(date.weekday());
        date.checked_add_days(Days::new(days.into()))?
            .and_hms_opt(hour, 0, 0)
    };
    // atZone uses the pre-transition offset during a gap or overlap. Spring
    // local times use standard time until 03:00, autumn times use DST until 02:00.
    let (start_month, start_day) = match year {
        1974 => (1, 6),
        1975 => (2, 23),
        _ => (4, 24),
    };
    let (end_month, end_day) = if year == 1974 { (11, 24) } else { (10, 25) };
    let start = sunday_on_or_after(start_month, start_day, 3)?;
    let end = sunday_on_or_after(end_month, end_day, 2)?;
    let daylight = (start..end).contains(&local);
    FixedOffset::west_opt((west - i32::from(daylight)) * 3600)
}

#[cfg(test)]
mod tests {
    use super::zoned_micros;

    #[test]
    fn canonical_timestamp_handles_offsets_gaps_and_overlaps() {
        for (local, utc) in [
            (
                "2020-01-01 08:10:10.1234569 America/New_York",
                "2020-01-01 13:10:10.123456 UTC",
            ),
            (
                "2024-03-10 02:30:00 America/New_York",
                "2024-03-10 07:30:00 UTC",
            ),
            (
                "2024-11-03 01:30:00 America/New_York",
                "2024-11-03 05:30:00 UTC",
            ),
            ("2020-01-01 05:30:00 GMT+05:30", "2020-01-01 00:00:00 UTC"),
            (
                "1974-01-06 02:30:00 SystemV/EST5EDT",
                "1974-01-06 07:30:00 UTC",
            ),
        ] {
            let expected = zoned_micros(utc).unwrap();
            assert_eq!(zoned_micros(local), Some(expected));
        }
    }
}
