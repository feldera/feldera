//! Duration validation without runtime or DBSP dependencies.

use serde_json::Value;

const MAX_NANOS: u128 = (u64::MAX as u128) * 1_000_000_000 + 999_999_999;

pub(crate) fn valid(value: &Value) -> bool {
    match value {
        Value::String(text) => nanos(text).is_some(),
        Value::Number(number) => {
            if number.as_u64().is_some() {
                return true;
            }
            let text = number.to_string();
            if !text.contains(['.', 'e', 'E']) {
                return false;
            }
            number.as_f64().is_some_and(|n| {
                n.is_finite() && n >= 0.0 && n.fract() == 0.0 && n < 18446744073709551616.0
            })
        }
        _ => false,
    }
}

fn nanos(text: &str) -> Option<u128> {
    let mut rest = text.strip_prefix('+').unwrap_or(text);
    if rest == "0" {
        return Some(0);
    }
    if rest.is_empty() {
        return None;
    }
    let mut total = 0u128;
    while !rest.is_empty() {
        let unit_start = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        let (number, tail) = rest.split_at(unit_start);
        let unit_end = tail
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(tail.len());
        let (unit, tail) = tail.split_at(unit_end);
        let multiplier: u128 = match unit {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            "d" => 86_400_000_000_000,
            _ => return None,
        };
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if whole.is_empty() && fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let whole = if whole.is_empty() {
            0
        } else {
            whole.parse::<u128>().ok()?
        };
        let mut term = whole.checked_mul(multiplier)?;
        if !fraction.is_empty() {
            let kept = &fraction[..fraction.len().min(18)];
            let numerator: u128 = kept.parse().ok()?;
            let denominator = 10u128.pow(kept.len() as u32);
            term = term.checked_add((numerator * multiplier + denominator / 2) / denominator)?;
        }
        total = total.checked_add(term)?;
        if total > MAX_NANOS {
            return None;
        }
        rest = tail;
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::nanos;

    #[test]
    fn rounds_terms_and_checks_full_duration_range() {
        assert_eq!(nanos("0.5ns0.5ns"), Some(2));
        assert_eq!(
            nanos("18446744073709551615s999999999ns"),
            Some(super::MAX_NANOS)
        );
        assert_eq!(nanos("18446744073709551616s"), None);
        assert_eq!(nanos("1s0"), None);
        assert_eq!(nanos("1.2.3s"), None);
    }
}
