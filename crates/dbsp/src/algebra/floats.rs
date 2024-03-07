use ordered_float::OrderedFloat;

pub type F32 = OrderedFloat<f32>;
pub type F64 = OrderedFloat<f64>;

#[cfg(test)]
mod tests {
    use rkyv::Deserialize;

    use super::{F32, F64};
    use std::str::FromStr;

    #[test]
    fn fromstr() {
        assert_eq!(Ok(F32::from(10.0)), F32::from_str("10"));
        assert_eq!(Ok(F64::from(-10.0)), F64::from_str("-10"));
        assert!(F32::from_str("what").is_err());
    }

    #[test]
    fn f64_decode_encode() {
        for input in [
            F64::from(-1.0),
            F64::from(0.0),
            F64::from(1.0),
            F64::from(f64::MAX),
            F64::from(f64::MIN),
            F64::from(f64::NAN),
            F64::from(f64::INFINITY),
        ]
        .into_iter()
        {
            let encoded = rkyv::to_bytes::<_, 256>(&input).unwrap();
            let archived = unsafe { rkyv::archived_root::<F64>(&encoded[..]) };
            let decoded: F64 = archived.deserialize(&mut rkyv::Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }

    #[test]
    fn f32_decode_encode() {
        for input in [
            F32::from(-1.0),
            F32::from(0.0),
            F32::from(1.0),
            F32::from(f32::MAX),
            F32::from(f32::MIN),
            F32::from(f32::NAN),
            F32::from(f32::INFINITY),
        ]
        .into_iter()
        {
            let encoded = rkyv::to_bytes::<_, 256>(&input).unwrap();
            let archived = unsafe { rkyv::archived_root::<F32>(&encoded[..]) };
            let decoded: F32 = archived.deserialize(&mut rkyv::Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }
}
