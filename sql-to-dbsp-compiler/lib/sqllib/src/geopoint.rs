// I cannot use the standard geopoint object because it doesn't implement Ord

use crate::{casts::cast_to_d_decimal, some_polymorphic_function2, Decimal};
use ::serde::{Deserialize, Serialize};
use dbsp::algebra::{F32, F64};
use dbsp::num_entries_scalar;
use geo::EuclideanDistance;
use geo::Point;
use size_of::*;

#[derive(
    Default,
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    SizeOf,
    Serialize,
    Deserialize,
    Debug,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct GeoPoint(F64, F64);

num_entries_scalar! {
    GeoPoint,
}

impl GeoPoint {
    pub fn new<T, S>(left: T, right: S) -> Self
    where
        F64: From<T>,
        F64: From<S>,
    {
        Self(F64::from(left), F64::from(right))
    }

    pub fn to_point(&self) -> Point {
        Point::new(self.0.into_inner(), self.1.into_inner())
    }

    pub fn distance(&self, other: &GeoPoint) -> F64 {
        let left = self.to_point();
        let right = other.to_point();
        F64::from(left.euclidean_distance(&right))
    }
}

pub fn make_geopoint_d_d(left: F64, right: F64) -> GeoPoint {
    GeoPoint::new(left, right)
}

some_polymorphic_function2!(make_geopoint, d, F64, d, F64, GeoPoint);

pub fn make_geopoint_decimal_decimal(left: Decimal, right: Decimal) -> GeoPoint {
    GeoPoint::new(cast_to_d_decimal(left), cast_to_d_decimal(right))
}

some_polymorphic_function2!(make_geopoint, decimal, Decimal, decimal, Decimal, GeoPoint);

pub fn make_geopoint_f_f(left: F32, right: F32) -> GeoPoint {
    GeoPoint::new(left, right)
}

some_polymorphic_function2!(make_geopoint, f, F32, f, F32, GeoPoint);

pub fn make_geopoint_i64_i64(left: i64, right: i64) -> GeoPoint {
    GeoPoint::new(F64::from(left as f64), F64::from(right as f64))
}

some_polymorphic_function2!(make_geopoint, i64, i64, i64, i64, GeoPoint);

pub fn make_geopoint_i32_i32(left: i32, right: i32) -> GeoPoint {
    GeoPoint::new(F64::from(left), F64::from(right))
}

some_polymorphic_function2!(make_geopoint, i32, i32, i32, i32, GeoPoint);

pub fn make_geopoint_i16_i16(left: i16, right: i16) -> GeoPoint {
    GeoPoint::new(F64::from(left), F64::from(right))
}

some_polymorphic_function2!(make_geopoint, i16, i16, i16, i16, GeoPoint);

pub fn make_geopoint_i8_i8(left: i8, right: i8) -> GeoPoint {
    GeoPoint::new(F64::from(left), F64::from(right))
}

some_polymorphic_function2!(make_geopoint, i8, i8, i8, i8, GeoPoint);

pub fn st_distance_geopoint_geopoint(left: GeoPoint, right: GeoPoint) -> F64 {
    left.distance(&right)
}

some_polymorphic_function2!(st_distance, geopoint, GeoPoint, geopoint, GeoPoint, F64);
