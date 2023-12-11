// I cannot use the standard geopoint object because it doesn't implement Ord

use crate::some_polymorphic_function2;
use ::serde::{Deserialize, Serialize};
use dbsp::algebra::F64;
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

pub fn st_point_d_d(left: F64, right: F64) -> GeoPoint {
    GeoPoint::new(left, right)
}

some_polymorphic_function2!(st_point, d, F64, d, F64, GeoPoint);

pub fn st_distance_geopoint_geopoint(left: GeoPoint, right: GeoPoint) -> F64 {
    left.distance(&right)
}

some_polymorphic_function2!(st_distance, geopoint, GeoPoint, geopoint, GeoPoint, F64);
