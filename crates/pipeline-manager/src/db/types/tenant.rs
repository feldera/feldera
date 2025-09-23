use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(
    Clone, Copy, Debug, Eq, Ord, Hash, PartialEq, PartialOrd, Serialize, Deserialize, ToSchema,
)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct TenantId(
    #[cfg_attr(test, proptest(strategy = "crate::db::test::limited_uuid()"))] pub Uuid,
);
impl Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
