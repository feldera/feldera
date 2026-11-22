use crate::limiters::Projection;
use std::rc::Rc;

/// Local variables can refer to the same row many times. Share its children
/// and cache their combined bound so inspecting a nested alias stays constant time.
#[derive(Clone)]
pub(super) enum Shape {
    Scalar(bool),
    Tuple {
        fields: Rc<[Self]>,
        any_bound: bool,
        nullable: bool,
    },
}

impl Shape {
    pub fn tuple(fields: Vec<Self>, nullable: bool) -> Self {
        let any_bound = fields.iter().any(Self::monotone);
        Self::Tuple {
            fields: fields.into(),
            any_bound,
            nullable,
        }
    }

    pub fn from_input(shape: &Projection) -> Option<Self> {
        Some(match shape {
            Projection::Other { .. } => return None,
            Projection::Scalar { monotone } => Self::Scalar(*monotone),
            Projection::Tuple {
                fields, nullable, ..
            } => Self::tuple(
                fields.iter().map(Self::from_input).collect::<Option<_>>()?,
                *nullable,
            ),
        })
    }

    pub fn monotone(&self) -> bool {
        match self {
            Self::Scalar(bound) => *bound,
            Self::Tuple {
                fields,
                any_bound,
                nullable,
            } => *any_bound || fields.is_empty() && !nullable,
        }
    }
}
