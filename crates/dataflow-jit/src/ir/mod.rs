pub mod block;
pub mod exprs;
pub mod function;
pub mod graph;
pub mod literal;
pub mod nodes;
pub mod pretty;
pub mod row_layout;
pub mod stream_layout;
pub mod terminator;
pub mod visit;

mod ids;
mod layout_cache;
mod optimize;
mod row_or_scalar;
mod types;
mod validate;

pub use row_or_scalar::RowOrScalar;
// TODO: Remove `exprs` re-export
pub use exprs::{
    BinaryOp, BinaryOpKind, Cast, Constant, Copy, CopyRowTo, Expr, IsNull, Load, NullRow, RValue,
    Select, SetNull, Store, UnaryOp, UnaryOpKind, UninitRow,
};
pub use function::{Function, FunctionBuilder, InputFlags};
pub use graph::{Graph, GraphExt};
pub use ids::{BlockId, ExprId, LayoutId, NodeId};
pub use layout_cache::RowLayoutCache;
pub use row_layout::{RowLayout, RowLayoutBuilder};
pub use terminator::{Branch, Jump, Return, Terminator};
pub use types::{ColumnType, Signature};
pub use validate::{ValidationError, ValidationResult, Validator};

pub(crate) use ids::{BlockIdGen, ExprIdGen, NodeIdGen};
