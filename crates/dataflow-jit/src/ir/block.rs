use crate::ir::{
    pretty::{DocAllocator, DocBuilder, Pretty},
    BlockId, Expr, ExprId, RowLayoutCache, RowOrScalar, Terminator,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A single basic block within a function
///
/// Each basic block has a unique id, zero or more expressions within its body
/// and a single terminator
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Block {
    /// The id of the block
    id: BlockId,
    /// The parameters passed to this basic block
    #[serde(default)]
    params: Vec<(ExprId, RowOrScalar)>,
    /// The ids of all expressions within the block
    body: Vec<(ExprId, Expr)>,
    /// The block's terminator
    terminator: Terminator,
}

impl Block {
    /// Returns the block's id
    #[inline]
    pub const fn id(&self) -> BlockId {
        self.id
    }

    /// Returns the block's parameters
    #[inline]
    pub fn params(&self) -> &[(ExprId, RowOrScalar)] {
        &self.params
    }

    /// Returns a mutable reference to the block's parameters
    #[inline]
    pub fn params_mut(&mut self) -> &mut Vec<(ExprId, RowOrScalar)> {
        &mut self.params
    }

    /// Returns the ids of all expressions within the block
    #[inline]
    pub fn body(&self) -> &[(ExprId, Expr)] {
        &self.body
    }

    /// Returns a mutable reference to the ids of all expressions within the
    /// block
    #[inline]
    pub fn body_mut(&mut self) -> &mut [(ExprId, Expr)] {
        &mut self.body
    }

    #[inline]
    pub(crate) fn body_vec_mut(&mut self) -> &mut Vec<(ExprId, Expr)> {
        &mut self.body
    }

    /// Removes all expressions within the current block's body for which the
    /// predicate returns `false`
    #[inline]
    pub fn retain<F>(&mut self, mut retain: F)
    where
        F: FnMut(ExprId, &Expr) -> bool,
    {
        self.body.retain(|(expr_id, expr)| retain(*expr_id, expr));
    }

    /// Returns the block's terminator
    #[inline]
    pub fn terminator(&self) -> &Terminator {
        &self.terminator
    }

    /// Returns a mutable reference to the block's terminator
    #[inline]
    pub fn terminator_mut(&mut self) -> &mut Terminator {
        &mut self.terminator
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Block
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized + 'a,
    DocBuilder<'a, D, A>: Clone,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        self.id
            .pretty(alloc, cache)
            .append(if self.params.is_empty() {
                alloc.nil()
            } else {
                alloc
                    .intersperse(
                        self.params.iter().map(|(expr, ty)| {
                            ty.pretty(alloc, cache)
                                .append(alloc.space())
                                .append(expr.pretty(alloc, cache))
                        }),
                        alloc.text(",").append(alloc.space()),
                    )
                    .parens()
            })
            .append(alloc.text(":"))
            .append(alloc.hardline())
            .append(
                alloc
                    .intersperse(
                        self.body.iter().map(|(id, expr)| {
                            if expr.needs_assign() {
                                id.pretty(alloc, cache)
                                    .append(alloc.space())
                                    .append(alloc.text("="))
                                    .append(alloc.space())
                                    .append(expr.pretty(alloc, cache))
                            } else {
                                expr.pretty(alloc, cache)
                                    .append(alloc.space())
                                    .append(alloc.text(";"))
                                    .append(alloc.space())
                                    .append(id.pretty(alloc, cache))
                            }
                        }),
                        alloc.hardline(),
                    )
                    .append(if self.body.is_empty() {
                        alloc.nil()
                    } else {
                        alloc.hardline()
                    })
                    .append(self.terminator.pretty(alloc, cache))
                    .indent(2),
            )
    }
}

/// An unfinished basic block
pub(crate) struct UnsealedBlock {
    pub(crate) id: BlockId,
    pub(crate) params: Vec<(ExprId, RowOrScalar)>,
    pub(crate) body: Vec<(ExprId, Expr)>,
    pub(crate) terminator: Option<Terminator>,
}

impl UnsealedBlock {
    #[inline]
    pub(crate) fn new(id: BlockId) -> Self {
        Self {
            id,
            params: Vec::new(),
            body: Vec::new(),
            terminator: None,
        }
    }

    #[track_caller]
    pub(crate) fn into_block(self) -> Block {
        let terminator = self.terminator.unwrap_or_else(|| {
            panic!(
                "Called `FunctionBuilder::build()` with unfinished blocks: {} has no terminator",
                self.id,
            )
        });

        Block {
            id: self.id,
            params: self.params,
            body: self.body,
            terminator,
        }
    }
}
