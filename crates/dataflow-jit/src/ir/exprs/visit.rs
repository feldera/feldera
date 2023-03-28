use crate::ir::{
    exprs::{
        ArgType, BinaryOp, Call, Cast, Constant, Copy, CopyRowTo, Expr, IsNull, Load, NullRow,
        Select, SetNull, Store, UnaryOp, UninitRow,
    },
    LayoutId,
};

pub trait ExprVisitor {
    fn visit_call(&mut self, _call: &Call) {}
    fn visit_cast(&mut self, _cast: &Cast) {}
    fn visit_load(&mut self, _load: &Load) {}
    fn visit_store(&mut self, _store: &Store) {}
    fn visit_select(&mut self, _select: &Select) {}
    fn visit_is_null(&mut self, _is_null: &IsNull) {}
    fn visit_bin_op(&mut self, _binop: &BinaryOp) {}
    fn visit_copy(&mut self, _copy: &Copy) {}
    fn visit_unary_op(&mut self, _unary_op: &UnaryOp) {}
    fn visit_null_row(&mut self, _null_row: &NullRow) {}
    fn visit_set_null(&mut self, _set_null: &SetNull) {}
    fn visit_constant(&mut self, _constant: &Constant) {}
    fn visit_copy_row_to(&mut self, _copy_row_to: &CopyRowTo) {}
    fn visit_uninit_row(&mut self, _uninit_row: &UninitRow) {}
}

pub trait MutExprVisitor {
    fn visit_call(&mut self, _call: &mut Call) {}
    fn visit_cast(&mut self, _cast: &mut Cast) {}
    fn visit_load(&mut self, _load: &mut Load) {}
    fn visit_store(&mut self, _store: &mut Store) {}
    fn visit_select(&mut self, _select: &mut Select) {}
    fn visit_is_null(&mut self, _is_null: &mut IsNull) {}
    fn visit_bin_op(&mut self, _binop: &mut BinaryOp) {}
    fn visit_copy(&mut self, _copy: &mut Copy) {}
    fn visit_unary_op(&mut self, _unary_op: &mut UnaryOp) {}
    fn visit_null_row(&mut self, _null_row: &mut NullRow) {}
    fn visit_set_null(&mut self, _set_null: &mut SetNull) {}
    fn visit_constant(&mut self, _constant: &mut Constant) {}
    fn visit_copy_row_to(&mut self, _copy_row_to: &mut CopyRowTo) {}
    fn visit_uninit_row(&mut self, _uninit_row: &mut UninitRow) {}
}

impl Expr {
    pub fn apply<V>(&self, visitor: &mut V)
    where
        V: ExprVisitor,
    {
        match self {
            Self::Call(call) => visitor.visit_call(call),
            Self::Cast(cast) => visitor.visit_cast(cast),
            Self::Load(load) => visitor.visit_load(load),
            Self::Store(store) => visitor.visit_store(store),
            Self::Select(select) => visitor.visit_select(select),
            Self::IsNull(is_null) => visitor.visit_is_null(is_null),
            Self::BinOp(binop) => visitor.visit_bin_op(binop),
            Self::Copy(copy) => visitor.visit_copy(copy),
            Self::UnaryOp(unary_op) => visitor.visit_unary_op(unary_op),
            Self::NullRow(null_row) => visitor.visit_null_row(null_row),
            Self::SetNull(set_null) => visitor.visit_set_null(set_null),
            Self::Constant(constant) => visitor.visit_constant(constant),
            Self::CopyRowTo(copy_row_to) => visitor.visit_copy_row_to(copy_row_to),
            Self::UninitRow(uninit_row) => visitor.visit_uninit_row(uninit_row),
        }
    }

    pub fn apply_mut<V>(&mut self, visitor: &mut V)
    where
        V: MutExprVisitor,
    {
        match self {
            Self::Call(call) => visitor.visit_call(call),
            Self::Cast(cast) => visitor.visit_cast(cast),
            Self::Load(load) => visitor.visit_load(load),
            Self::Store(store) => visitor.visit_store(store),
            Self::Select(select) => visitor.visit_select(select),
            Self::IsNull(is_null) => visitor.visit_is_null(is_null),
            Self::BinOp(binop) => visitor.visit_bin_op(binop),
            Self::Copy(copy) => visitor.visit_copy(copy),
            Self::UnaryOp(unary_op) => visitor.visit_unary_op(unary_op),
            Self::NullRow(null_row) => visitor.visit_null_row(null_row),
            Self::SetNull(set_null) => visitor.visit_set_null(set_null),
            Self::Constant(constant) => visitor.visit_constant(constant),
            Self::CopyRowTo(copy_row_to) => visitor.visit_copy_row_to(copy_row_to),
            Self::UninitRow(uninit_row) => visitor.visit_uninit_row(uninit_row),
        }
    }
}

pub struct MapLayouts<F> {
    map_layout: F,
}

impl<F> MapLayouts<F> {
    pub fn new(map_layout: F) -> Self {
        Self { map_layout }
    }
}

impl<F> ExprVisitor for MapLayouts<F>
where
    F: FnMut(LayoutId),
{
    fn visit_load(&mut self, load: &Load) {
        (self.map_layout)(load.source_layout);
    }

    fn visit_store(&mut self, store: &Store) {
        (self.map_layout)(store.target_layout);
    }

    fn visit_is_null(&mut self, is_null: &IsNull) {
        (self.map_layout)(is_null.target_layout);
    }

    fn visit_set_null(&mut self, set_null: &SetNull) {
        (self.map_layout)(set_null.target_layout);
    }

    fn visit_copy_row_to(&mut self, copy_row_to: &CopyRowTo) {
        (self.map_layout)(copy_row_to.layout);
    }

    fn visit_null_row(&mut self, null_row: &NullRow) {
        (self.map_layout)(null_row.layout);
    }

    fn visit_uninit_row(&mut self, uninit_row: &UninitRow) {
        (self.map_layout)(uninit_row.layout);
    }

    fn visit_call(&mut self, call: &Call) {
        for arg in call.arg_types() {
            if let ArgType::Row(layout) = *arg {
                (self.map_layout)(layout);
            }
        }
    }
}
