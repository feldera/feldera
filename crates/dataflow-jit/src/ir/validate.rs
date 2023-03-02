use crate::ir::{
    graph::GraphExt, BinaryOpKind, BlockId, Cast, ColumnType, Constant, Expr, ExprId, Function,
    Graph, InputFlags, IsNull, LayoutId, Load, Node, NodeId, NullRow, RValue, RowLayoutCache,
    SetNull, Store, StreamKind, StreamLayout, UnaryOpKind, UninitRow,
};
use derive_more::Display;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
};

type ValidationResult<T = ()> = Result<T, ValidationError>;

pub struct Validator {
    /// A set of all nodes that exist
    nodes: BTreeSet<NodeId>,
    /// A map of nodes to their inputs (if they accept inputs)
    // TODO: TinyVec<[LayoutId; 5]>
    node_inputs: BTreeMap<NodeId, Vec<NodeId>>,
    /// A map of nodes to their output layout (if they produce an output)
    node_outputs: BTreeMap<NodeId, StreamLayout>,
    function_validator: FunctionValidator,
}

impl Validator {
    pub fn new(layout_cache: RowLayoutCache) -> Self {
        Self {
            nodes: BTreeSet::new(),
            node_inputs: BTreeMap::new(),
            node_outputs: BTreeMap::new(),
            function_validator: FunctionValidator::new(layout_cache),
        }
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.node_inputs.clear();
        self.node_outputs.clear();
    }

    // FIXME: Make this return a result instead of panicking
    pub fn validate_graph(&mut self, graph: &Graph) -> ValidationResult {
        self.clear();

        // Collect all nodes and the layouts of their outputs
        for (&node_id, node) in graph.nodes() {
            // If the graph already contained a node with this id
            if !self.nodes.insert(node_id) {
                panic!("graph contained duplicate node id: {node_id}")
            }

            // FIXME: Factor a lot of this querying into the `DataflowNode` trait
            match node {
                Node::Map(map) => {
                    self.node_inputs.insert(node_id, vec![map.input()]);
                    self.node_outputs
                        .insert(node_id, StreamLayout::Set(map.layout()));
                }

                Node::Filter(filter) => {
                    self.node_inputs.insert(node_id, vec![filter.input()]);
                    self.node_outputs
                        .insert(node_id, self.node_outputs[&filter.input()]);
                }

                Node::Neg(neg) => {
                    self.node_inputs.insert(node_id, vec![neg.input()]);
                    self.node_outputs
                        .insert(node_id, StreamLayout::Set(neg.output_layout()));
                }

                Node::Sum(sum) => {
                    self.node_inputs.insert(node_id, sum.inputs().to_vec());
                }

                Node::Sink(sink) => {
                    self.node_inputs.insert(node_id, vec![sink.input()]);
                }

                Node::Source(source) => {
                    self.node_outputs
                        .insert(node_id, StreamLayout::Set(source.layout()));
                }

                Node::SourceMap(source) => {
                    self.node_outputs
                        .insert(node_id, StreamLayout::Map(source.key(), source.value()));
                }

                Node::IndexWith(index_with) => {
                    self.node_inputs.insert(node_id, vec![index_with.input()]);
                    self.node_outputs.insert(
                        node_id,
                        StreamLayout::Map(index_with.key_layout(), index_with.value_layout()),
                    );
                }

                Node::JoinCore(join) => {
                    self.node_inputs
                        .insert(node_id, vec![join.lhs(), join.rhs()]);

                    let output = match join.result_kind() {
                        StreamKind::Set => {
                            if join.value_layout() != self.function_validator.layout_cache.unit() {
                                return Err(ValidationError::JoinSetValueNotUnit {
                                    join: node_id,
                                    value_layout: join.value_layout(),
                                    layout: self
                                        .function_validator
                                        .layout_cache
                                        .get(join.value_layout())
                                        .to_string(),
                                });
                            }

                            StreamLayout::Set(join.key_layout())
                        }
                        StreamKind::Map => {
                            StreamLayout::Map(join.key_layout(), join.value_layout())
                        }
                    };
                    self.node_outputs.insert(node_id, output);
                }

                _ => todo!(),
            }
        }

        for (&node_id, node) in graph.nodes() {
            match node {
                Node::Map(map) => {
                    assert_eq!(map.map_fn().return_type(), ColumnType::Unit);

                    let input_layout = self.get_expected_input(node_id, map.input());
                    let expected = &[
                        (input_layout, false),
                        (StreamLayout::Set(map.layout()), true),
                    ];

                    assert_eq!(expected.len(), map.map_fn().args().len());
                    for (idx, (&(expected_layout, is_mutable), arg)) in
                        expected.iter().zip(map.map_fn().args()).enumerate()
                    {
                        assert_eq!(
                            expected_layout,
                            StreamLayout::Set(arg.layout),
                            "the {idx}th argument to a map function had an incorrect layout: expected {:?}, got {:?}",
                            graph.layout_cache().get(expected_layout.unwrap_set()),
                            graph.layout_cache().get(arg.layout),
                        );

                        assert_eq!(
                            arg.flags.contains(InputFlags::OUTPUT),
                            is_mutable,
                            "the {idx}th argument to a map function was {}mutable when it should {}have been",
                            if is_mutable { "not" } else { "" },
                            if is_mutable { "" } else { "not" },
                        );
                    }

                    self.function_validator.validate_function(map.map_fn())?;
                }

                Node::Filter(filter) => {
                    let _input_layout = self.get_expected_input(node_id, filter.input());
                    assert_eq!(filter.filter_fn().return_type(), ColumnType::Bool);

                    // TODO: Validate function arguments

                    self.function_validator
                        .validate_function(filter.filter_fn())?;
                }

                Node::Neg(neg) => {
                    let input_layout = self.get_expected_input(node_id, neg.input());
                    assert_eq!(input_layout, StreamLayout::Set(neg.output_layout()));
                }

                Node::IndexWith(index_with) => {
                    let _input_layout = self.get_expected_input(node_id, index_with.input());
                    assert_eq!(index_with.index_fn().return_type(), ColumnType::Unit);

                    // TODO: Validate function arguments

                    self.function_validator
                        .validate_function(index_with.index_fn())?;
                }

                Node::JoinCore(join) => {
                    let _lhs_layout = self.get_expected_input(node_id, join.lhs());
                    let _rhs_layout = self.get_expected_input(node_id, join.rhs());
                    assert_eq!(join.join_fn().return_type(), ColumnType::Unit);

                    // TODO: Validate function arguments

                    self.function_validator.validate_function(join.join_fn())?;
                }

                _ => {}
            }
        }

        Ok(())
    }

    #[track_caller]
    fn get_expected_input(&self, node: NodeId, input: NodeId) -> StreamLayout {
        if let Some(&input_layout) = self.node_outputs.get(&input) {
            input_layout
        } else {
            panic!("node {node}'s input {input} does not exist");
        }
    }
}

pub struct FunctionValidator {
    exprs: BTreeSet<ExprId>,
    /// Expressions that produce values will have a type which will
    /// either be a row type or an entire row
    expr_types: BTreeMap<ExprId, Result<ColumnType, LayoutId>>,
    /// A map from all expressions containing row types to their mutability
    expr_row_mutability: BTreeMap<ExprId, bool>,
    blocks: BTreeSet<BlockId>,
    // TODO: Block parameters once those are implemented
    // TODO: Control flow validation
    layout_cache: RowLayoutCache,
}

impl FunctionValidator {
    pub fn new(layout_cache: RowLayoutCache) -> Self {
        Self {
            exprs: BTreeSet::new(),
            expr_types: BTreeMap::new(),
            expr_row_mutability: BTreeMap::new(),
            blocks: BTreeSet::new(),
            layout_cache,
        }
    }

    pub fn clear(&mut self) {
        self.exprs.clear();
        self.expr_types.clear();
        self.expr_row_mutability.clear();
        self.blocks.clear();
    }

    pub fn validate_function(&mut self, func: &Function) -> ValidationResult {
        self.clear();

        self.blocks.extend(func.blocks().keys().copied());

        // Find any duplicated expressions
        for block in func.blocks().values() {
            for &(expr_id, _) in block.body() {
                if !self.exprs.insert(expr_id) {
                    return Err(ValidationError::DuplicateExpr { expr: expr_id });
                }
            }
        }

        for arg in func.args() {
            if !self.exprs.insert(arg.id) {
                return Err(ValidationError::DuplicateExpr { expr: arg.id });
            }

            self.expr_types.insert(arg.id, Err(arg.layout));
            self.expr_row_mutability
                .insert(arg.id, arg.flags.contains(InputFlags::OUTPUT));
        }

        // Infer expression types
        let mut stack = vec![func.entry_block()];
        while let Some(block_id) = stack.pop() {
            let block = func
                .blocks()
                .get(&block_id)
                .ok_or(ValidationError::MissingBlock { block: block_id })?;

            if block.id() != block_id {
                return Err(ValidationError::MismatchedBlockId {
                    block_id,
                    internal_block_id: block.id(),
                });
            }

            assert_eq!(
                block.id(),
                block_id,
                "block has mismatched id: {block_id} != {}",
                block.id(),
            );

            for &(expr_id, ref expr) in block.body() {
                match expr {
                    Expr::Cast(cast) => self.cast(expr_id, cast)?,
                    Expr::Constant(constant) => self.constant(expr_id, constant)?,
                    Expr::Load(load) => self.load(expr_id, load)?,
                    Expr::Store(store) => self.store(expr_id, store)?,
                    Expr::IsNull(is_null) => self.is_null(expr_id, is_null)?,
                    Expr::SetNull(set_null) => self.set_null(expr_id, set_null)?,
                    Expr::NullRow(null_row) => self.null_row(expr_id, null_row)?,
                    Expr::UninitRow(uninit_row) => self.uninit_row(expr_id, uninit_row)?,

                    // FIXME: Better errors
                    Expr::BinOp(binop) => {
                        let lhs_ty = self.expr_types.get(&binop.lhs()).unwrap().unwrap();
                        let rhs_ty = self.expr_types.get(&binop.rhs()).unwrap().unwrap();
                        assert_eq!(lhs_ty, rhs_ty);

                        match binop.kind() {
                            BinaryOpKind::Eq
                            | BinaryOpKind::Neq
                            | BinaryOpKind::LessThan
                            | BinaryOpKind::GreaterThan
                            | BinaryOpKind::LessThanOrEqual
                            | BinaryOpKind::GreaterThanOrEqual => {
                                let prev = self.expr_types.insert(expr_id, Ok(ColumnType::Bool));
                                assert!(prev.is_none());
                            }

                            BinaryOpKind::Add
                            | BinaryOpKind::Sub
                            | BinaryOpKind::Mul
                            | BinaryOpKind::Div
                            | BinaryOpKind::And
                            | BinaryOpKind::Or
                            | BinaryOpKind::Xor
                            | BinaryOpKind::Min
                            | BinaryOpKind::Max => {
                                assert_ne!(lhs_ty, ColumnType::String);

                                let prev = self.expr_types.insert(expr_id, Ok(lhs_ty));
                                assert!(prev.is_none());
                            }
                        }
                    }

                    Expr::UnaryOp(unary) => {
                        let value_ty = self.expr_types.get(&unary.value()).unwrap().unwrap();

                        match unary.kind() {
                            UnaryOpKind::Not => {
                                assert!(value_ty.is_int() || value_ty.is_bool());
                                let prev = self.expr_types.insert(expr_id, Ok(value_ty));
                                assert!(prev.is_none());
                            }

                            UnaryOpKind::Neg | UnaryOpKind::Abs => {
                                assert!(value_ty.is_float() || value_ty.is_int());
                                let prev = self.expr_types.insert(expr_id, Ok(value_ty));
                                assert!(prev.is_none());
                            }

                            UnaryOpKind::Ceil
                            | UnaryOpKind::Floor
                            | UnaryOpKind::Trunc
                            | UnaryOpKind::Sqrt => {
                                assert!(value_ty.is_float());
                                let prev = self.expr_types.insert(expr_id, Ok(value_ty));
                                assert!(prev.is_none());
                            }

                            UnaryOpKind::CountOnes
                            | UnaryOpKind::CountZeroes
                            | UnaryOpKind::LeadingOnes
                            | UnaryOpKind::LeadingZeroes
                            | UnaryOpKind::TrailingOnes
                            | UnaryOpKind::TrailingZeroes
                            | UnaryOpKind::BitReverse
                            | UnaryOpKind::ByteReverse => {
                                debug_assert!(value_ty.is_int());
                                let prev = self.expr_types.insert(expr_id, Ok(value_ty));
                                assert!(prev.is_none());
                            }

                            UnaryOpKind::StringLen => {
                                debug_assert!(value_ty.is_string());
                                let prev = self.expr_types.insert(expr_id, Ok(ColumnType::U64));
                                assert!(prev.is_none());
                            }
                        }
                    }

                    Expr::CopyVal(_) | Expr::CopyRowTo(_) => todo!(),
                }
            }
        }

        Ok(())
    }

    fn expr_type(&self, expr_id: ExprId) -> ValidationResult<Result<ColumnType, LayoutId>> {
        if let Some(&ty) = self.expr_types.get(&expr_id) {
            Ok(ty)
        } else {
            Err(ValidationError::MissingExpr { expr: expr_id })
        }
    }

    fn add_column_expr(&mut self, expr_id: ExprId, column_type: ColumnType) {
        let prev = self.expr_types.insert(expr_id, Ok(column_type));
        debug_assert!(
            prev.is_none(),
            "all duplicate expressions should be caught earlier on in validation",
        );
    }

    fn cast(&mut self, expr_id: ExprId, cast: &Cast) -> ValidationResult {
        if cast.is_valid_cast() {
            let prev = self.expr_types.insert(expr_id, Ok(cast.to()));
            assert!(prev.is_none());
            Ok(())
        } else {
            Err(ValidationError::InvalidCast {
                expr: expr_id,
                from: cast.from(),
                to: cast.to(),
            })
        }
    }

    fn constant(&mut self, expr_id: ExprId, constant: &Constant) -> ValidationResult {
        self.add_column_expr(expr_id, constant.column_type());
        Ok(())
    }

    fn load(&mut self, expr_id: ExprId, load: &Load) -> ValidationResult {
        let source_layout = if let Err(row_layout) = self.expr_type(load.source())? {
            row_layout
        } else {
            return Err(ValidationError::LoadFromScalar {
                load: expr_id,
                source: load.source(),
            });
        };

        if source_layout != load.source_layout() {
            return Err(ValidationError::MismatchedLoadLayout {
                load: expr_id,
                source: load.source(),
                expected_layout: load.source_layout(),
                actual_layout: source_layout,
            });
        }

        {
            let layout = self.layout_cache.get(source_layout);
            if let Some(source_type) = layout.try_column_type(load.column()) {
                if source_type != load.column_type() {
                    return Err(ValidationError::InvalidLoadType {
                        load: expr_id,
                        load_type: load.column_type(),
                        source: load.source(),
                        source_type,
                    });
                }
            } else {
                return Err(ValidationError::InvalidColumnLoad {
                    load: expr_id,
                    column: load.column(),
                    source: load.source(),
                    source_columns: layout.len(),
                    layout: layout.to_string(),
                });
            }
        }

        self.add_column_expr(expr_id, load.column_type());

        Ok(())
    }

    fn store(&mut self, expr_id: ExprId, store: &Store) -> ValidationResult {
        let target_layout = if let Err(row_layout) = self.expr_type(store.target())? {
            row_layout
        } else {
            return Err(ValidationError::StoreToScalar {
                store: expr_id,
                target: store.target(),
            });
        };

        if target_layout != store.target_layout() {
            return Err(ValidationError::MismatchedStoreLayout {
                store: expr_id,
                target: store.target(),
                expected_layout: store.target_layout(),
                actual_layout: target_layout,
            });
        }

        let value_type = match store.value() {
            &RValue::Expr(value) => {
                self.expr_type(value)?
                    .map_err(|_| ValidationError::StoreWithRow {
                        store: expr_id,
                        value,
                    })?
            }
            RValue::Imm(imm) => imm.column_type(),
        };

        {
            let layout = self.layout_cache.get(target_layout);
            if let Some(target_type) = layout.try_column_type(store.column()) {
                if target_type != store.value_type() {
                    return Err(ValidationError::InvalidStoreType {
                        store: expr_id,
                        store_type: store.value_type(),
                        target: store.target(),
                        target_type,
                    });
                } else if target_type != value_type {
                    todo!("invalid store value type in store {expr_id}, tried to store value of type {value_type} to column of type {target_type}")
                }
            } else {
                return Err(ValidationError::InvalidColumnStore {
                    store: expr_id,
                    column: store.column(),
                    target: store.target(),
                    target_columns: layout.len(),
                    layout: layout.to_string(),
                });
            }
        }

        // Ensure we're only storing to mutable rows
        if !*self.expr_row_mutability.get(&store.target()).unwrap() {
            panic!(
                "store {expr_id} stored to {} which is an immutable row",
                store.target(),
            );
        }

        Ok(())
    }

    fn is_null(&mut self, expr_id: ExprId, is_null: &IsNull) -> ValidationResult {
        let target_layout = if let Err(row_layout) = self.expr_type(is_null.target())? {
            row_layout
        } else {
            todo!("called IsNull on scalar")
        };

        {
            let layout = self.layout_cache.get(target_layout);
            if let Some(nullable) = layout.try_column_nullable(is_null.column()) {
                if !nullable {
                    todo!("called IsNull on non-nullable column")
                }
            } else {
                todo!("IsNull column {} doesn't exist", is_null.column())
            }
        }

        self.add_column_expr(expr_id, ColumnType::Bool);

        Ok(())
    }

    fn set_null(&mut self, expr_id: ExprId, set_null: &SetNull) -> ValidationResult {
        let target_layout = if let Err(row_layout) = self.expr_type(set_null.target())? {
            row_layout
        } else {
            todo!("called SetNull on scalar")
        };

        {
            let layout = self.layout_cache.get(target_layout);
            if let Some(nullable) = layout.try_column_nullable(set_null.column()) {
                if !nullable {
                    todo!("called SetNull on non-nullable column")
                }
            } else {
                todo!("SetNull column {} doesn't exist", set_null.column())
            }
        }

        // Make sure that is_null is a boolean value
        match set_null.is_null() {
            &RValue::Expr(is_null) => {
                let ty = self
                    .expr_type(is_null)?
                    .expect("attempted to use row value with SetNull");
                if !ty.is_bool() {
                    todo!("SetNull with non-bool value")
                }
            }

            RValue::Imm(constant) => {
                if !constant.is_bool() {
                    todo!("SetNull with non-bool constant")
                }
            }
        }

        // Ensure we're only storing to mutable rows
        if !*self.expr_row_mutability.get(&set_null.target()).unwrap() {
            panic!(
                "SetNull {expr_id} stored to {} which is an immutable row",
                set_null.target(),
            );
        }

        Ok(())
    }

    fn null_row(&mut self, expr_id: ExprId, null_row: &NullRow) -> ValidationResult {
        self.expr_row_mutability.insert(expr_id, true);
        self.expr_types.insert(expr_id, Err(null_row.layout()));
        Ok(())
    }

    fn uninit_row(&mut self, expr_id: ExprId, uninit_row: &UninitRow) -> ValidationResult {
        self.expr_row_mutability.insert(expr_id, true);
        self.expr_types.insert(expr_id, Err(uninit_row.layout()));
        Ok(())
    }
}

#[derive(Debug, Display)]
pub enum ValidationError {
    #[display(fmt = "attempted to use block that doesn't exist: {block}")]
    MissingBlock { block: BlockId },

    #[display(
        fmt = "mismatched block ids, block was listed under {block_id} but has an internal id of {internal_block_id}"
    )]
    MismatchedBlockId {
        block_id: BlockId,
        internal_block_id: BlockId,
    },

    #[display(fmt = "the expression {expr} was declared multiple times")]
    DuplicateExpr { expr: ExprId },

    #[display(fmt = "invalid cast in {expr}: cannot cast from {from} to {to}")]
    InvalidCast {
        expr: ExprId,
        from: ColumnType,
        to: ColumnType,
    },

    #[display(fmt = "attempted to use expression that doesn't exist: {expr}")]
    MissingExpr { expr: ExprId },

    #[display(
        fmt = "attempted to load from a scalar value and not a row value in load {load} from {source}"
    )]
    LoadFromScalar { load: ExprId, source: ExprId },

    #[display(
        fmt = "attempted to store to a scalar value and not a row value in store {store} to {target}"
    )]
    StoreToScalar { store: ExprId, target: ExprId },

    #[display(
        fmt = "attempted to load a value of type {source_type} from {source} when load {load} expected a value of type {load_type}"
    )]
    InvalidLoadType {
        load: ExprId,
        load_type: ColumnType,
        source: ExprId,
        source_type: ColumnType,
    },

    #[display(
        fmt = "attempted to store a value of type {target_type} to {target} when store {store} expected a value of type {store_type}"
    )]
    InvalidStoreType {
        store: ExprId,
        store_type: ColumnType,
        target: ExprId,
        target_type: ColumnType,
    },

    #[display(
        fmt = "load {load} attempted to load from column {column} of {source} when {source} only has {source_columns} (source layout is {layout})"
    )]
    InvalidColumnLoad {
        load: ExprId,
        column: usize,
        source: ExprId,
        source_columns: usize,
        layout: String,
    },

    #[display(
        fmt = "store {store} attempted to store to column {column} of {target} when {target} only has {target_columns} (source layout is {layout})"
    )]
    InvalidColumnStore {
        store: ExprId,
        column: usize,
        target: ExprId,
        target_columns: usize,
        layout: String,
    },

    #[display(
        fmt = "load {load} attempted to load from {source} with a layout of {expected_layout} but {source} has the layout {actual_layout}"
    )]
    MismatchedLoadLayout {
        load: ExprId,
        source: ExprId,
        expected_layout: LayoutId,
        actual_layout: LayoutId,
    },

    #[display(
        fmt = "store {store} attempted to store to {target} with a layout of {expected_layout} but {target} has the layout {actual_layout}"
    )]
    MismatchedStoreLayout {
        store: ExprId,
        target: ExprId,
        expected_layout: LayoutId,
        actual_layout: LayoutId,
    },

    #[display(
        fmt = "store {store} attempted to store the row value {value} (expected a scalar value)"
    )]
    StoreWithRow { store: ExprId, value: ExprId },

    #[display(
        fmt = "join {join} produces a set but its value layout {value_layout} was {layout}, not {{ unit }}"
    )]
    JoinSetValueNotUnit {
        join: NodeId,
        value_layout: LayoutId,
        layout: String,
    },
}

impl Error for ValidationError {}
