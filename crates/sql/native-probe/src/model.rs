use crate::Error;

const MAX_COLUMNS: usize = 64;
const MAX_NODES: usize = 4096;
const MAX_DEPTH: usize = 64;
const MAX_ROWS: usize = 65_536;
const MAX_CELLS: usize = 1_048_576;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i64)]
pub enum Arithmetic {
    Add = 1,
    Subtract = 2,
    Multiply = 3,
}

impl std::fmt::Display for Arithmetic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Comparison {
    GreaterThan,
    NotEqual,
}

/// References are indices into the program's expression list. Every child must
/// precede its parent; the final expression is the result.
#[derive(Clone, Debug)]
pub enum Expr {
    Column(usize),
    Literal(Option<i64>),
    Arithmetic {
        op: Arithmetic,
        left: usize,
        right: usize,
    },
    Compare {
        op: Comparison,
        left: usize,
        right: usize,
    },
    Coalesce(Vec<usize>),
}

/// A validated, immutable expression graph. Bounds apply to its expanded tree
/// too, since code generation duplicates shared expressions across lazy branches.
#[derive(Clone, Debug)]
pub struct Program {
    pub(crate) columns: usize,
    pub(crate) expressions: Vec<Expr>,
}

impl Program {
    pub fn new(columns: usize, expressions: Vec<Expr>) -> Result<Self, Error> {
        let invalid = |reason: &str| Error::InvalidProgram(reason.into());
        if columns > MAX_COLUMNS {
            return Err(invalid("at most 64 input columns are supported"));
        }
        if expressions.is_empty() || expressions.len() > MAX_NODES {
            return Err(invalid("expected 1..=4096 expressions"));
        }
        let mut depths = Vec::with_capacity(expressions.len());
        let mut sizes = Vec::with_capacity(expressions.len());
        for (index, expression) in expressions.iter().enumerate() {
            let mut depth = 1;
            let mut size = 1;
            let mut child = |child: usize| -> Result<(), Error> {
                if child >= index {
                    return Err(invalid("expression references must point to earlier nodes"));
                }
                depth = depth.max(depths[child] + 1);
                size += sizes[child];
                if size > MAX_NODES {
                    return Err(invalid("expanded expression exceeds 4096 nodes"));
                }
                Ok(())
            };
            match expression {
                Expr::Column(column) if *column >= columns => {
                    return Err(invalid("column index is outside the input schema"));
                }
                Expr::Column(_) | Expr::Literal(_) => {}
                Expr::Arithmetic { left, right, .. } | Expr::Compare { left, right, .. } => {
                    child(*left)?;
                    child(*right)?;
                }
                Expr::Coalesce(children) => {
                    if children.is_empty() {
                        return Err(invalid("COALESCE requires at least one argument"));
                    }
                    for &index in children {
                        child(index)?;
                    }
                }
            }
            if depth > MAX_DEPTH {
                return Err(invalid("expression depth exceeds 64"));
            }
            depths.push(depth);
            sizes.push(size);
        }
        Ok(Self {
            columns,
            expressions,
        })
    }

    /// Independent checked Rust evaluator used as the prototype oracle.
    /// Arithmetic evaluates both children; COALESCE evaluates only the needed
    /// prefix. An overflow rejects the whole batch, including earlier rows.
    pub fn evaluate(&self, batch: &Batch) -> Result<Vec<Option<i64>>, Error> {
        batch.check_columns(self.columns)?;
        (0..batch.rows())
            .map(|row| self.evaluate_at(self.expressions.len() - 1, batch, row))
            .collect()
    }

    fn evaluate_at(&self, index: usize, batch: &Batch, row: usize) -> Result<Option<i64>, Error> {
        match &self.expressions[index] {
            Expr::Column(column) => Ok(batch.value(row, *column)),
            Expr::Literal(value) => Ok(*value),
            Expr::Arithmetic { op, left, right } => {
                let left = self.evaluate_at(*left, batch, row)?;
                let right = self.evaluate_at(*right, batch, row)?;
                match (left, right) {
                    (Some(left), Some(right)) => match op {
                        Arithmetic::Add => left.checked_add(right),
                        Arithmetic::Subtract => left.checked_sub(right),
                        Arithmetic::Multiply => left.checked_mul(right),
                    }
                    .map(Some)
                    .ok_or(Error::Overflow {
                        row,
                        op: *op,
                        left,
                        right,
                    }),
                    _ => Ok(None),
                }
            }
            Expr::Compare { op, left, right } => {
                let left = self.evaluate_at(*left, batch, row)?;
                let right = self.evaluate_at(*right, batch, row)?;
                Ok(left.zip(right).map(|(left, right)| {
                    i64::from(match op {
                        Comparison::GreaterThan => left > right,
                        Comparison::NotEqual => left != right,
                    })
                }))
            }
            Expr::Coalesce(children) => {
                for &child in children {
                    if let Some(value) = self.evaluate_at(child, batch, row)? {
                        return Ok(Some(value));
                    }
                }
                Ok(None)
            }
        }
    }
}

/// Row-major BIGINT payloads and one NULL bitmap per row. NULL payloads are
/// initialized to zero. Private fields keep the native-call bounds invariant.
#[derive(Clone, Debug)]
pub struct Batch {
    columns: usize,
    pub(crate) values: Vec<i64>,
    pub(crate) nulls: Vec<u64>,
}

impl Batch {
    pub fn from_rows(columns: usize, rows: &[Vec<Option<i64>>]) -> Result<Self, Error> {
        if columns > MAX_COLUMNS
            || rows.len() > MAX_ROWS
            || columns.saturating_mul(rows.len()) > MAX_CELLS
        {
            return Err(Error::InvalidBatch(
                "batch exceeds 64 columns, 65536 rows or 1048576 cells".into(),
            ));
        }
        let mut values = Vec::with_capacity(rows.len() * columns);
        let mut nulls = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != columns {
                return Err(Error::InvalidBatch(
                    "row width differs from the input schema".into(),
                ));
            }
            let mut mask = 0;
            for (column, value) in row.iter().enumerate() {
                if value.is_none() {
                    mask |= 1_u64 << column;
                }
                values.push(value.unwrap_or(0));
            }
            nulls.push(mask);
        }
        Ok(Self {
            columns,
            values,
            nulls,
        })
    }

    pub fn rows(&self) -> usize {
        self.nulls.len()
    }

    /// Read a cell without exposing the batch's storage invariants. The outer
    /// option distinguishes an invalid coordinate from a SQL NULL value.
    pub fn get(&self, row: usize, column: usize) -> Option<Option<i64>> {
        (row < self.rows() && column < self.columns).then(|| self.value(row, column))
    }

    pub(crate) fn check_columns(&self, columns: usize) -> Result<(), Error> {
        if self.columns != columns {
            return Err(Error::InvalidBatch(
                "batch schema differs from compiled input schema".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn value(&self, row: usize, column: usize) -> Option<i64> {
        (self.nulls[row] & (1_u64 << column) == 0).then(|| self.values[row * self.columns + column])
    }
}
