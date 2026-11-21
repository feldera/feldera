use crate::{
    TypeId, TypeTable,
    source::Origin,
    types::{
        CharacterMetadata, Coercibility, CollationIdentity, IntervalEnd, IntervalUnit, Precision,
        TypeKind,
    },
};
use std::fmt;

/// Local verifier limits. These are not the product's G7 resource budgets.
#[derive(Clone, Copy, Debug)]
pub struct ValidationLimits {
    pub types: usize,
    pub references: usize,
    pub depth: usize,
    pub sources: usize,
    pub source_bytes: usize,
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self {
            types: 100_000,
            references: 1_000_000,
            depth: 256,
            sources: 10_000,
            source_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError {
    pub path: String,
    pub message: &'static str,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for ValidationError {}

type Result<T> = std::result::Result<T, ValidationError>;

fn invalid<T>(path: impl Into<String>, message: &'static str) -> Result<T> {
    Err(ValidationError {
        path: path.into(),
        message,
    })
}

impl TypeTable {
    /// Resolved references may widen outer nullability. Named identities and
    /// nested type IDs remain exact; this is not SQL coercion or type inference.
    pub(crate) fn reference_type_matches(&self, declared: TypeId, reference: TypeId) -> bool {
        if declared == reference {
            return true;
        }
        let declared = &self.types[declared.0 as usize];
        let reference = &self.types[reference.0 as usize];
        !matches!(declared.kind, TypeKind::Named { .. })
            && declared.kind == reference.kind
            && (!declared.nullable || reference.nullable)
    }

    /// Check every arena entry, including unreachable entries. This establishes
    /// structural validity, not that every type is supported by Java lowering.
    /// The caller must also bound input bytes before deserializing.
    pub fn validate(&self, limits: ValidationLimits) -> Result<()> {
        if self.types.len() > limits.types {
            return invalid("types", "type count exceeds the validation limit");
        }
        if self.sources.len() > limits.sources {
            return invalid("sources", "source count exceeds the validation limit");
        }
        let source_bytes = self.sources.iter().try_fold(0usize, |total, source| {
            total
                .checked_add(source.name.len())?
                .checked_add(source.text.len())
        });
        if source_bytes.is_none_or(|bytes| bytes > limits.source_bytes) {
            return invalid("sources", "source bytes exceed the validation limit");
        }
        if self.roots.len() > limits.references {
            return invalid("roots", "root count exceeds the reference limit");
        }
        for (index, root) in self.roots.iter().enumerate() {
            self.reference(*root, &format!("roots[{index}]"))?;
        }
        let mut remaining = limits.references - self.roots.len();
        let mut edges = Vec::with_capacity(self.types.len());
        for (index, ty) in self.types.iter().enumerate() {
            let path = format!("types[{index}]");
            self.origin(&ty.origin, &format!("{path}.origin"))?;
            self.kind(&ty.kind, &format!("{path}.kind"))?;
            let children = ty.kind.references();
            remaining = remaining
                .checked_sub(children.len())
                .ok_or_else(|| ValidationError {
                    path: path.clone(),
                    message: "type references exceed the validation limit",
                })?;
            for child in &children {
                self.reference(*child, &path)?;
            }
            edges.push(children);
        }
        check_graph(&edges, limits.depth)
    }

    pub(crate) fn reference(&self, id: TypeId, path: &str) -> Result<()> {
        if id.0 as usize >= self.types.len() {
            return invalid(path, "type reference is outside its arena");
        }
        Ok(())
    }

    pub(crate) fn origin(&self, origin: &Origin, path: &str) -> Result<()> {
        let (source, start, end, generated) = match origin {
            Origin::Absent {} => return Ok(()),
            Origin::Source { source, start, end } => (source, start, end, false),
            Origin::Generated { source, start, end } => (source, start, end, true),
        };
        if source.0 as usize >= self.sources.len() {
            return invalid(path, "source reference is outside its arena");
        }
        let coordinates = [start.line, start.column, end.line, end.column];
        if coordinates
            .iter()
            .any(|coordinate| *coordinate > i32::MAX as u32)
        {
            return invalid(
                path,
                "source coordinate exceeds the adapter's signed-integer range",
            );
        }
        let has_unknown = coordinates.contains(&0);
        if generated && (!has_unknown || coordinates == [0; 4]) {
            return invalid(
                path,
                "generated ranges require partially unknown coordinates",
            );
        }
        if !generated && has_unknown {
            return invalid(
                path,
                "source coordinates must be one-based; use absent for no origin",
            );
        }
        if start > end {
            return invalid(path, "source range ends before it starts");
        }
        // Source snippets and coordinates can refer to original submissions.
        // Exact text/range correspondence belongs to the submission adapter.
        Ok(())
    }

    fn kind(&self, kind: &TypeKind, path: &str) -> Result<()> {
        // Calcite represents each specified precision as a nonnegative Java int.
        // Preserve large supported values; do not apply the runtime DECIMAL cap.
        let precision = |value: Precision| match value {
            Precision::Specified(value) if value > i32::MAX as u32 => invalid(
                path,
                "type precision exceeds the adapter's signed-integer range",
            ),
            _ => Ok(()),
        };
        match kind {
            TypeKind::Decimal {
                precision: digits,
                scale,
                ..
            } => {
                precision(Precision::Specified(*digits))?;
                precision(Precision::Specified(*scale))?;
            }
            TypeKind::Character { length, .. } | TypeKind::Binary { length, .. } => {
                precision(*length)?
            }
            TypeKind::Time {
                precision: digits, ..
            }
            | TypeKind::Timestamp {
                precision: digits, ..
            } => precision(*digits)?,
            TypeKind::Interval {
                leading_precision,
                fractional_precision,
                ..
            } => {
                precision(*leading_precision)?;
                precision(*fractional_precision)?;
            }
            _ => {}
        }
        match kind {
            TypeKind::Decimal {
                precision, scale, ..
            } if *precision == 0 || scale > precision => {
                return invalid(
                    path,
                    "decimal needs positive precision and scale no greater than precision",
                );
            }
            TypeKind::Character {
                metadata:
                    CharacterMetadata::Present {
                        identity,
                        coercibility,
                        ..
                    },
                ..
            } => {
                let required = match identity {
                    CollationIdentity::BuiltinImplicit => Some(Coercibility::Implicit),
                    CollationIdentity::BuiltinCoercible => Some(Coercibility::Coercible),
                    CollationIdentity::Named => None,
                };
                if required.is_some_and(|required| required != *coercibility) {
                    return invalid(
                        path,
                        "built-in collation identity disagrees with coercibility",
                    );
                }
            }
            TypeKind::Row { fields, .. } => {
                for (index, field) in fields.iter().enumerate() {
                    self.origin(
                        &field.name.origin,
                        &format!("{path}.fields[{index}].name.origin"),
                    )?;
                }
            }
            TypeKind::Named { name, .. } => {
                if name.is_empty() {
                    return invalid(path, "named type needs a qualified name");
                }
                for (index, part) in name.iter().enumerate() {
                    self.origin(&part.origin, &format!("{path}.name[{index}].origin"))?;
                }
            }
            TypeKind::Interval {
                start,
                end,
                leading_precision,
                ..
            } => {
                if *leading_precision == Precision::Specified(0) {
                    return invalid(path, "interval leading precision must be positive");
                }
                if let IntervalEnd::Through(end) = end {
                    let year_month = |unit: IntervalUnit| {
                        matches!(
                            unit,
                            IntervalUnit::Year | IntervalUnit::Quarter | IntervalUnit::Month
                        )
                    };
                    if start >= end || year_month(*start) != year_month(*end) {
                        return invalid(
                            path,
                            "interval bounds must increase within one interval family",
                        );
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Iterative DFS avoids recursive stack overflow. Memoized heights also catch a
/// long path through a shared subtree previously visited through a short path.
fn check_graph(edges: &[Vec<TypeId>], max_depth: usize) -> Result<()> {
    #[derive(Clone, Copy)]
    enum State {
        Unseen,
        Active,
        Done(usize),
    }
    let mut states = vec![State::Unseen; edges.len()];
    for root in 0..edges.len() {
        if matches!(states[root], State::Done(_)) {
            continue;
        }
        let mut stack = vec![(root, 0)];
        states[root] = State::Active;
        while let Some((node, next)) = stack.last_mut() {
            if let Some(child) = edges[*node].get(*next) {
                *next += 1;
                let child = child.0 as usize;
                match states[child] {
                    State::Active => {
                        return invalid(format!("types[{child}]"), "cyclic type reference");
                    }
                    State::Done(_) => {}
                    State::Unseen => {
                        if stack.len() >= max_depth {
                            return invalid(
                                format!("types[{child}]"),
                                "type depth exceeds the validation limit",
                            );
                        }
                        states[child] = State::Active;
                        stack.push((child, 0));
                    }
                }
            } else {
                let node = *node;
                let height = edges[node]
                    .iter()
                    .map(|child| {
                        let State::Done(height) = states[child.0 as usize] else {
                            unreachable!("DFS child must be complete")
                        };
                        height
                    })
                    .max()
                    .unwrap_or(0)
                    + 1;
                if height > max_depth {
                    return invalid(
                        format!("types[{node}]"),
                        "type depth exceeds the validation limit",
                    );
                }
                states[node] = State::Done(height);
                stack.pop();
            }
        }
    }
    Ok(())
}
