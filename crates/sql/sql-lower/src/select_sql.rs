//! Render SQL provenance for a chain of scans, filters, projections and groups.

use super::{
    projection::{self, Field, Projection},
    Error,
};
use feldera_sql_rel_model::catalog::CatalogSchema;
use serde_json::Value;

/// The relational SQL printer can inline a projection or a grouping into an
/// existing SELECT. Keep the expressions separate so a subsequent group can
/// refer to its input by ordinal even when expressions contain commas or AS.
pub(super) struct SelectSql {
    expressions: Vec<SqlExpression>,
    from: String,
    grouped: bool,
    table: String,
    aliases: usize,
}

#[derive(Clone)]
struct SqlExpression {
    text: String,
    precedence: u8,
}

impl SelectSql {
    pub(super) fn scan(
        origin: &Value,
        schema: &CatalogSchema,
        filtered: bool,
    ) -> Result<Self, Error> {
        let from = origin["sql"]
            .as_str()
            .and_then(|s| s.strip_prefix("SELECT *\n"))
            .ok_or_else(|| Error("missing FROM clause for scan".into()))?
            .to_owned();
        Ok(Self {
            expressions: names(schema),
            from,
            grouped: false,
            table: schema.name.name.clone(),
            aliases: usize::from(filtered),
        })
    }

    fn alias(&self) -> String {
        // Calcite reserves the table name, then allocates t, t0, t1, ... for
        // the intermediate relations. Skip a candidate already used by the scan.
        std::iter::once("t".to_owned())
            .chain((0..).map(|i| format!("t{i}")))
            .filter(|name| !name.eq_ignore_ascii_case(&self.table))
            .nth(self.aliases - 1)
            .expect("unbounded alias sequence")
    }

    fn wrap(&mut self, input: &CatalogSchema) -> Result<(), Error> {
        self.from = format!("FROM ({}) AS `{}`", self.render(input, true)?, self.alias());
        self.expressions = names(input);
        self.grouped = false;
        Ok(())
    }

    pub(super) fn project(&mut self, project: &Projection<'_>) -> Result<(), Error> {
        if self.grouped
            && project
                .fields
                .iter()
                .all(|f| matches!(f, Field::Literal(_) | Field::NullableLiteral(_)))
        {
            self.wrap(project.input)?;
        }
        let input = |index| {
            self.expressions
                .get(index)
                .ok_or_else(|| Error("projection outside grouped input".into()))
        };
        self.expressions = project
            .fields
            .iter()
            .map(|field| {
                Ok(match field {
                    Field::Column(i) => input(*i)?.clone(),
                    Field::Computed(call) => SqlExpression {
                        text: call.sql(&|i, precedence| {
                            let expr = input(i)?;
                            Ok(if expr.precedence < precedence {
                                format!("({})", expr.text)
                            } else {
                                expr.text.clone()
                            })
                        })?,
                        precedence: call.precedence(),
                    },
                    value => SqlExpression {
                        text: value.expression_sql(project.input)?,
                        precedence: u8::MAX,
                    },
                })
            })
            .collect::<Result<_, Error>>()?;
        self.aliases += 1;
        Ok(())
    }

    pub(super) fn group(&mut self, keys: &[u32], input: &CatalogSchema) -> Result<(), Error> {
        if self.grouped {
            self.wrap(input)?;
        }
        self.expressions = keys
            .iter()
            .map(|&i| {
                self.expressions
                    .get(i as usize)
                    .cloned()
                    .ok_or_else(|| Error("grouping key outside input".into()))
            })
            .collect::<Result<_, _>>()?;
        self.from.push_str("\nGROUP BY ");
        self.from.push_str(
            &self
                .expressions
                .iter()
                .map(|e| e.text.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        );
        self.grouped = true;
        self.aliases += 1;
        Ok(())
    }

    pub(super) fn sql(&self, schema: &CatalogSchema) -> Result<String, Error> {
        self.render(schema, false)
    }

    fn render(&self, schema: &CatalogSchema, derived_table: bool) -> Result<String, Error> {
        if schema.columns.len() != self.expressions.len() {
            return Err(Error("SELECT list width differs from relation".into()));
        }
        let list = self
            .expressions
            .iter()
            .zip(&schema.columns)
            .map(|(expr, col)| {
                let name = &col.name.name;
                let quoted = projection::quote(name);
                if derived_table && expr.text != quoted {
                    format!("{} AS {quoted}", expr.text)
                } else {
                    projection::aliased(expr.text.clone(), name)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("SELECT {list}\n{}", self.from))
    }
}

fn names(schema: &CatalogSchema) -> Vec<SqlExpression> {
    schema
        .columns
        .iter()
        .map(|col| SqlExpression {
            text: projection::quote(&col.name.name),
            precedence: u8::MAX,
        })
        .collect()
}
