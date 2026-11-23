//! CSE compares the printed representation of an expression after replacing
//! each operand with its first representative. Different IR forms can have
//! identical spellings, so the form itself cannot be part of the interned key.
use feldera_sql_ir::Error;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fmt::{self, Display, Write};

pub(super) struct Number(pub u64);

impl Display for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}#", self.0)
    }
}

pub(super) struct Key {
    pub text: String,
    pub declarations: BTreeSet<u64>,
    pub expensive: bool,
    pub wrapper: bool,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub(super) enum Form {
    Literal {
        r#type: String,
        value: Option<String>,
    },
    Path {
        text: String,
    },
    Read,
    Variable {
        declaration: u64,
    },
    Let {
        name: String,
        declaration: u64,
    },
    Binary {
        r#type: String,
        opcode: String,
        symbol: String,
    },
    Unary {
        r#type: String,
        opcode: String,
        symbol: String,
    },
    Call {
        r#type: String,
        method: bool,
    },
    Cast {
        r#type: String,
        safety: String,
        source_string: bool,
        target_string: bool,
    },
    Tuple {
        r#type: String,
    },
    RawTuple,
    AbsentTuple {
        r#type: String,
        value: String,
    },
    Field {
        index: usize,
    },
    Simple {
        operator: Operator,
    },
}

#[derive(Deserialize)]
pub(super) enum Operator {
    Borrow,
    Clone,
    Deref,
    If,
    IsNull,
    #[serde(rename = "Some")]
    WrapSome,
}

fn argument_list(mut text: String, args: &[Number], suffix: &str) -> String {
    for arg in args {
        write!(text, "{arg},").expect("writing to a string");
    }
    text.push(')');
    text.push_str(suffix);
    text
}

impl Form {
    pub fn key(self, operands: &[Number]) -> Result<Key, Error> {
        use Form::*;
        use Operator::*;
        let mut declarations = BTreeSet::new();
        let mut expensive = false;
        let wrapper = matches!(
            &self,
            Tuple { .. }
                | RawTuple
                | AbsentTuple { .. }
                | Simple {
                    operator: Borrow | Clone | Deref
                }
        );
        let text = match (self, operands) {
            (Literal { r#type: ty, value }, []) => match value {
                Some(value) => format!("{value}:{ty}"),
                None => format!("None::{ty}"),
            },
            (Path { text }, []) => text,
            (Variable { declaration }, []) => {
                declarations.insert(declaration);
                declaration.to_string()
            }
            (Let { name, declaration }, [initializer, consumer]) => {
                declarations.insert(declaration);
                format!("let {name} = {initializer} in {consumer}")
            }
            (
                Binary {
                    r#type: ty,
                    opcode,
                    symbol,
                },
                [left, right],
            ) => {
                expensive = matches!(
                    opcode.as_str(),
                    "MUL"
                        | "MAP_CONVERT"
                        | "MUL_INTERVAL"
                        | "ADD"
                        | "SUB"
                        | "DIV"
                        | "DIV_NULL"
                        | "MOD"
                        | "MUL_WEIGHT"
                        | "CONCAT"
                        | "IS_DISTINCT"
                        | "SQL_INDEX"
                        | "MAP_INDEX"
                        | "VARIANT_INDEX"
                        | "RUST_INDEX"
                        | "DIV_INTERVAL"
                        | "ADD_SATURATING"
                        | "SUB_SATURATING"
                        | "MUL_SATURATING"
                        | "DIV_SATURATING"
                        | "MUL_INTERVAL_SATURATING"
                        | "DIV_INTERVAL_SATURATING"
                );
                format!("({ty}){left}{symbol}{right}")
            }
            (
                Unary {
                    r#type: ty,
                    opcode,
                    symbol,
                },
                [source],
            ) => {
                expensive = opcode != "NOT";
                format!("({ty}){symbol}{source}")
            }
            (
                Call {
                    r#type: ty,
                    method: false,
                },
                [function, args @ ..],
            ) => {
                expensive = true;
                argument_list(format!("{function}("), args, &ty)
            }
            (
                Call {
                    r#type: ty,
                    method: true,
                },
                [this, function, args @ ..],
            ) => {
                expensive = true;
                argument_list(format!("{this}.{function}("), args, &ty)
            }
            (
                Cast {
                    r#type: ty,
                    safety,
                    source_string,
                    target_string,
                },
                [source],
            ) => {
                expensive = source_string || target_string;
                format!("{safety}({ty},{safety}){source}")
            }
            (AbsentTuple { r#type: ty, value }, []) => format!("{value}:{ty}"),
            (Tuple { r#type: ty }, fields) => {
                expensive = !fields.is_empty();
                argument_list(format!("({ty})Tup("), fields, "")
            }
            (RawTuple, fields) => {
                expensive = !fields.is_empty();
                argument_list("(".into(), fields, "")
            }
            (Field { index }, [base]) => format!("{base}.{index}"),
            (Simple { operator: Borrow }, [value]) => format!("&{value}"),
            (Simple { operator: Clone }, [value]) => format!("{value}.clone()"),
            (Simple { operator: Deref }, [value]) => format!("*{value}"),
            (Simple { operator: IsNull }, [value]) => format!("{value}.is_null()"),
            (Simple { operator: WrapSome }, [value]) => format!("Some({value})"),
            (Simple { operator: If }, [condition, yes]) => format!("if{condition}?{yes}"),
            (Simple { operator: If }, [condition, yes, no]) => format!("if{condition}?{yes}:{no}"),
            _ => {
                return Err(Error::new(
                    "CSE expression has the wrong number of operands",
                ));
            }
        };
        Ok(Key {
            text,
            declarations,
            expensive,
            wrapper,
        })
    }
}
