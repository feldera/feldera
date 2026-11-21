//! Whole-relation decimal arithmetic through the selected Postgres join graphs.

use crate::{ErrorRow, integer_input::evaluate_columns};
use dbsp::{
    Circuit, OrdZSet, OutputHandle, RootCircuit, Stream, ZSetHandle,
    operator::Generator,
    utils::{Tup0, Tup1, Tup2, Tup3, Tup4},
};
use feldera_native_probe::{Arithmetic, Comparison, CompiledProgram, Error, Expr, Program};
use feldera_sqllib::{
    SqlDecimal, cast_to_SqlDecimalN_SqlDecimalN, plus_SqlDecimalN_SqlDecimalN,
    times_SqlDecimalN_SqlDecimalN,
};

type Decimal = SqlDecimal<38, 10>;
pub type NumericRow = Tup2<Option<i32>, Option<Decimal>>;
pub type ExpectedDecimalRow = Tup3<Option<i32>, Option<i32>, Option<Decimal>>;
type JoinedRow = Tup4<i32, i32, Option<Decimal>, Option<Decimal>>;
pub type DecimalMismatchRow = Tup4<Option<i32>, Option<i32>, Option<Decimal>, Option<Decimal>>;

pub enum DecimalInputPort {
    SingleKey(ZSetHandle<NumericRow>),
    PairKey(ZSetHandle<ExpectedDecimalRow>),
}

#[derive(Clone, Copy)]
pub enum DecimalOperation {
    Add,
    Multiply,
}

impl DecimalOperation {
    fn evaluate(self, left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
        match self {
            Self::Add => plus_SqlDecimalN_SqlDecimalN::<38, 10, 38, 10, 38, 10>(left, right),
            Self::Multiply => {
                let product = times_SqlDecimalN_SqlDecimalN::<38, 10, 38, 10, 38, 20>(left, right);
                cast_to_SqlDecimalN_SqlDecimalN::<38, 10, 38, 20>(product)
                    .expect("cast product to DECIMAL(38, 10)")
            }
        }
    }
}

pub struct DecimalJoinPorts {
    pub inputs: Vec<DecimalInputPort>,
    pub output: OutputHandle<OrdZSet<DecimalMismatchRow>>,
    pub compared: OutputHandle<OrdZSet<DecimalMismatchRow>>,
    pub errors: OutputHandle<OrdZSet<ErrorRow>>,
}

pub fn build_decimal_join(
    circuit: &RootCircuit,
    operation: DecimalOperation,
    expected_source: usize,
) -> Result<DecimalJoinPorts, Error> {
    if !(1..=4).contains(&expected_source) {
        return Err(Error::InvalidProgram(
            "expected a two-key reference input".into(),
        ));
    }
    let (source, data) = circuit.add_input_zset::<NumericRow>();
    let mut ports = vec![DecimalInputPort::SingleKey(data)];
    let mut expected = None;
    for index in 1..=4 {
        let (stream, input) = circuit.add_input_zset::<ExpectedDecimalRow>();
        ports.push(DecimalInputPort::PairKey(input));
        if index == expected_source {
            expected = Some(stream);
        }
    }
    for _ in 0..4 {
        let (_, input) = circuit.add_input_zset::<NumericRow>();
        ports.push(DecimalInputPort::SingleKey(input));
    }

    let outputs = decimal_join_projected(
        &source,
        &expected.expect("validated reference source"),
        operation,
    )?;
    Ok(DecimalJoinPorts {
        inputs: ports,
        output: outputs.output,
        compared: outputs.compared,
        errors: outputs.errors,
    })
}

pub struct DecimalJoinOutputs {
    pub output: OutputHandle<OrdZSet<DecimalMismatchRow>>,
    pub compared: OutputHandle<OrdZSet<DecimalMismatchRow>>,
    pub errors: OutputHandle<OrdZSet<ErrorRow>>,
}

/// Builds the bounded decimal comparison from its two live input relations.
pub fn decimal_join_projected(
    source: &Stream<RootCircuit, OrdZSet<NumericRow>>,
    expected: &Stream<RootCircuit, OrdZSet<ExpectedDecimalRow>>,
    operation: DecimalOperation,
) -> Result<DecimalJoinOutputs, Error> {
    let not_equal = CompiledProgram::compile(&Program::new(
        4,
        vec![
            Expr::Column(0),
            Expr::Column(1),
            Expr::Column(2),
            Expr::Column(3),
            Expr::Compare {
                op: Comparison::NotEqual,
                left: 0,
                right: 2,
            },
            Expr::Compare {
                op: Comparison::NotEqual,
                left: 1,
                right: 3,
            },
            Expr::Arithmetic {
                op: Arithmetic::Add,
                left: 4,
                right: 5,
            },
        ],
    )?)?;

    let by_unit = source.map_index(|row| (Tup0::new(), *row));
    let pairs = by_unit.stream_join(&by_unit, |_, left, right| {
        Tup4(left.0, left.1, right.0, right.1)
    });
    let actual = pairs.flat_map_index(move |row| {
        row.0
            .zip(row.2)
            .map(|(first, second)| (Tup2(first, second), Tup1(operation.evaluate(row.1, row.3))))
    });
    let expected = expected.flat_map_index(|row| {
        row.0
            .zip(row.1)
            .map(|(first, second)| (Tup2(first, second), Tup1(row.2)))
    });
    let joined = actual.stream_join(&expected, |ids, actual, expected| {
        Tup4(ids.0, ids.1, actual.0, expected.0)
    });
    let compared = joined.map(mismatch_row).output();
    let output = evaluate_columns(&joined, not_equal, comparison_columns)
        .flat_map(|Tup2(row, differences)| {
            differences
                .is_some_and(|n| n > 0)
                .then(|| mismatch_row(row))
        })
        .output();
    let errors = source
        .circuit()
        .add_source(Generator::new(|| {
            OrdZSet::<ErrorRow>::from_keys((), vec![])
        }))
        .output();
    Ok(DecimalJoinOutputs {
        output,
        compared,
        errors,
    })
}

fn mismatch_row(row: &JoinedRow) -> DecimalMismatchRow {
    Tup4(Some(row.0), Some(row.1), row.2, row.3)
}

fn comparison_columns(row: &JoinedRow) -> [Option<i64>; 4] {
    // Splitting a signed 128-bit mantissa into two words preserves equality.
    // Each missing decimal makes both comparisons NULL, as SQL <> requires.
    let parts = |value: Option<Decimal>| {
        let value = value.map(|value| value.mantissa());
        [
            value.map(|value| value as i64),
            value.map(|value| (value >> 64) as i64),
        ]
    };
    let [actual_low, actual_high] = parts(row.2);
    let [expected_low, expected_high] = parts(row.3);
    [actual_low, actual_high, expected_low, expected_high]
}
