//! The selected RANGE COUNT window over nullable DECIMAL(6, 2).

use crate::{
    IntegerCasePorts, IntegerCaseRow,
    integer_input::{evaluate_columns, output_ports},
};
use dbsp::{
    OrdZSet, RootCircuit, Stream,
    algebra::DefaultSemigroup,
    operator::{dynamic::aggregate::Fold, time_series::RelRange},
    utils::{Tup0, Tup1, Tup2},
};
use feldera_native_probe::{Arithmetic, CompiledProgram, Error, Expr, Program};
use feldera_sqllib::{SqlDecimal, UnsignedWrappers, decimal_to_integerN, integer_to_decimalN};

pub type RollingProjectedRow = Tup2<i32, Option<SqlDecimal<6, 2>>>;
pub type RollingCountRow = Tup2<i32, i64>;

pub fn build_rolling_count(
    circuit: &RootCircuit,
    projected_columns: [usize; 2],
    range: RelRange<u64>,
) -> Result<IntegerCasePorts<RollingCountRow>, Error> {
    let (source, input) = circuit.add_input_zset::<IntegerCaseRow>();
    let source = source.map(|row| Tup2(row.0, row.5));
    let output = rolling_count_projected(&source, projected_columns, range)?;
    Ok(output_ports(circuit, input, output))
}

/// Builds the selected rolling window over the retained input columns.
pub fn rolling_count_projected(
    projected: &Stream<RootCircuit, OrdZSet<RollingProjectedRow>>,
    projected_columns: [usize; 2],
    range: RelRange<u64>,
) -> Result<Stream<RootCircuit, OrdZSet<RollingCountRow>>, Error> {
    if projected_columns != [0, 5] {
        return Err(Error::InvalidProgram(
            "expected INTEGER at field 0 and nullable DECIMAL(6, 2) at field 5".into(),
        ));
    }

    // Match UnsignedWrappers::from_option for i32 mantissas, ascending with
    // NULLs first. All DECIMAL(6, 2) mantissas fit in i32.
    let bias = 1 - i64::from(i32::MIN);
    let timestamp = CompiledProgram::compile(&Program::new(
        1,
        vec![
            Expr::Column(0),
            Expr::Literal(Some(bias)),
            Expr::Arithmetic {
                op: Arithmetic::Add,
                left: 0,
                right: 1,
            },
            Expr::Literal(Some(0)),
            Expr::Coalesce(vec![2, 3]),
        ],
    )?)?;
    let indexed = evaluate_columns(projected, timestamp, |row| {
        [decimal_to_integerN(row.1).map(|mantissa| {
            i64::try_from(mantissa).expect("DECIMAL(6, 2) mantissa fits in BIGINT")
        })]
    })
    .map_index(|Tup2(row, timestamp)| {
        let timestamp = u64::try_from(timestamp.expect("timestamp is never NULL"))
            .expect("wrapped timestamp is nonnegative");
        (timestamp, Tup2(Tup0::new(), *row))
    });

    let rolling = indexed.partitioned_rolling_aggregate(
        |value| (value.0, value.1),
        Fold::<_, Tup1<i64>, DefaultSemigroup<Tup1<i64>>, _, _>::new(
            Tup1(0),
            |count: &mut Tup1<i64>, _: &RollingProjectedRow, weight| count.0 += weight,
        ),
        range,
    );
    let counts = rolling.map_index(|(_, value)| {
        let mantissa =
            UnsignedWrappers::to_signed_option::<i32, i32, i64, u64>(value.0, true, false);
        (
            Tup1(integer_to_decimalN::<6, 2>(mantissa.map(i128::from))),
            value.1.unwrap_or(Tup1(0)),
        )
    });
    let by_decimal = projected.map_index(|row| (Tup1(row.1), Tup1(row.0)));
    Ok(by_decimal.join_balanced(&counts, |_, id, count| Tup2(id.0, count.0)))
}
