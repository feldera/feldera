#![cfg(target_os = "linux")]

use feldera_native_probe::{
    ArchiveField, ArchiveInteger, ArchiveLayout, Arithmetic, Batch, CompiledArchive,
    CompiledProgram, Error, Expr, Program,
};

#[test]
fn fixed_width_archive_reader_checks_tags_widths_and_payloads() {
    let fields = [
        ArchiveInteger::Bool,
        ArchiveInteger::I32,
        ArchiveInteger::U32,
        ArchiveInteger::I64,
        ArchiveInteger::U64,
        ArchiveInteger::I128,
    ];
    let layout = ArchiveLayout::new(
        fields.len() * 32,
        fields
            .into_iter()
            .enumerate()
            .map(|(index, integer)| ArchiveField {
                integer,
                payload_offset: index * 32 + 16,
                null_tag_offset: (index == 1).then_some(32),
            })
            .collect(),
    )
    .unwrap();
    let writer = CompiledArchive::compile(layout).unwrap();
    let expected = vec![
        Some(1),
        None,
        Some(u32::MAX.into()),
        Some(i64::MIN.into()),
        Some(u64::MAX.into()),
        Some(i128::MIN),
    ];
    let batch = writer.encode_wide(std::slice::from_ref(&expected)).unwrap();
    let bytes = batch.rows().next().unwrap();
    assert_eq!(writer.decode_row(bytes).unwrap(), expected);
    assert!(writer.decode_row(&bytes[..bytes.len() - 1]).is_err());
    let mut invalid = bytes.to_vec();
    invalid[16] = 2;
    assert!(writer.decode_row(&invalid).is_err());
    invalid[16] = 1;
    invalid[32] = 2;
    assert!(writer.decode_row(&invalid).is_err());
}

fn arithmetic(op: Arithmetic, left: usize, right: usize) -> Expr {
    Expr::Arithmetic { op, left, right }
}

#[test]
fn checked_integer_boundaries_equal_the_independent_evaluator() {
    let values = [
        None,
        Some(i64::MIN),
        Some(i64::MIN + 1),
        Some(-4_294_967_296),
        Some(-3_037_000_500),
        Some(-2),
        Some(-1),
        Some(0),
        Some(1),
        Some(2),
        Some(3_037_000_499),
        Some(3_037_000_500),
        Some(i64::MAX - 1),
        Some(i64::MAX),
    ];
    for op in [Arithmetic::Add, Arithmetic::Subtract, Arithmetic::Multiply] {
        let program = Program::new(
            2,
            vec![Expr::Column(0), Expr::Column(1), arithmetic(op, 0, 1)],
        )
        .unwrap();
        let compiled = CompiledProgram::compile(&program).unwrap();
        for left in values {
            for right in values {
                let batch = Batch::from_rows(2, &[vec![left, right]]).unwrap();
                assert_eq!(
                    compiled.evaluate(&batch),
                    program.evaluate(&batch),
                    "{op:?}: {left:?}, {right:?}"
                );
            }
        }
    }
}

#[test]
fn batch_loop_keeps_row_order_nulls_and_first_error_position() {
    let program = Program::new(
        2,
        vec![
            Expr::Column(0),
            Expr::Column(1),
            arithmetic(Arithmetic::Add, 0, 1),
        ],
    )
    .unwrap();
    let compiled = CompiledProgram::compile(&program).unwrap();
    let rows = vec![
        vec![Some(1), Some(3)],
        vec![None, Some(7)],
        vec![Some(i64::MAX), Some(1)],
        vec![Some(i64::MIN), Some(-1)],
    ];
    let batch = Batch::from_rows(2, &rows).unwrap();
    assert_eq!(
        compiled.evaluate(&batch),
        Err(Error::Overflow {
            row: 2,
            op: Arithmetic::Add,
            left: i64::MAX,
            right: 1
        })
    );
    assert_eq!(
        program.evaluate(&batch),
        Err(Error::Overflow {
            row: 2,
            op: Arithmetic::Add,
            left: i64::MAX,
            right: 1
        })
    );
    let batch = Batch::from_rows(2, &rows[..2]).unwrap();
    assert_eq!(compiled.evaluate(&batch).unwrap(), vec![Some(4), None]);
    assert_eq!(
        compiled
            .evaluate(&Batch::from_rows(2, &[]).unwrap())
            .unwrap(),
        vec![]
    );
}

#[test]
fn coalesce_is_lazy_but_arithmetic_evaluates_both_children() {
    let mut expressions = vec![
        Expr::Column(0),
        Expr::Literal(Some(i64::MAX)),
        Expr::Literal(Some(1)),
        arithmetic(Arithmetic::Add, 1, 2),
        Expr::Coalesce(vec![0, 3]),
    ];
    let program = Program::new(1, expressions.clone()).unwrap();
    let compiled = CompiledProgram::compile(&program).unwrap();
    let batch = Batch::from_rows(1, &[vec![Some(42)], vec![Some(-7)]]).unwrap();
    assert_eq!(compiled.evaluate(&batch).unwrap(), vec![Some(42), Some(-7)]);
    let batch = Batch::from_rows(1, &[vec![Some(42)], vec![None]]).unwrap();
    assert_eq!(
        compiled.evaluate(&batch),
        Err(Error::Overflow {
            row: 1,
            op: Arithmetic::Add,
            left: i64::MAX,
            right: 1
        })
    );
    assert_eq!(compiled.evaluate(&batch), program.evaluate(&batch));

    expressions[4] = arithmetic(Arithmetic::Multiply, 0, 3);
    let program = Program::new(1, expressions).unwrap();
    let compiled = CompiledProgram::compile(&program).unwrap();
    let batch = Batch::from_rows(1, &[vec![None]]).unwrap();
    assert_eq!(
        compiled.evaluate(&batch),
        Err(Error::Overflow {
            row: 0,
            op: Arithmetic::Add,
            left: i64::MAX,
            right: 1
        })
    );
    assert_eq!(compiled.evaluate(&batch), program.evaluate(&batch));
}

#[test]
fn nested_nullable_expressions_match_across_many_batches() {
    let program = Program::new(
        3,
        vec![
            Expr::Column(0),
            Expr::Column(1),
            Expr::Column(2),
            arithmetic(Arithmetic::Multiply, 0, 1),
            Expr::Coalesce(vec![3, 2]),
            Expr::Literal(Some(11)),
            arithmetic(Arithmetic::Subtract, 4, 5),
        ],
    )
    .unwrap();
    let compiled = CompiledProgram::compile(&program).unwrap();
    let mut random = 0x5eed_u64;
    for length in 0..128 {
        let rows: Vec<_> = (0..length)
            .map(|_| {
                (0..3)
                    .map(|_| {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        (!random.is_multiple_of(5)).then_some((random % 100_001) as i64 - 50_000)
                    })
                    .collect()
            })
            .collect();
        let batch = Batch::from_rows(3, &rows).unwrap();
        assert_eq!(compiled.evaluate(&batch), program.evaluate(&batch));
    }
}

#[test]
fn generated_expression_graphs_preserve_lazy_branches_and_error_operands() {
    let mut random = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random
    };
    let values = [
        None,
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-1),
        Some(0),
        Some(1),
        Some(-3_037_000_500),
        Some(3_037_000_500),
    ];
    for graph in 0..128 {
        let mut expressions = vec![
            Expr::Column(0),
            Expr::Column(1),
            Expr::Literal(None),
            Expr::Literal(Some(i64::MAX)),
            Expr::Literal(Some(0)),
        ];
        for index in 5..15 {
            let left = next() as usize % index;
            let right = next() as usize % index;
            expressions.push(match next() % 4 {
                0 => arithmetic(Arithmetic::Add, left, right),
                1 => arithmetic(Arithmetic::Subtract, left, right),
                2 => arithmetic(Arithmetic::Multiply, left, right),
                _ => Expr::Coalesce(vec![left, right]),
            });
        }
        let program = Program::new(2, expressions).unwrap();
        let compiled = CompiledProgram::compile(&program).unwrap();
        for left in values {
            for right in values {
                let batch = Batch::from_rows(2, &[vec![left, right]]).unwrap();
                assert_eq!(
                    compiled.evaluate(&batch),
                    program.evaluate(&batch),
                    "graph {graph}: {program:?}, inputs {left:?}, {right:?}"
                );
            }
        }
    }
}

#[test]
fn highest_null_bit_and_maximum_batch_length_work() {
    let program = Program::new(64, vec![Expr::Column(63)]).unwrap();
    let compiled = CompiledProgram::compile(&program).unwrap();
    let mut row = vec![Some(17); 64];
    row[63] = None;
    let batch = Batch::from_rows(64, &[row, vec![Some(-23); 64]]).unwrap();
    assert_eq!(compiled.evaluate(&batch).unwrap(), vec![None, Some(-23)]);
    let program = Program::new(1, vec![Expr::Column(0)]).unwrap();
    let compiled = CompiledProgram::compile(&program).unwrap();
    let rows: Vec<_> = (0..65_536)
        .map(|row| vec![(row % 3 != 0).then_some(row)])
        .collect();
    let batch = Batch::from_rows(1, &rows).unwrap();
    assert_eq!(compiled.evaluate(&batch), program.evaluate(&batch));
}

#[test]
fn constants_need_no_input_payload_and_libraries_have_independent_lifetimes() {
    for value in [None, Some(i64::MIN), Some(0), Some(i64::MAX)] {
        let program = Program::new(0, vec![Expr::Literal(value), Expr::Coalesce(vec![0])]).unwrap();
        let first = CompiledProgram::compile(&program).unwrap();
        let second = CompiledProgram::compile(&program).unwrap();
        let batch = Batch::from_rows(0, &[vec![], vec![]]).unwrap();
        drop(first);
        assert_eq!(second.evaluate(&batch).unwrap(), vec![value, value]);
    }
}

#[test]
fn malformed_programs_and_batches_fail_before_native_access() {
    for (columns, expressions) in [
        (65, vec![Expr::Literal(None)]),
        (0, vec![]),
        (1, vec![Expr::Column(1)]),
        (0, vec![Expr::Coalesce(vec![])]),
        (0, vec![Expr::Coalesce(vec![0])]),
        (0, vec![Expr::Coalesce(vec![1]), Expr::Literal(None)]),
        (0, vec![Expr::Literal(None); 4097]),
    ] {
        assert!(matches!(
            Program::new(columns, expressions),
            Err(Error::InvalidProgram(_))
        ));
    }
    let mut deep = vec![Expr::Literal(None)];
    for index in 0..64 {
        deep.push(Expr::Coalesce(vec![index]));
    }
    assert!(matches!(
        Program::new(0, deep),
        Err(Error::InvalidProgram(_))
    ));
    let mut expanded = vec![Expr::Literal(Some(1))];
    for index in 0..13 {
        expanded.push(arithmetic(Arithmetic::Add, index, index));
    }
    assert!(matches!(
        Program::new(0, expanded),
        Err(Error::InvalidProgram(_))
    ));
    assert!(Batch::from_rows(1, &[vec![]]).is_err());
    assert!(Batch::from_rows(65, &[]).is_err());
    assert!(Batch::from_rows(0, &vec![vec![]; 65_537]).is_err());
    assert!(Batch::from_rows(64, &vec![vec![None; 64]; 16_385]).is_err());
    let program = Program::new(1, vec![Expr::Column(0)]).unwrap();
    let compiled = CompiledProgram::compile(&program).unwrap();
    let wrong_schema = Batch::from_rows(0, &[vec![]]).unwrap();
    assert!(matches!(
        compiled.evaluate(&wrong_schema),
        Err(Error::InvalidBatch(_))
    ));
    assert_eq!(
        compiled.evaluate(&wrong_schema),
        program.evaluate(&wrong_schema)
    );
}
