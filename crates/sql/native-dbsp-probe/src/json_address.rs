//! The two nested-row conversions from the selected JSON program.

use crate::ErrorRow;
use dbsp::{
    Circuit, OrdZSet, OutputHandle, RootCircuit, ZSetHandle,
    operator::Generator,
    typed_batch::IndexedZSetReader,
    utils::{Tup1, Tup2, Tup3},
};
use feldera_native_probe::{CompiledNullableStringMap, Error};
use feldera_sqllib::{
    SqlString, SqlVariant, cast_to_i32N_FV, cast_to_sN_FV, from_json_string, indexFV__,
    parse_json_fv_s,
};
use feldera_types::deserialize_table_record;

pub type JsonAddressInput = Tup1<Option<SqlString>>;
type Address = Tup3<Option<SqlString>, Option<SqlString>, Option<i32>>;
pub type JsonAddressRow = Tup1<Option<Address>>;

pub struct JsonAddressPorts {
    pub input: ZSetHandle<JsonAddressInput>,
    pub parsed: OutputHandle<OrdZSet<JsonAddressRow>>,
    pub decoded: OutputHandle<OrdZSet<JsonAddressRow>>,
    pub errors: OutputHandle<OrdZSet<ErrorRow>>,
}

#[derive(Clone)]
pub struct JsonAddressSpec {
    pub source_column: usize,
    pub variant_fields: [String; 3],
}

pub fn build_json_address(
    circuit: &RootCircuit,
    spec: &JsonAddressSpec,
) -> Result<JsonAddressPorts, Error> {
    let (parsed, decoded) = JsonAddressProjection::compile(spec)?;
    let (source, input) = circuit.add_input_zset::<JsonAddressInput>();
    let parsed = source
        .apply(move |batch| parsed.evaluate(batch.iter().map(|(row, (), weight)| (row.0, weight))));
    let decoded = source.apply(move |batch| {
        decoded.evaluate(batch.iter().map(|(row, (), weight)| (row.0, weight)))
    });
    let errors = circuit
        .add_source(Generator::new(|| {
            OrdZSet::<ErrorRow>::from_keys((), vec![])
        }))
        .output();
    Ok(JsonAddressPorts {
        input,
        parsed: parsed.output(),
        decoded: decoded.output(),
        errors,
    })
}

#[derive(Default)]
struct JsonAddress {
    city: Option<SqlString>,
    street: Option<SqlString>,
    number: Option<i32>,
}

deserialize_table_record!(JsonAddress["address2", SqlVariant, 3] {
    (city, "city", false, Option<SqlString>, |_| Some(None)),
    (street, "street", false, Option<SqlString>, |_| Some(None)),
    (number, "number", false, Option<i32>, |_| Some(None))
});

/// Compiled JSON-to-address conversion over signed strings.
pub struct JsonAddressProjection {
    native: CompiledNullableStringMap,
    variant_fields: Option<[SqlString; 3]>,
}

impl JsonAddressProjection {
    /// Compiles the variant and record decoders selected by the frozen graph.
    pub fn compile(spec: &JsonAddressSpec) -> Result<(Self, Self), Error> {
        if spec.source_column != 0 {
            return Err(Error::InvalidProgram(
                "expected nullable VARCHAR at field 0".into(),
            ));
        }
        Ok((
            Self {
                native: CompiledNullableStringMap::compile()?,
                variant_fields: Some(
                    spec.variant_fields
                        .each_ref()
                        .map(|name| SqlString::from_ref(name)),
                ),
            },
            Self {
                native: CompiledNullableStringMap::compile()?,
                variant_fields: None,
            },
        ))
    }

    pub fn evaluate(
        &self,
        rows: impl IntoIterator<Item = (Option<SqlString>, i64)>,
    ) -> OrdZSet<JsonAddressRow> {
        let rows: Vec<_> = rows.into_iter().collect();
        let mut output = Vec::with_capacity(rows.len());
        for chunk in rows.chunks(65_536) {
            let strings: Vec<_> = chunk
                .iter()
                .map(|(text, _)| text.as_ref().map(SqlString::str))
                .collect();
            let addresses = match &self.variant_fields {
                Some(fields) => self
                    .native
                    .evaluate(&strings, |text| parse_address(text, fields)),
                None => self.native.evaluate(&strings, decode_address),
            }
            .expect("bounded native JSON batch");
            output.extend(
                chunk
                    .iter()
                    .zip(addresses)
                    .map(|((_, weight), value)| Tup2(Tup1(value), *weight)),
            );
        }
        OrdZSet::from_keys((), output)
    }
}

fn parse_address(text: &str, [city, street, number]: &[SqlString; 3]) -> Option<Address> {
    let value = parse_json_fv_s(SqlString::from_ref(text));
    let city = indexFV__(&value, city.clone())
        .map(|value| cast_to_sN_FV(value, -1, false))
        .transpose();
    let street = indexFV__(&value, street.clone())
        .map(|value| cast_to_sN_FV(value, -1, false))
        .transpose();
    let number = indexFV__(&value, number.clone())
        .map(cast_to_i32N_FV)
        .transpose();
    Some(Tup3(
        city.ok()?.flatten(),
        street.ok()?.flatten(),
        number.ok()?.flatten(),
    ))
}

fn decode_address(text: &str) -> Option<Address> {
    from_json_string::<JsonAddress>(text).map(|value| Tup3(value.city, value.street, value.number))
}
