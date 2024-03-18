use anyhow::Result;
use dbsp::{
    circuit::CircuitConfig,
    utils::{Tup2, Tup3},
    Runtime,
};
use itertools::Itertools;
use serde_json::json;

type Json = ijson::IValue;

fn parse_json(val: String) -> Json {
    serde_json::from_str(&val).expect("failed to deserialize json")
}

fn json_field(val: Json, field: &str) -> Json {
    val[field].clone()
}

fn cast_to_string_json(val: Json) -> String {
    serde_json::to_string(&val).expect("failed to cast to string")
}

fn json_index(val: Json, mut idx: usize) -> Json {
    // start indexing at 1, to be consisent with other array functions
    idx = idx
        .checked_sub(1)
        .expect("invalid: JSON_INDEX called with index 0");

    val[idx].clone()
}

fn main() -> Result<()> {
    let (mut dbsp, (input, output)) =
        Runtime::init_circuit(CircuitConfig::with_workers(1), |circuit| {
            let (stream0, handle0) = circuit.add_input_zset::<Tup3<String, String, usize>>();

            let stream1 = stream0.map(move |Tup3(json, field, idx)| {
                Tup3::new(parse_json(json.clone()), field.to_owned(), *idx)
            });

            let stream2 = stream1.map(move |Tup3(json, field, idx)| {
                Tup2::new(json_index(json.clone(), *idx), field.to_owned())
            });

            let stream3 = stream2.map(move |Tup2(json, field)| json_field(json.clone(), field));

            let stream4 = stream3.map(move |json| cast_to_string_json(json.clone()));

            let handle1 = stream4.output();

            Ok((handle0, handle1))
        })?;

    let data = vec![
        json!(
            [
                {"song": "Fairies Wear Boots", "artist": "Black Sabbath"},
                {"song": "Whole Lotta Love", "artist": "Led Zeppelin"},
                {"song": "Hysteria", "artist": "Muse"}
            ]
        ),
        json!(
            [
                {"song": "Whole Lotta Love", "artist": "Led Zeppelin"},
                {"song": "Fairies Wear Boots", "artist": "Black Sabbath"}
            ]
        ),
        json!(
            [
                {"song": "Hysteria", "artist": "Muse"}
            ]
        ),
    ];

    let expected = data
        .iter()
        .map(|x| x[0]["artist"].to_string())
        .collect_vec();

    for datum in data {
        input.push(Tup3::new(datum.to_string(), "artist".to_owned(), 1), 1);
    }

    dbsp.step()?;

    _ = dbsp.kill();

    let got = output
        .consolidate()
        .iter()
        .map(|(x, _, _)| x)
        .sorted()
        .collect_vec();

    assert_eq!(expected, got);

    println!("artists: {got:#?}");

    Ok(())
}
