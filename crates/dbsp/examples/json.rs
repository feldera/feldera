use anyhow::Result;
use dbsp::{
    circuit::CircuitConfig,
    utils::{Tup2, Tup3, Tup4},
    Runtime,
};
use itertools::Itertools;
use serde_json::json;

type Json = ijson::IValue;

fn parse_json(val: String) -> Json {
    serde_json::from_str(&val).expect("failed to deserialize json")
}

fn json_field(val: Json, field: &str) -> Json {
    val.get(field)
        .expect("invalid: JSON_FIELD called with non existent field")
        .to_owned()
}

fn to_string(val: Json) -> String {
    serde_json::to_string(&val).expect("failed to cast to string")
}

fn json_index(val: Json, mut idx: usize) -> Json {
    // start indexing at 1, to be consisent with other array functions
    idx = idx
        .checked_sub(1)
        .expect("invalid: JSON_INDEX called with index 0");

    val.get(idx)
        .expect("invalid: JSON_INDEX called on a value that isn't an array literal")
        .to_owned()
}

fn as_array(val: Json) -> Vec<Json> {
    let arr = val
        .into_array()
        .expect("invalid: AS_ARRAY called on a value that isn't an array literal");

    arr.into_iter().collect_vec()
}

// from sqllib: src/array.rs
fn map<T, S, F>(vector: &[T], func: F) -> Vec<S>
where
    F: FnMut(&T) -> S,
{
    vector.iter().map(func).collect()
}

const WORKERS: usize = 2;

fn main() {
    circuit0().unwrap();
    circuit1().unwrap();
}

fn circuit0() -> Result<()> {
    let (mut dbsp, (input, output)) =
        Runtime::init_circuit(CircuitConfig::with_workers(WORKERS), |circuit| {
            let (stream0, handle0) = circuit.add_input_zset::<Tup3<String, String, usize>>();

            let stream1 = stream0.map(move |Tup3(json, field, idx)| {
                Tup3::new(parse_json(json.clone()), field.to_owned(), *idx)
            });

            let stream2 = stream1.map(move |Tup3(json, field, idx)| {
                Tup2::new(json_index(json.clone(), *idx), field.to_owned())
            });

            let stream3 = stream2.map(move |Tup2(json, field)| json_field(json.clone(), field));

            let stream4 = stream3.map(move |json| to_string(json.clone()));

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

    dbsp.transaction()?;

    _ = dbsp.kill();

    let got = output
        .consolidate()
        .iter()
        .map(|(x, (), _)| x)
        .sorted()
        .collect_vec();

    assert_eq!(expected, got);

    println!("artists: {got:#?}");

    Ok(())
}

fn circuit1() -> Result<()> {
    let (mut dbsp, (input, output)) =
        Runtime::init_circuit(CircuitConfig::with_workers(WORKERS), |circuit| {
            let (stream0, handle0) =
                circuit.add_input_zset::<Tup4<String, String, String, usize>>();

            // parse string to json
            let stream1 = stream0.map(move |Tup4(json, field1, field2, idx)| {
                Tup4::new(
                    parse_json(json.clone()),
                    field1.clone(),
                    field2.clone(),
                    *idx,
                )
            });

            // get the songs field
            let stream2 = stream1.map(move |Tup4(json, field1, field2, idx)| {
                Tup3::new(json_field(json.clone(), field1), field2.clone(), *idx)
            });

            // convert JSON array literals to JSON ARRAY (Vec<Json>)
            let stream3 = stream2.map(move |Tup3(json, field, idx)| {
                Tup3::new(as_array(json.clone()), field.clone(), *idx)
            });

            // extract a field from an all JSON ARRAYs
            let stream4 = stream3.map(move |Tup3(json_vec, field, idx)| {
                Tup2::new(map(json_vec, |x| json_field(x.clone(), field)), *idx)
            });

            // index the JSON array literal
            let stream5 = stream4
                .map(move |Tup2(json_vec, idx)| map(json_vec, |x| json_index(x.clone(), *idx)));

            // convert JSON to string
            let stream6 = stream5.map(move |json_vec| map(json_vec, |x| to_string(x.clone())));

            let handle1 = stream6.output();

            Ok((handle0, handle1))
        })?;

    let data = vec![json!(
            {
              "songs": [
                {
                  "title": "Fairies Wear Boots",
                  "artist": "Black Sabbath",
                  "album": "Paranoid",
                  "release_year": 1970,
                  "genre": ["Heavy Metal", "Hard Rock"]
                },
                {
                  "title": "Whole Lotta Love",
                  "artist": "Led Zeppelin",
                  "album": "Led Zeppelin II",
                  "release_year": 1969,
                  "genre": ["Hard Rock", "Blues Rock"]
                },
                {
                  "title": "Hysteria",
                  "artist": "Muse",
                  "album": "Absolution",
                  "release_year": 2003,
                  "genre": ["Alternative Rock", "Art Rock"]
                },
                {
                  "title": "Bohemian Rhapsody",
                  "artist": "Queen",
                  "album": "A Night at the Opera",
                  "release_year": 1975,
                  "genre": ["Progressive Rock", "Symphonic Rock"]
                },
                {
                  "title": "Hotel California",
                  "artist": "Eagles",
                  "album": "Hotel California",
                  "release_year": 1976,
                  "genre": ["Rock", "Soft Rock"]
                },
                {
                  "title": "Smells Like Teen Spirit",
                  "artist": "Nirvana",
                  "album": "Nevermind",
                  "release_year": 1991,
                  "genre": ["Grunge", "Alternative Rock"]
                },
                {
                  "title": "Stairway to Heaven",
                  "artist": "Led Zeppelin",
                  "album": "Led Zeppelin IV",
                  "release_year": 1971,
                  "genre": ["Hard Rock", "Folk Rock"]
                },
                {
                  "title": "Imagine",
                  "artist": "John Lennon",
                  "album": "Imagine",
                  "release_year": 1971,
                  "genre": ["Soft Rock", "Pop"]
                },
                {
                  "title": "Yesterday",
                  "artist": "The Beatles",
                  "album": "Help!",
                  "release_year": 1965,
                  "genre": ["Folk Rock", "Baroque Pop"]
                }
              ]
            }
    )];

    let expected: Vec<String> = data
        .iter()
        .filter_map(|x| x["songs"].as_array())
        .flatten()
        .map(|x| x["genre"][0].to_string())
        .sorted()
        .collect();

    for datum in data {
        input.push(
            Tup4::new(datum.to_string(), "songs".to_owned(), "genre".to_owned(), 1),
            1,
        );
    }

    dbsp.transaction()?;

    _ = dbsp.kill();

    let got = output
        .consolidate()
        .iter()
        .flat_map(|(x, (), _)| x)
        .sorted()
        .collect_vec();

    assert_eq!(expected, got);

    println!("genre: {got:#?}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit0() {
        circuit0().unwrap();
    }

    #[test]
    fn test_circuit1() {
        circuit1().unwrap();
    }
}
