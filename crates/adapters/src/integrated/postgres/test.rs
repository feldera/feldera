use postgres::NoTls;
use tempfile::NamedTempFile;

use crate::test::TestStruct;

fn postgres_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or("postgres://postgres:password@localhost:5432".to_string())
}

#[test]
fn test_postgres_simple() {
    let url = postgres_url();
    let table_name = "simple_test";

    let mut client = postgres::Client::connect(&url, NoTls).expect("failed to connect to postgres");
    client
        .execute(
            &format!(
                r#"CREATE TABLE IF NOT EXISTS {table_name} (
    id unsigned int primary key,
    b bool not null,
    i int,
    s varchar not null
)"#
            ),
            &[],
        )
        .expect("failed to create test table in postgres");

    let temp_file = NamedTempFile::new().unwrap();

    let schema = TestStruct::schema();
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
  file1:
    stream: test_input1
    transport:
      name: file_input
      config:
        path: {}
    format:
      name: json
      config:
        update_format: raw
        array: false
outputs:
  test_output1:
    stream: test_output1
    transport:
      name: postgres_output
      config:
        uri: {url}
        table: {table_name}
    index: v1_idx
"#,
        temp_file.path().display(),
    );
}
