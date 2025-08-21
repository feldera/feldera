use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use comfy_table::{Cell, Table};

pub(crate) fn create_table(results: &[RecordBatch]) -> Result<Table, ArrowError> {
    let options = FormatOptions::default().with_display_error(true);
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    for batch in results {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for formatter in &formatters {
                const CELL_MAX_LENGTH: usize = 64;
                let mut content = formatter.value(row).to_string();
                if content.len() > CELL_MAX_LENGTH {
                    content.truncate(CELL_MAX_LENGTH);
                    content.push_str("...");
                }
                cells.push(Cell::new(content));
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}
