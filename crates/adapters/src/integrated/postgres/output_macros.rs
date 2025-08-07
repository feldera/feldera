#[macro_export]
macro_rules! buffer_op {
    (
        $this:ident,
        buf = $buf_field:ident,
        counter = $counter_field:ident,
        stmt = $stmt_field:ident,
        flush_fn = $flush_fn:ident,
        name = $name:literal,
        value = $value:ident
    ) => {{
        let max_buf = $this.config.max_buffer_size_bytes;
        let max_records = $this.config.max_records_in_buffer.unwrap_or(usize::MAX);

        if $this.$buf_field.is_empty() {
            $this.$buf_field.push(b'[');
        }

        if $value.len() + $this.$buf_field.len() + 1 >= max_buf
            || $this.$counter_field >= max_records
        {
            $this.$flush_fn();
        }

        $this.$counter_field += 1;
        if $this.$buf_field.last() != Some(&b'[') {
            $this.$buf_field.push(b',');
        }

        $this.$buf_field.append(&mut $value);
    }};
}

/// Flushes the appropriate buffer and resets it to `[`.
/// Resets the appropriate counter to 0.
#[macro_export]
macro_rules! flush_op {
    (
        $this:ident,
        buf = $buf_field:ident,
        counter = $counter_field:ident,
        stmt = $stmt_field:ident,
        name = $name:literal
    ) => {{
        let stmt = &$this.prepared_statements.$stmt_field;

        let val = $this.$buf_field.drain(..).collect();
        $this.exec_statement(stmt.clone(), val, $name);
        $this.$counter_field = 0;
        $this.$buf_field.push(b'[');
    }};
}
