use std::collections::VecDeque;

/// The LogsBuffer maintains internally a circular buffer of Strings whose
/// size in byte and number of elements does not exceed the limits.
/// When appending new log lines (Strings) to the buffer, the limits are
/// enforced by discarding existing lines if the limits would be exceeded.
pub struct LogsBuffer {
    /// Buffer size limit in byte.
    size_limit_byte: usize,
    /// Buffer size limit in number of lines.
    size_limit_num_lines: usize,
    /// The lines buffer.
    buffer: VecDeque<String>,
    /// Current lines buffer size.
    size_byte: usize,
    /// Number of lines that have been discarded to enforce size limit.
    num_discarded_lines: usize,
}

impl LogsBuffer {
    /// Construct a new logs buffer.
    pub fn new(size_limit_byte: usize, size_limit_num_lines: usize) -> Self {
        Self {
            size_limit_byte,
            size_limit_num_lines,
            buffer: VecDeque::new(),
            size_byte: 0,
            num_discarded_lines: 0,
        }
    }

    /// Append a new line to the buffer.
    /// - If the new line exceeds the buffer size limit by itself, all the lines in the buffer and
    ///   the new line are discarded, leaving an empty buffer.
    /// - Otherwise, lines are removed from the buffer until the new line will fit. Once there
    ///   is sufficient space, the line is added to the buffer.
    pub fn append(&mut self, line: String) {
        if line.len() > self.size_limit_byte {
            self.num_discarded_lines += self.buffer.len() + 1;
            self.buffer.clear();
            self.size_byte = 0;
        } else {
            // Ensure size in byte is not exceeded
            while self.size_byte + line.len() > self.size_limit_byte {
                let popped_line = self
                    .buffer
                    .pop_front()
                    .expect("Cannot remove log line even though size is non-zero");
                self.size_byte -= popped_line.len();
                self.num_discarded_lines += 1;
            }

            // Ensure size in number of lines is not exceeded
            if self.size_limit_num_lines > 0 {
                while self.buffer.len() + 1 > self.size_limit_num_lines {
                    let popped_line = self
                        .buffer
                        .pop_front()
                        .expect("Cannot remove log line even though length is non-zero");
                    self.size_byte -= popped_line.len();
                    self.num_discarded_lines += 1;
                }
                self.size_byte += line.len();
                self.buffer.push_back(line);
            }
        }
    }

    /// Retrieves the lines in the buffer.
    pub fn lines(&self) -> &VecDeque<String> {
        &self.buffer
    }

    /// Retrieves the number of lines in the buffer.
    pub fn num_lines(&self) -> usize {
        self.buffer.len()
    }

    /// Retrieves the number of lines discarded due to buffer limit enforcement.
    pub fn num_discarded_lines(&self) -> usize {
        self.num_discarded_lines
    }

    /// Retrieves the total buffer size.
    pub fn size_byte(&self) -> usize {
        self.size_byte
    }

    /// Retrieves the buffer size limit in byte.
    pub fn size_limit_byte(&self) -> usize {
        self.size_limit_byte
    }

    /// Retrieves the buffer size limit in number of lines.
    pub fn size_limit_num_lines(&self) -> usize {
        self.size_limit_num_lines
    }
}

#[cfg(test)]
mod test {
    use super::LogsBuffer;
    use std::collections::VecDeque;

    #[test]
    #[rustfmt::skip] // Skip formatting to keep the test cases readable
    fn logs_buffer_variety() {
        let test_cases: Vec<(usize, usize, Vec<&str>, Vec<&str>)> = vec![
            // Potentially exceed number of byte
            (0, 1000, vec!["a"], vec![]),
            (0, 1000, vec!["a"], vec![]),
            (0, 1000, vec!["a", "b"], vec![]),
            (1, 1000, vec!["a"], vec!["a"]),
            (1, 1000, vec!["a", "b"], vec!["b"]),
            (1, 1000, vec!["a", "b", "c"], vec!["c"]),
            (2, 1000, vec!["a", "b", "c"], vec!["b", "c"]),
            (2, 1000, vec!["a", "b", "c", "d"], vec!["c", "d"]),
            // Potentially exceed number of lines
            (1000, 0, vec![], vec![]),
            (1000, 0, vec!["a"], vec![]),
            (1000, 0, vec!["a", "b"], vec![]),
            (1000, 1, vec!["a"], vec!["a"]),
            (1000, 1, vec!["a", "b"], vec!["b"]),
            (1000, 1, vec!["a", "b", "c"], vec!["c"]),
            (1000, 2, vec!["a", "b", "c"], vec!["b", "c"]),
            (1000, 2, vec!["a", "b", "c", "d"], vec!["c", "d"]),
            // Empty lines exceed number of lines
            (1000, 0, vec![""], vec![]),
            (1000, 1, vec![""], vec![""]),
            (1000, 1, vec!["", ""], vec![""]),
            (1000, 5, vec!["", "", "", "", "", ""], vec!["", "", "", "", ""]),
            // Exceed both potentially
            (0, 0, vec!["a"], vec![]),
            (0, 0, vec!["a", "b"], vec![]),
            (1, 1, vec!["a"], vec!["a"]),
            (1, 1, vec!["a", "b"], vec!["b"]),
            (1, 1, vec!["a", "b", "c"], vec!["c"]),
            (2, 2, vec!["a", "b", "c"], vec!["b", "c"]),
            (2, 2, vec!["a", "b", "c", "d"], vec!["c", "d"]),
            // Others
            (5, 10, vec!["abc", "def"], vec!["def"]),
            (5, 2, vec!["abc", "def"], vec!["def"]),
            (6, 2, vec!["abc", "def"], vec!["abc", "def"]),
        ];

        // Run the test cases
        for (limit_byte, limit_num_lines, input, output) in test_cases {
            let mut buffer = LogsBuffer::new(limit_byte, limit_num_lines);
            for s in &input {
                buffer.append(s.to_string().clone());
            }
            let mut expected = VecDeque::new();
            for s in &output {
                expected.push_back(s.to_string());
            }
            assert_eq!(buffer.lines(), &expected, "Failed for test case (lb={}, ln={}, i={:?}) -> o={:?}", limit_byte, limit_num_lines, input, output);
        }
    }

    #[test]
    fn logs_buffer_normal() {
        // Buffer with 20 byte and 5 lines limit
        let mut buffer = LogsBuffer::new(20, 5);
        assert_eq!(buffer.lines(), &VecDeque::from([]));
        assert_eq!(buffer.num_lines(), 0);
        assert_eq!(buffer.num_discarded_lines(), 0);
        assert_eq!(buffer.size_byte(), 0);
        assert_eq!(buffer.size_limit_byte(), 20);
        assert_eq!(buffer.size_limit_num_lines(), 5);

        // Exceed the byte limit
        buffer.append("abcde".to_string());
        buffer.append("fghij".to_string());
        buffer.append("klmno".to_string());
        buffer.append("pqrst".to_string());
        assert_eq!(buffer.num_discarded_lines(), 0);
        buffer.append("uvwxyz1".to_string());
        assert_eq!(
            buffer.lines(),
            &VecDeque::from([
                "klmno".to_string(),
                "pqrst".to_string(),
                "uvwxyz1".to_string()
            ])
        );
        assert_eq!(buffer.num_lines(), 3);
        assert_eq!(buffer.num_discarded_lines(), 2);
        assert_eq!(buffer.size_byte(), 17);
        assert_eq!(buffer.size_limit_byte(), 20);
        assert_eq!(buffer.size_limit_num_lines(), 5);

        // Exceed the number of lines limit
        buffer.append("2".to_string());
        buffer.append("3".to_string());
        assert_eq!(buffer.num_discarded_lines(), 2);
        buffer.append("4".to_string());
        assert_eq!(
            buffer.lines(),
            &VecDeque::from([
                "pqrst".to_string(),
                "uvwxyz1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string()
            ])
        );
        assert_eq!(buffer.num_lines(), 5);
        assert_eq!(buffer.num_discarded_lines(), 3);
        assert_eq!(buffer.size_byte(), 15);
        assert_eq!(buffer.size_limit_byte(), 20);
        assert_eq!(buffer.size_limit_num_lines(), 5);

        // Exceed the number of bytes with a string larger than can fit in buffer
        buffer.append("aaaaabbbbbcccccddddde".to_string());
        assert_eq!(buffer.num_lines(), 0);
        assert_eq!(buffer.num_discarded_lines(), 9);
        assert_eq!(buffer.size_byte(), 0);
        assert_eq!(buffer.size_limit_byte(), 20);
        assert_eq!(buffer.size_limit_num_lines(), 5);
    }
}
