/// Returns the index of the first character following the last newline
/// in `data`.
pub(crate) fn split_on_newline(data: &[u8]) -> usize {
    let data_len = data.len();
    let index = data
        .iter()
        .rev()
        .position(|&x| x == b'\n')
        .unwrap_or(data_len);

    data_len - index
}
