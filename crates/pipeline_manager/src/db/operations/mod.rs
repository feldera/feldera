// Database operations that can be performed, grouped into categories.
// Every operation is implemented to be part of a transaction.
// These operations are building blocks and should be used accordingly as incorrect
// usage may result in a panic.
//
// An operation should:
// * Upon success, always result in a consistent database state
// * Otherwise:
//   - An error if the parameterized operation failed in the current database state
//     but could be valid in another database state (for example, trying to set
//     program compilation status to Success while it is currently Pending).
//   - Panic if the operation is invalid in any database state (for example, trying
//     to provide an program error message when transitioning to Success state).
pub mod api_key;
pub mod connectivity;
pub mod pipeline;
pub mod tenant;
pub(crate) mod utils;
