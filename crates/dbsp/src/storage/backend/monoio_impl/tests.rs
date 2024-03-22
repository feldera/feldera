//! Tests for [`MonoioBackend`].
//!
//! The main test makes sure we correspond to the model defined in
//! [`InMemoryBackend`].
use std::fs;

use pretty_assertions::assert_eq;
use proptest::{proptest, test_runner::Config};
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use tempfile::TempDir;

use crate::storage::{
    backend::{
        monoio_impl::MonoioBackend,
        tests::{InMemoryBackend, Transition, MAX_TRANSITIONS},
        FileHandle, ImmutableFileHandle, Storage,
    },
    test::init_test_logger,
};

// Setup the state machine test using the `prop_state_machine!` macro
prop_state_machine! {
    #![proptest_config(Config {
        verbose: 1,
        .. Config::default()
    })]

    #[test]
    fn monoio_behaves_like_model(
        sequential
        1..MAX_TRANSITIONS
        =>
        MonoioBackend
    );
}

pub struct MonoioTest {
    backend: MonoioBackend,
    tmpdir: TempDir,
}

impl StateMachineTest for MonoioBackend {
    type SystemUnderTest = MonoioTest;
    type Reference = InMemoryBackend<true>;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        init_test_logger();
        let tmpdir = tempfile::tempdir().unwrap();
        let backend = MonoioBackend::new(tmpdir.path(), Default::default());

        MonoioTest { backend, tmpdir }
    }

    fn apply(
        state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::Create => {
                let _r = state.backend.create().expect("create failed");
                state
            }
            Transition::DeleteMut(id) => {
                state
                    .backend
                    .delete_mut(FileHandle(id))
                    .expect("delete failed");
                state
            }
            Transition::Write(id, offset, content) => {
                let mut wb = MonoioBackend::allocate_buffer(content.len());
                wb.resize(content.len(), 0);
                wb.copy_from_slice(content.as_bytes());
                state
                    .backend
                    .write_block(&FileHandle(id), offset, wb)
                    .expect("write failed");
                state
            }
            Transition::Complete(id) => {
                state
                    .backend
                    .complete(FileHandle(id))
                    .expect("complete failed");
                state
            }
            Transition::Read(id, offset, length) => {
                let result_impl =
                    state
                        .backend
                        .read_block(&ImmutableFileHandle(id), offset, length as usize);
                let model_impl =
                    ref_state.read_block(&ImmutableFileHandle(id), offset, length as usize);
                assert_eq!(&model_impl, &result_impl);
                state
            }
        }
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // inv1: the immutable file contents of model and implementation must match

        // inv2: we don't need more storage space than the in-memory implementation
        let all_bytes: usize = ref_state
            .immutable_files
            .borrow()
            .values()
            .map(|v| v.len())
            .sum::<usize>()
            + ref_state
                .files
                .borrow()
                .values()
                .map(|v| v.len())
                .sum::<usize>();

        let paths = fs::read_dir(&state.tmpdir).unwrap();
        let files_bytes = paths
            .into_iter()
            .map(|p| {
                if let Ok(p) = p {
                    fs::metadata(p.path())
                        .expect("Can't get metadata for {p}")
                        .len() as usize
                } else {
                    0
                }
            })
            .sum::<usize>();

        assert_eq!(files_bytes, all_bytes);
    }
}
