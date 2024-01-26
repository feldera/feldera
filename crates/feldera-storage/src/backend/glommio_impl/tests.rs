//! Tests for [`GlommioBackend`].
//!
//! The main test makes sure we correspond to the model defined in
//! [`InMemoryBackend`].

use pretty_assertions::assert_eq;
use proptest::proptest;
use proptest::test_runner::Config;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use tempfile::TempDir;

use crate::backend::glommio_impl::GlommioBackend;
use crate::backend::tests::{InMemoryBackend, Transition, MAX_TRANSITIONS};
use crate::backend::{
    FileHandle, ImmutableFileHandle, StorageControl, StorageExecutor, StorageRead, StorageWrite,
};

prop_state_machine! {
    #![proptest_config(Config {
        verbose: 1,
        .. Config::default()
    })]
    #[test]
    fn glommio_behaves_like_model(
        sequential
        1..MAX_TRANSITIONS
        =>
        GlommioBackend
    );
}

pub struct GlommioTest {
    backend: GlommioBackend,
    _tmpdir: TempDir,
}

impl StateMachineTest for GlommioBackend {
    type SystemUnderTest = GlommioTest;
    type Reference = InMemoryBackend<true>;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        let _tmpdir = tempfile::tempdir().unwrap();
        let backend = GlommioBackend::new(_tmpdir.path());

        GlommioTest { backend, _tmpdir }
    }

    fn apply(
        state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::Create => {
                state.backend.block_on(async {
                    let _r = state.backend.create().await.expect("create failed");
                });
                state
            }
            Transition::DeleteMut(id) => {
                state.backend.block_on(async {
                    state
                        .backend
                        .delete_mut(FileHandle(id))
                        .await
                        .expect("delete failed");
                });
                state
            }
            Transition::Write(id, offset, content) => {
                state.backend.block_on(async {
                    let mut wb = GlommioBackend::allocate_buffer(content.len());
                    wb.resize(content.len(), 0);
                    wb.copy_from_slice(content.as_bytes());
                    state
                        .backend
                        .write_block(&FileHandle(id), offset, wb)
                        .await
                        .expect("write failed");
                });
                state
            }
            Transition::Complete(id) => {
                state.backend.block_on(async {
                    state
                        .backend
                        .complete(FileHandle(id))
                        .await
                        .expect("complete failed");
                });
                state
            }
            Transition::Read(id, offset, length) => {
                let result_impl = state.backend.block_on(async {
                    state
                        .backend
                        .read_block(&ImmutableFileHandle(id), offset, length as usize)
                        .await
                });
                let model_impl = futures::executor::block_on(ref_state.read_block(
                    &ImmutableFileHandle(id),
                    offset,
                    length as usize,
                ));
                assert_eq!(&model_impl, &result_impl);
                state
            }
        }
    }

    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // TODO: add invariant checks from `monoio::tests`
    }
}
