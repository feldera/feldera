use proptest::{proptest, test_runner::Config};
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use std::sync::Arc;
use tempfile::TempDir;

use crate::storage::{
    backend::{
        monoio_impl::MonoioBackend,
        tests::{InMemoryBackend, Transition, MAX_TRANSITIONS},
        FileHandle, ImmutableFileHandle, StorageControl, StorageExecutor, StorageRead,
        StorageWrite,
    },
    buffer_cache::{BufferCache, TinyLfuCache},
    test::init_test_logger,
};

#[monoio::test]
#[should_panic]
async fn overlaps_test() {
    use super::FBuf;
    use crate::storage::backend::tests::InMemoryBackend;

    let cache = BufferCache::with_backend_lfu(
        InMemoryBackend::<true>::default(),
        Arc::new(TinyLfuCache::default()),
    );
    let fd = cache.create().await.unwrap();
    let _fd2 = cache.create().await.unwrap();

    let mut b1 = FBuf::with_capacity(2048);
    b1.extend_from_slice(&vec![b'a'; 2048]);

    cache.write_block(&fd, 1024, b1).await.unwrap();

    let mut b2 = FBuf::with_capacity(1024);
    b2.extend_from_slice(&vec![b'a'; 1024]);

    cache.write_block(&fd, 512, b2).await.unwrap();
}

// Set up the state machine test using the `prop_state_machine!` macro
prop_state_machine! {
    #![proptest_config(Config {
        verbose: 1,
        .. Config::default()
    })]

    #[test]
    fn cached_monoio_behaves_like_model(
        sequential
        1..MAX_TRANSITIONS
        =>
        BufferCache<MonoioBackend>
    );
}

pub struct BufferCacheTest {
    backend: BufferCache<MonoioBackend>,
    _tmpdir: TempDir,
}

impl StateMachineTest for BufferCache<MonoioBackend> {
    type SystemUnderTest = BufferCacheTest;
    type Reference = InMemoryBackend<false>;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        init_test_logger();

        let _tmpdir = tempfile::tempdir().unwrap();
        let storage_backend = MonoioBackend::new(_tmpdir.path(), Default::default());
        let backend = BufferCache::with_backend_lfu(storage_backend, Default::default());

        BufferCacheTest { backend, _tmpdir }
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
                        .delete_mut(FileHandle::new(id))
                        .await
                        .expect("delete failed");
                });
                state
            }
            Transition::Write(id, offset, content) => {
                let r = state.backend.block_on(async {
                    let mut wb = Self::allocate_buffer(content.len());
                    wb.resize(content.len(), 0);
                    wb.copy_from_slice(content.as_bytes());
                    state
                        .backend
                        .write_block(&FileHandle::new(id), offset, wb)
                        .await
                });
                if ref_state.error.is_some() || r.is_err() {
                    assert_eq!(ref_state.error, r.err());
                }
                state
            }
            Transition::Complete(id) => {
                state.backend.block_on(async {
                    state
                        .backend
                        .complete(FileHandle::new(id))
                        .await
                        .expect("complete failed");
                });
                state
            }
            Transition::Read(id, offset, length) => {
                let result_impl = state.backend.block_on(async {
                    state
                        .backend
                        .read_block(&ImmutableFileHandle::new(id), offset, length as usize)
                        .await
                });
                let model_impl = futures::executor::block_on(ref_state.read_block(
                    &ImmutableFileHandle::new(id),
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
    }
}
