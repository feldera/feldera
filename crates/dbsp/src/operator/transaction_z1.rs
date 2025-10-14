use std::{borrow::Cow, sync::Arc};

use feldera_storage::{FileCommitter, StoragePath};
use size_of::SizeOf;

use crate::{
    circuit::{
        checkpointer::Checkpoint,
        operator_traits::{Operator, UnaryOperator},
        GlobalNodeId, OwnershipPreference,
    },
    operator::require_persistent_id,
    storage::file::to_bytes,
    Circuit, Error, NumEntries, Runtime, Scope, Stream,
};

impl<C, D> Stream<C, D>
where
    C: Circuit,
{
    /// Applies [`TransactionZ1`] operator to `self`.
    #[track_caller]
    pub fn transaction_delay_with_initial_value(&self, initial: D) -> Stream<C, D>
    where
        D: Checkpoint + SizeOf + NumEntries + Clone + 'static,
    {
        let delay_pid = self
            .get_persistent_id()
            .map(|pid| format!("{pid}.transaction_delay"));

        self.circuit()
            .add_unary_operator(TransactionZ1::new(initial.clone()), self)
            .set_persistent_id(delay_pid.as_deref())
    }
}

pub struct TransactionZ1<T> {
    zero: T,
    // For error reporting,
    global_id: GlobalNodeId,
    empty_output: bool,
    flush: bool,
    old_value: T,
    new_value: T,
}

#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct CommittedTransactionZ1 {
    old_value: Vec<u8>,
    new_value: Vec<u8>,
}

impl<T> TryFrom<&TransactionZ1<T>> for CommittedTransactionZ1
where
    T: Checkpoint,
{
    type Error = Error;

    fn try_from(z1: &TransactionZ1<T>) -> Result<CommittedTransactionZ1, Error> {
        Ok(CommittedTransactionZ1 {
            old_value: z1.old_value.checkpoint()?,
            new_value: z1.new_value.checkpoint()?,
        })
    }
}

impl<T> TransactionZ1<T>
where
    T: Checkpoint + Clone,
{
    pub fn new(zero: T) -> Self {
        Self {
            empty_output: false,
            flush: false,
            global_id: GlobalNodeId::root(),
            zero: zero.clone(),
            old_value: zero.clone(),
            new_value: zero.clone(),
        }
    }

    /// Return the absolute path of the file for this operator.
    ///
    /// # Arguments
    /// - `cid`: The checkpoint id.
    /// - `persistent_id`: The persistent id that identifies the spine within
    ///   the circuit for a given checkpoint.
    fn checkpoint_file<P: AsRef<str>>(base: &StoragePath, persistent_id: P) -> StoragePath {
        base.child(format!("transaction-z1-{}.dat", persistent_id.as_ref()))
    }
}

impl<T> Operator for TransactionZ1<T>
where
    T: Checkpoint + SizeOf + NumEntries + Clone + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Transaction Z^-1")
    }

    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {
        self.empty_output = false;
        self.new_value = self.zero.clone();
        self.old_value = self.zero.clone();
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        if scope == 0 {
            self.new_value.num_entries_shallow() == 0
                && self.old_value.num_entries_shallow() == 0
                && self.empty_output
        } else {
            true
        }
    }

    fn checkpoint(
        &mut self,
        base: &StoragePath,
        persistent_id: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;

        let committed: CommittedTransactionZ1 = (self as &Self).try_into()?;
        let as_bytes =
            to_bytes(&committed).expect("Serializing CommittedTransactionZ1 should work.");
        files.push(
            Runtime::storage_backend()
                .unwrap()
                .write(&Self::checkpoint_file(base, persistent_id), as_bytes)?,
        );
        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;

        let z1_path = Self::checkpoint_file(base, persistent_id);
        let content = Runtime::storage_backend().unwrap().read(&z1_path)?;
        let committed = unsafe { rkyv::archived_root::<CommittedTransactionZ1>(&content) };

        let mut old_value = self.zero.clone();
        let mut new_value = self.zero.clone();
        old_value.restore(committed.old_value.as_slice())?;
        new_value.restore(committed.new_value.as_slice())?;
        self.empty_output = false;
        self.old_value = old_value;
        self.new_value = new_value;
        Ok(())
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        self.empty_output = false;
        self.old_value = self.zero.clone();
        self.new_value = self.zero.clone();
        Ok(())
    }

    fn flush(&mut self) {
        self.flush = true;
    }
}

impl<T> UnaryOperator<T, T> for TransactionZ1<T>
where
    T: Checkpoint + SizeOf + NumEntries + Clone + 'static,
{
    async fn eval(&mut self, i: &T) -> T {
        if self.flush {
            self.flush = false;
            self.old_value = self.new_value.clone();
            self.new_value = i.clone();
        }

        self.old_value.clone()
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

#[cfg(test)]
mod test {
    use crate::{
        circuit::operator_traits::{Operator, UnaryOperator},
        operator::TransactionZ1,
    };

    #[tokio::test]
    async fn transaction_z1_test() {
        let mut z1 = TransactionZ1::new(0);

        z1.clock_start(0);
        assert_eq!(z1.eval(&1).await, 0);
        assert_eq!(z1.eval(&2).await, 0);
        assert_eq!(z1.eval(&3).await, 0);

        z1.flush();
        assert_eq!(z1.eval(&4).await, 0);

        assert_eq!(z1.eval(&5).await, 0);
        assert_eq!(z1.eval(&6).await, 0);

        z1.flush();
        assert_eq!(z1.eval(&7).await, 4);

        assert_eq!(z1.eval(&8).await, 4);
        assert_eq!(z1.eval(&9).await, 4);

        z1.clock_end(0);

        assert_eq!(z1.eval(&10).await, 0);
    }
}
