//! Replay storage for the exactly-once Kafka sink.
//!
//! The sink keeps a copy of every record of the open transaction, so that it
//! can abort the transaction and send the records again after a delivery
//! failure. Retrying the open transaction for as long as it takes is not an
//! option: librdkafka fails a transaction at its first failed delivery and
//! purges the queued records when the transaction is aborted, and the broker
//! aborts a transaction older than `transaction.max.timeout.ms` (15 minutes
//! by default). librdkafka also frees an acknowledged message before the
//! commit, so a transaction can hold far more than the producer queue. The
//! spool keeps the records in memory up to a limit and spills to an unnamed
//! temporary file above it. The file goes away at commit or when the sink is
//! dropped.
//!
//! The file lives in the pipeline's storage directory when there is one, since
//! the system temporary directory of a container is often small, read-only or
//! a `tmpfs` that counts against the pod's memory.

use anyhow::{Context, Result as AnyResult, anyhow};
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// One record of the open transaction, as the sink sends it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SizeOf)]
pub(crate) struct SpooledRecord {
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

impl SpooledRecord {
    /// Bytes this record holds in memory, including the container overhead.
    pub fn size(&self) -> usize {
        self.size_of().total_bytes()
    }
}

/// The spilled part of the spool.
struct SpoolFile {
    writer: BufWriter<File>,
    records: u64,
}

/// Records of the open transaction, in send order.
pub(crate) struct ReplaySpool {
    memory_limit: usize,
    /// Where the spool file goes; the system temporary directory if `None`.
    spill_dir: Option<PathBuf>,
    memory: Vec<SpooledRecord>,
    memory_bytes: usize,
    file: Option<SpoolFile>,
    records: u64,
    bytes: u64,
}

impl ReplaySpool {
    /// A spool that spills to a temporary file in `spill_dir`, or in the
    /// system temporary directory, once its records hold more than
    /// `memory_limit` bytes.
    pub fn new(memory_limit: usize, spill_dir: Option<PathBuf>) -> Self {
        Self {
            memory_limit,
            spill_dir,
            memory: Vec::new(),
            memory_bytes: 0,
            file: None,
            records: 0,
            bytes: 0,
        }
    }

    /// Number of records in the spool.
    pub fn len(&self) -> u64 {
        self.records
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    /// Bytes of record data in the spool, in memory and on disk.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Bytes the spool holds in memory.
    pub fn memory_bytes(&self) -> usize {
        self.memory_bytes
    }

    /// True once the spool has spilled to a file.
    #[cfg(test)]
    pub fn is_spilled(&self) -> bool {
        self.file.is_some()
    }

    /// Where the spool file goes, for error messages.
    pub fn location(&self) -> String {
        match &self.spill_dir {
            Some(dir) => format!("'{}'", dir.display()),
            None => "the system temporary directory".to_owned(),
        }
    }

    /// Appends `record`. An I/O error leaves the spool incomplete, so the
    /// caller must not commit the transaction after one.
    pub fn push(&mut self, record: SpooledRecord) -> AnyResult<()> {
        let size = record.size();
        if self.file.is_none() && self.memory_bytes + size > self.memory_limit {
            let file = match &self.spill_dir {
                Some(dir) => tempfile::tempfile_in(dir).with_context(|| {
                    format!(
                        "creating the replay spool file for Kafka output in '{}'",
                        dir.display()
                    )
                })?,
                None => tempfile::tempfile()
                    .context("creating the replay spool file for Kafka output")?,
            };
            let mut writer = BufWriter::new(file);
            for record in &self.memory {
                Self::write(&mut writer, record)?;
            }
            self.file = Some(SpoolFile {
                writer,
                records: self.records,
            });
            self.memory = Vec::new();
            self.memory_bytes = 0;
        }
        match &mut self.file {
            Some(file) => {
                Self::write(&mut file.writer, &record)?;
                file.records += 1;
            }
            None => {
                self.memory_bytes += size;
                self.memory.push(record);
            }
        }
        self.records += 1;
        self.bytes += size as u64;
        Ok(())
    }

    fn write(writer: &mut BufWriter<File>, record: &SpooledRecord) -> AnyResult<()> {
        rmp_serde::encode::write(writer, record)
            .context("writing a record to the replay spool file for Kafka output")
    }

    /// Calls `f` on every record in send order, memory first and then the
    /// file, with the record's index in the spool. Stops at the first error.
    /// The spool stays intact, so it can be replayed again and appended to.
    pub fn replay(
        &mut self,
        mut f: impl FnMut(u64, &SpooledRecord) -> AnyResult<()>,
    ) -> AnyResult<()> {
        for (index, record) in self.memory.iter().enumerate() {
            f(index as u64, record)?;
        }
        let Some(file) = &mut self.file else {
            return Ok(());
        };
        file.writer
            .flush()
            .context("flushing the replay spool file for Kafka output")?;
        let inner = file.writer.get_mut();
        inner
            .seek(SeekFrom::Start(0))
            .context("rewinding the replay spool file for Kafka output")?;
        let result = (|| {
            let mut deserializer = rmp_serde::Deserializer::new(BufReader::new(&mut *inner));
            for index in 0..file.records {
                let record = SpooledRecord::deserialize(&mut deserializer)
                    .map_err(|e| anyhow!("reading the replay spool file for Kafka output: {e}"))?;
                f(index, &record)?;
            }
            Ok(())
        })();
        inner
            .seek(SeekFrom::End(0))
            .context("repositioning the replay spool file for Kafka output")?;
        result
    }

    /// Drops every record and releases the temporary file.
    pub fn clear(&mut self) {
        self.memory.clear();
        self.memory_bytes = 0;
        self.file = None;
        self.records = 0;
        self.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(i: usize) -> SpooledRecord {
        SpooledRecord {
            partition: (i % 3) as i32,
            key: Some(format!("key {i}").into_bytes()),
            payload: (!i.is_multiple_of(4)).then(|| vec![i as u8; 100 + i]),
            headers: vec![
                ("h".to_owned(), Some(vec![1, 2, 3])),
                ("null".to_owned(), None),
            ],
        }
    }

    fn collect(spool: &mut ReplaySpool) -> Vec<SpooledRecord> {
        let mut out = Vec::new();
        spool
            .replay(|index, r| {
                assert_eq!(index, out.len() as u64, "indexes follow send order");
                out.push(r.clone());
                Ok(())
            })
            .unwrap();
        out
    }

    /// The spool spills to a file past its memory limit and replays every
    /// record, with null payloads, headers and partitions intact, as often as
    /// needed, while later records still append after a replay.
    #[test]
    fn a_spool_spills_and_replays_in_order() {
        let limit = 1000;
        let mut spool = ReplaySpool::new(limit, None);
        let expected: Vec<_> = (0..20).map(record).collect();
        for r in &expected[..10] {
            spool.push(r.clone()).unwrap();
        }
        assert!(spool.is_spilled(), "{limit} bytes must not hold 10 records");
        assert_eq!(spool.memory_bytes(), 0);
        assert_eq!(collect(&mut spool), &expected[..10]);
        for r in &expected[10..] {
            spool.push(r.clone()).unwrap();
        }
        assert_eq!(spool.len(), 20);
        assert_eq!(collect(&mut spool), expected);
        assert_eq!(collect(&mut spool), expected, "replay must be repeatable");
        spool.clear();
        assert!(spool.is_empty());
        assert!(!spool.is_spilled());
        assert_eq!(spool.bytes(), 0);
        assert_eq!(collect(&mut spool), Vec::new());
    }

    /// Below the limit the spool stays in memory and replays from there.
    #[test]
    fn a_small_spool_stays_in_memory() {
        let mut spool = ReplaySpool::new(1 << 20, None);
        let expected: Vec<_> = (0..5).map(record).collect();
        for r in &expected {
            spool.push(r.clone()).unwrap();
        }
        assert!(!spool.is_spilled());
        assert!(spool.memory_bytes() > 0);
        assert_eq!(collect(&mut spool), expected);
    }

    /// The spool file goes where the sink says, and a directory that cannot
    /// take it fails the push that spills.
    #[test]
    fn a_spool_spills_into_the_given_directory() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = ReplaySpool::new(10, Some(dir.path().to_owned()));
        spool.push(record(0)).unwrap();
        assert!(spool.is_spilled());
        assert_eq!(collect(&mut spool), vec![record(0)]);

        let missing = dir.path().join("missing");
        let mut spool = ReplaySpool::new(10, Some(missing.clone()));
        let error = spool.push(record(0)).unwrap_err();
        assert!(
            format!("{error:#}").contains(&missing.display().to_string()),
            "{error:#}"
        );
    }

    /// A replay stops at the first error and leaves the spool usable.
    #[test]
    fn a_failed_replay_keeps_the_spool() {
        let mut spool = ReplaySpool::new(10, None);
        for i in 0..4 {
            spool.push(record(i)).unwrap();
        }
        let mut seen = 0;
        let error = spool
            .replay(|_, _| {
                seen += 1;
                if seen == 2 {
                    Err(anyhow!("stop"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "stop");
        assert_eq!(seen, 2);
        spool.push(record(4)).unwrap();
        assert_eq!(collect(&mut spool), (0..5).map(record).collect::<Vec<_>>());
    }
}
