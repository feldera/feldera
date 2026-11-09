use super::*;

/// The etl `Destination` of a [`SharedCdcPipeline`]: hands each call to the
/// members that read the table it is about.
#[derive(Clone)]
pub(super) struct DispatchingDestination {
    members: Arc<Vec<Arc<FelderaDestination>>>,
    /// etl is being stopped; see [`FelderaDestination::wait_unpaused`].
    stopping: Arc<AtomicBool>,
    /// Results the stop could not answer; see [`ParkedResults`].
    parked: Arc<ParkedResults>,
}

impl DispatchingDestination {
    pub(super) fn new(
        members: Vec<Arc<FelderaDestination>>,
        stopping: Arc<AtomicBool>,
        parked: Arc<ParkedResults>,
    ) -> Self {
        assert!(
            !members.is_empty(),
            "a pipeline without members is never started"
        );
        Self {
            members: Arc::new(members),
            stopping,
            parked,
        }
    }

    /// The members that read the table `schema` describes. Several Feldera
    /// tables can read one source table.
    fn members_for(&self, schema: &ReplicatedTableSchema) -> Vec<&Arc<FelderaDestination>> {
        let name = schema.name();
        self.members
            .iter()
            .filter(|member| member.is_target_table(&name.schema, &name.name))
            .collect()
    }

    /// Wait until no member is paused. The WAL stream is one ordered
    /// sequence, so a write waits for every member it could feed.
    async fn wait_unpaused(&self) -> Admission {
        if self.stopping.load(Ordering::SeqCst) {
            return Admission::Stopping;
        }
        for member in self.members.iter() {
            if member.wait_unpaused().await == Admission::Stopping {
                return Admission::Stopping;
            }
        }
        Admission::Admit
    }
}

impl Destination for DispatchingDestination {
    fn name() -> &'static str {
        "feldera"
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        if self.wait_unpaused().await == Admission::Stopping {
            self.parked.park(async_result);
            return Ok(());
        }

        // Feldera owns no physical destination object to drop; the data lives
        // in the circuit, and a copy that starts over simply re-snapshots
        // through `write_table_rows`.
        //
        // The call still carries the news that a copy is starting over, and it
        // is the earliest such news etl gives: it precedes both the durable
        // state of the new copy and its first row. Take it for the table's
        // members, and let `write_table_rows` take it again if this hook stays
        // silent, as it does whenever etl finds no destination metadata to
        // drop.
        for member in self.members_for(replicated_table_schema) {
            member.note_copy_batch();
        }

        debug!(
            "postgres_cdc: nothing to drop for table '{}'; the copy is read into the circuit",
            replicated_table_schema.name()
        );
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        batch_id: Option<TableCopyBatchId>,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        if self.wait_unpaused().await == Admission::Stopping {
            self.parked.park(async_result);
            return Ok(());
        }

        let members = self.members_for(replicated_table_schema);
        if members.is_empty() {
            // Rows of another table of the publication reach no Feldera table,
            // so they are durable the moment they are dropped, and so is the
            // barrier that closes their copy.
            async_result.send(Ok(DestinationWriteStatus::Durable));
            return Ok(());
        }

        // etl carries a batch id with every batch of rows and none with the
        // terminal write it issues once every copy worker has finished, so the
        // absent batch id is what marks the copy's durability barrier.
        if batch_id.is_none() {
            let fan_in = AckFanIn::new(
                Box::new(move || async_result.send(Ok(DestinationWriteStatus::Durable))),
                members.len(),
            );
            for member in members {
                member.hold_copy_barrier(fan_in.share());
            }
            return Ok(());
        }

        for member in members {
            member.push_copy_rows(replicated_table_schema, &table_rows)?;
        }
        // The rows are queued, not yet in the circuit. etl's terminal barrier
        // is what answers for them, once they have been flushed.
        async_result.send(Ok(DestinationWriteStatus::Accepted));
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        // The connector reports no streaming write as merely accepted, so it
        // owes etl no durability and answers `RequireDurable` exactly as it
        // answers `MayDefer`: `Durable`, once the data has been stepped through
        // the circuit.
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        if self.wait_unpaused().await == Admission::Stopping {
            self.parked.park(async_result);
            return Ok(());
        }

        // The members that got rows: each owes one share of the answer, and
        // the last share to fire answers etl. A write with rows for no member
        // is answered at once.
        let members = self
            .members
            .iter()
            .filter(|member| {
                events
                    .iter()
                    .filter_map(event_table)
                    .any(|name| member.is_target_table(&name.schema, &name.name))
            })
            .collect::<Vec<_>>();
        let fan_in = AckFanIn::new(
            Box::new(move || async_result.send(Ok(DestinationWriteStatus::Durable))),
            members.len(),
        );
        for member in members {
            member.push_events(&events, fan_in.share())?;
        }
        Ok(())
    }
}

/// The table an event carries rows of, if any.
fn event_table(event: &Event) -> Option<&etl::schema::TableName> {
    match event {
        Event::Insert(insert) => Some(insert.replicated_table_schema.name()),
        Event::Update(update) => Some(update.replicated_table_schema.name()),
        Event::Delete(delete) => Some(delete.replicated_table_schema.name()),
        Event::Truncate(_)
        | Event::Relation(_)
        | Event::Begin(_)
        | Event::Commit(_)
        | Event::Unsupported => None,
    }
}

/// The part of the destination that feeds one Feldera table.
#[derive(Clone)]
pub(super) struct FelderaDestination {
    pub(super) input_stream: Arc<Mutex<Box<dyn DeCollectionStream>>>,
    pub(super) queue: Arc<InputQueue<QueueAux>>,
    pub(super) source_table: String,
    pub(super) endpoint_name: String,
    /// Canonical names of the non-nullable Feldera columns. Each must be present
    /// (by name) in the target Postgres table schema etl passes with each target
    /// batch/event. Nullable and extra columns need not match.
    pub(super) feldera_required_columns: Vec<String>,
    /// Wait for a step to hold a streamed write before answering etl for it.
    /// False when the consumer reports no step progress to wait for, which
    /// leaves the write as durable as it will ever be once it is queued.
    pub(super) defer_acks: bool,
    /// Pipeline state receiver used to stop accepting new etl batches while the
    /// Feldera pipeline is paused.
    pub(super) pipeline_state_rx: Receiver<PipelineState>,
    /// Checkpoint barrier of the initial copy, which every batch of copy rows
    /// raises and etl's terminal barrier lowers; see
    /// [`FelderaDestination::hold_copy_barrier`].
    pub(super) copy: Arc<CopyBarrier>,
    /// Asks the controller for the step that reports a barrier the destination
    /// lowered away from the reader; see
    /// [`FelderaDestination::hold_copy_barrier`].
    pub(super) consumer: Box<dyn InputConsumer>,
    /// The connector is shutting etl down; see
    /// [`FelderaDestination::wait_unpaused`].
    pub(super) stopping: Arc<AtomicBool>,
}

/// What the reader reports a step as resumable from. etl's `PostgresStore`
/// holds the replication position, so a resumable step needs no metadata
/// beyond an identifier. While a copy of the table is on its way to the
/// circuit, the step is a barrier instead: the controller then defers a
/// checkpoint or a suspend rather than recording a partial copy it could not
/// finish on resume.
pub(super) fn resume_for(copy_open: bool, pipeline_id: u64, sync_done_lsn: Option<u64>) -> Resume {
    if copy_open {
        Resume::Barrier
    } else {
        // NOTE: `pipeline_id` is an identity, not a position. The position a
        // restart resumes from is the replication slot's confirmed LSN in
        // etl's store, which this id keys. The controller hands the metadata
        // back as it was, and `ResumedCopy::of` reads only `copy_sync_lsn`.
        Resume::Seek {
            seek: json!({
                "pipeline_id": pipeline_id,
                COPY_SYNC_LSN_KEY: sync_done_lsn,
            }),
        }
    }
}

/// Key under which the resume metadata records etl's `sync_done` LSN for the
/// source table at the time of the step; see [`CopyBarrier::sync_done_lsn`].
pub(super) const COPY_SYNC_LSN_KEY: &str = "copy_sync_lsn";

impl FelderaDestination {
    /// Queue the rows of this member's table from a copy batch.
    pub(super) fn push_copy_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: &[TableRow],
    ) -> EtlResult<()> {
        let Some(column_names) = self.column_names_for_target_schema(replicated_table_schema)?
        else {
            return Ok(());
        };

        self.note_copy_batch();

        let mut stream = self.input_stream.lock().unwrap();
        let mut bytes = 0;
        let mut errors = Vec::new();
        let timestamp = Utc::now();

        for row in table_rows {
            let cells = row.values();
            let json_value = row_to_json(cells, &column_names, &self.endpoint_name);

            let json_str = json_value.to_string();
            if let Err(e) = stream.insert(json_str.as_bytes(), &None) {
                errors.push(ParseError::text_event_error(
                    "Failed to deserialize CDC snapshot row",
                    e,
                    0,
                    Some(&json_str),
                    None,
                ));
            }
            bytes += json_str.len();

            if bytes >= 2 * 1024 * 1024 {
                self.push_snapshot_buffer((stream.take_all(), errors), timestamp);
                bytes = 0;
                errors = Vec::new();
            }
        }

        if bytes > 0 || !errors.is_empty() {
            self.push_snapshot_buffer((stream.take_all(), errors), timestamp);
        }
        Ok(())
    }

    /// Queue the rows of this member's table from a streamed write, and
    /// arrange for `share` to answer for them: at once when nothing waits on
    /// a step, otherwise with the flush that puts the write's last row into
    /// the circuit (#7122).
    pub(super) fn push_events(&self, events: &[Event], share: Ack) -> EtlResult<()> {
        let mut stream = self.input_stream.lock().unwrap();
        let mut queued = 0usize;
        let mut bytes = 0;
        let mut errors = Vec::new();
        let timestamp = Utc::now();

        for event in events {
            match event {
                Event::Insert(insert) => {
                    let Some(cols) =
                        self.column_names_for_target_schema(&insert.replicated_table_schema)?
                    else {
                        continue;
                    };
                    let json_value =
                        row_to_json(insert.table_row.values(), &cols, &self.endpoint_name);
                    let json_str = json_value.to_string();
                    if let Err(e) = stream.insert(json_str.as_bytes(), &None) {
                        errors.push(ParseError::text_event_error(
                            "Failed to deserialize CDC insert",
                            e,
                            0,
                            Some(&json_str),
                            None,
                        ));
                    }
                    bytes += json_str.len();
                }
                Event::Update(update) => {
                    let Some(cols) =
                        self.column_names_for_target_schema(&update.replicated_table_schema)?
                    else {
                        continue;
                    };
                    // The new row is authoritative only when complete. A partial
                    // image (PostgreSQL `UnchangedToast` columns etl could not
                    // reconstruct) cannot be turned into a correct Feldera row,
                    // so skip the whole update rather than emit a half-applied
                    // delete-without-insert.
                    let UpdatedTableRow::Full(new_row) = &update.updated_table_row else {
                        warn!(
                            "postgres_cdc {}: skipping update with a partial row image \
                             (unchanged TOAST columns); set REPLICA IDENTITY FULL on the source \
                             table to receive complete rows",
                            &self.endpoint_name
                        );
                        continue;
                    };
                    // Delete the old row first, if PostgreSQL supplied one.
                    if let Some(old_row) = &update.old_table_row {
                        let old_str = old_row_to_json(
                            &update.replicated_table_schema,
                            &cols,
                            old_row,
                            &self.endpoint_name,
                        )
                        .to_string();
                        if let Err(e) = stream.delete(old_str.as_bytes(), &None) {
                            errors.push(ParseError::text_event_error(
                                "Failed to deserialize CDC update (old)",
                                e,
                                0,
                                Some(&old_str),
                                None,
                            ));
                        }
                        bytes += old_str.len();
                    }
                    // Insert the new row.
                    let new_str =
                        row_to_json(new_row.values(), &cols, &self.endpoint_name).to_string();
                    if let Err(e) = stream.insert(new_str.as_bytes(), &None) {
                        errors.push(ParseError::text_event_error(
                            "Failed to deserialize CDC update (new)",
                            e,
                            0,
                            Some(&new_str),
                            None,
                        ));
                    }
                    bytes += new_str.len();
                }
                Event::Delete(delete) => {
                    let Some(cols) =
                        self.column_names_for_target_schema(&delete.replicated_table_schema)?
                    else {
                        continue;
                    };
                    if let Some(old_row) = &delete.old_table_row {
                        let old_str = old_row_to_json(
                            &delete.replicated_table_schema,
                            &cols,
                            old_row,
                            &self.endpoint_name,
                        )
                        .to_string();
                        if let Err(e) = stream.delete(old_str.as_bytes(), &None) {
                            errors.push(ParseError::text_event_error(
                                "Failed to deserialize CDC delete",
                                e,
                                0,
                                Some(&old_str),
                                None,
                            ));
                        }
                        bytes += old_str.len();
                    }
                }
                Event::Truncate(_) => {
                    warn!(
                        "postgres_cdc {}: received TRUNCATE event, ignoring",
                        &self.endpoint_name
                    );
                }
                // Relation events carry only schema, no row data. etl detects
                // schema changes upstream (it refuses to forward a Relation
                // whose schema differs from the resolved one) and marks the
                // table errored; we surface that via `TableErrorMonitor`.
                Event::Relation(_) | Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }

            if bytes >= 2 * 1024 * 1024 {
                // More rows of this write may follow, so this entry carries no
                // acknowledgment: the write's last entry does.
                self.push_event_buffer((stream.take_all(), errors), timestamp, None);
                queued += 1;
                bytes = 0;
                errors = Vec::new();
            }
        }

        let tail_rows = bytes > 0 || !errors.is_empty();
        // `bytes` is bumped for every row string pushed into the stream above,
        // success or parse failure, so `tail_rows` false means the stream is
        // empty. `AnswerUnqueued` leans on that: it is the one branch that never
        // calls `take_all`, so a row that reached the stream without bumping
        // `bytes` would be dropped and etl told the write was durable. Pin the
        // coupling in debug builds.
        debug_assert!(
            tail_rows || stream.is_empty(),
            "no rows were accounted for, so the stream must be empty"
        );

        match write_ending(queued, tail_rows, self.defer_acks) {
            WriteEnding::AnswerUnqueued => share(),
            WriteEnding::QueueTailAndAnswer => {
                self.push_event_buffer((stream.take_all(), errors), timestamp, None);
                share();
            }
            WriteEnding::QueueClosingAck => {
                self.push_event_buffer((stream.take_all(), errors), timestamp, Some(share))
            }
        }

        Ok(())
    }
}

impl FelderaDestination {
    /// Raise the checkpoint barrier for the copy this call belongs to, and
    /// report a copy that starts over.
    ///
    /// etl announces an attempt before it hands over any row of it, so no row
    /// of a new copy can reach the queue while the barrier is down (#6121).
    pub(super) fn note_copy_batch(&self) {
        if self.copy.note_copy_batch() {
            info!(
                "postgres_cdc {}: etl is copying table '{}' again; checkpoints wait \
                 for the new copy",
                &self.endpoint_name, &self.source_table
            );
        }
    }

    /// Hold etl's terminal copy barrier until every queued row of the copy has
    /// reached the circuit.
    ///
    /// etl waits for this result before it records the copy as finished, so a
    /// copy the circuit does not hold in full can neither be checkpointed nor
    /// be skipped on the next start (#6121).
    pub(super) fn hold_copy_barrier(&self, terminal: Ack) {
        let lowered = self.copy.hold_terminal(terminal);
        if lowered {
            // The circuit already held every row of the copy, so no flush is
            // left to report the lowered barrier. A checkpoint deferred on that
            // barrier stops the controller from stepping on its own, so ask for
            // the step that reports it; without it the checkpoint waits for
            // good.
            self.consumer.request_step();
        }
    }

    /// Queue a buffer of streamed rows, carrying `ack` when the buffer ends its
    /// write; see [`QueueAux::ack`].
    pub(super) fn push_event_buffer(
        &self,
        buffer: (
            Option<Box<dyn feldera_adapterlib::format::InputBuffer>>,
            Vec<ParseError>,
        ),
        timestamp: chrono::DateTime<Utc>,
        ack: Option<Ack>,
    ) {
        self.queue.push_with_aux(
            buffer,
            timestamp,
            QueueAux {
                snapshot: false,
                ack,
            },
        );
    }

    /// Queue a buffer of initial-copy rows and account for it, so the reader
    /// can tell when the last snapshot row has reached the circuit.
    pub(super) fn push_snapshot_buffer(
        &self,
        buffer: (
            Option<Box<dyn feldera_adapterlib::format::InputBuffer>>,
            Vec<ParseError>,
        ),
        timestamp: chrono::DateTime<Utc>,
    ) {
        // Account for the buffer first, so the reader never counts a flush it
        // has not seen queued.
        self.copy.note_buffer_queued();
        self.queue.push_with_aux(
            buffer,
            timestamp,
            QueueAux {
                snapshot: true,
                // The copy answers etl through the barrier, not per entry.
                ack: None,
            },
        );
    }

    /// Wait until the Feldera pipeline is running before accepting a new etl
    /// batch, and say whether the batch may still be accepted.
    ///
    /// Once the connector is stopping, a write queues nothing: etl may outlive
    /// the stop by up to [`RUNTIME_SHUTDOWN_TIMEOUT`] after the reader has
    /// signalled end of input, and rows that reach the queue then never reach
    /// the circuit. The write is not refused either, since etl would record
    /// the refusal as a table error. Its result is parked, and etl, asked to
    /// stop before the flag went up, abandons the write and reads its rows
    /// again on the next start.
    pub(super) async fn wait_unpaused(&self) -> Admission {
        if self.stopping.load(Ordering::SeqCst) {
            return Admission::Stopping;
        }
        let mut rx = self.pipeline_state_rx.clone();
        match rx.wait_for(|state| state != &PipelineState::Paused).await {
            // A stop on a table error leaves the pipeline running, so the
            // flag is the authority, not the state.
            Ok(state) if *state == PipelineState::Running => {
                if self.stopping.load(Ordering::SeqCst) {
                    Admission::Stopping
                } else {
                    Admission::Admit
                }
            }
            Ok(_) | Err(_) => Admission::Stopping,
        }
    }

    pub(super) fn is_target_table(&self, schema_name: &str, table_name: &str) -> bool {
        is_target_table(&self.source_table, schema_name, table_name)
    }

    /// Resolve the replicated column names for `schema`.
    ///
    /// Returns `Some(column_names)`, in row-payload order, if `schema` describes
    /// the configured `source_table`, or `None` if it is a different table in
    /// the publication (whose rows are skipped).
    ///
    /// etl carries the table schema with every batch and event, so the connector
    /// uses that schema directly instead of caching target-table metadata.
    fn column_names_for_target_schema(
        &self,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<Option<Vec<String>>> {
        // A different table in the publication — not ours.
        let name = schema.name();
        if !self.is_target_table(&name.schema, &name.name) {
            return Ok(None);
        }

        let column_names: Vec<String> = replicated_column_names(schema);
        self.validate_columns(&name.name, &column_names)?;
        Ok(Some(column_names))
    }

    /// Verify that every non-nullable Feldera column exists (by name) in the
    /// target Postgres table.
    /// Nullable Feldera columns and extra Postgres columns are allowed to differ.
    fn validate_columns(&self, pg_table: &str, pg_columns: &[String]) -> EtlResult<()> {
        let pg_set: BTreeSet<&str> = pg_columns.iter().map(String::as_str).collect();
        let missing: Vec<&str> = self
            .feldera_required_columns
            .iter()
            .map(String::as_str)
            .filter(|c| !pg_set.contains(c))
            .collect();

        if missing.is_empty() {
            return Ok(());
        }

        Err(etl_error!(
            ErrorKind::ValidationError,
            "Postgres CDC source table is missing required Feldera columns",
            format!(
                "table '{pg_table}': non-nullable Feldera columns absent from the Postgres table: \
                 {missing:?}. Every non-nullable Feldera column must exist (by name) in the \
                 source table."
            )
        ))
    }
}

#[cfg(any(test, feature = "testing"))]
/// What one `write_table_rows` call asks of the connector, where
/// `target_columns` holds the replicated column names when the call describes
/// the source table and `has_batch_id` says whether it carries rows.
///
/// etl carries a batch id with every batch of rows and none with the terminal
/// write it issues once every copy worker has finished, so the absent batch id
/// is what marks the copy's durability barrier.
pub(super) fn classify_copy_write(
    target_columns: Option<Vec<String>>,
    has_batch_id: bool,
) -> CopyWrite {
    match (target_columns, has_batch_id) {
        (Some(column_names), true) => CopyWrite::TargetBatch(column_names),
        (Some(_), false) => CopyWrite::TargetBarrier,
        (None, _) => CopyWrite::OtherTable,
    }
}

#[cfg(any(test, feature = "testing"))]
/// What one `write_table_rows` call asks of the connector.
#[derive(Debug)]
pub(super) enum CopyWrite {
    /// Rows of the source table, to queue for the circuit.
    TargetBatch(Vec<String>),
    /// The terminal durability barrier of the source table's copy.
    TargetBarrier,
    /// A table this connector does not read; nothing of it reaches the circuit.
    OtherTable,
}

/// Replicated column names of `schema`, in the order etl emits cell values for
/// a row.
pub(super) fn replicated_column_names(schema: &ReplicatedTableSchema) -> Vec<String> {
    schema.column_schemas().map(|c| c.name.clone()).collect()
}

/// Convert an old-row image (carried by updates and deletes) to JSON.
///
/// A [`OldTableRow::Full`] image holds every replicated column, in the same
/// order as `full_columns`. A [`OldTableRow::Key`] image holds only the
/// replica-identity columns, so its values must be paired with the identity
/// column names instead.
pub(super) fn old_row_to_json(
    schema: &ReplicatedTableSchema,
    full_columns: &[String],
    old_row: &OldTableRow,
    endpoint_name: &str,
) -> Value {
    match old_row {
        OldTableRow::Full(row) => row_to_json(row.values(), full_columns, endpoint_name),
        OldTableRow::Key(row) => {
            let identity_columns: Vec<String> = schema
                .identity_column_schemas()
                .map(|c| c.name.clone())
                .collect();
            row_to_json(row.values(), &identity_columns, endpoint_name)
        }
    }
}

/// Convert a row of cells to a JSON object using the given column names.
pub(super) fn row_to_json(cells: &[Cell], column_names: &[String], endpoint_name: &str) -> Value {
    if cells.len() != column_names.len() {
        warn!(
            "postgres_cdc {endpoint_name}: a row holds {} values for {} column names; values \
             past the names are keyed col_<index> and names past the values are left out",
            cells.len(),
            column_names.len()
        );
    }
    let mut map = serde_json::Map::new();
    for (i, cell) in cells.iter().enumerate() {
        let col_name = column_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{i}"));
        map.insert(col_name, cell_to_json(cell));
    }
    Value::Object(map)
}

/// Postgres `infinity` and `-infinity` dates keep their Postgres spelling:
/// Feldera's `DATE` has no such values, so the row fails to parse and is
/// reported like any other malformed record, instead of silently moving.
pub(super) fn date_to_json(d: &Date<NaiveDate>) -> Value {
    match d {
        Date::Value(d) => json!(d.to_string()),
        Date::PosInfinity => json!("infinity"),
        Date::NegInfinity => json!("-infinity"),
    }
}

/// Postgres allows `24:00:00`, which `PgTime` spells as such and Feldera's
/// `TIME` rejects; see [`date_to_json`].
pub(super) fn time_to_json(t: &PgTime) -> Value {
    json!(t.to_string())
}

/// See [`date_to_json`] for the infinite values.
pub(super) fn timestamp_to_json(ts: &Timestamp<NaiveDateTime>) -> Value {
    match ts {
        Timestamp::Value(ts) => json!(ts.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
        Timestamp::PosInfinity => json!("infinity"),
        Timestamp::NegInfinity => json!("-infinity"),
    }
}

/// See [`date_to_json`] for the infinite values.
pub(super) fn timestamptz_to_json(ts: &Timestamp<DateTime<Utc>>) -> Value {
    match ts {
        Timestamp::Value(ts) => json!(ts.to_rfc3339()),
        Timestamp::PosInfinity => json!("infinity"),
        Timestamp::NegInfinity => json!("-infinity"),
    }
}

/// Convert an etl Cell to a serde_json Value.
pub(super) fn cell_to_json(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => json!(b),
        Cell::String(s) => json!(s),
        Cell::I16(n) => json!(n),
        Cell::I32(n) => json!(n),
        Cell::U32(n) => json!(n),
        Cell::I64(n) => json!(n),
        Cell::F32(f) => {
            if f.is_nan() || f.is_infinite() {
                Value::Null
            } else {
                json!(f)
            }
        }
        Cell::F64(f) => {
            if f.is_nan() || f.is_infinite() {
                Value::Null
            } else {
                json!(f)
            }
        }
        Cell::Numeric(n) => {
            // Preserve precision by encoding as string.
            json!(n.to_string())
        }
        Cell::Date(d) => date_to_json(d),
        Cell::Time(t) => time_to_json(t),
        // Feldera has no time-with-time-zone type, so the cell keeps the text
        // form Postgres itself uses, UTC offset included.
        Cell::TimeTz(t) => json!(t.to_string()),
        Cell::Timestamp(ts) => timestamp_to_json(ts),
        Cell::TimestampTz(ts) => timestamptz_to_json(ts),
        Cell::Uuid(u) => json!(u.to_string()),
        Cell::Json(j) => j.clone(),
        Cell::Bytes(b) => {
            // Encode as byte array to match Datagen's BinaryFormat::Array.
            json!(b)
        }
        Cell::Array(arr) => array_cell_to_json(arr),
    }
}

/// Convert an etl ArrayCell to a JSON array.
pub(super) fn array_cell_to_json(arr: &ArrayCell) -> Value {
    match arr {
        ArrayCell::Bool(v) => json!(v),
        ArrayCell::String(v) => json!(v),
        ArrayCell::I16(v) => json!(v),
        ArrayCell::I32(v) => json!(v),
        ArrayCell::U32(v) => json!(v),
        ArrayCell::I64(v) => json!(v),
        ArrayCell::F32(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(f) if f.is_nan() || f.is_infinite() => Value::Null,
                    Some(f) => json!(f),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::F64(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(f) if f.is_nan() || f.is_infinite() => Value::Null,
                    Some(f) => json!(f),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Numeric(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(n) => json!(n.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Date(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(d) => date_to_json(d),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Time(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(t) => time_to_json(t),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::TimeTz(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(t) => json!(t.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Timestamp(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(ts) => timestamp_to_json(ts),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::TimestampTz(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(ts) => timestamptz_to_json(ts),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Uuid(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(u) => json!(u.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Json(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(j) => j.clone(),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Bytes(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(b) => json!(b),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
    }
}
