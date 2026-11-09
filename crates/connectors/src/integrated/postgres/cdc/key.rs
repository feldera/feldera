use super::*;
use feldera_types::circuit::Layout;
use feldera_types::config::PipelineConfig as FelderaPipelineConfig;

/// What one etl pipeline, and so one replication slot, is keyed by.
///
/// [`Self::per_table`] is the key the connector has always used: one etl
/// pipeline per connector, named after the source table. [`Self::per_publication`]
/// is shared by every connector of one Feldera pipeline, on one host, that
/// reads the same publication of the same database. The two never hash to
/// the same id, so a slot of one kind is never mistaken for the other.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct PublicationKey {
    /// `host:port/database`, or the raw URI when it does not parse.
    connection: String,
    publication: String,
    /// Set for a per-table key only.
    source_table: Option<String>,
    /// `PipelineConfig::name`, set for a per-publication key only.
    feldera_pipeline: Option<String>,
    /// `Layout::local_host_idx()`, set for a per-publication key of a
    /// multihost pipeline only. Each host has its own slot.
    host_idx: Option<usize>,
}

impl PublicationKey {
    /// One etl pipeline per connector, as before slot sharing.
    pub(super) fn per_table(config: &PostgresCdcReaderConfig) -> Self {
        Self {
            connection: connection_identity(&config.uri),
            publication: config.publication.clone(),
            source_table: Some(config.source_table.clone()),
            feldera_pipeline: None,
            host_idx: None,
        }
    }

    /// One etl pipeline for every connector of this Feldera pipeline and host
    /// that reads the publication of `config`.
    ///
    /// Two Feldera pipelines, or two hosts of one pipeline, that read
    /// different tables of one publication must not share a slot or etl's
    /// `replication_state` rows, so the key needs the pipeline name. The
    /// pipeline manager always sets it.
    pub(super) fn per_publication(
        config: &PostgresCdcReaderConfig,
        pipeline_config: &FelderaPipelineConfig,
        layout: &Layout,
    ) -> AnyResult<Self> {
        let Some(feldera_pipeline) = pipeline_config.name.clone() else {
            return Err(anyhow!(
                "sharing one replication slot per publication needs the pipeline name to keep the \
                 slot name unique, but the pipeline configuration has no 'name'. Set 'name' in \
                 the pipeline configuration, or turn off the shared slot setting"
            ));
        };
        Ok(Self {
            connection: connection_identity(&config.uri),
            publication: config.publication.clone(),
            source_table: None,
            feldera_pipeline: Some(feldera_pipeline),
            host_idx: (!layout.is_solo()).then(|| layout.local_host_idx()),
        })
    }

    /// `host:port/database` of the source, for messages.
    pub(super) fn connection(&self) -> &str {
        &self.connection
    }

    pub(super) fn publication(&self) -> &str {
        &self.publication
    }

    /// The etl pipeline id. etl names the replication slots after it
    /// (`supabase_etl_apply_<id>`) and keys its state rows by it.
    pub(super) fn pipeline_id(&self) -> u64 {
        xxh3_64(self.identity().as_bytes())
    }

    /// The string the id hashes. Stable across Rust versions (xxh3) and across
    /// password rotations, which would otherwise orphan the slot and the
    /// stored etl state and force a full re-snapshot.
    fn identity(&self) -> String {
        match &self.source_table {
            // The per-table identity the connector has always hashed.
            Some(source_table) => {
                format!("{}\0{}\0{source_table}", self.connection, self.publication)
            }
            // Four separators, so that no per-table identity can equal it.
            None => format!(
                "{}\0{}\0shared\0{}\0{}",
                self.connection,
                self.publication,
                self.feldera_pipeline.as_deref().unwrap_or(""),
                self.host_idx.map_or(String::new(), |idx| idx.to_string()),
            ),
        }
    }
}

/// `host:port/database` of `uri`, without the password, the username and the
/// query parameters. Falls back to the raw URI if parsing fails; the pipeline
/// will likely fail startup shortly after anyway with a clearer error.
fn connection_identity(uri: &str) -> String {
    match Url::parse(uri) {
        Ok(url) => format!(
            "{}:{}/{}",
            url.host_str().unwrap_or(""),
            url.port().unwrap_or(5432),
            url.path().trim_start_matches('/')
        ),
        Err(_) => uri.to_string(),
    }
}

/// Deterministic pipeline id of the per-table key, for tests that
/// reconstruct it to clean up slots.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn pipeline_id(uri: &str, publication: &str, source_table: &str) -> u64 {
    xxh3_64(stable_connection_identity(uri, publication, source_table).as_bytes())
}

/// Deterministic pipeline id of the per-publication key of a solo pipeline
/// named `feldera_pipeline`, for tests that reconstruct it to clean up slots.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn shared_pipeline_id(uri: &str, publication: &str, feldera_pipeline: &str) -> u64 {
    PublicationKey {
        connection: connection_identity(uri),
        publication: publication.to_string(),
        source_table: None,
        feldera_pipeline: Some(feldera_pipeline.to_string()),
        host_idx: None,
    }
    .pipeline_id()
}

/// The identity string of the per-table key.
#[cfg(any(test, feature = "testing"))]
pub(super) fn stable_connection_identity(
    uri: &str,
    publication: &str,
    source_table: &str,
) -> String {
    PublicationKey {
        connection: connection_identity(uri),
        publication: publication.to_string(),
        source_table: Some(source_table.to_string()),
        feldera_pipeline: None,
        host_idx: None,
    }
    .identity()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn config(uri: &str, publication: &str, source_table: &str) -> PostgresCdcReaderConfig {
        serde_json::from_value(json!({
            "uri": uri,
            "publication": publication,
            "source_table": source_table,
        }))
        .unwrap()
    }

    fn pipeline(name: Option<&str>) -> FelderaPipelineConfig {
        FelderaPipelineConfig {
            global: Default::default(),
            multihost: None,
            name: name.map(str::to_string),
            given_name: None,
            storage_config: None,
            secrets_dir: None,
            inputs: Default::default(),
            outputs: Default::default(),
            program_ir: None,
        }
    }

    fn multihost(local: usize) -> Layout {
        let addresses = (0..3)
            .map(|i| (SocketAddr::from(([127, 0, 0, 1], 1000 + i)), 1))
            .collect::<Vec<_>>();
        Layout::new_multihost(&addresses, addresses[local].0).unwrap()
    }

    const URI: &str = "postgres://alice:secret@db.example.com:5433/app?sslmode=require";

    /// The id before slot sharing, computed by the released connector for
    /// `URI`, `pub` and `public.orders`. A change would orphan every
    /// existing slot and force a re-snapshot.
    #[test]
    fn per_table_id_is_the_released_one() {
        let legacy = xxh3_64("db.example.com:5433/app\0pub\0public.orders".as_bytes());
        assert_eq!(
            PublicationKey::per_table(&config(URI, "pub", "public.orders")).pipeline_id(),
            legacy
        );
        assert_eq!(pipeline_id(URI, "pub", "public.orders"), legacy);
    }

    #[test]
    fn per_table_ignores_credentials_and_query() {
        let a = PublicationKey::per_table(&config(URI, "pub", "t"));
        let b = PublicationKey::per_table(&config(
            "postgres://bob@db.example.com:5433/app",
            "pub",
            "t",
        ));
        assert_eq!(a, b);
        assert_ne!(a, PublicationKey::per_table(&config(URI, "pub", "u")));
    }

    #[test]
    fn per_publication_is_shared_by_the_tables_of_one_pipeline() {
        let solo = Layout::new_solo(2);
        let p1 = pipeline(Some("pipeline-1"));
        let orders =
            PublicationKey::per_publication(&config(URI, "pub", "orders"), &p1, &solo).unwrap();
        let items =
            PublicationKey::per_publication(&config(URI, "pub", "items"), &p1, &solo).unwrap();
        assert_eq!(orders, items);

        let other_publication =
            PublicationKey::per_publication(&config(URI, "pub2", "orders"), &p1, &solo).unwrap();
        assert_ne!(orders, other_publication);

        let other_pipeline = PublicationKey::per_publication(
            &config(URI, "pub", "orders"),
            &pipeline(Some("pipeline-2")),
            &solo,
        )
        .unwrap();
        assert_ne!(orders.pipeline_id(), other_pipeline.pipeline_id());
    }

    #[test]
    fn each_host_gets_its_own_key() {
        let p = pipeline(Some("pipeline-1"));
        let c = config(URI, "pub", "orders");
        let host0 = PublicationKey::per_publication(&c, &p, &multihost(0)).unwrap();
        let host1 = PublicationKey::per_publication(&c, &p, &multihost(1)).unwrap();
        let solo = PublicationKey::per_publication(&c, &p, &Layout::new_solo(1)).unwrap();
        assert_ne!(host0, host1);
        assert_ne!(host0, solo);
    }

    #[test]
    fn per_publication_never_collides_with_per_table() {
        let c = config(URI, "pub", "orders");
        let shared =
            PublicationKey::per_publication(&c, &pipeline(Some("orders")), &Layout::new_solo(1))
                .unwrap();
        assert_ne!(
            shared.pipeline_id(),
            PublicationKey::per_table(&c).pipeline_id()
        );
    }

    #[test]
    fn unnamed_pipeline_is_rejected() {
        let error = PublicationKey::per_publication(
            &config(URI, "pub", "orders"),
            &pipeline(None),
            &Layout::new_solo(1),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("'name'"), "{error}");
    }
}
