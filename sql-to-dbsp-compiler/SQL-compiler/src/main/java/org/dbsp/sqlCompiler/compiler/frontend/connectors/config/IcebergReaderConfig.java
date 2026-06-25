package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for reading data from an Iceberg table. */
@SuppressWarnings("unused")
public class IcebergReaderConfig implements IValidateConfig {
    /** Required. */
    @Nullable
    @JsonProperty("mode")
    public IcebergIngestMode mode = null;

    @Nullable
    @JsonProperty("timestamp_column")
    public String timestampColumn = null;

    @Nullable
    @JsonProperty("snapshot_filter")
    public String snapshotFilter = null;

    @Nullable
    @JsonProperty("snapshot_id")
    public Long snapshotId = null;

    @Nullable
    @JsonProperty("datetime")
    public String datetime = null;

    @Nullable
    @JsonProperty("metadata_location")
    public String metadataLocation = null;

    @Nullable
    @JsonProperty("table_name")
    public String tableName = null;

    @Nullable
    @JsonProperty("catalog_type")
    public IcebergCatalogType catalogType = null;

    // Glue catalog fields (inlined from GlueCatalogConfig)
    @Nullable @JsonProperty("glue.warehouse")        public String glueWarehouse = null;
    @Nullable @JsonProperty("glue.endpoint")         public String glueEndpoint = null;
    @Nullable @JsonProperty("glue.access-key-id")    public String glueAccessKeyId = null;
    @Nullable @JsonProperty("glue.secret-access-key") public String glueSecretAccessKey = null;
    @Nullable @JsonProperty("glue.profile-name")     public String glueProfileName = null;
    @Nullable @JsonProperty("glue.region")           public String glueRegion = null;
    @Nullable @JsonProperty("glue.session-token")    public String glueSessionToken = null;
    @Nullable @JsonProperty("glue.id")               public String glueId = null;

    // REST catalog fields (inlined from RestCatalogConfig)
    @Nullable @JsonProperty("rest.uri")              public String restUri = null;
    @Nullable @JsonProperty("rest.warehouse")        public String restWarehouse = null;
    @Nullable @JsonProperty("rest.oauth2-server-uri") public String restOauth2ServerUri = null;
    @Nullable @JsonProperty("rest.credential")       public String restCredential = null;
    @Nullable @JsonProperty("rest.token")            public String restToken = null;
    @Nullable @JsonProperty("rest.scope")            public String restScope = null;
    @Nullable @JsonProperty("rest.prefix")           public String restPrefix = null;
    @Nullable @JsonProperty("rest.headers")          public Object restHeaders = null;
    @Nullable @JsonProperty("rest.audience")         public String restAudience = null;
    @Nullable @JsonProperty("rest.resource")         public String restResource = null;

    /** Absorbs flattened {@code fileio_config} keys so they are not rejected as unknown. */
    @JsonAnySetter
    public void setFileioOption(String key, Object value) {}

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (mode == null) {
            reporter.warn("Invalid configuration", "required field \"mode\" is missing");
            return false;
        }
        if (snapshotId != null && datetime != null) {
            reporter.warnPath("snapshot_id", "Invalid configuration",
                    "\"snapshot_id\" and \"datetime\" are mutually exclusive");
            ok = false;
        }
        if (catalogType == null && metadataLocation == null) {
            reporter.warn("Invalid configuration",
                    "missing metadata location: specify an Iceberg catalog via \"catalog_type\""
                            + " or provide a table metadata location via \"metadata_location\"");
            ok = false;
        } else if (catalogType != null && metadataLocation != null) {
            reporter.warnPath("metadata_location", "Invalid configuration",
                    "\"metadata_location\" is not supported when \"catalog_type\" is set");
            ok = false;
        }
        if (catalogType == null && tableName != null) {
            reporter.warnPath("table_name", "Invalid configuration",
                    "\"table_name\" is only valid when \"catalog_type\" is set");
            ok = false;
        } else if (catalogType != null && tableName == null) {
            reporter.warn("Invalid configuration",
                    "\"table_name\" must be specified when \"catalog_type\" is set");
            ok = false;
        }
        if (catalogType == IcebergCatalogType.Glue) {
            if (glueWarehouse == null) {
                reporter.warn("Invalid configuration",
                        "missing Iceberg warehouse location—set \"glue.warehouse\" when using"
                                + " \"catalog_type\": \"glue\"");
                ok = false;
            }
        } else {
            ok &= checkGluePropAbsent(reporter, glueWarehouse, "warehouse");
            ok &= checkGluePropAbsent(reporter, glueEndpoint, "endpoint");
            ok &= checkGluePropAbsent(reporter, glueAccessKeyId, "access-key-id");
            ok &= checkGluePropAbsent(reporter, glueSecretAccessKey, "secret-access-key");
            ok &= checkGluePropAbsent(reporter, glueProfileName, "profile-name");
            ok &= checkGluePropAbsent(reporter, glueRegion, "region");
            ok &= checkGluePropAbsent(reporter, glueSessionToken, "session-token");
            ok &= checkGluePropAbsent(reporter, glueId, "id");
        }
        if (catalogType == IcebergCatalogType.Rest) {
            if (restUri == null) {
                reporter.warn("Invalid configuration",
                        "missing Iceberg REST catalog URI—set \"rest.uri\" when using"
                                + " \"catalog_type\": \"rest\"");
                ok = false;
            }
        } else {
            ok &= checkRestPropAbsent(reporter, restUri, "uri");
            ok &= checkRestPropAbsent(reporter, restWarehouse, "warehouse");
            ok &= checkRestPropAbsent(reporter, restOauth2ServerUri, "oauth2-server-uri");
            ok &= checkRestPropAbsent(reporter, restCredential, "credential");
            ok &= checkRestPropAbsent(reporter, restToken, "token");
            ok &= checkRestPropAbsent(reporter, restScope, "scope");
            ok &= checkRestPropAbsent(reporter, restPrefix, "prefix");
            ok &= checkRestPropAbsent(reporter, restHeaders, "headers");
            ok &= checkRestPropAbsent(reporter, restAudience, "audience");
            ok &= checkRestPropAbsent(reporter, restResource, "resource");
        }
        return ok;
    }

    private boolean checkGluePropAbsent(ConfigReporter reporter, Object value, String name) {
        if (value != null) {
            reporter.warnPath("glue." + name, "Invalid configuration",
                    "\"glue." + name + "\" is only valid when \"catalog_type\": \"glue\"");
            return false;
        }
        return true;
    }

    private boolean checkRestPropAbsent(ConfigReporter reporter, Object value, String name) {
        if (value != null) {
            reporter.warnPath("rest." + name, "Invalid configuration",
                    "\"rest." + name + "\" is only valid when \"catalog_type\": \"rest\"");
            return false;
        }
        return true;
    }
}
