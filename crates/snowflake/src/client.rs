use crate::auth::{jwt_token, login_account_name};
use anyhow::{anyhow, bail, Context, Result as AnyResult};
use feldera_types::transport::snowflake::SnowflakeReaderConfig;
use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, USER_AGENT},
    Client, Url,
};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::{env, sync::atomic::AtomicU64, time::Duration};
use uuid::Uuid;

// Match Snowflake's C/C++ connector, which defines its client id as "C API":
// https://github.com/snowflakedb/libsnowflakeclient/blob/master/include/snowflake/client.h
const CLIENT_APP_ID: &str = "C API";
const CLIENT_APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const QUERY_RESULT_FORMAT: &str = "ARROW_FORCE";

pub(crate) struct SnowflakeClient {
    pub(super) http: Client,
    pub(super) base_url: Url,
    session_token: String,
    pub(super) sequence_id: AtomicU64,
}

#[derive(Deserialize)]
pub(super) struct SnowflakeApiResponse<T> {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    pub(super) code: Option<String>,
    #[serde(default)]
    message: Option<String>,
    pub(super) data: Option<T>,
}

#[derive(Deserialize)]
struct LoginData {
    #[serde(default)]
    token: Option<String>,
    #[serde(default, rename = "sessionToken")]
    session_token: Option<String>,
}

impl SnowflakeClient {
    pub(crate) async fn from_reader_config(config: &SnowflakeReaderConfig) -> AnyResult<Self> {
        config.validate().map_err(|error| anyhow!(error))?;
        let http = Client::builder()
            .user_agent(user_agent())
            .connect_timeout(Duration::from_secs(30))
            .no_gzip()
            .build()
            .context("error building Snowflake HTTP client")?;
        let base_url = account_base_url(&config.account)?;
        let jwt = jwt_token(config)?;

        let login_url = login_url(&base_url, config)?;
        let body = login_body(config, &jwt);
        let response = http
            .post(login_url)
            .header(ACCEPT, "application/snowflake")
            .json(&body)
            .send()
            .await
            .context("error sending Snowflake login request")?
            .error_for_status()
            .context("Snowflake login request failed")?
            .json::<SnowflakeApiResponse<LoginData>>()
            .await
            .context("error parsing Snowflake login response")?;

        let data = response_data(response, "Snowflake login")?;

        let client = Self {
            http,
            base_url,
            session_token: data.token.or(data.session_token).ok_or_else(|| {
                anyhow!("Snowflake login response did not include a session token")
            })?,
            sequence_id: AtomicU64::new(0),
        };

        // Snowflake's C/C++ connector enables Arrow with this session parameter:
        // https://github.com/snowflakedb/libsnowflakeclient/blob/master/cpp/lib/ArrowChunkIterator.hpp
        client
            .execute_statement(&format!(
                "alter session set C_API_QUERY_RESULT_FORMAT={QUERY_RESULT_FORMAT}"
            ))
            .await
            .context("error enabling Snowflake Arrow result format")?;

        Ok(client)
    }

    pub(super) fn endpoint(&self, path: &str) -> AnyResult<Url> {
        self.base_url
            .join(path.trim_start_matches('/'))
            .with_context(|| format!("invalid Snowflake endpoint path: {path}"))
    }

    pub(super) fn session_headers(&self) -> AnyResult<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/snowflake"));
        headers.insert(USER_AGENT, HeaderValue::from_str(&user_agent())?);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Snowflake Token=\"{}\"", self.session_token))
                .context("invalid Snowflake session token header")?,
        );
        Ok(headers)
    }
}

fn account_base_url(account: &str) -> AnyResult<Url> {
    let account = account.trim();
    validate_account_identifier(account)?;
    let top_level_domain = if account
        .split('.')
        .nth(1)
        .is_some_and(|region| region.to_ascii_lowercase().starts_with("cn-"))
    {
        "cn"
    } else {
        "com"
    };
    let url = format!("https://{account}.snowflakecomputing.{top_level_domain}");
    Url::parse(&url)
        .with_context(|| format!("invalid Snowflake account URL derived from '{account}'"))
}

fn validate_account_identifier(account: &str) -> AnyResult<()> {
    if account.is_empty()
        || account
            .to_ascii_lowercase()
            .contains(".snowflakecomputing.")
        || account.split('.').any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
    {
        bail!(
            "invalid Snowflake account identifier '{account}'; expected dot-separated letters, digits, underscores, or hyphens"
        );
    }
    Ok(())
}

fn login_url(base_url: &Url, config: &SnowflakeReaderConfig) -> AnyResult<Url> {
    let mut url = base_url.join("session/v1/login-request")?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("request_id", &Uuid::new_v4().to_string());
        if let Some(database) = nonempty(config.database.as_deref()) {
            query.append_pair("databaseName", database);
        }
        if let Some(schema) = nonempty(config.schema.as_deref()) {
            query.append_pair("schemaName", schema);
        }
        if let Some(warehouse) = nonempty(config.warehouse.as_deref()) {
            query.append_pair("warehouse", warehouse);
        }
        if let Some(role) = nonempty(config.role.as_deref()) {
            query.append_pair("roleName", role);
        }
    }
    Ok(url)
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then_some(value)
    })
}

fn login_body(config: &SnowflakeReaderConfig, jwt: &str) -> JsonValue {
    let mut session_parameters = serde_json::Map::new();
    session_parameters.insert(
        "AUTOCOMMIT".to_string(),
        JsonValue::String("true".to_string()),
    );
    session_parameters.insert("TIMEZONE".to_string(), JsonValue::String("UTC".to_string()));

    json!({
        "data": {
            "CLIENT_APP_ID": CLIENT_APP_ID,
            "CLIENT_APP_VERSION": CLIENT_APP_VERSION,
            "ACCOUNT_NAME": login_account_name(&config.account),
            "LOGIN_NAME": config.user,
            "AUTHENTICATOR": "SNOWFLAKE_JWT",
            "TOKEN": jwt,
            "CLIENT_ENVIRONMENT": {
                "APPLICATION": CLIENT_APP_ID,
                "OS": env::consts::OS,
                "ARCH": env::consts::ARCH,
            },
            "SESSION_PARAMETERS": session_parameters,
        }
    })
}

pub(super) fn response_data<T>(response: SnowflakeApiResponse<T>, operation: &str) -> AnyResult<T> {
    if response.success == Some(false) {
        bail!(
            "{operation} failed: code={} message={}",
            response.code.unwrap_or_default(),
            response.message.unwrap_or_default()
        );
    }

    response.data.ok_or_else(|| {
        anyhow!(
            "{operation} response did not include a data object: code={} message={}",
            response.code.unwrap_or_default(),
            response.message.unwrap_or_default()
        )
    })
}

fn user_agent() -> String {
    format!("{CLIENT_APP_ID}/{CLIENT_APP_VERSION}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, Decimal128Array, Float64Array},
        datatypes::DataType,
    };
    use feldera_types::transport::snowflake::{SnowflakeAuthenticator, SnowflakeNumberMode};
    use futures_util::StreamExt;

    #[test]
    fn derives_account_urls() {
        assert_eq!(
            account_base_url("org-account").unwrap().as_str(),
            "https://org-account.snowflakecomputing.com/"
        );
        assert_eq!(
            account_base_url("xy12345.us-east-1").unwrap().as_str(),
            "https://xy12345.us-east-1.snowflakecomputing.com/"
        );
        assert!(account_base_url("http://localhost:8080").is_err());
        assert!(account_base_url("xy12345.snowflakecomputing.com").is_err());
    }

    #[ignore]
    #[tokio::test]
    async fn snowflake_arrow_smoke_test() -> AnyResult<()> {
        let config = SnowflakeReaderConfig {
            account: env::var("SNOWFLAKE_ACCOUNT").context("SNOWFLAKE_ACCOUNT is not set")?,
            user: env::var("SNOWFLAKE_USER").context("SNOWFLAKE_USER is not set")?,
            authenticator: SnowflakeAuthenticator::SnowflakeJwt,
            role: env::var("SNOWFLAKE_ROLE").ok(),
            warehouse: env::var("SNOWFLAKE_WAREHOUSE").ok(),
            database: env::var("SNOWFLAKE_DATABASE").ok(),
            schema: env::var("SNOWFLAKE_SCHEMA").ok(),
            private_key_file: env::var("SNOWFLAKE_PRIVATE_KEY_FILE")
                .context("SNOWFLAKE_PRIVATE_KEY_FILE is not set")?,
            private_key_file_pwd: env::var("SNOWFLAKE_PRIVATE_KEY_FILE_PWD").ok(),
            table: "unused".to_string(),
            column_mapping: Default::default(),
            number_mode: Default::default(),
            mode: Default::default(),
            transaction_mode: Default::default(),
            snapshot_filter: None,
            num_parsers: 4,
            max_concurrent_readers: None,
        };
        let client = SnowflakeClient::from_reader_config(&config).await?;
        let custom_query = env::var("SNOWFLAKE_TEST_QUERY").ok();
        let using_default_query = custom_query.is_none();
        let query = custom_query.unwrap_or_else(|| {
            "select -1.23::number(10,2) as AMOUNT, 12::number(10,0) as ID, null::number(10,2) as OPTIONAL_AMOUNT"
                .to_string()
        });
        for (number_mode, expected_type) in [
            (SnowflakeNumberMode::Decimal, DataType::Decimal128(10, 2)),
            (SnowflakeNumberMode::Double, DataType::Float64),
        ] {
            let result = client
                .query_arrow_batch_stream(&query, 1, number_mode)
                .await?;
            let batches = result.batches.collect::<Vec<_>>().await;
            let batches = batches.into_iter().collect::<AnyResult<Vec<_>>>()?;
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                1
            );
            if using_default_query {
                assert_eq!(batches[0].schema().field(0).data_type(), &expected_type);
                assert!(matches!(
                    batches[0].schema().field(1).data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Decimal128(_, 0)
                ));
                match number_mode {
                    SnowflakeNumberMode::Decimal => {
                        let amounts = batches[0]
                            .column(0)
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap();
                        assert_eq!(amounts.value(0), -123);
                        assert!(batches[0].column(2).is_null(0));
                    }
                    SnowflakeNumberMode::Double => {
                        let amounts = batches[0]
                            .column(0)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap();
                        assert_eq!(amounts.value(0), -1.23);
                        assert!(batches[0].column(2).is_null(0));
                    }
                }
            }
        }
        Ok(())
    }
}
