use std::{
    collections::BTreeMap,
    fmt::Write,
    fs::File,
    io::{BufRead, BufReader, ErrorKind, Read},
    path::Path,
};

use feldera_rest_api::Client;
use feldera_rest_api::types::*;
use feldera_types::config::RuntimeConfig;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde_json::Value;
use tracing::{debug, info, warn};
use zip::ZipArchive;

use crate::cli::DebugActions;
use crate::{OutputFormat, PipelineAction, handle_errors_fatal, pipeline};

fn parse_metrics(file_name: &Path) -> anyhow::Result<()> {
    #[derive(Clone, Debug, Default)]
    struct Metric {
        values: BTreeMap<Vec<(String, String)>, MetricValue>,
    }

    #[derive(Clone, Debug)]
    enum MetricValue {
        Number(f64),
        Histogram(Histogram),
    }

    impl MetricValue {
        fn as_histogram_mut(&mut self) -> Option<&mut Histogram> {
            match self {
                MetricValue::Histogram(histogram) => Some(histogram),
                _ => None,
            }
        }
    }

    #[derive(Clone, Debug, Default)]
    struct Histogram {
        sum: f64,
        buckets: BTreeMap<OrderedFloat<f64>, f64>,
    }

    fn read_metrics<R>(reader: R) -> anyhow::Result<BTreeMap<String, Metric>>
    where
        R: BufRead,
    {
        fn get_metric<'a>(metrics: &'a mut BTreeMap<String, Metric>, name: &str) -> &'a mut Metric {
            metrics.entry(String::from(name)).or_default()
        }

        let mut metrics = BTreeMap::new();
        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, rest) = line.split_once('{').unwrap();
            let (pairs, rest) = rest.split_once('}').unwrap();
            let mut fields = pairs
                .split(',')
                .map(|pair| pair.split_once('=').unwrap())
                .filter(|(key, _value)| *key != "pipeline")
                .map(|(key, value)| (String::from(key), String::from(value.trim_matches('"'))))
                .collect::<Vec<_>>();
            let value: f64 = rest.trim().parse().unwrap();

            if let Some(name) = name.strip_suffix("_bucket") {
                let (key, le) = fields.pop().unwrap();
                assert_eq!(&key, "le");
                let le: f64 = le.parse().unwrap();
                let metric = get_metric(&mut metrics, name);
                metric
                    .values
                    .entry(fields)
                    .or_insert_with(|| MetricValue::Histogram(Histogram::default()))
                    .as_histogram_mut()
                    .unwrap()
                    .buckets
                    .insert(OrderedFloat(le), value);
            } else if let Some(name) = name.strip_suffix("_sum") {
                let metric = get_metric(&mut metrics, name);
                metric
                    .values
                    .entry(fields)
                    .or_insert_with(|| MetricValue::Histogram(Histogram::default()))
                    .as_histogram_mut()
                    .unwrap()
                    .sum = value;
            } else if name.starts_with("_count") {
                // Ignore.
            } else {
                let metric = get_metric(&mut metrics, name);
                metric.values.insert(fields, MetricValue::Number(value));
            }
        }
        Ok(metrics)
    }

    let metrics = read_metrics(BufReader::new(File::open(file_name)?))?;
    let mut by_fields: BTreeMap<&[(String, String)], String> = BTreeMap::new();
    for (name, metric) in &metrics {
        for (fields, value) in &metric.values {
            let s = by_fields.entry(fields.as_slice()).or_default();
            write!(s, "{name:<40}: ").unwrap();

            match value {
                MetricValue::Number(number) => writeln!(s, "{number:15.2}").unwrap(),
                MetricValue::Histogram(histogram) => {
                    let max = histogram.buckets.last_key_value().unwrap().1;
                    let mut sequence = histogram
                        .buckets
                        .values()
                        .copied()
                        .map(|value| ((value / max * 4.0 + 0.5) as usize).clamp(0, 4))
                        .collect::<Vec<_>>();

                    // Convert the histogram into a sequence of [Braille
                    // pattern] characters, with two buckets per Braille character.
                    //
                    // [Braille pattern]: https://en.wikipedia.org/wiki/Braille_Patterns
                    if sequence.len() % 2 == 1 {
                        sequence.push(0);
                    }
                    for (a, b) in sequence.into_iter().tuples() {
                        s.push(
                            ([
                                a >= 4, // ⠁ (dot 1)
                                a >= 3, // ⠂ (dot 2)
                                a >= 2, // ⠄ (dot 3)
                                b >= 4, // ⠈ (dot 4)
                                b >= 3, // ⠐ (dot 5)
                                b >= 2, // ⠠ (dot 6)
                                a >= 1, // ⡀ (dot 7)
                                b >= 1, // ⢀ (dot 8)
                            ]
                            .into_iter()
                            .enumerate()
                            .filter_map(|(index, on)| on.then_some(1u32 << index))
                            .sum::<u32>()
                                + 0x2800)
                                .try_into()
                                .unwrap(),
                        );
                    }
                    writeln!(s, " (count{:13.0}) (sum{:15.2})", histogram.sum, max).unwrap();
                }
            }
        }
    }
    for (fields, output) in by_fields {
        if !fields.is_empty() {
            println!("\n{fields:?}");
        }
        print!("{output}");
    }
    Ok(())
}

async fn unbundle_support_bundle(
    format: OutputFormat,
    zip_path: &Path,
    force: bool,
    dry_run: bool,
    client: Client,
) -> anyhow::Result<()> {
    // Get current Feldera instance configuration
    let instance_config = if !dry_run {
        match client.get_config().send().await {
            Ok(config) => {
                // Check if runtime_version is supported
                let config = config.into_inner();
                let supports_runtime_version = config
                    .unstable_features
                    .as_ref()
                    .map(|features| {
                        features
                            .split(',')
                            .map(|s| s.trim())
                            .any(|feature| feature == "runtime_version")
                    })
                    .unwrap_or(false);
                Some((config.version, supports_runtime_version))
            }
            Err(e) => {
                warn!("Could not retrieve feldera platform config: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Read the zip file
    let zip_file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(zip_file)?;

    // Find all pipeline config files and their corresponding platform versions
    let mut pipeline_configs = Vec::new();

    for i in 0..archive.len() {
        let file = archive.by_index(i)?;
        let filename = file.name().to_string();
        drop(file); // Drop the file reference before getting it again

        if filename.ends_with("_pipeline_config.json") {
            pipeline_configs.push(filename);
        }
    }
    // Make sure latest config taken comes first
    pipeline_configs.sort_unstable_by(|a, b| b.cmp(a));

    if pipeline_configs.is_empty() {
        warn!("No pipeline config files found in the support bundle.");
        return Ok(());
    }

    info!("Found {} pipeline config file(s):", pipeline_configs.len());
    for config in &pipeline_configs {
        info!("  - {}", config);
    }

    let mut prev_code: Option<String> = None;
    let mut prev_udf: Option<String> = None;
    let mut prev_toml: Option<String> = None;

    // Process each pipeline config file
    for config_file in pipeline_configs.iter() {
        debug!("Processing {}...", config_file);

        // Extract the config file from the zip
        let mut file = archive.by_name(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        // Parse the pipeline config JSON
        let config: Value = serde_json::from_str(&contents)?;

        // Extract the required fields
        let name = config["name"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Pipeline name not found in config"))?;
        // Extract timestamp from config file name to make pipeline name unique
        let timestamp = config_file
            .split('_')
            .next()
            .unwrap_or("unknown")
            .replace([':', '.', '+'], "-");
        let pipeline_name = format!("{}_unbundled_{}", name, timestamp);
        let program_code = config["program_code"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Program code not found in config"))?;
        let udf_rust = config["udf_rust"].as_str().unwrap_or("").to_string();
        let udf_toml = config["udf_toml"].as_str().unwrap_or("").to_string();
        let description = config["description"].as_str().map(|s| s.to_string());
        let platform_version = config["platform_version"].as_str().map(|s| s.to_string());
        let runtime_config: Option<RuntimeConfig> =
            serde_json::from_value(config["runtime_config"].clone()).ok();

        let udf_rust = if udf_rust.is_empty() {
            None
        } else {
            Some(udf_rust.clone())
        };
        let udf_toml = if udf_toml.is_empty() {
            None
        } else {
            Some(udf_toml.clone())
        };

        if prev_code.as_deref() == Some(program_code)
            && prev_udf == udf_rust
            && prev_toml == udf_toml
        {
            info!("Skip {pipeline_name}, already created this pipeline from the bundle.");
            continue;
        }

        // Get the corresponding platform version for this pipeline config
        let platform_version_tag = platform_version
            .as_ref()
            .map(|platform_version| format!("v{platform_version}"));

        // Determine if we should use runtime_version based on instance capabilities
        let runtime_version = if let Some((instance_version, supports_runtime_version)) =
            &instance_config
        {
            if *supports_runtime_version {
                platform_version_tag.clone()
            } else {
                if let Some(bundle_version_str) = &platform_version
                    && bundle_version_str != instance_version
                {
                    warn!(
                        "This Feldera instance does not enable `runtime_version`. Pipelines will be created using the instance's current version instead of the bundle's version, which may lead to incompatibilities or unexpected behavior. To ensure pipelines match the bundle's version, restart the platform with `FELDERA_UNSTABLE_FEATURES='runtime_version'`."
                    );
                }
                None
            }
        } else {
            platform_version_tag.clone() // Use it in dry-run or if we couldn't check
        };

        // Extract program config if available, using corresponding platform version as runtime_version if not specified
        let program_config = if let Some(pc) = config.get("program_config") {
            Some(ProgramConfig {
                cache: pc["cache"].as_bool().unwrap_or(true),
                profile: pc["profile"].as_str().map(|s| match s {
                    "dev" => CompilationProfile::Dev,
                    "unoptimized" => CompilationProfile::Unoptimized,
                    "optimized_symbols" => CompilationProfile::OptimizedSymbols,
                    _ => CompilationProfile::Optimized,
                }),
                runtime_version: pc["runtime_version"]
                    .as_str()
                    .map(|s| s.to_string())
                    .or(runtime_version.clone()),
            })
        } else {
            Some(ProgramConfig {
                cache: true,
                profile: Some(CompilationProfile::Optimized),
                runtime_version: runtime_version.clone(),
            })
        };

        info!("Found pipeline: {}", pipeline_name);
        info!("SQL code: {} characters", program_code.len());

        prev_code = Some(program_code.to_string());
        prev_udf = udf_rust.clone();
        prev_toml = udf_toml.clone();

        if dry_run {
            println!("[DRY RUN] Create: {pipeline_name}");
            if let Some(ref version) = runtime_version {
                println!("  Runtime version: {}", version);
            } else if platform_version_tag.is_some() {
                println!("  Runtime version: <not set - feature not supported>");
            }
            let preview_lines: Vec<&str> = program_code.lines().take(3).collect();
            if !preview_lines.is_empty() {
                println!("  SQL code:");
                for line in preview_lines {
                    if !line.trim().is_empty() {
                        println!("    {}", line.trim());
                    }
                }
                if program_code.lines().count() > 3 {
                    println!("    ... ({} more lines)", program_code.lines().count() - 3);
                }
            }
            if let Some(udf_rust) = udf_rust {
                println!("  UDF code: {} characters", udf_rust.len());
            }
            if let Some(udf_toml) = udf_toml {
                println!("  UDF config: {} characters", udf_toml.len());
            }
        } else {
            // Delete pipeline if already exists
            if force {
                let res = client
                    .get_pipeline()
                    .pipeline_name(pipeline_name.clone())
                    .send()
                    .await;
                if res.is_ok() {
                    Box::pin(pipeline(
                        format,
                        PipelineAction::Delete {
                            name: pipeline_name.clone(),
                            force: true,
                        },
                        client.clone(),
                    ))
                    .await;
                }
            }
            client
                .post_pipeline()
                .body(PostPutPipeline {
                    description,
                    name: pipeline_name.clone(),
                    program_code: program_code.to_string(),
                    udf_rust,
                    udf_toml,
                    program_config,
                    runtime_config,
                })
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    format!("Unable to create pipeline {pipeline_name}").leak(),
                    1,
                ))
                .unwrap();
        }
    }

    Ok(())
}

pub async fn debug(format: OutputFormat, action: DebugActions, client: Client) {
    match action {
        DebugActions::MsgpCat { path } => {
            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(error) => {
                    eprintln!("{}: open failed ({error})", path.display());
                    std::process::exit(1);
                }
            };
            loop {
                let value = match rmpv::decode::value::read_value(&mut file) {
                    Ok(value) => value,
                    Err(rmpv::decode::Error::InvalidMarkerRead(error))
                        if error.kind() == ErrorKind::UnexpectedEof =>
                    {
                        break;
                    }
                    Err(error) => {
                        eprintln!("{}: read failed ({error})", path.display());
                        std::process::exit(1);
                    }
                };
                println!("{value}");
            }
        }
        DebugActions::Metrics { path } => {
            if let Err(error) = parse_metrics(&path) {
                eprintln!("{}", error);
                std::process::exit(1);
            }
        }
        DebugActions::Unbundle {
            path,
            dry_run,
            force,
        } => match unbundle_support_bundle(format, &path, force, dry_run, client).await {
            Ok(()) => {
                if dry_run {
                    println!("\nNo pipelines were created.");
                    println!("Remove --dry-run flag to actually create the pipelines.");
                }
            }
            Err(error) => {
                eprintln!("Failed to unbundle support bundle: {}", error);
                std::process::exit(1);
            }
        },
    }
}
