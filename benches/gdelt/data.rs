//! GKG Entries take the form of this sql table (translated from the [GKG
//! cookbook])
//!
//! ```sql
//! CREATE TABLE gdeltv2.gkg (
//!     GKGRECORDID TEXT,
//!     V2.1DATE INT,
//!     V2SOURCECOLLECTIONIDENTIFIER INT,
//!     V2SOURCECOMMONNAME TEXT,
//!     V2DOCUMENTIDENTIFIER TEXT,
//!     -- Semicolon delimited blocks with pound (`#`) delimited fields
//!     V1COUNTS TEXT,
//!     -- Semicolon delimited blocks with pound (`#`) delimited fields
//!     V2.1COUNTS TEXT,
//!     -- Semicolon delimited
//!     V1THEMES TEXT,
//!     -- Semicolon delimited blocks with comma delimited fields
//!     V2ENHANCEDTHEMES TEXT,
//!     -- Semicolon delimited blocks with pound (`#`) delimited fields
//!     V1LOCATIONS TEXT,
//!     -- Semicolon delimited blocks with pound (`#`) delimited fields
//!     V2ENHANCEDLOCATIONS TEXT,
//!     -- Semicolon delimited
//!     V1PERSONS TEXT,
//!     -- Semicolon delimited blocks with comma delimited fields
//!     V2ENHANCEDPERSONS TEXT,
//!     -- Semicolon delimited
//!     V1ORGANIZATIONS TEXT,
//!     -- Semicolon delimited blocks with comma delimited fields
//!     V2ENHANCEDORGANIZATIONS TEXT,
//!     -- Comma delimited fields
//!     V1.5TONE TEXT,
//!     -- Semicolon delimited blocks with comma delimited fields
//!     V2.1ENHANCEDDATES TEXT,
//!     -- Comma delimited blocks with colon (`:`) delimited key/value pairs
//!     V2GCAM TEXT,
//!     V2.1SHARINGIMAGE TEXT,
//!     -- Semicolon delimited list of urls
//!     V2.1RELATEDIMAGES TEXT,
//!     -- Semicolon delimited list of urls
//!     V2.1SOCIALIMAGEEMBEDS TEXT,
//!     -- Semicolon delimited list of urls
//!     V2.1SOCIALVIDEOEMBEDS TEXT,
//!     -- Pound delimited (`#`) blocks, with pipe delimited (`|`) fields
//!     V2.1QUOTATIONS TEXT,
//!     -- Semicolon delimited blocks with comma delimited fields
//!     V2.1ALLNAMES TEXT,
//!     -- Semicolon delimited blocks with comma delimited fields
//!     V2.1AMOUNTS TEXT,
//!     -- Semicolon delimited fields
//!     V2.1TRANSLATIONINFO TEXT,
//!     -- XML data
//!     V2EXTRASXML TEXT,
//! );
//! ```
//!
//! [GKG cookbook]: http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf

use arcstr::{literal, ArcStr};
use csv::{ReaderBuilder, Trim};
use dbsp::CollectionHandle;
use hashbrown::{HashMap, HashSet};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{BufReader, BufWriter, Write},
    path::Path,
};
use xxhash_rust::xxh3::Xxh3Builder;
use zip::ZipArchive;

const DATA_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/benches/gdelt-data");

const MASTER_LIST: &str = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt";
// const LAST_15_MINUTES: &str = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt";

pub const GKG_SUFFIX: &str = ".gkg.csv.zip";
pub const GDELT_URL: &str = "http://data.gdeltproject.org/gdeltv2/";

type Interner = HashSet<ArcStr, Xxh3Builder>;
type Invalid = HashSet<&'static str, Xxh3Builder>;
type Normalizations = HashMap<&'static str, &'static [ArcStr], Xxh3Builder>;

#[derive(Debug, Clone, SizeOf)]
pub struct PersonalNetworkGkgEntry {
    pub id: ArcStr,
    pub date: u64,
    pub people: Vec<ArcStr>,
}

impl PartialEq for PersonalNetworkGkgEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PersonalNetworkGkgEntry {}

impl PartialOrd for PersonalNetworkGkgEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for PersonalNetworkGkgEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for PersonalNetworkGkgEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

// TODO: Probably want to check via `If-Modified-Since` header if the master
// file list has been updated since the last time we downloaded it since it
// likely has
pub fn get_master_file(update: bool) -> File {
    fs::create_dir_all(DATA_PATH).unwrap();

    let master_path = Path::new(DATA_PATH).join("masterfilelist.txt");
    if update || !master_path.exists() {
        print!(
            "{} master file list... ",
            if update { "updating" } else { "downloading" },
        );
        std::io::stdout().flush().unwrap();

        reqwest::blocking::get(MASTER_LIST)
            .unwrap()
            .copy_to(&mut BufWriter::new(File::create(&master_path).unwrap()))
            .unwrap();

        println!("done");
    }

    File::open(master_path).unwrap()
}

pub fn get_gkg_file(url: &str) -> Option<File> {
    let name = url.strip_prefix(GDELT_URL).unwrap();
    let zip_path = Path::new(DATA_PATH).join(name);
    let path = zip_path.with_extension("");

    if !path.exists() {
        // Download the zip file if it doesn't exist
        if !zip_path.exists() {
            match reqwest::blocking::get(url) {
                Ok(mut response) => {
                    let mut zip_file = match File::create(&zip_path) {
                        Ok(zip_file) => BufWriter::new(zip_file),
                        Err(error) => {
                            eprintln!(
                                "failed to create file '{}' while downloading '{url}': {error}",
                                zip_path.display(),
                            );
                            return None;
                        }
                    };

                    if let Err(error) = response.copy_to(&mut zip_file) {
                        eprintln!(
                            "failed to copy response body from '{url}' to '{}' to disk: {error}",
                            zip_path.display(),
                        );
                        return None;
                    }

                    if let Err(error) = zip_file.flush() {
                        eprintln!("failed to flush '{}' to disk: {error}", zip_path.display());
                        return None;
                    }
                }

                Err(error) => {
                    eprintln!("error occurred while downloading '{url}': {error}");
                    return None;
                }
            }
        }

        // Extract the zip file to the data directory
        let failed = ZipArchive::new(BufReader::new(File::open(&zip_path).unwrap()))
            .and_then(|mut archive| archive.extract(DATA_PATH))
            .is_err();

        // Delete the zip file now that we've extracted it
        if let Err(error) = fs::remove_file(&zip_path) {
            eprintln!(
                "failed to remove zip file '{}': {error}",
                zip_path.display(),
            );
        }

        if failed {
            return None;
        }
    }

    // Open the data file
    Some(File::open(path).unwrap())
}

pub fn parse_personal_network_gkg(
    handle: &mut CollectionHandle<PersonalNetworkGkgEntry, i32>,
    interner: &mut Interner,
    normalizations: &Normalizations,
    invalid: &Invalid,
    file: File,
) -> usize {
    let mut records = 0;
    let reader = ReaderBuilder::new()
        .flexible(true)
        .trim(Trim::All)
        .delimiter(b'\t')
        .has_headers(false)
        .from_reader(file)
        .into_records();

    // We're insanely lenient on our parsing since GDELT's "data format" is more of
    // a suggestion than anything else
    for record in reader.flatten() {
        if let Some(id) = record.get(0).map(ArcStr::from) {
            if let Some(date) = record.get(1).and_then(|date| date.parse().ok()) {
                if let Some(people_record) = record.get(11) {
                    let mut people = Vec::new();

                    for person in people_record.to_lowercase().split(';').map(str::trim) {
                        if !person.is_empty() && !invalid.contains(person) {
                            if let Some(normals) = normalizations.get(person) {
                                people.extend(normals.iter().cloned());
                            } else {
                                people.push(
                                    interner
                                        .get_or_insert_with(person, |person| ArcStr::from(person))
                                        .clone(),
                                );
                            }
                        }
                    }

                    people.sort();
                    people.dedup();

                    let entry = PersonalNetworkGkgEntry { id, date, people };
                    handle.push(entry, 1);
                    records += 1;
                }
            }
        }
    }

    records
}

/// The GDELT data isn't perfect so we have to do some corrections to the
/// generated data
pub fn build_gdelt_normalizations() -> (Interner, Normalizations, Invalid) {
    let mut interner = HashSet::with_capacity_and_hasher(4096, Xxh3Builder::new());

    let normalizations = {
        static NORMALS: &[(&str, &[ArcStr])] = &[
            ("a los angeles", &[literal!("los angeles")]),
            ("a harry truman", &[literal!("harry truman")]),
            ("a ronald reagan", &[literal!("ronald reagan")]),
            ("a lyndon johnson", &[literal!("lyndon johnson")]),
            ("a sanatan dharam", &[literal!("sanatan dharam")]),
            ("b richard nixon", &[literal!("richard nixon")]),
            ("b dwight eisenhower", &[literal!("dwight eisenhower")]),
            ("c george w bush", &[literal!("george w bush")]),
            ("c gerald ford", &[literal!("gerald ford")]),
            ("c john f kennedy", &[literal!("john f kennedy")]),
            // I can't even begin to explain this one
            ("obama jeb bush", &[literal!("jeb bush")]),
            (
                "brandon morse thebrandonmorse",
                &[literal!("brandon morse")],
            ),
            ("lady michelle obama", &[literal!("michelle obama")]),
            ("jo biden", &[literal!("joe biden")]),
            ("joseph robinette biden jr", &[literal!("joe biden")]),
            ("brad thor bradthor", &[literal!("brad thor")]),
            ("hilary clinton", &[literal!("hillary clinton")]),
            ("hillary rodham clinton", &[literal!("hillary clinton")]),
            (
                "sherlockian a sherlock holmes",
                &[literal!("sherlock holmes")],
            ),
            ("america larry pratt", &[literal!("larry pratt")]),
            ("cullen hawkins sircullen", &[literal!("cullen hawkins")]),
            (
                "leslie knope joe biden",
                &[literal!("leslie knope"), literal!("joe biden")],
            ),
            ("jacquelyn martin europe", &[literal!("jacquelyn martin")]),
        ];
        // Add the static normals to the interner, might as well reuse them
        interner.extend(NORMALS.iter().flat_map(|&(_, person)| person).cloned());

        let mut map = HashMap::with_capacity_and_hasher(NORMALS.len(), Xxh3Builder::new());
        map.extend(NORMALS);
        map
    };

    // Invalid "people" that aren't really people
    let invalid = {
        // On one hand, "krispy kreme klub" is most definitely not a person. On the
        // other hand, it's kinda funny to see it pop up in the graph
        let invalid = ["whitehouse cvesummit", "islam obama"];

        let mut set = HashSet::with_capacity_and_hasher(invalid.len(), Xxh3Builder::new());
        set.extend(invalid.into_iter());
        set
    };

    (interner, normalizations, invalid)
}
