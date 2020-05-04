/*
 * Copyright 2020 Cargill Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

//! Enables loading smart contract archives from scar files.
//!
//! A scar file is a tar bzip2 archive that contains two required files:
//! - A `.wasm` file that contains a WASM smart contract
//! - A `manifest.yaml` file that contains, at a minimum:
//!   - The contract `name`
//!   - The contract `version`
//!   - The contract's `input` addresses
//!   - The contract's `output` addresses
//!
//! # Examples
//!
//! ```no_run
//! use transact::contract::archive::{default_scar_path, SmartContractArchive};
//!
//! let archive = SmartContractArchive::from_scar_file("xo", "0.1", &default_scar_path())
//!     .expect("failed to load scar file");
//! ```

mod error;

use std::env::{split_paths, var_os};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use bzip2::read::BzDecoder;
use glob::glob;
use semver::{Version, VersionReq};
use tar::Archive;

pub use error::Error;

const DEFAULT_SCAR_PATH: &str = "/usr/share/scar";
const SCAR_PATH_ENV_VAR: &str = "SCAR_PATH";

/// Load default location(s) for scar files.
pub fn default_scar_path() -> Vec<PathBuf> {
    match var_os(SCAR_PATH_ENV_VAR) {
        Some(paths) => split_paths(&paths).collect(),
        None => vec![PathBuf::from(DEFAULT_SCAR_PATH)],
    }
}

/// The definition of a smart contract, including the contract's associated metadata and the bytes
/// of the smart contract itself.
pub struct SmartContractArchive {
    /// The raw bytes of the WASM smart contract.
    pub contract: Vec<u8>,
    /// The metadata associated with this smart contract.
    pub metadata: SmartContractMetadata,
}

impl SmartContractArchive {
    /// Attempt to load a `SmartContractArchive` from a scar file in one of the given `paths`.
    ///
    /// The scar file to load will be named `<name>_<version>.scar`, where `<name>` is the name
    /// specified for this method and `<version>` is a valid semver string.
    ///
    /// The `name` argument for this method must not contain any underscores, '_'.
    ///
    /// The `version` argument for this method must be a valid semver requirement string. The
    /// latest contract (based on semver requirement rules) that matches the given name and meets
    /// the version requirements will be loaded.
    ///
    /// For more info on semver: https://semver.org/
    ///
    /// # Example
    ///
    /// If there are two files `/usr/share/scar/xo_1.2.3.scar` and `/some/other/dir/xo_1.2.4`, you
    /// can load v1.2.4 with:
    ///
    /// ```no_run
    /// use transact::contract::archive::SmartContractArchive;
    ///
    /// let archive = SmartContractArchive::from_scar_file(
    ///     "xo",
    ///     "1.2",
    ///     &["/usr/share/scar", "/some/other/dir"],
    /// )
    /// .expect("failed to load scar file");
    /// assert_eq!(&archive.metadata.version, "1.2.4");
    /// ```
    ///
    /// To load v1.2.3 explicitly, specify the exact version:
    ///
    /// ```no_run
    /// use transact::contract::archive::SmartContractArchive;
    ///
    /// let archive = SmartContractArchive::from_scar_file(
    ///     "xo",
    ///     "1.2.3",
    ///     &["/usr/share/scar", "/some/other/dir"],
    /// )
    /// .expect("failed to load scar file");
    /// assert_eq!(&archive.metadata.version, "1.2.3");
    /// ```
    pub fn from_scar_file<P: AsRef<Path>>(
        name: &str,
        version: &str,
        paths: &[P],
    ) -> Result<SmartContractArchive, Error> {
        let file_path = find_scar(name, version, paths)?;

        let scar_file = File::open(&file_path).map_err(|err| {
            Error::new_with_source(
                &format!("failed to open file {}", file_path.display()),
                err.into(),
            )
        })?;
        let mut archive = Archive::new(BzDecoder::new(scar_file));
        let archive_entries = archive.entries().map_err(|err| {
            Error::new_with_source(
                &format!("failed to read scar file {}", file_path.display()),
                err.into(),
            )
        })?;

        let mut metadata = None;
        let mut contract = None;

        for entry in archive_entries {
            let mut entry = entry.map_err(|err| {
                Error::new_with_source(
                    &format!(
                        "invalid scar file: failed to read archive entry from {}",
                        file_path.display()
                    ),
                    err.into(),
                )
            })?;
            let path = entry
                .path()
                .map_err(|err| {
                    Error::new_with_source(
                        &format!(
                            "invalid scar file: failed to get path of archive entry from {}",
                            file_path.display()
                        ),
                        err.into(),
                    )
                })?
                .into_owned();
            if path_is_manifest(&path) {
                metadata = Some(serde_yaml::from_reader(entry).map_err(|err| {
                    Error::new_with_source(
                        &format!(
                            "invalid scar file: manifest.yaml invalid in {}",
                            file_path.display()
                        ),
                        err.into(),
                    )
                })?);
            } else if path_is_wasm(&path) {
                let mut contract_bytes = vec![];
                entry.read_to_end(&mut contract_bytes).map_err(|err| {
                    Error::new_with_source(
                        &format!(
                            "invalid scar file: failed to read smart contract in {}",
                            file_path.display()
                        ),
                        err.into(),
                    )
                })?;
                contract = Some(contract_bytes);
            }
        }

        let contract = contract.ok_or_else(|| {
            Error::new(&format!(
                "invalid scar file: smart contract not found in {}",
                file_path.display()
            ))
        })?;
        let metadata: SmartContractMetadata = metadata.ok_or_else(|| {
            Error::new(&format!(
                "invalid scar file: manifest.yaml not found in {}",
                file_path.display()
            ))
        })?;

        validate_metadata(&name, &metadata.name)?;

        Ok(SmartContractArchive { contract, metadata })
    }
}

/// The metadata of a smart contract archive.
#[derive(Debug, Deserialize, Serialize)]
pub struct SmartContractMetadata {
    /// The name of the smart contract.
    pub name: String,
    /// The version of the smart contract.
    pub version: String,
    /// The list of input addresses used by this smart contract.
    pub inputs: Vec<String>,
    /// The list of output addresses used by this smart contract.
    pub outputs: Vec<String>,
}

fn find_scar<P: AsRef<Path>>(name: &str, version: &str, paths: &[P]) -> Result<PathBuf, Error> {
    let file_name_pattern = format!("{}_*.scar", name);

    validate_scar_file_name(name)?;

    let version_req = VersionReq::parse(version)?;

    // Start with all scar files that match the name, from all paths
    paths
        .iter()
        .map(|path| {
            let file_path_pattern = path.as_ref().join(&file_name_pattern);
            let pattern_string = file_path_pattern
                .to_str()
                .ok_or_else(|| Error::new("name is not valid UTF-8"))?;
            Ok(glob(pattern_string)?)
        })
        .collect::<Result<Vec<_>, Error>>()?
        .into_iter()
        .flatten()
        // Filter out any files that can't be read
        .filter_map(|path_res| path_res.ok())
        // Filter out any files that don't meet the version requirements
        .filter_map(|path| {
            let file_stem = path.file_stem()?.to_str()?;
            let version_str = (*file_stem.splitn(2, '_').collect::<Vec<_>>().get(1)?).to_string();
            let version = Version::parse(&version_str).ok()?;
            if version_req.matches(&version) {
                Some((path, version))
            } else {
                None
            }
        })
        // Get the file with the highest matching version
        .max_by(|(_, x_version), (_, y_version)| x_version.cmp(y_version))
        .map(|(path, _)| path)
        .ok_or_else(|| {
            let paths = paths
                .iter()
                .map(|path| path.as_ref().display())
                .collect::<Vec<_>>();
            Error::new(&format!(
                "could not find contract '{}' that meets version requirement '{}' in paths '{:?}'",
                name, version_req, paths,
            ))
        })
}

// Validate that the scar file name does not contain underscores, otherwise return an error.
fn validate_scar_file_name(name: &str) -> Result<(), Error> {
    if name.contains('_') {
        return Err(Error::new(&format!(
            "invalid scar file name, must not include '_': {}",
            name
        )));
    }
    Ok(())
}

// Validate that the metadata collected from the manifest contains a contract name which matches
// the name of the scar file. This includes swapping any underscores which appear in the contract
// name with dashes, as underscores are not allowed in scar file names.
fn validate_metadata(file_name: &str, contract_name: &str) -> Result<(), Error> {
    if file_name != contract_name.replace("_", "-") {
        return Err(Error::new(&format!(
            "scar file name `{}` does not match contract name in manifest `{}`",
            file_name, contract_name,
        )));
    }
    Ok(())
}

fn path_is_manifest(path: &std::path::Path) -> bool {
    path.file_name()
        .map(|file_name| file_name == "manifest.yaml")
        .unwrap_or(false)
}

fn path_is_wasm(path: &std::path::Path) -> bool {
    match path.extension() {
        Some(extension) => extension == "wasm",
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;
    use std::path::Path;

    use bzip2::write::BzEncoder;
    use bzip2::Compression;
    use serde::Serialize;
    use serial_test::serial;
    use tar::Builder;
    use tempdir::TempDir;

    const MOCK_CONTRACT_BYTES: &[u8] = &[0x00, 0x01, 0x02, 0x03];

    // The tests that modify and read environment variable(s) must be run serially to prevent
    // interference with each other. Each of these tests is annotated with `#[serial(scar_path)]`
    // to enforce this.

    /// Verify that the `default_scar_path()` method returns DEFAULT_SCAR_PATH when
    /// SCAR_PATH_ENV_VAR is unset.
    #[test]
    #[serial(scar_path)]
    fn default_scar_path_env_unset() {
        std::env::remove_var(SCAR_PATH_ENV_VAR);
        assert_eq!(default_scar_path(), vec![PathBuf::from(DEFAULT_SCAR_PATH)]);
    }

    /// Verify that the `default_scar_path()` method returns all paths in SCAR_PATH_ENV_VAR when it
    /// is set.
    #[test]
    #[serial(scar_path)]
    fn default_scar_path_env_set() {
        let paths = vec!["/test/dir", "/other/dir", ".", "~/"];
        let joined_paths = std::env::join_paths(&paths).expect("failed to join paths");
        std::env::set_var(SCAR_PATH_ENV_VAR, joined_paths);

        assert_eq!(
            default_scar_path(),
            paths
                .iter()
                .map(|path| PathBuf::from(path))
                .collect::<Vec<_>>()
        );
    }

    /// Verify that an error is returned when the contract name contains '_'.
    #[test]
    fn find_scar_invalid_name() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "non_existent", "0.1.0");

        assert!(find_scar("non_existent", "0.1.0", &[&dir]).is_err());
    }

    /// Verify that an error is returned when the contract version is an invalid semver string.
    #[test]
    fn find_scar_invalid_version() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "nonexistent", "0.1..0");

        assert!(find_scar("nonexistent", "0.1..0", &[&dir]).is_err());
    }

    /// Verify that an error is returned when no contract with the given name exists.
    #[test]
    fn find_scar_no_matching_name() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "0.1.0");

        assert!(find_scar("nonexistent", "0.1.0", &[&dir]).is_err());
    }

    /// Verify that an error is returned when no matching contract is found in the given path.
    #[test]
    fn find_scar_not_in_path() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "0.1.0");

        assert!(find_scar("mock", "0.1.0", &["/some/other/dir"]).is_err());
    }

    /// Verify that an error is returned when no contract is found with a sufficient patch version
    /// and no pre-release tag.
    #[test]
    fn find_scar_insufficient_release() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "0.1.0-dev");

        assert!(find_scar("mock", "0.1.0", &[&dir]).is_err());
    }

    /// Verify that an error is returned when no contract is found with a sufficient patch version.
    #[test]
    fn find_scar_insufficient_patch() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "0.1.0");

        assert!(find_scar("mock", "0.1.1", &[&dir]).is_err());
    }

    /// Verify that an error is returned when no contract is found with a sufficient minor version.
    #[test]
    fn find_scar_insufficient_minor() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "0.1.0");

        assert!(find_scar("mock", "0.2.0", &[&dir]).is_err());
    }

    /// Verify that an error is returned when no contract is found with a sufficient major version.
    #[test]
    fn find_scar_insufficient_major() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "0.1.0");

        assert!(find_scar("mock", "1.0.0", &[&dir]).is_err());
    }

    /// Verify that a scar file is correctly found when any version (`*`) is requested.
    #[test]
    fn find_scar_any_version() {
        let dir = new_temp_dir();
        let scar_path = write_mock_scar(&dir, "mock", "0.1.0");

        assert_eq!(
            find_scar("mock", "*", &[&dir]).expect("failed to find scar"),
            scar_path
        );
    }

    /// Verify that a scar file is correctly found when the exact version is requested.
    #[test]
    fn find_scar_exact_version() {
        let dir = new_temp_dir();
        let scar_path = write_mock_scar(&dir, "mock", "0.1.2-dev");

        assert_eq!(
            find_scar("mock", "0.1.2-dev", &[&dir]).expect("failed to find scar"),
            scar_path
        );
    }

    /// Verify that a scar file is correctly found when it meets the minimum minor version.
    #[test]
    fn find_scar_minimum_minor() {
        let dir = new_temp_dir();
        let scar_path = write_mock_scar(&dir, "mock", "0.1.2");

        assert_eq!(
            find_scar("mock", "0.1", &[&dir]).expect("failed to find scar"),
            scar_path
        );
    }

    /// Verify that a scar file is correctly found when it meets the minimum major version.
    #[test]
    fn find_scar_minimum_major() {
        let dir = new_temp_dir();
        let scar_path = write_mock_scar(&dir, "mock", "1.2.3");

        assert_eq!(
            find_scar("mock", "1", &[&dir]).expect("failed to find scar"),
            scar_path
        );
    }

    /// Verify that the scar file with no pre-release tag is returned rather than the same patch
    /// version with a pre-release tag.
    #[test]
    fn find_scar_highest_matching_patch() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "0.1.2-dev");
        let scar_path = write_mock_scar(&dir, "mock", "0.1.2");

        assert_eq!(
            find_scar("mock", "0.1.2", &[&dir]).expect("failed to find scar"),
            scar_path
        );
    }

    /// Verify that the scar file with the highest patch version is returned for the specified
    /// minor version.
    #[test]
    fn find_scar_highest_matching_minor() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "0.1.1");
        let scar_path = write_mock_scar(&dir, "mock", "0.1.2");

        assert_eq!(
            find_scar("mock", "0.1", &[&dir]).expect("failed to find scar"),
            scar_path
        );
    }

    /// Verify that the scar file with the highest minor version is returned for the specified
    /// major version.
    #[test]
    fn find_scar_highest_matching_major() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock", "1.1.0");
        let scar_path = write_mock_scar(&dir, "mock", "1.2.0");

        assert_eq!(
            find_scar("mock", "1", &[&dir]).expect("failed to find scar"),
            scar_path
        );
    }

    /// Verify that the scar file with the highest matching version is returned when there are
    /// versions in different paths.
    #[test]
    fn find_scar_highest_matching_across_paths() {
        let thread_id = format!("{:?}", std::thread::current().id());
        let dir1 = TempDir::new(&format!("{}1", thread_id)).expect("failed to create temp dir1");
        let dir2 = TempDir::new(&format!("{}2", thread_id)).expect("failed to create temp dir2");
        write_mock_scar(&dir1, "mock", "1.2.3");
        let scar_path = write_mock_scar(&dir2, "mock", "1.2.4");

        assert_eq!(
            find_scar("mock", "1", &[&dir1, &dir2]).expect("failed to find scar"),
            scar_path
        );
    }

    /// Verify that a valid scar file is successfully loaded.
    #[test]
    fn load_scar_file_successful() {
        let dir = new_temp_dir();
        write_mock_scar(&dir, "mock-scar", "1.0.0");

        let scar = SmartContractArchive::from_scar_file("mock-scar", "1.0.0", &[&dir])
            .expect("failed to load scar");

        assert_eq!(scar.contract, MOCK_CONTRACT_BYTES);
        assert_eq!(scar.metadata.name, mock_smart_contract_metadata().name);
        assert_eq!(
            scar.metadata.version,
            mock_smart_contract_metadata().version
        );
        assert_eq!(scar.metadata.inputs, mock_smart_contract_metadata().inputs);
        assert_eq!(
            scar.metadata.outputs,
            mock_smart_contract_metadata().outputs
        );
    }

    /// Verify that an error is returned when attempting to load a scar file that does not contain
    /// a `manifest.yaml` file.
    #[test]
    fn load_scar_manifest_not_found() {
        let dir = new_temp_dir();

        let contract_file_path = write_contract(&dir);
        write_scar_file::<&Path>(
            dir.as_ref(),
            "mock-scar",
            "1.0.0",
            None,
            Some(contract_file_path.as_path()),
        );

        assert!(SmartContractArchive::from_scar_file("mock-scar", "1.0.0", &[&dir]).is_err());
    }

    /// Verify that an error is returned when attempting to load a scar file whose `manifest.yaml`
    /// is invalidly formatted.
    #[test]
    fn load_scar_manifest_invalid() {
        let dir = new_temp_dir();

        let manifest_file_path = write_manifest(&dir, &[0x00]);
        let contract_file_path = write_contract(&dir);
        write_scar_file::<&Path>(
            dir.as_ref(),
            "mock-scar",
            "1.0.0",
            Some(manifest_file_path.as_path()),
            Some(contract_file_path.as_path()),
        );

        assert!(SmartContractArchive::from_scar_file("mock-scar", "1.0.0", &[&dir]).is_err());
    }

    /// Verify that an error is returned when attempting to load a scar file that does not contain
    /// a .wasm smart contract.
    #[test]
    fn load_scar_contract_not_found() {
        let dir = new_temp_dir();

        let manifest_file_path = write_manifest(&dir, &mock_smart_contract_metadata());
        write_scar_file::<&Path>(
            dir.as_ref(),
            "mock-scar",
            "1.0.0",
            Some(manifest_file_path.as_path()),
            None,
        );

        assert!(SmartContractArchive::from_scar_file("mock-scar", "1.0.0", &[&dir]).is_err());
    }

    /// Verify that an error is returned when attempting to load a scar file that does not contain
    /// a smart contract with a name that matches the file name.
    #[test]
    fn load_scar_manifest_not_matching() {
        let dir = new_temp_dir();
        let manifest_file_path = write_manifest(&dir, &mock_invalid_smart_contract_metadata());

        write_scar_file::<&Path>(
            dir.as_ref(),
            "mock-scar",
            "1.0.0",
            Some(&manifest_file_path),
            None,
        );

        assert!(SmartContractArchive::from_scar_file("mock-scar", "1.0.0", &[&dir]).is_err());
    }

    fn new_temp_dir() -> TempDir {
        let thread_id = format!("{:?}", std::thread::current().id());
        TempDir::new(&thread_id).expect("failed to create temp dir")
    }

    fn write_mock_scar<P: AsRef<Path>>(dir: P, name: &str, version: &str) -> PathBuf {
        let manifest_file_path = write_manifest(&dir, &mock_smart_contract_metadata());
        let contract_file_path = write_contract(&dir);
        write_scar_file::<&Path>(
            dir.as_ref(),
            name,
            version,
            Some(manifest_file_path.as_path()),
            Some(contract_file_path.as_path()),
        )
    }

    fn write_manifest<P: AsRef<Path>, T: Serialize>(dir: P, manifest: &T) -> PathBuf {
        let manifest_file_path = dir.as_ref().join("manifest.yaml");
        let manifest_file =
            File::create(manifest_file_path.as_path()).expect("failed to create manifest file");
        serde_yaml::to_writer(manifest_file, manifest).expect("failed to write manifest file");
        manifest_file_path
    }

    fn write_contract<P: AsRef<Path>>(dir: P) -> PathBuf {
        let contract_file_path = dir.as_ref().join("mock.wasm");
        let mut contract_file =
            File::create(contract_file_path.as_path()).expect("failed to create contract file");
        contract_file
            .write_all(MOCK_CONTRACT_BYTES)
            .expect("failed to write contract file");
        contract_file_path
    }

    fn write_scar_file<P: AsRef<Path>>(
        dir: P,
        name: &str,
        version: &str,
        manifest_file_path: Option<P>,
        contract_file_path: Option<P>,
    ) -> PathBuf {
        let scar_file_path = dir.as_ref().join(format!("{}_{}.scar", name, version));
        let scar_file = File::create(&scar_file_path).expect("failed to create scar file");
        let mut scar_file_builder = Builder::new(BzEncoder::new(scar_file, Compression::Default));

        if let Some(manifest_file_path) = manifest_file_path {
            scar_file_builder
                .append_path_with_name(manifest_file_path, "manifest.yaml")
                .expect("failed to add manifest to scar file");
        }

        if let Some(contract_file_path) = contract_file_path {
            scar_file_builder
                .append_path_with_name(contract_file_path, "mock.wasm")
                .expect("failed to add contract to scar file");
        }

        scar_file_builder
            .finish()
            .expect("failed to write scar file");

        scar_file_path
    }

    fn mock_smart_contract_metadata() -> SmartContractMetadata {
        SmartContractMetadata {
            name: "mock_scar".into(),
            version: "1.0".into(),
            inputs: vec!["abcdef".into()],
            outputs: vec!["012345".into()],
        }
    }

    fn mock_invalid_smart_contract_metadata() -> SmartContractMetadata {
        SmartContractMetadata {
            name: "invalid_scar".into(),
            version: "1.0".into(),
            inputs: vec!["abcdef".into()],
            outputs: vec!["012345".into()],
        }
    }
}
