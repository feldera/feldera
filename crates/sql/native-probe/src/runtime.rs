use std::{fs, process::Command};

use dlopen2::wrapper::{Container, WrapperApi};
use tempfile::TempDir;

use crate::{Arithmetic, Batch, Error, Program, codegen};

#[derive(WrapperApi)]
struct Api {
    feldera_batch: unsafe extern "C" fn(
        values: *const i64,
        nulls: *const u64,
        rows: usize,
        output: *mut i64,
        output_nulls: *mut u8,
        error_record: *mut i64,
    ) -> u64,
}

/// Owns both the loaded code and its temporary files. No function pointer escapes
/// this owner. The library is unloaded before the directory is removed.
pub struct CompiledProgram {
    library: Container<Api>,
    _directory: TempDir,
    columns: usize,
}

impl CompiledProgram {
    pub fn compile(program: &Program) -> Result<Self, Error> {
        let (directory, library_path) = link_object(codegen::object(program)?)?;
        // SAFETY: We load only the object generated above in our private TempDir,
        // never a caller-supplied path. codegen emits this exact C ABI on the host
        // ISA, without constructors or callbacks. Container retains the library.
        let library = unsafe { Container::load(&library_path) }
            .map_err(|e| Error::Compile(format!("cannot load native batch: {e}")))?;
        Ok(Self {
            library,
            _directory: directory,
            columns: program.columns,
        })
    }

    pub fn evaluate(&self, batch: &Batch) -> Result<Vec<Option<i64>>, Error> {
        batch.check_columns(self.columns)?;
        if batch.rows() == 0 {
            return Ok(Vec::new());
        }
        let mut values = vec![0_i64; batch.rows()];
        let mut nulls = vec![0_u8; batch.rows()];
        let mut error_record = [0_i64; 3];
        // SAFETY: Batch privately maintains rows*columns initialized i64s and
        // rows initialized u64 masks; the schema was checked above. Validation
        // bounds every generated column access. The loop accesses rows elements
        // of each disjoint output allocation, never beyond the batch. All buffers
        // and the library stay alive for the call. Zero-column programs never
        // dereference the empty input payload. On overflow, codegen writes three
        // i64s (operation, left, right) into error_record. It cannot unwind.
        let status = unsafe {
            self.library.feldera_batch(
                batch.values.as_ptr(),
                batch.nulls.as_ptr(),
                batch.rows(),
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                error_record.as_mut_ptr(),
            )
        };
        if status != 0 {
            if status > batch.rows() as u64 {
                return Err(Error::InvalidNativeResult);
            }
            let op = match error_record[0] {
                tag if tag == Arithmetic::Add as i64 => Arithmetic::Add,
                tag if tag == Arithmetic::Subtract as i64 => Arithmetic::Subtract,
                tag if tag == Arithmetic::Multiply as i64 => Arithmetic::Multiply,
                _ => return Err(Error::InvalidNativeResult),
            };
            return Err(Error::Overflow {
                row: (status - 1) as usize,
                op,
                left: error_record[1],
                right: error_record[2],
            });
        }
        Ok(values
            .into_iter()
            .zip(nulls)
            .map(|(value, null)| (null == 0).then_some(value))
            .collect())
    }
}

pub(crate) fn link_object(object: Vec<u8>) -> Result<(TempDir, std::path::PathBuf), Error> {
    if !cfg!(target_os = "linux") {
        return Err(Error::Compile(
            "the shared-library spike currently requires Linux".into(),
        ));
    }
    let directory = tempfile::tempdir().map_err(|e| Error::Compile(e.to_string()))?;
    let object_path = directory.path().join("batch.o");
    let library_path = directory.path().join("batch.so");
    fs::write(&object_path, object).map_err(|e| Error::Compile(e.to_string()))?;
    let output = Command::new("cc")
        .arg("-shared")
        .arg("-Wl,-z,defs,-z,noexecstack")
        .arg(&object_path)
        .arg("-o")
        .arg(&library_path)
        .output()
        .map_err(|e| Error::Compile(format!("cannot run cc: {e}")))?;
    if !output.status.success() {
        return Err(Error::Compile(format!(
            "cc exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok((directory, library_path))
}
