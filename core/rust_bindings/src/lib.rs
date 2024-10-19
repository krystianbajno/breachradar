use pyo3::prelude::*;
use pyo3::exceptions::PyIOError;
use regex::Regex;
use sha2::{Sha256, Digest};
use std::fs::File;
use std::io::{BufReader, Read, BufRead};

#[pyfunction]
fn calculate_file_hash(file_path: &str) -> PyResult<String> {
    let file = File::open(file_path).map_err(|e| PyIOError::new_err(format!("Failed to open file: {}", e)))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let n = reader.read(&mut buffer).map_err(|e| PyIOError::new_err(format!("Failed to read file: {}", e)))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let hash = hasher.finalize();
    Ok(format!("{:x}", hash))
}

#[pyfunction]
fn scan_file_for_patterns(file_path: &str, patterns: Vec<&str>) -> PyResult<Vec<String>> {
    let file = File::open(file_path).map_err(|e| PyIOError::new_err(format!("Failed to open file: {}", e)))?;
    let reader = BufReader::new(file);

    let regexes: Vec<Regex> = patterns.iter()
        .map(|p| Regex::new(p).map_err(|e| PyErr::new::<PyIOError, _>(format!("Invalid regex pattern: {}", e))))
        .collect::<Result<Vec<_>, _>>()?;

    let mut matches = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(|e| PyIOError::new_err(format!("Failed to read line: {}", e)))?;
        for re in &regexes {
            for mat in re.find_iter(&line) {
                matches.push(mat.as_str().to_string());
            }
        }
    }

    Ok(matches)
}

#[pyfunction]
fn split_file_into_chunks(file_path: &str, chunk_size: usize) -> PyResult<Vec<(usize, String)>> {
    let file = File::open(file_path).map_err(|e| PyIOError::new_err(format!("Failed to open file: {}", e)))?;
    let mut reader = BufReader::new(file);
    let mut chunks = Vec::new();
    let mut buffer = Vec::with_capacity(chunk_size);
    let mut chunk_number = 1;

    loop {
        let mut buf = [0u8; 8192];
        let n = reader.read(&mut buf).map_err(|e| PyIOError::new_err(format!("Failed to read file: {}", e)))?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&buf[..n]);
        if buffer.len() >= chunk_size {
            let chunk_content = String::from_utf8_lossy(&buffer).to_string();
            chunks.push((chunk_number, chunk_content));
            buffer.clear();
            chunk_number += 1;
        }
    }

    if !buffer.is_empty() {
        let chunk_content = String::from_utf8_lossy(&buffer).to_string();
        chunks.push((chunk_number, chunk_content));
    }

    Ok(chunks)
}

#[pymodule]
fn rust_bindings(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calculate_file_hash, m)?)?;
    m.add_function(wrap_pyfunction!(scan_file_for_patterns, m)?)?;
    m.add_function(wrap_pyfunction!(split_file_into_chunks, m)?)?;
    Ok(())
}
