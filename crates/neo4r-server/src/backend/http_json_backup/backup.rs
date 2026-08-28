use super::*;

pub(crate) fn copy_dir_all(source: &Path, target: &Path) -> io::Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_all(&source_path, &target_path)?;
        } else {
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source_path, &target_path)?;
        }
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct BackupManifestStats {
    pub(crate) file_count: u64,
    pub(crate) total_bytes: u64,
    pub(crate) checksum: u64,
}

pub(crate) fn collect_backup_manifest_stats(path: &Path) -> io::Result<BackupManifestStats> {
    let mut stats = BackupManifestStats::default();
    collect_backup_manifest_stats_inner(path, &mut stats)?;
    Ok(stats)
}

pub(crate) fn collect_backup_manifest_stats_inner(
    path: &Path,
    stats: &mut BackupManifestStats,
) -> io::Result<()> {
    let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_backup_manifest_stats_inner(&entry.path(), stats)?;
        } else if entry.file_name().to_string_lossy() == BACKUP_MANIFEST_FILE {
            continue;
        } else {
            let path = entry.path();
            stats.file_count += 1;
            stats.total_bytes = stats.total_bytes.saturating_add(metadata.len());
            stats.checksum = checksum_file(&path, stats.checksum)?;
        }
    }
    Ok(())
}

pub(crate) fn verify_backup_manifest(path: &Path, stats: &BackupManifestStats) -> io::Result<()> {
    let manifest = fs::read_to_string(path.join(BACKUP_MANIFEST_FILE))?;
    let fields = manifest
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<HashMap<_, _>>();
    if fields.get("neo4r_backup_manifest_version") != Some(&"1") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported backup manifest version",
        ));
    }
    verify_manifest_u64(&fields, "file_count", stats.file_count)?;
    verify_manifest_u64(&fields, "total_bytes", stats.total_bytes)?;
    if fields.contains_key("checksum") {
        verify_manifest_u64(&fields, "checksum", stats.checksum)?;
    }
    Ok(())
}

pub(crate) fn verify_manifest_u64(
    fields: &HashMap<&str, &str>,
    key: &str,
    actual: u64,
) -> io::Result<()> {
    let expected = fields
        .get(key)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {key}")))?;
    let expected = expected.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid backup manifest {key}"),
        )
    })?;
    if expected != actual {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("backup manifest {key} mismatch: expected {expected}, actual {actual}"),
        ));
    }
    Ok(())
}

pub(crate) fn checksum_file(path: &Path, seed: u64) -> io::Result<u64> {
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; 8192];
    let mut hash = seed ^ 0xcbf29ce484222325;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hash);
        }
        for byte in &buffer[..read] {
            hash = hash.wrapping_mul(0x100000001b3).wrapping_add(*byte as u64);
        }
    }
}
