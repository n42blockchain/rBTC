//! Durable bounded names only: byte lengths are always observed from files.
use fs2::FileExt;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};
pub(super) const MAX_FILES: usize = 4_096;
const MAX_BYTES: u64 = 1024 * 1024;
const NAME: &str = ".rbtc-header-files.json";
const PENDING: &str = ".rbtc-header-files.pending";

pub(super) fn lock(root: &Path) -> io::Result<File> {
    let path = root.join(".rbtc-header-budget.lock");
    let _ = file_len(&path)?;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    file.try_lock_exclusive()?;
    Ok(file)
}

fn relative(root: &Path, path: &Path) -> io::Result<PathBuf> {
    let name = path
        .strip_prefix(root)
        .map_err(|_| io::Error::other("header inventory path escapes root"))?;
    if name.as_os_str().is_empty()
        || name
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(io::Error::other("invalid header inventory path"));
    }
    Ok(name.to_path_buf())
}
pub(super) fn file_len(path: &Path) -> io::Result<Option<u64>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(io::Error::other("header inventory requires regular files"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(io::Error::other("header inventory refuses hard links"));
        }
    }
    Ok(Some(metadata.len()))
}
pub(super) fn load(root: &Path) -> io::Result<HashMap<PathBuf, u64>> {
    let path = root.join(NAME);
    let Some(len) = file_len(&path)? else {
        return Ok(HashMap::new());
    };
    if len > MAX_BYTES {
        return Err(io::Error::other("header inventory exceeds byte bound"));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(io::Error::other("header inventory exceeds byte bound"));
    }
    let names: Vec<PathBuf> = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if names.len() > MAX_FILES {
        return Err(io::Error::other("header inventory exceeds file bound"));
    }
    let mut result = HashMap::new();
    for name in names {
        let path = root.join(&name);
        relative(root, &path)?;
        // Refuse linked ancestor directories as well as linked files.
        let mut parent = path.parent();
        while let Some(directory) = parent.filter(|p| *p != root) {
            match fs::symlink_metadata(directory) {
                Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
                    return Err(io::Error::other("linked header inventory directory"));
                }
                Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
                _ => {}
            }
            parent = directory.parent();
        }
        if let Some(len) = file_len(&path)? {
            if result.insert(path, len).is_some() {
                return Err(io::Error::other("duplicate header inventory path"));
            }
        }
    }
    Ok(result)
}
pub(super) fn save<'a>(root: &Path, paths: impl Iterator<Item = &'a PathBuf>) -> io::Result<()> {
    let mut names = paths
        .map(|path| relative(root, path))
        .collect::<io::Result<Vec<_>>>()?;
    names.sort();
    let bytes = serde_json::to_vec(&names).map_err(io::Error::other)?;
    if names.len() > MAX_FILES || bytes.len() as u64 > MAX_BYTES {
        return Err(io::Error::other("header inventory exceeds bounds"));
    }
    let _ = file_len(&root.join(NAME))?;
    // The directory owner lock and usage mutex serialize writers. A fixed
    // staging name bounds crash leftovers to one catalog, not one per restart.
    let pending = root.join(PENDING);
    let _ = file_len(&pending)?;
    let mut temporary = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&pending)?;
    temporary.write_all(&bytes)?;
    temporary.sync_all()?;
    drop(temporary);
    fs::rename(pending, root.join(NAME))?;
    #[cfg(unix)]
    File::open(root)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inventory_refuses_escaping_names_and_oversized_documents() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(NAME), br#"["../canonical.redb"]"#).unwrap();
        assert!(load(root.path()).is_err());
        let file = File::create(root.path().join(NAME)).unwrap();
        file.set_len(MAX_BYTES + 1).unwrap();
        assert!(load(root.path()).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn inventory_refuses_linked_files_directories_and_owner_lock() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let file = outside.path().join("headers.redb");
        fs::write(&file, b"preserve").unwrap();
        symlink(outside.path(), root.path().join("linked")).unwrap();
        fs::write(root.path().join(NAME), br#"["linked/headers.redb"]"#).unwrap();
        assert!(load(root.path()).is_err());
        symlink(&file, root.path().join("headers.redb")).unwrap();
        fs::write(root.path().join(NAME), br#"["headers.redb"]"#).unwrap();
        assert!(load(root.path()).is_err());
        symlink(&file, root.path().join(".rbtc-header-budget.lock")).unwrap();
        assert!(lock(root.path()).is_err());
        assert_eq!(fs::read(file).unwrap(), b"preserve");
    }
}
